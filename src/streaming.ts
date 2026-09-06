/**
 * Streaming mode pipeline runner.
 *
 * This module provides a streaming counterpart to the batch `runPipeline` found
 * in {@link module:Orchestrator}. Instead of buffering the entire dataset in
 * memory before transforming and loading it, the streaming runner processes the
 * data chunk-by-chunk as it flows from the source adapter, through any number of
 * transformer steps, and into the target adapter.
 *
 * Highlights:
 *  - Reuses the existing {@link Adapter} / {@link AdapterInstance} interface used
 *    by the regular pipeline runner.
 *  - Reuses the existing transformer steps ({@link Transformation}). Multiple
 *    transformers may be chained.
 *  - Supports a `dryRun` mode where no data is uploaded to the target.
 *  - Supports running multiple streaming pipelines in parallel
 *    ({@link StreamingOrchestratorApi.runStreamingPipelines}).
 *  - Supports chaining multiple streaming pipelines sequentially
 *    ({@link StreamingOrchestratorApi.runStreamingPipelineChain}).
 *  - Supports adapters exposing a native streaming API
 *    ({@link StreamingAdapterInstance}) and transparently falls back to the
 *    paginated `download` / batched `upload` API when the native streaming API is
 *    not implemented.
 *
 * @module StreamingOrchestrator
 */

import transform from './utils/transform';

import {
    Connector,
    AuthConfig,
    Pipeline,
    PipelineEvent,
    Adapter,
    Adapters,
    Vault,
    Transformation,
    StreamChunk,
    StreamingAdapterInstance,
} from './types';

/**
 * Default configuration values for the streaming orchestrator.
 */
export const STREAMING_DEFAULT_CONFIG = {
    /** Number of items requested per page when falling back to paginated download */
    ITEMS_PER_PAGE: 100,
    /** Maximum total items to fetch */
    TOTAL_ITEMS_LIMIT: 1000000,
} as const;

/**
 * Configuration extension of the standard {@link Pipeline} that adds
 * streaming-only options.
 */
export interface StreamingPipeline<T = object> extends Pipeline<T> {
    /**
     * When `true`, the pipeline extracts and transforms data but does NOT upload
     * it to the target. Useful for testing / previewing.
     */
    dryRun?: boolean;

    /**
     * Ordered list of transformer steps applied to every chunk. Each entry is an
     * independent transformer (an array of {@link Transformation}s) and they are
     * applied one after the other. This is additive to `pipeline.source.transform`.
     */
    transformers?: Transformation[][];

    /**
     * Size (in records) of the chunks flowing through the pipeline when the
     * source adapter does not provide a native streaming API. Defaults to
     * {@link STREAMING_DEFAULT_CONFIG.ITEMS_PER_PAGE}.
     */
    chunkSize?: number;
}

/**
 * Result returned by a streaming pipeline run.
 */
export interface StreamingResult<T = any> {
    /** All records that flowed through the pipeline (post-transform). */
    data: T[];
    /** Number of records read from the source. */
    read: number;
    /** Number of records written to the target (0 when `dryRun`). */
    written: number;
    /** Whether the run was a dry run. */
    dryRun: boolean;
}

// ---------------------------------------------------------------------------
// Critical, individually unit-testable helper functions
// ---------------------------------------------------------------------------

/**
 * Wraps a plain async chunk producer into an async iterable of {@link StreamChunk}.
 * Exposed for unit-testing of the streaming primitives.
 */
export async function* chunkIterableFromPages(
    getPage: (
        offset: number | string | undefined
    ) => Promise<StreamChunk>,
    options: {
        startOffset?: number | string;
        itemsPerPage?: number;
        totalLimit?: number;
        paginationType?: 'offset' | 'cursor' | undefined;
    } = {}
): AsyncIterable<StreamChunk> {
    const {
        startOffset,
        itemsPerPage,
        totalLimit = STREAMING_DEFAULT_CONFIG.TOTAL_ITEMS_LIMIT,
        paginationType,
    } = options;

    let offset: number | string | undefined = startOffset;
    let fetched = 0;
    let page: StreamChunk | undefined;

    do {
        page = await getPage(offset);
        const rows = page?.data ?? [];

        if (rows.length === 0 && fetched > 0 && !page?.options?.nextOffset) {
            break;
        }

        // Respect the total limit by trimming the final chunk.
        let emit = rows;
        if (fetched + rows.length > totalLimit) {
            emit = rows.slice(0, Math.max(0, totalLimit - fetched));
        }

        fetched += emit.length;

        if (emit.length > 0) {
            yield { data: emit, options: page?.options };
        }

        if (fetched >= totalLimit) {
            break;
        }

        // Determine the next offset.
        if (paginationType === 'cursor') {
            offset = page?.options?.nextOffset;
            if (offset === undefined) break;
        } else if (paginationType === 'offset' && typeof itemsPerPage === 'number') {
            const current = typeof offset === 'string' ? parseInt(offset, 10) || 0 : offset || 0;
            offset = current + itemsPerPage;
            // Stop when a short/empty page indicates the end.
            if (rows.length < itemsPerPage) break;
        } else {
            // No pagination configured: single page.
            break;
        }
    } while (true);
}

/**
 * Applies an ordered list of transformer steps to a single chunk of data.
 * Each transformer step is an array of {@link Transformation}s. This reuses the
 * existing {@link transform} utility so behaviour matches the batch pipeline.
 *
 * Exposed for unit-testing.
 */
export async function applyTransformers(
    data: any[],
    transformerSteps: Array<Transformation[] | undefined>,
    connector?: Connector
): Promise<any[]> {
    let current = data;
    for (const step of transformerSteps) {
        if (!step || step.length === 0) continue;
        // Build a minimal connector carrying just this transform step.
        const stepConnector: Connector = {
            ...(connector ?? {
                id: 'streaming',
                adapter_id: '',
                endpoint_id: '',
                credential_id: '',
                fields: [],
            }),
            transform: step,
        };
        current = await transform(stepConnector, current);
    }
    return current;
}

/**
 * Lazily maps every chunk of a source stream through the given transformer
 * steps. Produces a new async iterable, keeping the pipeline lazy end-to-end.
 *
 * Exposed for unit-testing.
 */
export async function* transformStream(
    source: AsyncIterable<StreamChunk>,
    transformerSteps: Array<Transformation[] | undefined>,
    connector?: Connector,
    onChunk?: (transformed: any[]) => void
): AsyncIterable<StreamChunk> {
    for await (const chunk of source) {
        const transformed = await applyTransformers(chunk.data, transformerSteps, connector);
        onChunk?.(transformed);
        yield { data: transformed, options: chunk.options };
    }
}

/**
 * Collects the transformer steps for a pipeline from both the source connector
 * (`source.transform`) and the streaming-only `transformers` list. Supports
 * multiple transformers.
 *
 * Exposed for unit-testing.
 */
export function collectTransformers<T>(
    pipeline: StreamingPipeline<T>
): Array<Transformation[]> {
    const steps: Array<Transformation[]> = [];
    if (pipeline.source?.transform && pipeline.source.transform.length > 0) {
        steps.push(pipeline.source.transform);
    }
    for (const step of pipeline.transformers ?? []) {
        if (step && step.length > 0) steps.push(step);
    }
    return steps;
}

/**
 * Determines whether the supplied adapter instance exposes a native download
 * streaming API. Exposed for unit-testing.
 */
export function hasDownloadStream(
    adapter: StreamingAdapterInstance | undefined
): adapter is StreamingAdapterInstance & {
    downloadStream: NonNullable<StreamingAdapterInstance['downloadStream']>;
} {
    return typeof adapter?.downloadStream === 'function';
}

/**
 * Determines whether the supplied adapter instance exposes a native upload
 * streaming API. Exposed for unit-testing.
 */
export function hasUploadStream(
    adapter: StreamingAdapterInstance | undefined
): adapter is StreamingAdapterInstance & {
    uploadStream: NonNullable<StreamingAdapterInstance['uploadStream']>;
} {
    return typeof adapter?.uploadStream === 'function';
}

// ---------------------------------------------------------------------------
// Adapter helpers
// ---------------------------------------------------------------------------

/** Locates the endpoint config for a connector on an adapter instance. */
function getEndpoint(connector: Connector, adapter: StreamingAdapterInstance) {
    const { endpoints } = adapter.getConfig();
    return endpoints.find(e => e.id === connector.endpoint_id);
}

/**
 * Resolves the pagination type and items-per-page for a connector, matching the
 * behaviour of the batch runner.
 */
function resolvePagination(
    connector: Connector,
    adapter: StreamingAdapterInstance
): { paginationType: 'offset' | 'cursor' | undefined; itemsPerPage: number | undefined } {
    const endpoint = getEndpoint(connector, adapter);
    if (!endpoint) {
        throw new Error(
            `Endpoint ${connector.endpoint_id} not found in adapter ${connector.adapter_id}`
        );
    }

    let itemsPerPage = connector.pagination?.itemsPerPage;
    const endpointPagination = endpoint.settings?.pagination;
    const adapterPagination =
        endpointPagination === false
            ? undefined
            : endpointPagination ?? adapter.getConfig().pagination;

    if (endpointPagination === false || !adapterPagination) {
        return { paginationType: undefined, itemsPerPage: undefined };
    }

    if (typeof itemsPerPage === 'undefined') {
        itemsPerPage =
            adapterPagination.maxItemsPerPage ?? STREAMING_DEFAULT_CONFIG.ITEMS_PER_PAGE;
    } else if (
        typeof adapterPagination.maxItemsPerPage === 'number' &&
        itemsPerPage > adapterPagination.maxItemsPerPage
    ) {
        itemsPerPage = adapterPagination.maxItemsPerPage;
    }

    const paginationType = adapterPagination.type as 'offset' | 'cursor' | undefined;
    return { paginationType, itemsPerPage };
}

/**
 * Builds a source stream for a connector, using the adapter's native
 * `downloadStream` when available, otherwise falling back to paginated
 * `download`. Exposed for unit-testing.
 */
export function buildSourceStream(
    connector: Connector,
    adapter: StreamingAdapterInstance,
    options: { chunkSize?: number; totalLimit?: number } = {},
    log: (event: PipelineEvent) => void = () => {}
): AsyncIterable<StreamChunk> {
    const totalLimit = options.totalLimit ?? connector.limit ?? STREAMING_DEFAULT_CONFIG.TOTAL_ITEMS_LIMIT;

    if (hasDownloadStream(adapter)) {
        log({ type: 'info', message: 'Using native downloadStream' });
        const downloadStream = adapter.downloadStream.bind(adapter);
        // Enforce the total limit on top of a native stream.
        return (async function* limited() {
            let fetched = 0;
            for await (const chunk of downloadStream({ limit: options.chunkSize })) {
                const rows = chunk.data ?? [];
                let emit = rows;
                if (fetched + rows.length > totalLimit) {
                    emit = rows.slice(0, Math.max(0, totalLimit - fetched));
                }
                fetched += emit.length;
                if (emit.length > 0) {
                    log({ type: 'extract', message: 'Streamed chunk', dataCount: emit.length });
                    yield { data: emit, options: chunk.options };
                }
                if (fetched >= totalLimit) break;
            }
        })();
    }

    log({ type: 'info', message: 'Falling back to paginated download' });
    const { paginationType, itemsPerPage: resolvedItems } = resolvePagination(connector, adapter);
    const itemsPerPage = options.chunkSize ?? resolvedItems;

    return chunkIterableFromPages(
        async (offset) => {
            const result = await adapter.download({ limit: itemsPerPage, offset });
            log({ type: 'extract', message: 'Downloaded page', dataCount: result.data.length });
            return result;
        },
        {
            startOffset: connector.pagination?.pageOffsetKey,
            itemsPerPage,
            totalLimit,
            paginationType,
        }
    );
}

/**
 * Consumes a stream and uploads each chunk to the target adapter, using the
 * adapter's native `uploadStream` when available, otherwise batched `upload`.
 * Returns the total number of records written. Exposed for unit-testing.
 */
export async function consumeToTarget(
    stream: AsyncIterable<StreamChunk>,
    adapter: StreamingAdapterInstance,
    log: (event: PipelineEvent) => void = () => {}
): Promise<number> {
    let written = 0;

    if (hasUploadStream(adapter)) {
        log({ type: 'info', message: 'Using native uploadStream' });
        const uploadStream = adapter.uploadStream.bind(adapter);
        // Wrap the stream to count records while streaming.
        const counting = (async function* () {
            for await (const chunk of stream) {
                written += chunk.data.length;
                yield chunk;
            }
        })();
        await uploadStream(counting);
        log({ type: 'load', message: 'Streamed upload complete', dataCount: written });
        return written;
    }

    if (!adapter.upload) {
        throw new Error('Upload not supported by target adapter');
    }

    for await (const chunk of stream) {
        if (chunk.data.length === 0) continue;
        await adapter.upload(chunk.data);
        written += chunk.data.length;
        log({ type: 'load', message: `Uploaded chunk`, dataCount: chunk.data.length });
    }
    return written;
}

// ---------------------------------------------------------------------------
// Orchestrator API
// ---------------------------------------------------------------------------

export interface StreamingOrchestratorApi {
    registerAdapter(id: string, adapter: Adapter): void;
    runStreamingPipeline<T>(pipeline: StreamingPipeline<T>): Promise<StreamingResult<T>>;
    runStreamingPipelines<T>(pipelines: StreamingPipeline<T>[]): Promise<StreamingResult<T>[]>;
    runStreamingPipelineChain<T>(pipelines: StreamingPipeline<T>[]): Promise<StreamingResult<T>[]>;
}

/**
 * Creates a streaming orchestrator instance.
 *
 * @param vault - Credential vault containing authentication configurations.
 * @param availableAdapters - Map of available adapter implementations.
 */
function StreamingOrchestrator(vault: Vault, availableAdapters: Adapters): StreamingOrchestratorApi {
    const adapters = new Map<string, Adapter>();

    function registerAdapter(id: string, adapter: Adapter): void {
        adapters.set(id, adapter);
    }

    function getCredentials(connector: Connector): AuthConfig {
        const auth = vault[connector.credential_id];
        if (!auth) {
            throw new Error(`Credentials not found for id: ${connector.credential_id}`);
        }
        return auth;
    }

    function instantiate(connector: Connector): StreamingAdapterInstance {
        const factory = adapters.get(connector.adapter_id);
        if (!factory) {
            throw new Error(`Adapter ${connector.adapter_id} not found`);
        }
        const auth = getCredentials(connector);
        return factory(connector, auth) as StreamingAdapterInstance;
    }

    async function runStreamingPipeline<T>(
        pipeline: StreamingPipeline<T>
    ): Promise<StreamingResult<T>> {
        let sourceAdapter: StreamingAdapterInstance | undefined;
        let targetAdapter: StreamingAdapterInstance | undefined;

        const log = (event: PipelineEvent): void => {
            pipeline.logging?.({ ...event, timestamp: new Date().toISOString() });
        };

        if (!pipeline.source && !pipeline.data) {
            throw new Error('Pipeline must have either a source or data');
        }

        const dryRun = !!pipeline.dryRun;
        const collected: T[] = [];
        let read = 0;
        let written = 0;

        log({ type: 'start', message: `Streaming pipeline started${dryRun ? ' (dry run)' : ''}` });

        try {
            // 1. Build the source stream.
            let sourceStream: AsyncIterable<StreamChunk>;

            if (pipeline.source) {
                sourceAdapter = instantiate(pipeline.source);
                if (typeof sourceAdapter.connect === 'function') {
                    await sourceAdapter.connect();
                    log({ type: 'info', message: 'Connected to source adapter' });
                }
                sourceStream = buildSourceStream(
                    pipeline.source,
                    sourceAdapter,
                    { chunkSize: pipeline.chunkSize },
                    log
                );
            } else {
                // Data-only pipeline: emit provided data as one chunk.
                const chunkSize = pipeline.chunkSize ?? STREAMING_DEFAULT_CONFIG.ITEMS_PER_PAGE;
                const provided = pipeline.data!;
                sourceStream = (async function* () {
                    for (let i = 0; i < provided.length; i += chunkSize) {
                        yield { data: provided.slice(i, i + chunkSize) };
                    }
                })();
            }

            // Count reads by wrapping the source stream.
            const countedSource = (async function* () {
                for await (const chunk of sourceStream) {
                    read += chunk.data.length;
                    yield chunk;
                }
            })();

            // 2. Apply transformer steps lazily.
            const transformerSteps = collectTransformers(pipeline);
            const transformed = transformStream(
                countedSource,
                transformerSteps,
                pipeline.source,
                (rows) => {
                    if (transformerSteps.length > 0) {
                        log({ type: 'transform', message: 'Transformed chunk', dataCount: rows.length });
                    }
                }
            );

            // 3. Tap the stream to collect results and support onload.
            const tapped = (async function* () {
                for await (const chunk of transformed) {
                    collected.push(...(chunk.data as T[]));
                    yield chunk;
                }
            })();

            // 4. Load into target (unless dry run or no target).
            if (pipeline.target && !dryRun) {
                targetAdapter = instantiate(pipeline.target);
                if (typeof targetAdapter.connect === 'function') {
                    await targetAdapter.connect();
                    log({ type: 'info', message: 'Connected to target adapter' });
                }

                // Respect onbeforesend on a per-chunk basis when provided.
                const gated = (async function* () {
                    for await (const chunk of tapped) {
                        const beforeSend = pipeline.onbeforesend?.(chunk.data as T[]);
                        if (beforeSend === false) {
                            log({ type: 'info', message: 'Chunk skipped by onbeforesend' });
                            continue;
                        }
                        const finalData = Array.isArray(beforeSend) ? beforeSend : chunk.data;
                        yield { data: finalData, options: chunk.options };
                    }
                })();

                written = await consumeToTarget(gated, targetAdapter, log);
                pipeline.onupload?.();
            } else {
                // Drain the stream so transforms/collection happen even without a target.
                // eslint-disable-next-line @typescript-eslint/no-unused-vars
                for await (const _chunk of tapped) {
                    /* consumed for side effects (collect / read counting) */
                }
                if (dryRun && pipeline.target) {
                    log({ type: 'info', message: 'Dry run: skipping upload to target' });
                }
            }

            pipeline.onload?.(collected);

            log({
                type: 'complete',
                message: `Streaming pipeline finished (read ${read}, written ${written})`,
                dataCount: written,
            });
        } catch (error) {
            log({ type: 'error', message: `Streaming pipeline failed: ${(error as Error).message}` });
            throw error;
        } finally {
            try {
                if (sourceAdapter?.disconnect) {
                    await sourceAdapter.disconnect();
                    log({ type: 'info', message: 'Source adapter disconnected' });
                }
                if (targetAdapter?.disconnect) {
                    await targetAdapter.disconnect();
                    log({ type: 'info', message: 'Target adapter disconnected' });
                }
            } catch (cleanupError) {
                log({
                    type: 'error',
                    message: `Connection cleanup failed: ${(cleanupError as Error).message}`,
                });
            }
        }

        return { data: collected, read, written, dryRun };
    }

    /**
     * Runs multiple streaming pipelines concurrently (in parallel).
     */
    async function runStreamingPipelines<T>(
        pipelines: StreamingPipeline<T>[]
    ): Promise<StreamingResult<T>[]> {
        return Promise.all(pipelines.map(p => runStreamingPipeline(p)));
    }

    /**
     * Runs multiple streaming pipelines sequentially, one after the other.
     * The output data of each pipeline is passed as the `data` input of the
     * next pipeline when that next pipeline has no `source` of its own, enabling
     * true pipeline chaining.
     */
    async function runStreamingPipelineChain<T>(
        pipelines: StreamingPipeline<T>[]
    ): Promise<StreamingResult<T>[]> {
        const results: StreamingResult<T>[] = [];
        let previous: StreamingResult<T> | undefined;

        for (const pipeline of pipelines) {
            const toRun: StreamingPipeline<T> =
                previous && !pipeline.source && !pipeline.data
                    ? { ...pipeline, data: previous.data }
                    : pipeline;

            const result = await runStreamingPipeline(toRun);
            results.push(result);
            previous = result;
        }

        return results;
    }

    Object.entries(availableAdapters).forEach(([id, adapter]) => {
        registerAdapter(id, adapter);
    });

    return {
        registerAdapter,
        runStreamingPipeline,
        runStreamingPipelines,
        runStreamingPipelineChain,
    };
}

export { StreamingOrchestrator };
