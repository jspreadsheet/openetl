import {
    Connector,
    AuthConfig,
    PipelineEvent,
    AdapterInstance,
    Adapters,
    Vault,
    Transformation,
    AdapterPagination,
    StreamingAdapterInstance,
    StreamingAdapterOptions,
    StreamingPipeline,
    StreamingPipelineResult,
    ParallelStreamingPipelineResult,
    ChainedStreamingPipelineResult,
    TransformerStep
} from './types';
import transform from './utils/transform';

/**
 * Retrieves credentials for a connector from the vault
 */
async function getCredentials(connector: Connector, vault: Vault, availableAdapters: Adapters): Promise<AuthConfig> {
    const adapterFactory = availableAdapters[connector.adapter_id];
    if (!adapterFactory) {
        throw new Error(`Adapter ${connector.adapter_id} not found`);
    }
    const auth = vault[connector.credential_id];
    if (!auth) {
        throw new Error(`Credentials not found for id: ${connector.credential_id}`);
    }
    return auth;
}

/**
 * Finds the endpoint configuration for a connector
 */
function getEndpoint(connector: Connector, adapter: AdapterInstance) {
    const adapterConfig = adapter.getConfig();
    const { endpoints } = adapterConfig;
    const { endpoint_id: endpointId } = connector;
    return endpoints.find(e => e.id === endpointId);
}

/**
 * Creates an AsyncIterable stream of data chunks from a source connector and adapter.
 * Supports both streaming APIs (downloadStream) and paginated/batch APIs (download).
 */
export async function* createSourceStream(
    connector: Connector,
    adapter: AdapterInstance | StreamingAdapterInstance,
    options?: { chunkSize?: number; limit?: number; pageOffsetKey?: number | string },
    errorHandling?: { max_retries: number; retry_interval: number; use_exponential_backoff?: boolean },
    log?: (event: PipelineEvent) => void
): AsyncGenerator<any[], void, unknown> {
    const limit = options?.limit ?? connector.limit ?? 1000000;
    const chunkSize = options?.chunkSize ?? connector.pagination?.itemsPerPage ?? 1000;
    const initialOffset = options?.pageOffsetKey ?? connector.pagination?.pageOffsetKey;

    const endpoint = getEndpoint(connector, adapter);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in adapter ${connector.adapter_id}`);
    }

    if (
        connector.fields &&
        connector.fields.length === 0 &&
        'defaultFields' in endpoint &&
        endpoint.defaultFields
    ) {
        connector.fields = [...endpoint.defaultFields];
    }

    // Check if adapter supports streaming natively
    if (typeof (adapter as StreamingAdapterInstance).downloadStream === 'function') {
        const streamOptions: StreamingAdapterOptions = {
            limit,
            chunkSize,
            offset: initialOffset
        };
        const stream = (adapter as StreamingAdapterInstance).downloadStream!(streamOptions);
        let totalFetched = 0;
        for await (let chunk of stream) {
            if (!Array.isArray(chunk) || chunk.length === 0) continue;
            if (totalFetched + chunk.length > limit) {
                chunk = chunk.slice(0, limit - totalFetched);
            }
            if (chunk.length > 0) {
                totalFetched += chunk.length;
                log?.({
                    type: 'extract',
                    message: `Extracted stream chunk`,
                    dataCount: chunk.length
                });
                yield chunk;
            }
            if (totalFetched >= limit) break;
        }
        return;
    }

    // Fallback to paginated/batch download
    let paginationConfig: AdapterPagination | false | undefined;
    const endpointPagination = endpoint.settings?.pagination;
    if (typeof endpointPagination !== "undefined") {
        paginationConfig = endpointPagination;
    }
    if (typeof paginationConfig === "undefined") {
        const adapterConfig = adapter.getConfig();
        paginationConfig = adapterConfig.pagination || false;
    }

    let itemsPerPage: number | undefined = chunkSize;
    if (!paginationConfig) {
        if (typeof itemsPerPage !== 'undefined') {
            log?.({
                type: 'info',
                message: `since the ${connector.endpoint_id} endpoint of the ${connector.adapter_id} adapter does not support pagination, the number of items per page set will be ignored`,
            });
            itemsPerPage = undefined;
        }
    } else if (typeof paginationConfig.maxItemsPerPage !== 'undefined') {
        if (typeof itemsPerPage === 'undefined' || itemsPerPage > paginationConfig.maxItemsPerPage) {
            itemsPerPage = paginationConfig.maxItemsPerPage;
        }
    }

    const paginationType = (paginationConfig && paginationConfig.type) || undefined;
    let pageOffset = initialOffset;
    let totalFetched = 0;
    let pageResult: any;

    const maxRetries = errorHandling?.max_retries ?? 0;
    const retryInterval = errorHandling?.retry_interval ?? 1000;
    const useExponentialBackoff = errorHandling?.use_exponential_backoff ?? false;

    do {
        if (pageResult) {
            if (paginationType === 'cursor') {
                pageOffset = pageResult.options?.nextOffset;
            } else {
                pageOffset = (typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : (pageOffset || 0)) + (itemsPerPage || 100);
            }
        }

        let attempt = 0;
        let lastError: unknown;
        pageResult = undefined;

        while (attempt <= maxRetries) {
            if (attempt > 0) {
                const delayMs = useExponentialBackoff
                    ? Math.min(retryInterval * Math.pow(2, attempt - 1), 30000)
                    : retryInterval;
                log?.({
                    type: 'info',
                    message: `Retrying download in ${delayMs}ms (attempt ${attempt + 1}/${maxRetries + 1})`
                });
                await new Promise(res => setTimeout(res, delayMs));
            }
            try {
                pageResult = await adapter.download({
                    limit: itemsPerPage,
                    offset: pageOffset
                });
                break;
            } catch (error) {
                lastError = error;
                log?.({
                    type: 'error',
                    message: `Attempt ${attempt + 1} failed in download: ${error instanceof Error ? error.message : String(error)}`
                });
                attempt++;
            }
        }

        if (!pageResult) {
            throw lastError || new Error('Download failed');
        }

        if (!Array.isArray(pageResult.data)) break;

        let pageData = pageResult.data;
        if (totalFetched + pageData.length > limit) {
            pageData = pageData.slice(0, limit - totalFetched);
        }

        if (pageData.length > 0) {
            totalFetched += pageData.length;
            if (typeof paginationType !== 'undefined') {
                log?.({
                    type: 'extract',
                    message: `Extracted page${paginationType === 'cursor' && pageResult.options?.nextOffset !== undefined ? ` with cursor ${pageResult.options.nextOffset}` : ` at offset ${pageOffset || 0}`}`,
                    dataCount: pageData.length
                });
            }
            yield pageData;
        }

    } while (
        totalFetched < limit &&
        typeof paginationType !== 'undefined' &&
        typeof itemsPerPage !== 'undefined' &&
        (
            paginationType === 'cursor'
                ? pageResult.options?.nextOffset !== undefined
                : pageResult.data.length === itemsPerPage
        )
    );
}

/**
 * Applies transformations to a chunk of data.
 * Executes both existing adapter transformations (connector.transform) and pipeline-level transformer steps.
 */
export async function applyTransformers(
    chunk: any[],
    transformers?: TransformerStep[],
    connector?: Connector
): Promise<any[]> {
    if (!Array.isArray(chunk) || chunk.length === 0) {
        return [];
    }

    let currentData = [...chunk];

    // Apply connector transformations first
    if (connector?.transform && connector.transform.length > 0) {
        currentData = await transform(connector, currentData);
    }

    // Apply additional pipeline transformers
    if (transformers && transformers.length > 0) {
        for (const step of transformers) {
            if (typeof step === 'function') {
                const res = await step(currentData, connector);
                currentData = Array.isArray(res) ? res : currentData;
            } else if (step && typeof step === 'object' && 'type' in step) {
                if (typeof step.type === 'function') {
                    const res = await step.type(currentData, connector);
                    currentData = Array.isArray(res) ? res : currentData;
                } else {
                    const tempConnector: Connector = {
                        ...(connector || {
                            id: 'temp',
                            adapter_id: 'temp',
                            endpoint_id: 'temp',
                            credential_id: 'temp',
                            fields: []
                        }),
                        transform: [step as Transformation]
                    };
                    currentData = await transform(tempConnector, currentData);
                }
            }
        }
    }

    return currentData;
}

/**
 * Uploads a batch of data with retry support
 */
export async function uploadBatchWithRetry(
    adapter: StreamingAdapterInstance | AdapterInstance,
    batch: any[],
    errorHandling?: { max_retries: number; retry_interval: number; use_exponential_backoff?: boolean },
    log?: (event: PipelineEvent) => void
): Promise<void> {
    if (!Array.isArray(batch) || batch.length === 0) {
        return;
    }

    // If adapter supports uploadStream and not upload, wrap batch
    if (typeof (adapter as StreamingAdapterInstance).uploadStream === 'function' && typeof adapter.upload !== 'function') {
        async function* stream() {
            yield batch;
        }
        await (adapter as StreamingAdapterInstance).uploadStream!(stream());
        return;
    }

    if (typeof adapter.upload !== 'function') {
        throw new Error(`Upload not supported by target adapter`);
    }

    const maxRetries = errorHandling?.max_retries ?? 0;
    const retryInterval = errorHandling?.retry_interval ?? 1000;
    const useExponentialBackoff = errorHandling?.use_exponential_backoff ?? false;

    let attempt = 0;
    let lastError: unknown;

    while (attempt <= maxRetries) {
        if (attempt > 0) {
            const delayMs = useExponentialBackoff
                ? Math.min(retryInterval * Math.pow(2, attempt - 1), 30000)
                : retryInterval;
            log?.({
                type: 'info',
                message: `Retrying upload in ${delayMs}ms (attempt ${attempt + 1}/${maxRetries + 1})`
            });
            await new Promise(res => setTimeout(res, delayMs));
        }

        try {
            await adapter.upload(batch);
            return;
        } catch (error) {
            lastError = error;
            log?.({
                type: 'error',
                message: `Attempt ${attempt + 1} failed in upload: ${error instanceof Error ? error.message : String(error)}`
            });
            attempt++;
        }
    }

    throw lastError;
}

/**
 * Executes a single streaming mode pipeline
 */
export async function runStreamingPipeline<T = any>(
    pipeline: StreamingPipeline<T>,
    vault: Vault = {},
    adapters: Adapters = {},
    options?: { dryRun?: boolean }
): Promise<StreamingPipelineResult<T>> {
    const isDryRun = options?.dryRun ?? pipeline.dryRun ?? false;
    const chunkSize = pipeline.chunkSize ?? 1000;
    const eh = pipeline.error_handling || { max_retries: 0, retry_interval: 1000 };

    const log = (event: PipelineEvent): void => {
        pipeline.logging?.({
            ...event,
            timestamp: new Date().toISOString()
        });
    };

    if (!pipeline.source && !pipeline.data) {
        throw new Error('Pipeline must have either a source or data');
    }

    log({ type: 'start', message: `Streaming pipeline started${isDryRun ? ' (DRY RUN)' : ''}` });

    let sourceAdapter: AdapterInstance | StreamingAdapterInstance | undefined;
    let targetAdapter: AdapterInstance | StreamingAdapterInstance | undefined;

    try {
        if (pipeline.source) {
            const adapterFactory = adapters[pipeline.source.adapter_id];
            if (!adapterFactory) {
                throw new Error(`Adapter ${pipeline.source.adapter_id} not found`);
            }
            const auth = await getCredentials(pipeline.source, vault, adapters);
            sourceAdapter = adapterFactory(pipeline.source, auth);

            if (typeof sourceAdapter.connect === 'function') {
                try {
                    await sourceAdapter.connect();
                } catch (error) {
                    throw new Error(
                        error instanceof Error
                            ? error.message
                            : 'Something went wrong while establishing a connection to the source connector'
                    );
                }
            }
            log({ type: 'info', message: 'Connected to source adapter' });
        }

        if (pipeline.target && !isDryRun) {
            const adapterFactory = adapters[pipeline.target.adapter_id];
            if (!adapterFactory) {
                throw new Error(`Target adapter ${pipeline.target.adapter_id} not found`);
            }
            const auth = await getCredentials(pipeline.target, vault, adapters);
            targetAdapter = adapterFactory(pipeline.target, auth);

            if (typeof targetAdapter.connect === 'function') {
                try {
                    await targetAdapter.connect();
                } catch (error) {
                    throw new Error(
                        error instanceof Error
                            ? error.message
                            : 'Something went wrong while establishing a connection to the target connector'
                    );
                }
            }
            log({ type: 'info', message: 'Connected to target adapter' });
        }

        const allData: T[] = [];
        let chunksCount = 0;
        let totalProcessed = 0;

        async function* transformedStreamGenerator(): AsyncGenerator<T[], void, unknown> {
            let sourceStream: AsyncIterable<any[]>;

            if (pipeline.source && sourceAdapter) {
                sourceStream = createSourceStream(
                    pipeline.source,
                    sourceAdapter,
                    { chunkSize, limit: pipeline.source.limit },
                    eh,
                    log
                );
            } else if (pipeline.data) {
                const staticData = [...pipeline.data];
                async function* staticStream() {
                    for (let i = 0; i < staticData.length; i += chunkSize) {
                        yield staticData.slice(i, i + chunkSize);
                    }
                }
                sourceStream = staticStream();
            } else {
                return;
            }

            for await (let chunk of sourceStream) {
                if (!Array.isArray(chunk) || chunk.length === 0) {
                    continue;
                }

                if (pipeline.onbeforesend) {
                    const beforeSendResult = pipeline.onbeforesend(chunk);
                    if (beforeSendResult === false) {
                        log({ type: 'complete', message: 'Pipeline halted by onbeforesend' });
                        break;
                    }
                    if (Array.isArray(beforeSendResult)) {
                        chunk = beforeSendResult;
                    }
                }

                const steps: TransformerStep[] = [
                    ...(pipeline.transform || []),
                    ...(pipeline.transformers || [])
                ];
                const transformedChunk = await applyTransformers(chunk, steps, pipeline.source);

                chunksCount++;
                totalProcessed += transformedChunk.length;
                allData.push(...transformedChunk);

                log({
                    type: 'transform',
                    message: `Transformed chunk ${chunksCount}, items: ${transformedChunk.length}`,
                    dataCount: transformedChunk.length
                });

                if (pipeline.onChunk) {
                    await pipeline.onChunk(transformedChunk, chunksCount - 1);
                }

                yield transformedChunk;
            }
        }

        const generator = transformedStreamGenerator();

        if (isDryRun) {
            for await (const chunk of generator) {
                log({
                    type: 'info',
                    message: `Dry run: processed chunk of ${chunk.length} items without uploading`,
                    dataCount: chunk.length
                });
            }
        } else if (pipeline.target && targetAdapter) {
            if (typeof (targetAdapter as StreamingAdapterInstance).uploadStream === 'function') {
                await (targetAdapter as StreamingAdapterInstance).uploadStream!(generator);
                log({ type: 'load', message: 'Uploaded data using uploadStream' });
            } else if (typeof targetAdapter.upload === 'function') {
                for await (const chunk of generator) {
                    await uploadBatchWithRetry(targetAdapter, chunk, eh, log);
                    log({
                        type: 'load',
                        message: `Uploaded batch of ${chunk.length} items`,
                        dataCount: chunk.length
                    });
                }
            } else {
                throw new Error(`Upload not supported by target adapter ${pipeline.target.adapter_id}`);
            }
            pipeline.onupload?.();
        } else {
            for await (const _ of generator) {}
        }

        pipeline.onload?.(allData);
        log({ type: 'complete', message: 'Streaming pipeline finished' });

        return {
            data: allData,
            chunksCount,
            totalProcessed,
            dryRun: isDryRun
        };

    } catch (error) {
        log({
            type: 'error',
            message: `Streaming pipeline failed: ${error instanceof Error ? error.message : String(error)}`
        });
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
                message: `Connection cleanup failed: ${cleanupError instanceof Error ? cleanupError.message : String(cleanupError)}`
            });
        }
    }
}

/**
 * Executes multiple streaming pipelines in parallel
 */
export async function runStreamingPipelinesInParallel<T = any>(
    pipelines: Array<StreamingPipeline<T>>,
    vault: Vault = {},
    adapters: Adapters = {},
    options?: { dryRun?: boolean; stopOnError?: boolean }
): Promise<Array<ParallelStreamingPipelineResult<T>>> {
    const stopOnError = options?.stopOnError ?? true;

    const promises = pipelines.map(async (pipeline) => {
        try {
            const res = await runStreamingPipeline<T>(pipeline, vault, adapters, options);
            return {
                pipelineId: pipeline.id,
                data: res.data,
                chunksCount: res.chunksCount,
                totalProcessed: res.totalProcessed,
                success: true
            };
        } catch (error) {
            if (stopOnError) {
                throw error;
            }
            return {
                pipelineId: pipeline.id,
                data: [],
                chunksCount: 0,
                totalProcessed: 0,
                success: false,
                error: error instanceof Error ? error : new Error(String(error))
            };
        }
    });

    return Promise.all(promises);
}

/**
 * Chains multiple streaming pipelines to run sequentially.
 * If passData is true or a subsequent pipeline has no source, the output data of the previous pipeline is passed as input data.
 */
export async function chainStreamingPipelines<T = any>(
    pipelines: Array<StreamingPipeline<T>>,
    vault: Vault = {},
    adapters: Adapters = {},
    options?: { dryRun?: boolean; passData?: boolean }
): Promise<ChainedStreamingPipelineResult<T>> {
    const results: Array<{
        pipelineId: string;
        data: T[];
        chunksCount: number;
        totalProcessed: number;
    }> = [];

    let currentData: T[] | undefined = undefined;

    for (let i = 0; i < pipelines.length; i++) {
        const pipeline: StreamingPipeline<T> = { ...pipelines[i] };

        if (i > 0 && currentData !== undefined) {
            if (options?.passData || !pipeline.source) {
                pipeline.data = currentData;
            }
        }

        const res = await runStreamingPipeline<T>(pipeline, vault, adapters, options);
        currentData = res.data;
        results.push({
            pipelineId: pipeline.id,
            data: res.data,
            chunksCount: res.chunksCount,
            totalProcessed: res.totalProcessed
        });
    }

    return {
        results,
        finalData: currentData || []
    };
}

/**
 * Builder/Runner class for managing and executing a streaming pipeline
 */
export class StreamingPipelineRunner<T = any> {
    private pipeline: StreamingPipeline<T>;
    private vault: Vault;
    private adapters: Adapters;

    constructor(pipeline: StreamingPipeline<T>, vault: Vault = {}, adapters: Adapters = {}) {
        this.pipeline = {
            ...pipeline,
            transform: [...(pipeline.transform || [])],
            transformers: [...(pipeline.transformers || [])]
        };
        this.vault = vault;
        this.adapters = adapters;
    }

    addTransformer(step: TransformerStep): this {
        if (!this.pipeline.transformers) {
            this.pipeline.transformers = [];
        }
        this.pipeline.transformers.push(step);
        return this;
    }

    setChunkSize(size: number): this {
        this.pipeline.chunkSize = size;
        return this;
    }

    async run(options?: { dryRun?: boolean }): Promise<StreamingPipelineResult<T>> {
        return runStreamingPipeline(this.pipeline, this.vault, this.adapters, options);
    }

    async runDry(): Promise<StreamingPipelineResult<T>> {
        return this.run({ dryRun: true });
    }
}
