import axios from "axios";
import transform from './utils/transform';

/**
 * Core orchestration module for managing data pipelines
 * @module Orchestrator
 */

import {
    Connector,
    AuthConfig,
    Pipeline,
    PipelineEvent,
    Adapter,
    Adapters,
    Vault,
    AdapterInstance,
    AdapterPagination
} from './types';

/**
 * Creates a promise that resolves after the specified delay
 * @param ms - Delay duration in milliseconds
 * @returns Promise that resolves after the delay
 */
async function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Default configuration values
const DEFAULT_CONFIG = {
    TOTAL_ITEMS_LIMIT: 1000000,
    TIMEOUT_MS: 30000,
} as const;

async function fetchData(
    sourceAdapter: ReturnType<Adapter>,
    itemsPerPage: number | undefined,
    pageOffset: number | string | undefined,
    downloadStartTime: number,
    timeoutMs: number,
    errorHandling: {
        max_retries: number,
        retry_interval: number
    },
    log: (event: PipelineEvent) => void,
) {
    let result,
        lastError,
        attempt = 0;

    do {
        if (attempt > 0) {
            await delay(errorHandling.retry_interval);
        }

        // Check for timeout
        if (Date.now() - downloadStartTime >= timeoutMs) {
            log({ type: 'error', message: `Download timeout exceeded (${timeoutMs}ms)` });
            throw new Error('Download timeout exceeded');
        }

        try {
            result = await sourceAdapter.download({
                limit: itemsPerPage,
                offset: pageOffset
            });
        } catch (error) {
            log({
                type: 'error',
                message: `Attempt ${attempt + 1} failed in download: ${(error as Error).message}`
            });

            lastError = error;
        }

        attempt++;
    } while (!result && attempt <= errorHandling.max_retries);

    if (result) {
        return result;
    }

    log({
        type: 'error',
        message: 'max_retries reached',
    });

    throw lastError;
}

interface PipelineWithSource<T = object> extends Pipeline<T> {
    source: Connector;
}

function getEndpoint (
    connector: Connector,
    adapter: AdapterInstance,
) {
    const adapterConfig = adapter.getConfig();

    const { endpoints } = adapterConfig;

    const { endpoint_id: endpointId } = connector;

    const endpoint = endpoints.find(e => e.id === endpointId);

    return endpoint;
}

function getPaginationFromEndpoint (
    connector: Connector,
    adapter: AdapterInstance,
    itemsPerPage: number | undefined,
    log: (event: PipelineEvent) => void,
) {
    const endpoint = getEndpoint(connector, adapter);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in adapter ${connector.adapter_id}`);
    }

    let paginationConfig: AdapterPagination | false | undefined;

    const pagination = endpoint.settings?.pagination;

    if (typeof pagination !== "undefined") {
        paginationConfig = pagination;
    }

    if (typeof paginationConfig === "undefined") {
        const adapterConfig = adapter.getConfig();

        paginationConfig = adapterConfig.pagination || false;
    }

    if (!paginationConfig) {
        if (typeof itemsPerPage !== 'undefined') {
            log({
                type: 'info',
                message: `since the ${connector.endpoint_id} endpoint of the ${connector.adapter_id} adapter does not support pagination, the number of items per page set will be ignored`,
            });

            itemsPerPage = undefined;
        }
    } else if (typeof paginationConfig.maxItemsPerPage !== 'undefined') {
        if (typeof itemsPerPage === 'undefined') {
            log({
                type: 'info',
                message: `Since the number of items per page was not defined, the maximum items per page defined by the adapter (${paginationConfig.maxItemsPerPage}) will be used instead`,
            })

            itemsPerPage = paginationConfig.maxItemsPerPage;
        } else if (itemsPerPage > paginationConfig.maxItemsPerPage) {
            log({
                type: 'info',
                message: `The number of items per page (${itemsPerPage}) is greater than the maximum allowed by the adapter (${paginationConfig.maxItemsPerPage}), so it will be reduced to the maximum allowed by the adapter`,
            })

            itemsPerPage = paginationConfig.maxItemsPerPage;
        }
    }

    const paginationType = (paginationConfig && paginationConfig.type) || undefined;

    return {
        paginationType,
        itemsPerPage,
    };
}

async function getDataSerially<T>(
    pipeline: PipelineWithSource<T>,
    sourceAdapter: AdapterInstance,
    errorHandling: {
        max_retries: number,
        retry_interval: number
    },
    log: (event: PipelineEvent) => void,
) {
    const rl = pipeline.rate_limiting || {
        requests_per_second: Infinity,
        max_retries_on_rate_limit: 0
    };

    const minIntervalMs = rl.requests_per_second === Infinity
        ? 0
        : 1000 / rl.requests_per_second;

    const totalItemsToFetch = pipeline.source.limit ?? DEFAULT_CONFIG.TOTAL_ITEMS_LIMIT;
    const timeoutMs = pipeline.source.timeout ?? DEFAULT_CONFIG.TIMEOUT_MS;
    const downloadStartTime = Date.now();

    const endpoint = getEndpoint(pipeline.source, sourceAdapter);
    if (!endpoint) {
        const { endpoint_id, adapter_id } = pipeline.source;

        throw new Error(`Endpoint ${endpoint_id} not found in adapter ${adapter_id}`);
    }

    if (
        pipeline.source.fields.length === 0 &&
        'defaultFields' in endpoint &&
        endpoint.defaultFields
    ) {
        pipeline.source.fields = [...endpoint.defaultFields];
    }

    let {
        itemsPerPage,
        paginationType,
    } = getPaginationFromEndpoint(
        pipeline.source,
        sourceAdapter,
        pipeline.source.pagination?.itemsPerPage || undefined,
        log,
    );

    let pageResult,
        fetchDataMoment,
        pageOffset = pipeline.source.pagination?.pageOffsetKey || undefined;

    let data: T[] = [];

    do {
        if (pageResult) {
            if (paginationType === 'cursor') {
                pageOffset = pageResult.options!.nextOffset; // Accept string or number
                log({ type: 'info', message: `Next cursor set to ${pageOffset}` });
            } else {
                pageOffset = (typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : (pageOffset || 0)) + (itemsPerPage as number);
                log({ type: 'info', message: `Next offset incremented to ${pageOffset}` });
            }

            // Apply rate limiting if configured
            if (minIntervalMs > 0) {
                const elapsedMs = Date.now() - (fetchDataMoment as number);
                const delayMs = Math.max(0, minIntervalMs - elapsedMs);
                if (delayMs > 0) {
                    log({ type: 'info', message: `Rate limiting: waiting ${delayMs}ms` });
                    await delay(delayMs);
                }
            }
        }

        try {
            fetchDataMoment = Date.now();
            pageResult = await fetchData(
                sourceAdapter,
                itemsPerPage,
                pageOffset,
                downloadStartTime,
                timeoutMs,
                errorHandling,
                log,
            );
        } catch (error) {
            if (error instanceof Error && error.message === 'Download timeout exceeded') {
                break;
            }

            throw error
        }

        // Process page data
        data.push(...pageResult.data);
        if (typeof paginationType !== 'undefined') {
            log({
                type: 'extract',
                message: `Extracted page${paginationType === 'cursor' && pageResult.options?.nextOffset !== undefined ? ` with cursor ${pageResult.options.nextOffset}` : ` at offset ${pageOffset || 0}`}`,
                dataCount: pageResult.data.length
            });
        }
    } while (
        data.length < totalItemsToFetch &&
        typeof paginationType !== 'undefined' &&
        typeof itemsPerPage !== 'undefined' &&
        (
            paginationType === 'cursor'
                ? pageResult.options?.nextOffset !== undefined
                : pageResult.data.length === itemsPerPage
        )
    );

    if (data.length > totalItemsToFetch) {
        data.splice(totalItemsToFetch, data.length - totalItemsToFetch)
    }

    if (typeof paginationType === 'undefined' || typeof itemsPerPage === 'undefined') {
        log({ type: 'info', message: 'Search without pagination finished' });
    } else if (data.length === totalItemsToFetch) {
        log({ type: 'info', message: `Reached total items limit of ${totalItemsToFetch}` });
    } else if (paginationType === 'cursor') {
        if (pageResult!.options?.nextOffset === undefined) {
            log({ type: 'info', message: 'No more data to fetch' });
        }
    } else {
        if (pageResult!.data.length === 0) {
            log({ type: 'info', message: 'No more data to fetch' });
        } else if (pageResult!.data.length < itemsPerPage) {
            log({
                type: 'info',
                message: `Received ${pageResult!.data.length} items, less than ${itemsPerPage}, so it's the last page`
            });
        }
    }

    return data;
}

/**
 * Creates an orchestrator instance for managing data pipelines
 * @param vault - Credential vault containing authentication configurations
 * @param availableAdapters - Map of available adapter implementations
 * @returns Object containing pipeline management methods
 */
function Orchestrator(vault: Vault, availableAdapters: Adapters) {
    const adapters = new Map<string, Adapter>();

    /**
     * Registers a new adapter implementation
     * @param id - Unique identifier for the adapter
     * @param adapter - Adapter implementation
     */
    function registerAdapter(id: string, adapter: Adapter): void {
        adapters.set(id, adapter);
    }

    /**
     * Retrieves an access token for a single connector using its adapter and credentials.
     * @param connector - The connector to retrieve the token for
     * @returns {Promise<AuthConfig>} Updated auth configuration with access token
     * @throws Error if credentials are invalid or token cannot be obtained
     */
    async function getCredentials(connector: Connector): Promise<AuthConfig> {
        try {
            const adapterFactory = availableAdapters[connector.adapter_id];
            if (!adapterFactory) {
                throw new Error(`Adapter ${connector.adapter_id} not found`);
            }

            const auth = vault[connector.credential_id];
            if (!auth) {
                throw new Error(`Credentials not found for id: ${connector.credential_id}`);
            }

            // Return auth
            return auth;
        } catch (error) {
            throw error instanceof Error ? error : new Error(`Unknown error in getCredentials: ${String(error)}`);
        }
    }

    /**
     * Executes a data pipeline
     * @param pipeline - Pipeline configuration and callbacks
     * @throws Error if pipeline execution fails
     */
    async function runPipeline<T>(pipeline: Pipeline<T>): Promise<{ data: T[] }> {
        let sourceAdapter: ReturnType<Adapter> | undefined;
        let targetAdapter: ReturnType<Adapter> | undefined;

        // Create logging wrapper with timestamp
        const log = (event: PipelineEvent): void => {
            pipeline.logging?.({
                ...event,
                timestamp: new Date().toISOString()
            });
        };

        // Validate pipeline configuration
        if (!pipeline.source && !pipeline.data) {
            throw new Error('Pipeline must have either a source or data');
        }

        // Initialize configuration with defaults
        const eh = pipeline.error_handling || {
            max_retries: 0,
            retry_interval: 1000
        };

        let data: T[] = [];
        log({ type: 'start', message: 'Pipeline started' });

        try {
            // Source data extraction
            if (pipeline.source) {
                const Adapter = adapters.get(pipeline.source.adapter_id);
                if (!Adapter) {
                    throw new Error(`Adapter ${pipeline.source.adapter_id} not found`);
                }

                // Get authentication
                const auth = await getCredentials(pipeline.source);
                sourceAdapter = Adapter(pipeline.source, auth);

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

                // Fetch data in pages
                data = await getDataSerially(
                    pipeline as PipelineWithSource<T>,
                    sourceAdapter,
                    eh,
                    log,
                );

                // Transform data if specified
                if (pipeline.source.transform && transform) {
                    data = await transform(pipeline.source, data);
                }

                log({
                    type: 'extract',
                    message: 'Data extraction complete',
                    dataCount: data.length
                });

                // Execute onload callback if specified
                pipeline.onload?.(data);
            } else {
                // Use provided data
                data = pipeline.data!;
                log({
                    type: 'extract',
                    message: 'Using provided data',
                    dataCount: data.length
                });
            }

            // Handle target operations
            if (pipeline.target) {
                // Execute before-send hook if specified
                const beforeSendResult = pipeline.onbeforesend?.(data);
                if (beforeSendResult === false) {
                    log({ type: 'complete', message: 'Pipeline halted by onbeforesend' });
                    return { data };
                }

                const finalData = Array.isArray(beforeSendResult) ? beforeSendResult : data;

                // Initialize target adapter
                const targetAdapterFactory = adapters.get(pipeline.target.adapter_id);
                if (!targetAdapterFactory) {
                    throw new Error(`Target adapter ${pipeline.target.adapter_id} not found`);
                }

                const targetAuth = await getCredentials(pipeline.target);
                targetAdapter = targetAdapterFactory(pipeline.target, targetAuth);

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

                if (!targetAdapter.upload) {
                    throw new Error(`Upload not supported by adapter ${pipeline.target.adapter_id}`);
                }

                // Upload data in batches
                let { paginationType, itemsPerPage: itemsPerBatch } = getPaginationFromEndpoint(
                    pipeline.target,
                    targetAdapter,
                    pipeline.target.pagination?.itemsPerPage || undefined,
                    log,
                );

                if (paginationType !== 'offset' || typeof itemsPerBatch === 'undefined') {
                    itemsPerBatch = finalData.length;
                }

                for (let i = 0; i < finalData.length; i += itemsPerBatch) {
                    const batch = finalData.slice(i, i + itemsPerBatch);

                    // Upload batch with retry logic
                    let attempt, lastError;

                    for (attempt = 0; attempt <= eh.max_retries; attempt++) {
                        if (attempt > 0) {
                            await delay(eh.retry_interval);
                        }

                        try {
                            await targetAdapter.upload(batch);
                            break;
                        } catch (error) {
                            log({
                                type: 'error',
                                message: `Attempt ${attempt + 1} failed in upload: ${(error as Error).message}`
                            });

                            lastError = error;
                        }
                    }

                    if (attempt > eh.max_retries) {
                        throw lastError;
                    }

                    log({
                        type: 'load',
                        message: `Uploaded batch at offset ${i}, count: ${batch.length}`,
                        dataCount: batch.length
                    });
                }

                pipeline.onupload?.();
            }

            log({ type: 'complete', message: 'Pipeline finished' });
        } catch (error) {
            log({
                type: 'error',
                message: `Pipeline failed: ${(error as Error).message}`
            });

            throw error;
        } finally {
            // Cleanup connections
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
                log({ type: 'error', message: `Connection cleanup failed: ${(cleanupError as Error).message}` });
                // Don't throw cleanup errors if the main operation succeeded
            }
        }

        return { data }
    }

    // Register provided adapters
    Object.entries(availableAdapters).forEach(([id, adapter]) => {
        registerAdapter(id, adapter);
    });

    return { registerAdapter, runPipeline };
}

export { Orchestrator };

export { DatabaseAdapter, BasicAuth, HttpAdapter, Connector, AuthConfig, OAuth2Auth, ApiKeyAuth, AdapterInstance, Filter } from './types'