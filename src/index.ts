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
    AdapterInstance
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
    ITEMS_PER_PAGE: 100,
    TOTAL_ITEMS_LIMIT: 1000000,
    TIMEOUT_MS: 30000,
} as const;

async function fetchData(
    sourceAdapter: ReturnType<Adapter>,
    itemsPerPage: number | undefined,
    pageOffset: number | string,
    downloadStartTime: number,
    timeoutMs: number,
    errorHandling: {
        max_retries: number,
        retry_interval: number,
        fail_on_error: boolean
    },
    log: (event: PipelineEvent) => void,
) {
    let result,
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
                offset: typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : pageOffset
            });
        } catch (error) {
            log({
                type: 'error',
                message: `Attempt ${attempt + 1} failed in download: ${(error as Error).message}`
            });

            if (errorHandling.fail_on_error) {
                throw error;
            }
        }

        attempt++;
    } while (!result && attempt <= errorHandling.max_retries);

    if (result) {
        return result;
    }

    throw new Error('max_retries reached');
}

interface PipelineWithSource<T = object> extends Pipeline<T> {
    source: Connector;
}

async function getDataSerially<T>(
    pipeline: PipelineWithSource<T>,
    sourceAdapter: AdapterInstance,
    errorHandling: {
        max_retries: number,
        retry_interval: number,
        fail_on_error: boolean
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

    // Initialize pagination parameters
    let itemsPerPage = pipeline.source.pagination?.itemsPerPage || undefined;

    if (typeof sourceAdapter.maxItemsPerPage !== 'undefined') {
        if (typeof itemsPerPage === 'undefined') {
            log({
                type: 'info',
                message: `Since the number of items per page was not defined, the maximum items per page defined by the adapter (${sourceAdapter.maxItemsPerPage}) will be used instead.`,
            })

            itemsPerPage = sourceAdapter.maxItemsPerPage;
        } else if (itemsPerPage < sourceAdapter.maxItemsPerPage) {
            log({
                type: 'info',
                message: `The number of items per page (${itemsPerPage}) is greater than the maximum allowed by the adapter (${sourceAdapter.maxItemsPerPage}), so it will be reduced to the maximum allowed by the adapter`,
            })

            itemsPerPage = sourceAdapter.maxItemsPerPage;
        }
    }

    const totalItemsToFetch = pipeline.source.limit ?? DEFAULT_CONFIG.TOTAL_ITEMS_LIMIT;
    const timeoutMs = pipeline.source.timeout ?? DEFAULT_CONFIG.TIMEOUT_MS;
    const downloadStartTime = Date.now();

    const cursorType = sourceAdapter.paginationType;

    let pageResult,
        fetchDataMoment,
        pageOffset: string | number = pipeline.source.pagination?.pageOffsetKey || '0'; // Start as string

    let data: T[] = [];

    do {
        if (pageResult) {
            if (cursorType === 'cursor') {
                pageOffset = pageResult.options!.nextOffset; // Accept string or number
                log({ type: 'info', message: `Next cursor set to ${pageOffset}` });
            } else {
                pageOffset = (typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : pageOffset) + (itemsPerPage as number);
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
        if (typeof cursorType !== 'undefined') {
            log({
                type: 'extract',
                message: `Extracted page${cursorType === 'cursor' && pageResult.options?.nextOffset !== undefined ? ` with cursor ${pageResult.options.nextOffset}` : ` at offset ${pageOffset}`}`,
                dataCount: pageResult.data.length
            });
        }
    } while (
        data.length < totalItemsToFetch &&
        typeof cursorType !== 'undefined' &&
        typeof itemsPerPage !== 'undefined' &&
        (
            cursorType === 'cursor'
                ? pageResult.options?.nextOffset !== undefined
                : pageResult.data.length === itemsPerPage
        )
    );

    if (data.length > totalItemsToFetch) {
        data.splice(totalItemsToFetch, data.length - totalItemsToFetch)
    }

    if (typeof cursorType === 'undefined' || typeof itemsPerPage === 'undefined') {
        log({ type: 'info', message: 'Search without pagination finished' });
    } else if (data.length === totalItemsToFetch) {
        log({ type: 'info', message: `Reached total items limit of ${totalItemsToFetch}` });
    } else if (cursorType === 'cursor') {
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

            if (auth.type === 'oauth2' && auth.credentials.token_url) {
                const { client_id, client_secret, refresh_token, access_token, token_url } = auth.credentials;

                // Check if access_token exists and is still valid
                if (access_token && auth.expires_at) {
                    const expiresAt = typeof auth.expires_at === 'string' ? new Date(auth.expires_at).getTime() : auth.expires_at;
                    if (expiresAt > Date.now()) {
                        return auth; // Return early if token is still valid
                    }
                }

                // If refresh_token is available, use it to refresh the access token
                if (refresh_token) {
                    const baseData = {
                        grant_type: 'refresh_token',
                        client_id,
                        client_secret,
                        refresh_token,
                    } as Record<string, string>;

                    const response = await axios.post(token_url, new URLSearchParams(baseData).toString(), {
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
                    });

                    auth.credentials.access_token = response.data.access_token;
                    auth.credentials.refresh_token = response.data.refresh_token || refresh_token; // Update if new refresh_token provided
                    auth.expires_at = response.data.expires_in ? Date.now() + response.data.expires_in * 1000 : undefined;
                }
                // If no refresh_token but client_id and client_secret are present, try client_credentials grant
                else if (client_id && client_secret) {
                    const baseData = {
                        grant_type: 'client_credentials',
                        client_id,
                        client_secret,
                    } as Record<string, string>;

                    const response = await axios.post(token_url, new URLSearchParams(baseData).toString(), {
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
                    });

                    auth.credentials.access_token = response.data.access_token;
                    auth.expires_at = response.data.expires_in ? Date.now() + response.data.expires_in * 1000 : undefined;
                }
                // If neither refresh_token nor a valid access_token is available, throw an error
                else if (!access_token) {
                    throw new Error(
                        `OAuth2 credentials for ${connector.credential_id} lack a valid access_token or refresh_token. Initial authorization required.`
                    );
                }
            }

            // Return auth (with existing access_token if no refresh was needed)
            return auth;
        } catch (error) {
            throw error instanceof Error ? error : new Error(`Unknown error in getCredentials: ${String(error)}`);
        }
    }

    /**
     * Executes a data pipeline
     * @param pipeline - Pipeline configuration and callbacks
     * @throws Error if pipeline execution fails and fail_on_error is true
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
            retry_interval: 1000,
            fail_on_error: true
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
                    await sourceAdapter.connect();
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
                    await targetAdapter.connect();
                }

                log({ type: 'info', message: 'Connected to target adapter' });

                if (!targetAdapter.upload) {
                    throw new Error(`Upload not supported by adapter ${pipeline.target.adapter_id}`);
                }

                // Upload data in batches
                const itemsPerBatch = pipeline.target.pagination?.itemsPerPage || DEFAULT_CONFIG.ITEMS_PER_PAGE;
                for (let i = 0; i < finalData.length; i += itemsPerBatch) {
                    const batch = finalData.slice(i, i + itemsPerBatch);

                    // Upload batch with retry logic
                    for (let attempt = 0; attempt <= eh.max_retries; attempt++) {
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

                            if (eh.fail_on_error) {
                                throw error;
                            }
                        }
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

            if (eh.fail_on_error) {
                throw error;
            }
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

export { DatabaseAdapter, BasicAuth, HttpAdapter, Connector, AuthConfig, OAuth2Auth, AdapterInstance, FilterGroup, Filter } from './types'