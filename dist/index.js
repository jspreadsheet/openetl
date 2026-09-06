
if (! axios && typeof(require) === 'function') {
    var axios = require('axios');
}

;(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
    typeof define === 'function' && define.amd ? define(factory) :
    global.openetl = factory();
}(this, (function () {

var openetl;
/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ 156
(__unused_webpack_module, exports, __webpack_require__) {


var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.uploadBatchWithRetry = exports.applyTransformers = exports.createSourceStream = exports.StreamingPipelineRunner = exports.chainStreamingPipelines = exports.runStreamingPipelinesInParallel = exports.runStreamingPipeline = exports.validatePipeline = exports.AdapterError = exports.DEFAULT_CONFIG = void 0;
exports.Orchestrator = Orchestrator;
const transform_1 = __importDefault(__webpack_require__(514));
const streaming_1 = __webpack_require__(120);
Object.defineProperty(exports, "runStreamingPipeline", ({ enumerable: true, get: function () { return streaming_1.runStreamingPipeline; } }));
Object.defineProperty(exports, "runStreamingPipelinesInParallel", ({ enumerable: true, get: function () { return streaming_1.runStreamingPipelinesInParallel; } }));
Object.defineProperty(exports, "chainStreamingPipelines", ({ enumerable: true, get: function () { return streaming_1.chainStreamingPipelines; } }));
Object.defineProperty(exports, "StreamingPipelineRunner", ({ enumerable: true, get: function () { return streaming_1.StreamingPipelineRunner; } }));
Object.defineProperty(exports, "createSourceStream", ({ enumerable: true, get: function () { return streaming_1.createSourceStream; } }));
Object.defineProperty(exports, "applyTransformers", ({ enumerable: true, get: function () { return streaming_1.applyTransformers; } }));
Object.defineProperty(exports, "uploadBatchWithRetry", ({ enumerable: true, get: function () { return streaming_1.uploadBatchWithRetry; } }));
/**
 * Creates a promise that resolves after the specified delay
 * @param ms - Delay duration in milliseconds
 * @returns Promise that resolves after the delay
 */
async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
/**
 * Default configuration values for the orchestrator
 */
exports.DEFAULT_CONFIG = {
    ITEMS_PER_PAGE: 100,
    TOTAL_ITEMS_LIMIT: 1000000,
    TIMEOUT_MS: 30000,
    RETRY_INTERVAL_MS: 1000,
    MAX_RETRIES: 0,
    MAX_BACKOFF_MS: 30000,
};
/**
 * Calculates exponential backoff delay with jitter
 * @param attempt - Current retry attempt (0-indexed)
 * @param baseDelay - Base delay in milliseconds
 * @param maxDelay - Maximum delay cap in milliseconds
 * @returns Calculated delay with jitter
 */
function calculateBackoff(attempt, baseDelay = exports.DEFAULT_CONFIG.RETRY_INTERVAL_MS, maxDelay = exports.DEFAULT_CONFIG.MAX_BACKOFF_MS) {
    // Exponential backoff: baseDelay * 2^attempt
    const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
    // Add jitter: random value between 0-30% of the delay
    const jitter = Math.random() * 0.3 * exponentialDelay;
    return Math.floor(exponentialDelay + jitter);
}
async function fetchData(sourceAdapter, itemsPerPage, pageOffset, downloadStartTime, timeoutMs, errorHandling, log) {
    let result, lastError, attempt = 0;
    do {
        if (attempt > 0) {
            // Use exponential backoff with jitter if enabled, otherwise use fixed interval
            const delayMs = errorHandling.use_exponential_backoff
                ? calculateBackoff(attempt - 1, errorHandling.retry_interval)
                : errorHandling.retry_interval;
            log({ type: 'info', message: `Retrying in ${delayMs}ms (attempt ${attempt + 1}/${errorHandling.max_retries + 1})` });
            await delay(delayMs);
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
        }
        catch (error) {
            log({
                type: 'error',
                message: `Attempt ${attempt + 1} failed in download: ${error.message}`
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
function getEndpoint(connector, adapter) {
    const adapterConfig = adapter.getConfig();
    const { endpoints } = adapterConfig;
    const { endpoint_id: endpointId } = connector;
    const endpoint = endpoints.find(e => e.id === endpointId);
    return endpoint;
}
function getPaginationFromEndpoint(connector, adapter, itemsPerPage, log) {
    const endpoint = getEndpoint(connector, adapter);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in adapter ${connector.adapter_id}`);
    }
    let paginationConfig;
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
    }
    else if (typeof paginationConfig.maxItemsPerPage !== 'undefined') {
        if (typeof itemsPerPage === 'undefined') {
            log({
                type: 'info',
                message: `Since the number of items per page was not defined, the maximum items per page defined by the adapter (${paginationConfig.maxItemsPerPage}) will be used instead`,
            });
            itemsPerPage = paginationConfig.maxItemsPerPage;
        }
        else if (itemsPerPage > paginationConfig.maxItemsPerPage) {
            log({
                type: 'info',
                message: `The number of items per page (${itemsPerPage}) is greater than the maximum allowed by the adapter (${paginationConfig.maxItemsPerPage}), so it will be reduced to the maximum allowed by the adapter`,
            });
            itemsPerPage = paginationConfig.maxItemsPerPage;
        }
    }
    const paginationType = (paginationConfig && paginationConfig.type) || undefined;
    return {
        paginationType,
        itemsPerPage,
    };
}
async function getDataSerially(pipeline, sourceAdapter, errorHandling, log) {
    const rl = pipeline.rate_limiting || {
        requests_per_second: Infinity,
        max_retries_on_rate_limit: 0
    };
    const minIntervalMs = rl.requests_per_second === Infinity
        ? 0
        : 1000 / rl.requests_per_second;
    const totalItemsToFetch = pipeline.source.limit ?? exports.DEFAULT_CONFIG.TOTAL_ITEMS_LIMIT;
    const timeoutMs = pipeline.source.timeout ?? exports.DEFAULT_CONFIG.TIMEOUT_MS;
    const downloadStartTime = Date.now();
    const endpoint = getEndpoint(pipeline.source, sourceAdapter);
    if (!endpoint) {
        const { endpoint_id, adapter_id } = pipeline.source;
        throw new Error(`Endpoint ${endpoint_id} not found in adapter ${adapter_id}`);
    }
    if (pipeline.source.fields.length === 0 &&
        'defaultFields' in endpoint &&
        endpoint.defaultFields) {
        pipeline.source.fields = [...endpoint.defaultFields];
    }
    let { itemsPerPage, paginationType, } = getPaginationFromEndpoint(pipeline.source, sourceAdapter, pipeline.source.pagination?.itemsPerPage || undefined, log);
    let pageResult, fetchDataMoment, pageOffset = pipeline.source.pagination?.pageOffsetKey || undefined;
    let data = [];
    do {
        if (pageResult) {
            if (paginationType === 'cursor') {
                pageOffset = pageResult.options.nextOffset; // Accept string or number
                log({ type: 'info', message: `Next cursor set to ${pageOffset}` });
            }
            else {
                pageOffset = (typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : (pageOffset || 0)) + itemsPerPage;
                log({ type: 'info', message: `Next offset incremented to ${pageOffset}` });
            }
            // Apply rate limiting if configured
            if (minIntervalMs > 0) {
                const elapsedMs = Date.now() - fetchDataMoment;
                const delayMs = Math.max(0, minIntervalMs - elapsedMs);
                if (delayMs > 0) {
                    log({ type: 'info', message: `Rate limiting: waiting ${delayMs}ms` });
                    await delay(delayMs);
                }
            }
        }
        try {
            fetchDataMoment = Date.now();
            pageResult = await fetchData(sourceAdapter, itemsPerPage, pageOffset, downloadStartTime, timeoutMs, errorHandling, log);
        }
        catch (error) {
            if (error instanceof Error && error.message === 'Download timeout exceeded') {
                break;
            }
            throw error;
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
    } while (data.length < totalItemsToFetch &&
        typeof paginationType !== 'undefined' &&
        typeof itemsPerPage !== 'undefined' &&
        (paginationType === 'cursor'
            ? pageResult.options?.nextOffset !== undefined
            : pageResult.data.length === itemsPerPage));
    if (data.length > totalItemsToFetch) {
        data.splice(totalItemsToFetch, data.length - totalItemsToFetch);
    }
    if (typeof paginationType === 'undefined' || typeof itemsPerPage === 'undefined') {
        log({ type: 'info', message: 'Search without pagination finished' });
    }
    else if (data.length === totalItemsToFetch) {
        log({ type: 'info', message: `Reached total items limit of ${totalItemsToFetch}` });
    }
    else if (paginationType === 'cursor') {
        if (pageResult.options?.nextOffset === undefined) {
            log({ type: 'info', message: 'No more data to fetch' });
        }
    }
    else {
        if (pageResult.data.length === 0) {
            log({ type: 'info', message: 'No more data to fetch' });
        }
        else if (pageResult.data.length < itemsPerPage) {
            log({
                type: 'info',
                message: `Received ${pageResult.data.length} items, less than ${itemsPerPage}, so it's the last page`
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
function Orchestrator(vault, availableAdapters) {
    const adapters = new Map();
    /**
     * Registers a new adapter implementation
     * @param id - Unique identifier for the adapter
     * @param adapter - Adapter implementation
     */
    function registerAdapter(id, adapter) {
        adapters.set(id, adapter);
    }
    /**
     * Retrieves an access token for a single connector using its adapter and credentials.
     * @param connector - The connector to retrieve the token for
     * @returns {Promise<AuthConfig>} Updated auth configuration with access token
     * @throws Error if credentials are invalid or token cannot be obtained
     */
    async function getCredentials(connector) {
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
        }
        catch (error) {
            throw error instanceof Error ? error : new Error(`Unknown error in getCredentials: ${String(error)}`);
        }
    }
    /**
     * Executes a data pipeline
     * @param pipeline - Pipeline configuration and callbacks
     * @throws Error if pipeline execution fails
     */
    async function runPipeline(pipeline) {
        let sourceAdapter;
        let targetAdapter;
        // Create logging wrapper with timestamp
        const log = (event) => {
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
        let data = [];
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
                    }
                    catch (error) {
                        throw new Error(error instanceof Error
                            ? error.message
                            : 'Something went wrong while establishing a connection to the source connector');
                    }
                }
                log({ type: 'info', message: 'Connected to source adapter' });
                // Fetch data in pages
                data = await getDataSerially(pipeline, sourceAdapter, eh, log);
                // Transform data if specified
                if (pipeline.source.transform && transform_1.default) {
                    data = await (0, transform_1.default)(pipeline.source, data);
                }
                log({
                    type: 'extract',
                    message: 'Data extraction complete',
                    dataCount: data.length
                });
                // Execute onload callback if specified
                pipeline.onload?.(data);
            }
            else {
                // Use provided data
                data = pipeline.data;
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
                    }
                    catch (error) {
                        throw new Error(error instanceof Error
                            ? error.message
                            : 'Something went wrong while establishing a connection to the target connector');
                    }
                }
                log({ type: 'info', message: 'Connected to target adapter' });
                if (!targetAdapter.upload) {
                    throw new Error(`Upload not supported by adapter ${pipeline.target.adapter_id}`);
                }
                // Upload data in batches
                let { paginationType, itemsPerPage: itemsPerBatch } = getPaginationFromEndpoint(pipeline.target, targetAdapter, pipeline.target.pagination?.itemsPerPage || undefined, log);
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
                        }
                        catch (error) {
                            log({
                                type: 'error',
                                message: `Attempt ${attempt + 1} failed in upload: ${error.message}`
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
        }
        catch (error) {
            log({
                type: 'error',
                message: `Pipeline failed: ${error.message}`
            });
            throw error;
        }
        finally {
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
            }
            catch (cleanupError) {
                log({ type: 'error', message: `Connection cleanup failed: ${cleanupError.message}` });
                // Don't throw cleanup errors if the main operation succeeded
            }
        }
        return { data };
    }
    async function runStreamingPipeline(pipeline, options) {
        const adaptersObj = {};
        adapters.forEach((adapter, id) => {
            adaptersObj[id] = adapter;
        });
        return (0, streaming_1.runStreamingPipeline)(pipeline, vault, adaptersObj, options);
    }
    async function runParallelStreamingPipelines(pipelines, options) {
        const adaptersObj = {};
        adapters.forEach((adapter, id) => {
            adaptersObj[id] = adapter;
        });
        return (0, streaming_1.runStreamingPipelinesInParallel)(pipelines, vault, adaptersObj, options);
    }
    async function chainStreamingPipelines(pipelines, options) {
        const adaptersObj = {};
        adapters.forEach((adapter, id) => {
            adaptersObj[id] = adapter;
        });
        return (0, streaming_1.chainStreamingPipelines)(pipelines, vault, adaptersObj, options);
    }
    function createStreamingRunner(pipeline) {
        const adaptersObj = {};
        adapters.forEach((adapter, id) => {
            adaptersObj[id] = adapter;
        });
        return new streaming_1.StreamingPipelineRunner(pipeline, vault, adaptersObj);
    }
    // Register provided adapters
    Object.entries(availableAdapters).forEach(([id, adapter]) => {
        registerAdapter(id, adapter);
    });
    return {
        registerAdapter,
        runPipeline,
        runStreamingPipeline,
        runParallelStreamingPipelines,
        runStreamingPipelinesInParallel: runParallelStreamingPipelines,
        chainStreamingPipelines,
        createStreamingRunner
    };
}
var types_1 = __webpack_require__(613);
Object.defineProperty(exports, "AdapterError", ({ enumerable: true, get: function () { return types_1.AdapterError; } }));
Object.defineProperty(exports, "validatePipeline", ({ enumerable: true, get: function () { return types_1.validatePipeline; } }));


/***/ },

/***/ 120
(__unused_webpack_module, exports, __webpack_require__) {


var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.StreamingPipelineRunner = void 0;
exports.createSourceStream = createSourceStream;
exports.applyTransformers = applyTransformers;
exports.uploadBatchWithRetry = uploadBatchWithRetry;
exports.runStreamingPipeline = runStreamingPipeline;
exports.runStreamingPipelinesInParallel = runStreamingPipelinesInParallel;
exports.chainStreamingPipelines = chainStreamingPipelines;
const transform_1 = __importDefault(__webpack_require__(514));
/**
 * Retrieves credentials for a connector from the vault
 */
async function getCredentials(connector, vault, availableAdapters) {
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
function getEndpoint(connector, adapter) {
    const adapterConfig = adapter.getConfig();
    const { endpoints } = adapterConfig;
    const { endpoint_id: endpointId } = connector;
    return endpoints.find(e => e.id === endpointId);
}
/**
 * Creates an AsyncIterable stream of data chunks from a source connector and adapter.
 * Supports both streaming APIs (downloadStream) and paginated/batch APIs (download).
 */
async function* createSourceStream(connector, adapter, options, errorHandling, log) {
    const limit = options?.limit ?? connector.limit ?? 1000000;
    const chunkSize = options?.chunkSize ?? connector.pagination?.itemsPerPage ?? 1000;
    const initialOffset = options?.pageOffsetKey ?? connector.pagination?.pageOffsetKey;
    const endpoint = getEndpoint(connector, adapter);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in adapter ${connector.adapter_id}`);
    }
    if (connector.fields &&
        connector.fields.length === 0 &&
        'defaultFields' in endpoint &&
        endpoint.defaultFields) {
        connector.fields = [...endpoint.defaultFields];
    }
    // Check if adapter supports streaming natively
    if (typeof adapter.downloadStream === 'function') {
        const streamOptions = {
            limit,
            chunkSize,
            offset: initialOffset
        };
        const stream = adapter.downloadStream(streamOptions);
        let totalFetched = 0;
        for await (let chunk of stream) {
            if (!Array.isArray(chunk) || chunk.length === 0)
                continue;
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
            if (totalFetched >= limit)
                break;
        }
        return;
    }
    // Fallback to paginated/batch download
    let paginationConfig;
    const endpointPagination = endpoint.settings?.pagination;
    if (typeof endpointPagination !== "undefined") {
        paginationConfig = endpointPagination;
    }
    if (typeof paginationConfig === "undefined") {
        const adapterConfig = adapter.getConfig();
        paginationConfig = adapterConfig.pagination || false;
    }
    let itemsPerPage = chunkSize;
    if (!paginationConfig) {
        if (typeof itemsPerPage !== 'undefined') {
            log?.({
                type: 'info',
                message: `since the ${connector.endpoint_id} endpoint of the ${connector.adapter_id} adapter does not support pagination, the number of items per page set will be ignored`,
            });
            itemsPerPage = undefined;
        }
    }
    else if (typeof paginationConfig.maxItemsPerPage !== 'undefined') {
        if (typeof itemsPerPage === 'undefined' || itemsPerPage > paginationConfig.maxItemsPerPage) {
            itemsPerPage = paginationConfig.maxItemsPerPage;
        }
    }
    const paginationType = (paginationConfig && paginationConfig.type) || undefined;
    let pageOffset = initialOffset;
    let totalFetched = 0;
    let pageResult;
    const maxRetries = errorHandling?.max_retries ?? 0;
    const retryInterval = errorHandling?.retry_interval ?? 1000;
    const useExponentialBackoff = errorHandling?.use_exponential_backoff ?? false;
    do {
        if (pageResult) {
            if (paginationType === 'cursor') {
                pageOffset = pageResult.options?.nextOffset;
            }
            else {
                pageOffset = (typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : (pageOffset || 0)) + (itemsPerPage || 100);
            }
        }
        let attempt = 0;
        let lastError;
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
            }
            catch (error) {
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
        if (!Array.isArray(pageResult.data))
            break;
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
    } while (totalFetched < limit &&
        typeof paginationType !== 'undefined' &&
        typeof itemsPerPage !== 'undefined' &&
        (paginationType === 'cursor'
            ? pageResult.options?.nextOffset !== undefined
            : pageResult.data.length === itemsPerPage));
}
/**
 * Applies transformations to a chunk of data.
 * Executes both existing adapter transformations (connector.transform) and pipeline-level transformer steps.
 */
async function applyTransformers(chunk, transformers, connector) {
    if (!Array.isArray(chunk) || chunk.length === 0) {
        return [];
    }
    let currentData = [...chunk];
    // Apply connector transformations first
    if (connector?.transform && connector.transform.length > 0) {
        currentData = await (0, transform_1.default)(connector, currentData);
    }
    // Apply additional pipeline transformers
    if (transformers && transformers.length > 0) {
        for (const step of transformers) {
            if (typeof step === 'function') {
                const res = await step(currentData, connector);
                currentData = Array.isArray(res) ? res : currentData;
            }
            else if (step && typeof step === 'object' && 'type' in step) {
                if (typeof step.type === 'function') {
                    const res = await step.type(currentData, connector);
                    currentData = Array.isArray(res) ? res : currentData;
                }
                else {
                    const tempConnector = {
                        ...(connector || {
                            id: 'temp',
                            adapter_id: 'temp',
                            endpoint_id: 'temp',
                            credential_id: 'temp',
                            fields: []
                        }),
                        transform: [step]
                    };
                    currentData = await (0, transform_1.default)(tempConnector, currentData);
                }
            }
        }
    }
    return currentData;
}
/**
 * Uploads a batch of data with retry support
 */
async function uploadBatchWithRetry(adapter, batch, errorHandling, log) {
    if (!Array.isArray(batch) || batch.length === 0) {
        return;
    }
    // If adapter supports uploadStream and not upload, wrap batch
    if (typeof adapter.uploadStream === 'function' && typeof adapter.upload !== 'function') {
        async function* stream() {
            yield batch;
        }
        await adapter.uploadStream(stream());
        return;
    }
    if (typeof adapter.upload !== 'function') {
        throw new Error(`Upload not supported by target adapter`);
    }
    const maxRetries = errorHandling?.max_retries ?? 0;
    const retryInterval = errorHandling?.retry_interval ?? 1000;
    const useExponentialBackoff = errorHandling?.use_exponential_backoff ?? false;
    let attempt = 0;
    let lastError;
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
        }
        catch (error) {
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
async function runStreamingPipeline(pipeline, vault = {}, adapters = {}, options) {
    const isDryRun = options?.dryRun ?? pipeline.dryRun ?? false;
    const chunkSize = pipeline.chunkSize ?? 1000;
    const eh = pipeline.error_handling || { max_retries: 0, retry_interval: 1000 };
    const log = (event) => {
        pipeline.logging?.({
            ...event,
            timestamp: new Date().toISOString()
        });
    };
    if (!pipeline.source && !pipeline.data) {
        throw new Error('Pipeline must have either a source or data');
    }
    log({ type: 'start', message: `Streaming pipeline started${isDryRun ? ' (DRY RUN)' : ''}` });
    let sourceAdapter;
    let targetAdapter;
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
                }
                catch (error) {
                    throw new Error(error instanceof Error
                        ? error.message
                        : 'Something went wrong while establishing a connection to the source connector');
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
                }
                catch (error) {
                    throw new Error(error instanceof Error
                        ? error.message
                        : 'Something went wrong while establishing a connection to the target connector');
                }
            }
            log({ type: 'info', message: 'Connected to target adapter' });
        }
        const allData = [];
        let chunksCount = 0;
        let totalProcessed = 0;
        async function* transformedStreamGenerator() {
            let sourceStream;
            if (pipeline.source && sourceAdapter) {
                sourceStream = createSourceStream(pipeline.source, sourceAdapter, { chunkSize, limit: pipeline.source.limit }, eh, log);
            }
            else if (pipeline.data) {
                const staticData = [...pipeline.data];
                async function* staticStream() {
                    for (let i = 0; i < staticData.length; i += chunkSize) {
                        yield staticData.slice(i, i + chunkSize);
                    }
                }
                sourceStream = staticStream();
            }
            else {
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
                const steps = [
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
        }
        else if (pipeline.target && targetAdapter) {
            if (typeof targetAdapter.uploadStream === 'function') {
                await targetAdapter.uploadStream(generator);
                log({ type: 'load', message: 'Uploaded data using uploadStream' });
            }
            else if (typeof targetAdapter.upload === 'function') {
                for await (const chunk of generator) {
                    await uploadBatchWithRetry(targetAdapter, chunk, eh, log);
                    log({
                        type: 'load',
                        message: `Uploaded batch of ${chunk.length} items`,
                        dataCount: chunk.length
                    });
                }
            }
            else {
                throw new Error(`Upload not supported by target adapter ${pipeline.target.adapter_id}`);
            }
            pipeline.onupload?.();
        }
        else {
            for await (const _ of generator) { }
        }
        pipeline.onload?.(allData);
        log({ type: 'complete', message: 'Streaming pipeline finished' });
        return {
            data: allData,
            chunksCount,
            totalProcessed,
            dryRun: isDryRun
        };
    }
    catch (error) {
        log({
            type: 'error',
            message: `Streaming pipeline failed: ${error instanceof Error ? error.message : String(error)}`
        });
        throw error;
    }
    finally {
        try {
            if (sourceAdapter?.disconnect) {
                await sourceAdapter.disconnect();
                log({ type: 'info', message: 'Source adapter disconnected' });
            }
            if (targetAdapter?.disconnect) {
                await targetAdapter.disconnect();
                log({ type: 'info', message: 'Target adapter disconnected' });
            }
        }
        catch (cleanupError) {
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
async function runStreamingPipelinesInParallel(pipelines, vault = {}, adapters = {}, options) {
    const stopOnError = options?.stopOnError ?? true;
    const promises = pipelines.map(async (pipeline) => {
        try {
            const res = await runStreamingPipeline(pipeline, vault, adapters, options);
            return {
                pipelineId: pipeline.id,
                data: res.data,
                chunksCount: res.chunksCount,
                totalProcessed: res.totalProcessed,
                success: true
            };
        }
        catch (error) {
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
async function chainStreamingPipelines(pipelines, vault = {}, adapters = {}, options) {
    const results = [];
    let currentData = undefined;
    for (let i = 0; i < pipelines.length; i++) {
        const pipeline = { ...pipelines[i] };
        if (i > 0 && currentData !== undefined) {
            if (options?.passData || !pipeline.source) {
                pipeline.data = currentData;
            }
        }
        const res = await runStreamingPipeline(pipeline, vault, adapters, options);
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
class StreamingPipelineRunner {
    constructor(pipeline, vault = {}, adapters = {}) {
        this.pipeline = {
            ...pipeline,
            transform: [...(pipeline.transform || [])],
            transformers: [...(pipeline.transformers || [])]
        };
        this.vault = vault;
        this.adapters = adapters;
    }
    addTransformer(step) {
        if (!this.pipeline.transformers) {
            this.pipeline.transformers = [];
        }
        this.pipeline.transformers.push(step);
        return this;
    }
    setChunkSize(size) {
        this.pipeline.chunkSize = size;
        return this;
    }
    async run(options) {
        return runStreamingPipeline(this.pipeline, this.vault, this.adapters, options);
    }
    async runDry() {
        return this.run({ dryRun: true });
    }
}
exports.StreamingPipelineRunner = StreamingPipelineRunner;


/***/ },

/***/ 613
(__unused_webpack_module, exports) {


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.AdapterError = void 0;
exports.validatePipeline = validatePipeline;
function isAxiosError(error) {
    return (typeof error === 'object' &&
        error !== null &&
        'response' in error &&
        typeof error.response === 'object');
}
/**
 * Standardized error class for adapter operations with retry support and context
 */
class AdapterError extends Error {
    constructor(message, code = 'UNKNOWN', retryable = false, context, cause) {
        super(message);
        this.name = 'AdapterError';
        this.code = code;
        this.retryable = retryable;
        this.context = context;
        this.cause = cause;
        // Maintains proper stack trace for where error was thrown (V8 engines)
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, AdapterError);
        }
    }
    /**
     * Creates an AdapterError from an unknown error type
     */
    static from(error, code = 'UNKNOWN', retryable = false, context) {
        if (error instanceof AdapterError) {
            return error;
        }
        const message = error instanceof Error ? error.message : String(error);
        const cause = error instanceof Error ? error : undefined;
        // Detect specific error types from axios or other sources
        if (isAxiosError(error)) {
            const status = error.response?.status;
            if (status === 401 || status === 403) {
                return new AdapterError(message, 'AUTHENTICATION_FAILED', false, context, cause);
            }
            if (status === 404) {
                return new AdapterError(message, 'NOT_FOUND', false, context, cause);
            }
            if (status === 429) {
                return new AdapterError(message, 'RATE_LIMITED', true, context, cause);
            }
            if (status && status >= 500) {
                return new AdapterError(message, code, true, context, cause);
            }
        }
        return new AdapterError(message, code, retryable, context, cause);
    }
    /**
     * Serializes the error for logging or transmission
     */
    toJSON() {
        return {
            name: this.name,
            message: this.message,
            code: this.code,
            retryable: this.retryable,
            context: this.context,
            cause: this.cause ? {
                name: this.cause.name,
                message: this.cause.message
            } : undefined,
            stack: this.stack
        };
    }
}
exports.AdapterError = AdapterError;
;
/**
 * Validates a pipeline configuration before execution
 * @param pipeline - Pipeline configuration to validate
 * @param adapters - Available adapters
 * @param vault - Credential vault
 * @returns Validation result with errors and warnings
 */
function validatePipeline(pipeline, adapters, vault) {
    const errors = [];
    const warnings = [];
    // Validate pipeline has source or data
    if (!pipeline.source && !pipeline.data) {
        errors.push('Pipeline must have either a source or data');
    }
    // Validate source configuration
    if (pipeline.source) {
        const { adapter_id, endpoint_id, credential_id } = pipeline.source;
        // Check adapter exists
        if (!adapters[adapter_id]) {
            errors.push(`Source adapter '${adapter_id}' not found`);
        }
        // Check credentials exist
        if (!vault[credential_id]) {
            errors.push(`Source credentials '${credential_id}' not found in vault`);
        }
        // Validate endpoint_id is provided
        if (!endpoint_id) {
            errors.push('Source endpoint_id is required');
        }
        // Warn if no fields specified
        if (!pipeline.source.fields || pipeline.source.fields.length === 0) {
            warnings.push('No fields specified for source - will use defaults if available');
        }
        // Validate filters if present
        if (pipeline.source.filters) {
            pipeline.source.filters.forEach((filter, index) => {
                if ('field' in filter) {
                    if (!filter.field) {
                        errors.push(`Filter at index ${index} has empty field`);
                    }
                    if (!filter.operator) {
                        errors.push(`Filter at index ${index} has empty operator`);
                    }
                }
            });
        }
    }
    // Validate target configuration
    if (pipeline.target) {
        const { adapter_id, endpoint_id, credential_id } = pipeline.target;
        // Check adapter exists
        if (!adapters[adapter_id]) {
            errors.push(`Target adapter '${adapter_id}' not found`);
        }
        // Check credentials exist
        if (!vault[credential_id]) {
            errors.push(`Target credentials '${credential_id}' not found in vault`);
        }
        // Validate endpoint_id is provided
        if (!endpoint_id) {
            errors.push('Target endpoint_id is required');
        }
    }
    // Validate error_handling if present
    if (pipeline.error_handling) {
        if (pipeline.error_handling.max_retries < 0) {
            errors.push('max_retries must be non-negative');
        }
        if (pipeline.error_handling.retry_interval < 0) {
            errors.push('retry_interval must be non-negative');
        }
    }
    // Validate rate_limiting if present
    if (pipeline.rate_limiting) {
        if (pipeline.rate_limiting.requests_per_second <= 0) {
            errors.push('requests_per_second must be positive');
        }
    }
    return {
        valid: errors.length === 0,
        errors,
        warnings
    };
}


/***/ },

/***/ 514
(__unused_webpack_module, exports) {


Object.defineProperty(exports, "__esModule", ({ value: true }));
exports["default"] = Transform;
async function Transform(connector, data) {
    let transformedData = [...data];
    for (const transformation of connector.transform || []) {
        switch (transformation.type) {
            case 'concat': {
                // Narrow options to ConcatOptions
                const options = transformation.options;
                const { properties, glue = ' ', to } = options;
                if (properties && to) {
                    transformedData = transformedData.map(item => {
                        const concatenated = properties.map((prop) => item[prop]).filter(Boolean).join(glue);
                        return { ...item, [to]: concatenated };
                    });
                }
                break;
            }
            case 'renameKey': {
                const options = transformation.options;
                const { from, to } = options;
                if (from && to) {
                    transformedData = transformedData.map(item => {
                        const value = from.split('.').reduce((obj, key) => obj?.[key], item);
                        return { ...item, [to]: value };
                    });
                }
                break;
            }
            case 'uppercase': {
                const options = transformation.options;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().toUpperCase() || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }
            case 'lowercase': {
                const options = transformation.options;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().toLowerCase() || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }
            case 'trim': {
                const options = transformation.options;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().trim() || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }
            case 'split': {
                const options = transformation.options;
                const { field, delimiter, to } = options;
                if (field && delimiter && to) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().split(delimiter) || [];
                        return { ...item, [to]: value };
                    });
                }
                break;
            }
            case 'replace': {
                const options = transformation.options;
                const { field, search, replace, to } = options;
                if (field && search && replace !== undefined) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString().replace(new RegExp(search, 'g'), replace) || '';
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }
            case 'addPrefix': {
                const options = transformation.options;
                const { field, prefix, to } = options;
                if (field && prefix) {
                    transformedData = transformedData.map(item => {
                        const value = `${prefix}${item[field] || ''}`;
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }
            case 'addSuffix': {
                const options = transformation.options;
                const { field, suffix, to } = options;
                if (field && suffix) {
                    transformedData = transformedData.map(item => {
                        const value = `${item[field] || ''}${suffix}`;
                        return { ...item, [to || field]: value };
                    });
                }
                break;
            }
            case 'toNumber': {
                const options = transformation.options;
                const { field, to } = options;
                if (field) {
                    transformedData = transformedData.map(item => {
                        const value = parseFloat(item[field]?.toString()) || 0;
                        return { ...item, [to || field]: isNaN(value) ? 0 : value };
                    });
                }
                break;
            }
            case 'extract': {
                const options = transformation.options;
                const { field, pattern, start, end, to } = options;
                if (field && to) {
                    transformedData = transformedData.map(item => {
                        const value = item[field]?.toString() || '';
                        if (pattern) {
                            const match = value.match(new RegExp(pattern));
                            return { ...item, [to]: match ? match[1] || match[0] : '' };
                        }
                        else if (start !== undefined && end !== undefined) {
                            return { ...item, [to]: value.slice(start, end) };
                        }
                        return item;
                    });
                }
                break;
            }
            case 'mergeObjects': {
                const options = transformation.options;
                const { fields, to } = options;
                if (fields && to) {
                    transformedData = transformedData.map(item => {
                        const merged = fields.reduce((obj, field) => {
                            if (item[field] !== undefined) {
                                obj[field] = item[field];
                            }
                            return obj;
                        }, {});
                        return { ...item, [to]: merged };
                    });
                }
                break;
            }
            default:
                console.warn(`Unknown transformation type: ${transformation.type}`);
                break;
        }
    }
    return transformedData;
}


/***/ }

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	const __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		const cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		const module = __webpack_module_cache__[moduleId] = {
/******/ 			// no module.id needed
/******/ 			// no module.loaded needed
/******/ 			exports: {}
/******/ 		};
/******/ 	
/******/ 		// Execute the module function
/******/ 		__webpack_modules__[moduleId].call(module.exports, module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
/******/ 	
/******/ 	// startup
/******/ 	// Load entry module and return exports
/******/ 	// This entry module is referenced by other modules so it can't be inlined
/******/ 	let __webpack_exports__ = __webpack_require__(156);
/******/ 	openetl = __webpack_exports__;
/******/ 	
/******/ })()
;

    return openetl;
})));