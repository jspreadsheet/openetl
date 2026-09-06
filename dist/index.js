
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
exports.validatePipeline = exports.AdapterError = exports.DEFAULT_CONFIG = void 0;
exports.Orchestrator = Orchestrator;
const transform_1 = __importDefault(__webpack_require__(514));
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
    // Register provided adapters
    Object.entries(availableAdapters).forEach(([id, adapter]) => {
        registerAdapter(id, adapter);
    });
    return { registerAdapter, runPipeline };
}
var types_1 = __webpack_require__(613);
Object.defineProperty(exports, "AdapterError", ({ enumerable: true, get: function () { return types_1.AdapterError; } }));
Object.defineProperty(exports, "validatePipeline", ({ enumerable: true, get: function () { return types_1.validatePipeline; } }));


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