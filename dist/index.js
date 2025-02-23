
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

/***/ 607:
/***/ (function(__unused_webpack_module, exports, __webpack_require__) {


var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.Orchestrator = Orchestrator;
const axios_1 = __importDefault(__webpack_require__(467));
const transform_1 = __importDefault(__webpack_require__(626));
/**
 * Creates a promise that resolves after the specified delay
 * @param ms - Delay duration in milliseconds
 * @returns Promise that resolves after the delay
 */
async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
// Default configuration values
const DEFAULT_CONFIG = {
    ITEMS_PER_PAGE: 100,
    TOTAL_ITEMS_LIMIT: 1000000,
    TIMEOUT_MS: 30000,
};
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
            if (auth.type === 'oauth2' && auth.credentials.token_url) {
                const { client_id, client_secret, refresh_token, access_token, token_url } = auth.credentials;
                // Check if access_token exists and is still valid
                if (access_token && auth.expires_at) {
                    const expiresAt = typeof auth.expires_at === 'string' ? new Date(auth.expires_at).getTime() : auth.expires_at;
                    if (expiresAt > Date.now()) {
                        console.log(`Using existing valid access token for ${connector.id}`);
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
                    };
                    const response = await axios_1.default.post(token_url, new URLSearchParams(baseData).toString(), {
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
                    };
                    const response = await axios_1.default.post(token_url, new URLSearchParams(baseData).toString(), {
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' }
                    });
                    auth.credentials.access_token = response.data.access_token;
                    auth.expires_at = response.data.expires_in ? Date.now() + response.data.expires_in * 1000 : undefined;
                }
                // If neither refresh_token nor a valid access_token is available, throw an error
                else if (!access_token) {
                    throw new Error(`OAuth2 credentials for ${connector.credential_id} lack a valid access_token or refresh_token. Initial authorization required.`);
                }
            }
            // Return auth (with existing access_token if no refresh was needed)
            return auth;
        }
        catch (error) {
            throw error instanceof Error ? error : new Error(`Unknown error in getCredentials: ${String(error)}`);
        }
    }
    /**
     * Executes a data pipeline
     * @param pipeline - Pipeline configuration and callbacks
     * @throws Error if pipeline execution fails and fail_on_error is true
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
            log({ type: 'error', message: 'Pipeline must have either a source or data' });
            return;
        }
        // Initialize configuration with defaults
        const eh = pipeline.error_handling || {
            max_retries: 0,
            retry_interval: 1000,
            fail_on_error: true
        };
        const rl = pipeline.rate_limiting || {
            requests_per_second: Infinity,
            concurrent_requests: Infinity,
            max_retries_on_rate_limit: 0
        };
        const minIntervalMs = rl.requests_per_second === Infinity
            ? 0
            : 1000 / rl.requests_per_second;
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
                await sourceAdapter.connect();
                log({ type: 'info', message: 'Connected to source adapter' });
                // In runPipeline
                // Initialize pagination parameters
                const itemsPerPage = pipeline.source.pagination?.itemsPerPage || DEFAULT_CONFIG.ITEMS_PER_PAGE;
                let pageOffset = pipeline.source.pagination?.pageOffsetKey || '0'; // Start as string
                const totalItemsToFetch = pipeline.source.limit ?? DEFAULT_CONFIG.TOTAL_ITEMS_LIMIT;
                const timeoutMs = pipeline.source.timeout ?? DEFAULT_CONFIG.TIMEOUT_MS;
                const downloadStartTime = Date.now();
                const isCursorBased = pipeline.source.pagination?.type === 'cursor';
                // Fetch data in pages
                while (true) {
                    // Check for timeout
                    if (Date.now() - downloadStartTime >= timeoutMs) {
                        log({ type: 'error', message: `Download timeout exceeded (${timeoutMs}ms)` });
                        break;
                    }
                    // Download page with retry logic
                    let pageResult = null;
                    const startTime = Date.now();
                    for (let attempt = 0; attempt <= eh.max_retries; attempt++) {
                        if (attempt > 0) {
                            await delay(eh.retry_interval);
                        }
                        try {
                            pageResult = await sourceAdapter.download({
                                limit: itemsPerPage,
                                offset: typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : pageOffset
                            });
                            break;
                        }
                        catch (error) {
                            log({
                                type: 'error',
                                message: `Attempt ${attempt + 1} failed in download: ${error.message}`
                            });
                            if (eh.fail_on_error) {
                                throw error;
                            }
                        }
                    }
                    // Break if no more data
                    if (!pageResult || pageResult.data.length === 0) {
                        log({ type: 'info', message: 'No more data to fetch' });
                        break;
                    }
                    // Process page data
                    const pageData = pageResult.data;
                    data.push(...pageData);
                    log({
                        type: 'extract',
                        message: `Extracted page${isCursorBased && pageResult.options?.nextOffset !== undefined ? ` with cursor ${pageResult.options.nextOffset}` : ` at offset ${pageOffset}`}`,
                        dataCount: pageData.length
                    });
                    // Check for potential last page
                    if (pageData.length < itemsPerPage) {
                        log({
                            type: 'info',
                            message: `Received ${pageData.length} items, less than ${itemsPerPage}, possibly last page`
                        });
                    }
                    const previousOffset = pageOffset;
                    if (isCursorBased && pageResult.options?.nextOffset !== undefined) {
                        pageOffset = pageResult.options.nextOffset; // Accept string or number
                        log({ type: 'info', message: `Next cursor set to ${pageOffset}` });
                    }
                    else {
                        pageOffset = (typeof pageOffset === 'string' ? parseInt(pageOffset, 10) || 0 : pageOffset) + itemsPerPage;
                        log({ type: 'info', message: `Next offset incremented to ${pageOffset}` });
                    }
                    if (data.length >= totalItemsToFetch) {
                        log({ type: 'info', message: `Reached total items limit of ${totalItemsToFetch}` });
                        break;
                    }
                    // Apply rate limiting if configured
                    if (minIntervalMs > 0) {
                        const elapsedMs = Date.now() - startTime;
                        const delayMs = Math.max(0, minIntervalMs - elapsedMs);
                        if (delayMs > 0) {
                            log({ type: 'info', message: `Rate limiting: waiting ${delayMs}ms` });
                            await delay(delayMs);
                        }
                    }
                }
                // Transform data if specified
                if (pipeline.source.transform && transform_1.default) {
                    data = await (0, transform_1.default)(pipeline.source, data);
                }
                log({
                    type: 'extract',
                    message: 'Data extraction complete',
                    dataCount: data.length
                });
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
            // Execute onload callback if specified
            pipeline.onload?.(data);
            // Handle target operations
            if (pipeline.target) {
                // Execute before-send hook if specified
                const beforeSendResult = pipeline.onbeforesend?.(data);
                if (beforeSendResult === false) {
                    log({ type: 'complete', message: 'Pipeline halted by onbeforesend' });
                    return;
                }
                const finalData = Array.isArray(beforeSendResult) ? beforeSendResult : data;
                // Initialize target adapter
                const targetAdapterFactory = adapters.get(pipeline.target.adapter_id);
                if (!targetAdapterFactory) {
                    throw new Error(`Target adapter ${pipeline.target.adapter_id} not found`);
                }
                const targetAuth = await getCredentials(pipeline.target);
                targetAdapter = targetAdapterFactory(pipeline.target, targetAuth);
                await targetAdapter.connect();
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
                        }
                        catch (error) {
                            log({
                                type: 'error',
                                message: `Attempt ${attempt + 1} failed in upload: ${error.message}`
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
            }
            log({ type: 'complete', message: 'Pipeline finished' });
        }
        catch (error) {
            log({
                type: 'error',
                message: `Pipeline failed: ${error.message}`
            });
            if (eh.fail_on_error) {
                throw error;
            }
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
    }
    // Register provided adapters
    Object.entries(availableAdapters).forEach(([id, adapter]) => {
        registerAdapter(id, adapter);
    });
    return { registerAdapter, runPipeline };
}


/***/ }),

/***/ 626:
/***/ ((__unused_webpack_module, exports) => {


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


/***/ }),

/***/ 467:
/***/ ((module) => {

module.exports = axios;

/***/ })

/******/ 	});
/************************************************************************/
/******/ 	// The module cache
/******/ 	var __webpack_module_cache__ = {};
/******/ 	
/******/ 	// The require function
/******/ 	function __webpack_require__(moduleId) {
/******/ 		// Check if module is in cache
/******/ 		var cachedModule = __webpack_module_cache__[moduleId];
/******/ 		if (cachedModule !== undefined) {
/******/ 			return cachedModule.exports;
/******/ 		}
/******/ 		// Create a new module (and put it into the cache)
/******/ 		var module = __webpack_module_cache__[moduleId] = {
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
/******/ 	var __webpack_exports__ = __webpack_require__(607);
/******/ 	openetl = __webpack_exports__;
/******/ 	
/******/ })()
;

    return openetl;
})));