
if (! axios && typeof(require) === 'function') {
    var axios = require('axios');
}

;(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
    typeof define === 'function' && define.amd ? define(factory) :
    global.hubspot = factory();
}(this, (function () {

var hubspot;
/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ 920:
/***/ (function(__unused_webpack_module, exports, __webpack_require__) {


/**
 * HubSpot Adapter for OpenETL
 * https://componade.com/openetl
 *
 * @TODO:
 * Performance Optimization
 * Issue: The upload function processes items sequentially with individual POST requests, which could be slow for large datasets. HubSpot supports batch endpoints (e.g., /crm/v3/objects/contacts/batch/create).
 * Fix: Add batch upload support for efficiency.
 */
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.HubSpotAdapter = void 0;
exports.hubspot = hubspot;
const axios_1 = __importStar(__webpack_require__(467));
const maxItemsPerPage = 100;
const HubSpotAdapter = {
    id: "hubspot",
    name: "HubSpot CRM Adapter",
    type: "http",
    category: 'SaaS & CRM Applications',
    image: 'https://static.cdnlogo.com/logos/h/24/hubspot.svg',
    action: ["download", "upload", "sync"],
    credential_type: "oauth2", // Update to reflect actual usage
    base_url: "https://api.hubapi.com",
    metadata: {
        provider: "hubspot",
        description: "Adapter for HubSpot CRM and Marketing APIs",
        version: "v3", // Most current stable API version as of Feb 2025
    },
    pagination: {
        type: 'cursor',
        maxItemsPerPage,
    },
    endpoints: [
        // CRM Objects
        {
            id: "contacts",
            path: "/crm/v3/objects/contacts/search",
            method: "POST",
            description: "Retrieve all contacts from HubSpot",
            supported_actions: ["download", "sync"],
            tool: 'hubspot_search_contacts',
        },
        {
            id: "create-contact",
            path: "/crm/v3/objects/contacts/batch/create",
            method: "POST",
            description: "Create a new contact in HubSpot",
            supported_actions: ["upload"],
            tool: 'hubspot_create_contacts',
        },
        {
            id: "companies",
            path: "/crm/v3/objects/companies/search",
            method: "POST",
            description: "Retrieve all companies from HubSpot",
            supported_actions: ["download", "sync"],
            tool: 'hubspot_search_companies',
        },
        {
            id: "create-company",
            path: "/crm/v3/objects/companies/batch/create",
            method: "POST",
            description: "Create a new company in HubSpot",
            supported_actions: ["upload"],
            tool: 'hubspot_create_companies',
        },
        {
            id: "deals",
            path: "/crm/v3/objects/deals/search",
            method: "POST",
            description: "Retrieve all deals from HubSpot",
            supported_actions: ["download", "sync"],
            tool: 'hubspot_search_deals',
        },
        {
            id: "create-deal",
            path: "/crm/v3/objects/deals/batch/create",
            method: "POST",
            description: "Create a new deal in HubSpot",
            supported_actions: ["upload"],
            tool: 'hubspot_create_deals',
        },
        {
            id: "tickets",
            path: "/crm/v3/objects/tickets/search",
            method: "POST",
            description: "Retrieve all support tickets from HubSpot",
            supported_actions: ["download", "sync"],
        },
        {
            id: "create-ticket",
            path: "/crm/v3/objects/tickets/batch/create",
            method: "POST",
            description: "Create a new support ticket in HubSpot",
            supported_actions: ["upload"],
        },
        {
            id: "products",
            path: "/crm/v3/objects/products/search",
            method: "POST",
            description: "Retrieve all products from HubSpot",
            supported_actions: ["download", "sync"],
        },
        {
            id: "create-product",
            path: "/crm/v3/objects/products",
            method: "POST",
            description: "Create a new product in HubSpot",
            supported_actions: ["upload"],
        },
        // // Marketing Endpoints
        // {
        //   id: "marketing-emails",
        //   path: "/marketing/v3/emails",
        //   method: "GET",
        //   description: "Retrieve all marketing emails from HubSpot",
        //   supported_actions: ["download", "sync"],
        // },
        // {
        //   id: "create-marketing-email",
        //   path: "/marketing/v3/emails",
        //   method: "POST",
        //   description: "Create a new marketing email in HubSpot",
        //   supported_actions: ["upload"],
        // },
        // {
        //   id: "forms",
        //   path: "/forms/v2/forms",
        //   method: "GET",
        //   description: "Retrieve all forms from HubSpot",
        //   supported_actions: ["download", "sync"],
        // },
        // {
        //   id: "create-form",
        //   path: "/forms/v2/forms",
        //   method: "POST",
        //   description: "Create a new form in HubSpot",
        //   supported_actions: ["upload"],
        // },
        // Analytics Endpoints
        // {
        //   id: "analytics-events",
        //   path: "/events/v3/events",
        //   method: "GET",
        //   description: "Retrieve analytics events from HubSpot",
        //   supported_actions: ["download", "sync"],
        // },
        // // Engagements (Activities)
        // {
        //   id: "engagements",
        //   path: "/engagements/v1/engagements/paged",
        //   method: "GET",
        //   description: "Retrieve all engagements (notes, emails, calls, etc.)",
        //   supported_actions: ["download", "sync"],
        // },
        // {
        //   id: "create-engagement",
        //   path: "/engagements/v1/engagements",
        //   method: "POST",
        //   description: "Create a new engagement (e.g., note, email, call)",
        //   supported_actions: ["upload"],
        // },
        // Pipelines
        // {
        //   id: "pipelines",
        //   path: "/crm/v3/pipelines/deals",
        //   method: "GET",
        //   description: "Retrieve all deal pipelines from HubSpot",
        //   supported_actions: ["download", "sync"],
        // },
        // {
        //   id: "ticket-pipelines",
        //   path: "/crm/v3/pipelines/tickets",
        //   method: "GET",
        //   description: "Retrieve all ticket pipelines from HubSpot",
        //   supported_actions: ["download", "sync"],
        // },
        // Owners
        // {
        //   id: "owners",
        //   path: "/crm/v3/owners",
        //   method: "GET",
        //   description: "Retrieve all owners (users) in HubSpot",
        //   supported_actions: ["download", "sync"],
        // },
    ],
    helpers: {
        getCode: function (redirectUrl, client_id) {
            let result = `https://app.hubspot.com/oauth/authorize?client_id=${client_id}`;
            result += `&redirect_uri=${redirectUrl}`;
            result += '&scope=content%20business-intelligence%20oauth%20crm.objects.owners.read%20forms%20tickets%20crm.objects.contacts.write%20e-commerce%20crm.objects.companies.write%20crm.objects.companies.read%20crm.objects.deals.read%20crm.objects.deals.write%20crm.objects.contacts.read';
            return result;
        },
        getTokens: async function (redirectUrl, client_id, secret_id, code) {
            try {
                const tokenResponse = await (0, axios_1.default)({
                    method: 'post',
                    url: 'https://api.hubapi.com/oauth/v1/token',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded'
                    },
                    data: new URLSearchParams({
                        grant_type: 'authorization_code',
                        client_id: client_id,
                        client_secret: secret_id,
                        redirect_uri: redirectUrl,
                        code: code
                    })
                });
                return tokenResponse.data;
            }
            catch (error) {
                if ((0, axios_1.isAxiosError)(error)) {
                    return error.response?.data.message;
                }
                else {
                    return error instanceof Error ? error.message : 'Unknown error';
                }
            }
        }
    }
};
exports.HubSpotAdapter = HubSpotAdapter;
async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
function hubspot(connector, auth) {
    const log = function (...args) {
        if (connector.debug) {
            console.log(...arguments);
        }
    };
    const endpoint = HubSpotAdapter.endpoints.find(e => e.id === connector.endpoint_id);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in HubSpot adapter`);
    }
    function isOAuth2Auth(auth) {
        return auth.type === 'oauth2' && typeof auth.credentials === 'object';
    }
    async function refreshOAuthToken() {
        if (!isOAuth2Auth(auth)) {
            throw new Error("Not an OAuth2 authentication");
        }
        if (!auth.credentials.refresh_token) {
            throw new Error("Refresh token missing; obtain initial tokens manually and update vault");
        }
        log("Refreshing OAuth token...");
        try {
            const response = await axios_1.default.post('https://api.hubapi.com/oauth/v1/token', new URLSearchParams({
                grant_type: 'refresh_token',
                client_id: auth.credentials.client_id,
                client_secret: auth.credentials.client_secret,
                refresh_token: auth.credentials.refresh_token,
            }).toString(), { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } });
            auth.credentials.access_token = response.data.access_token;
            auth.credentials.refresh_token = response.data.refresh_token || auth.credentials.refresh_token;
            auth.expires_at = new Date(Date.now() + response.data.expires_in * 1000).toISOString();
            log("Token refreshed successfully");
        }
        catch (error) {
            let errorMessage;
            if ((0, axios_1.isAxiosError)(error)) {
                errorMessage = error.response?.data.message;
            }
            else {
                errorMessage = error instanceof Error ? error.message : 'Unknown error';
            }
            console.error("Token refresh failed:", errorMessage);
            throw new Error(`OAuth token refresh failed: ${errorMessage}`);
        }
    }
    async function buildRequestConfig() {
        if (!isOAuth2Auth(auth)) {
            throw new Error("HubSpot adapter requires OAuth2 authentication");
        }
        if (!auth.credentials.access_token || (auth.expires_at && new Date(auth.expires_at) < new Date())) {
            await refreshOAuthToken();
        }
        return {
            headers: {
                'Authorization': `Bearer ${auth.credentials.access_token}`,
                'Content-Type': 'application/json',
                ...connector.config?.headers,
            },
            params: {
                ...connector.config?.query_params,
            },
        };
    }
    function getSearchBody(limit, after) {
        const body = {};
        if (limit) {
            body.limit = limit;
        }
        if (after) {
            body.after = after;
        }
        if (connector.fields.length > 0)
            body.properties = connector.fields.join(',');
        if (connector.filters && connector.filters.length > 0) {
            body.filterGroups = [{
                    filters: connector.filters.map(filter => ({
                        propertyName: filter.field,
                        operator: mapOperator(filter.operator),
                        value: filter.value,
                    }))
                }];
        }
        if (connector.sort && connector.sort.length > 0) {
            body.sorts = connector.sort.map(sort => ({
                propertyName: sort.field,
                direction: sort.type === 'asc' ? 'ASCENDING' : 'DESCENDING',
            }));
        }
        return body;
    }
    function mapOperator(operator) {
        const operatorMap = {
            '=': 'EQ', '!=': 'NEQ', '>': 'GT', '>=': 'GTE', '<': 'LT', '<=': 'LTE',
            'contains': 'CONTAINS_TOKEN', 'not_contains': 'NOT_CONTAINS_TOKEN',
            'in': 'IN', 'not_in': 'NOT_IN', 'between': 'BETWEEN', 'not_between': 'NOT_BETWEEN',
            'is_null': 'IS_NULL', 'is_not_null': 'NOT_NULL',
        };
        return operatorMap[operator] || operator;
    }
    const download = async function (pageOptions) {
        const config = await buildRequestConfig();
        const { limit, offset } = pageOptions;
        if (typeof limit === 'undefined') {
            throw new Error('Number of items per page is required by the HubSpot adapter');
        }
        if (limit > maxItemsPerPage) {
            throw new Error('Number of items per page is greater than the maximum allowed by the HubSpot adapter');
        }
        const after = typeof offset === 'number' ? offset.toString() : offset;
        let response;
        if (endpoint.method === 'POST') {
            const body = getSearchBody(limit, after);
            response = await axios_1.default.post(`${HubSpotAdapter.base_url}${endpoint.path}`, body, config);
        }
        else {
            config.params.limit = limit;
            if (after) {
                config.params.after = after;
            }
            response = await axios_1.default.get(`${HubSpotAdapter.base_url}${endpoint.path}`, config);
        }
        log("API Response:", JSON.stringify(response.data, null, 2));
        const { paging, results } = response.data;
        if (!Array.isArray(results)) {
            console.warn("Results is not an array or is undefined:", response.data);
            return { data: [], options: { nextOffset: paging?.next?.after } };
        }
        let filteredResults;
        if (connector.fields.length > 0) {
            filteredResults = results.map((item) => {
                const filteredItem = {};
                connector.fields.forEach(field => {
                    if (item.properties && item.properties[field] !== undefined && item.properties[field] !== null) {
                        filteredItem[field] = item.properties[field];
                    }
                });
                log("Filtered Result:", JSON.stringify(filteredItem, null, 2));
                return filteredItem;
            });
        }
        else {
            filteredResults = results;
        }
        return {
            data: filteredResults,
            options: {
                nextOffset: paging?.next?.after ? paging.next.after : undefined,
            },
        };
    };
    const handleDownloadError = (error) => {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        if (error.response && typeof error.response.status === 'number') {
            const status = error.response.status;
            log('Error status:', status);
            console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
        }
        else {
            console.error("Download error:", errorMessage);
        }
        return new Error(`Download failed: ${errorMessage}`);
    };
    return {
        getConfig: () => {
            return HubSpotAdapter;
        },
        download: async function (pageOptions) {
            if (!endpoint.supported_actions.includes('download')) {
                throw new Error(`${endpoint.id} endpoint don't support download`);
            }
            try {
                return await download(pageOptions);
            }
            catch (error) {
                // Check for error with response structure
                if (error.response && typeof error.response.status === 'number') {
                    const status = error.response.status;
                    if (status === 401) {
                        log('Error status 401 detected, refreshing token');
                        await refreshOAuthToken();
                        log('Token refreshed, retrying');
                        try {
                            return await download(pageOptions);
                        }
                        catch (error) {
                            throw handleDownloadError(error);
                        }
                    }
                    else if (status === 429) {
                        const retryAfter = error.response.headers['retry-after'] ? parseInt(error.response.headers['retry-after'], 10) * 1000 : 1000;
                        log(`Rate limit hit, waiting ${retryAfter}ms`);
                        await delay(retryAfter);
                        log('Retrying download after delay');
                        try {
                            return await download(pageOptions);
                        }
                        catch (error) {
                            throw handleDownloadError(error);
                        }
                    }
                }
                throw handleDownloadError(error);
            }
        },
        upload: async function (data) {
            if (!endpoint.supported_actions.includes('upload')) {
                throw new Error(`${endpoint.id} endpoint don't support upload`);
            }
            const config = await buildRequestConfig();
            try {
                await axios_1.default.post(`${HubSpotAdapter.base_url}${endpoint.path}`, {
                    inputs: data,
                }, config);
            }
            catch (error) {
                let errorMessage;
                if (!(error instanceof Error)) {
                    errorMessage = 'Unknown error';
                }
                else if ((0, axios_1.isAxiosError)(error) && error.response?.data.message) {
                    errorMessage = error.response?.data.message;
                    error = new Error(errorMessage);
                }
                else {
                    errorMessage = error.message;
                }
                console.error("Upload error:", errorMessage);
                throw error;
            }
        },
    };
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
/******/ 	var __webpack_exports__ = __webpack_require__(920);
/******/ 	hubspot = __webpack_exports__;
/******/ 	
/******/ })()
;

    return hubspot;
})));