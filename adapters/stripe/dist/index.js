
if (! axios && typeof(require) === 'function') {
    var axios = require('axios');
}

;(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
    typeof define === 'function' && define.amd ? define(factory) :
    global.adapter = factory();
}(this, (function () {

var adapter;
/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ 156:
/***/ (function(__unused_webpack_module, exports, __webpack_require__) {


/**
 * Stripe Adapter for OpenETL
 * https://componade.com/openetl
 *
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
exports.StripeAdapter = void 0;
exports.appendObject = appendObject;
exports.stripe = stripe;
const axios_1 = __importStar(__webpack_require__(719));
const maxItemsPerPage = 100;
const StripeAdapter = {
    id: "stripe",
    name: "Stripe Payments Adapter",
    category: 'E-commerce & Payment Platforms',
    image: 'https://static.cdnlogo.com/logos/s/83/stripe.svg',
    type: "http",
    action: ["download", "upload", "sync"],
    credential_type: "api_key",
    base_url: "https://api.stripe.com/v1",
    metadata: {
        provider: "stripe",
        description: "Adapter for Stripe Payments API",
        version: "v1",
    },
    pagination: {
        type: 'cursor',
        maxItemsPerPage,
    },
    endpoints: [
        {
            id: "charges",
            path: "/charges/search",
            method: "GET",
            description: "Retrieve all charges from Stripe",
            supported_actions: ["download", "sync"],
            tool: 'stripe_search_charges',
            defaultFields: [
                'id',
                'currency',
                'amount',
                'customer',
                'description',
                'status',
                'paid',
                'refunded',
                'captured',
                'metadata',
                'created',
            ],
        },
        {
            id: "customers",
            path: "/customers/search",
            method: "GET",
            description: "Retrieve all customers from Stripe",
            supported_actions: ["download", "sync"],
            tool: 'stripe_search_customers',
            defaultFields: [
                'id',
                'name',
                'address',
                'email',
                'metadata',
                'phone',
                'currency',
                'balance',
                'tax_exempt',
                'delinquent',
                'created',
            ]
        },
        {
            id: "create-customer",
            path: "/customers",
            method: "POST",
            description: "Create a new customer in Stripe",
            supported_actions: ["upload"],
            tool: 'stripe_create_customers',
        },
        {
            id: "invoices",
            path: "/invoices/search",
            method: "GET",
            description: "Retrieve all invoices from Stripe",
            supported_actions: ["download", "sync"],
            tool: 'stripe_search_invoices',
            defaultFields: [
                'id',
                'customer',
                'status',
                'due_date',
                'currency',
                'amount_due',
                'amount_paid',
                'created',
                'invoice_pdf',
            ]
        },
        {
            id: "create-invoice",
            path: "/invoices",
            method: "POST",
            description: "Create a new invoice in Stripe",
            supported_actions: ["upload"],
            tool: 'stripe_create_invoices',
        },
        // {
        // 	id: "refunds",
        // 	path: "/refunds",
        // 	method: "GET",
        // 	description: "Retrieve all refunds from Stripe",
        // 	supported_actions: ["download", "sync"],
        // },
        // {
        // 	id: "create-refund",
        // 	path: "/refunds",
        // 	method: "POST",
        // 	description: "Create a new refund in Stripe",
        // 	supported_actions: ["upload"],
        // },
        // {
        // 	id: "payment_intents",
        // 	path: "/payment_intents/search",
        // 	method: "GET",
        // 	description: "Retrieve all payment intents from Stripe",
        // 	supported_actions: ["download", "sync"],
        // },
        // {
        // 	id: "create-payment-intent",
        // 	path: "/payment_intents",
        // 	method: "POST",
        // 	description: "Create a new payment intent in Stripe",
        // 	supported_actions: ["upload"],
        // },
        {
            id: "products",
            path: "/products/search",
            method: "GET",
            description: "Retrieve all products from Stripe",
            supported_actions: ["download", "sync"],
            tool: 'stripe_search_products',
            defaultFields: [
                'id',
                'name',
                'active',
                'description',
                'default_price',
                'images',
                'metadata',
                'created',
            ],
        },
        {
            id: "create-product",
            path: "/products",
            method: "POST",
            description: "Create a new product in Stripe",
            supported_actions: ["upload"],
            tool: 'stripe_create_products',
        },
        // {
        // 	id: "subscriptions",
        // 	path: "/subscriptions/search",
        // 	method: "GET",
        // 	description: "Retrieve all subscriptions from Stripe",
        // 	supported_actions: ["download", "sync"],
        // },
        // {
        // 	id: "create-subscription",
        // 	path: "/subscriptions",
        // 	method: "POST",
        // 	description: "Create a new subscription in Stripe",
        // 	supported_actions: ["upload"],
        // },
        // {
        // 	id: "prices",
        // 	path: "/prices/search",
        // 	method: "GET",
        // 	description: "Retrieve all prices from Stripe",
        // 	supported_actions: ["download", "sync"],
        // },
        // {
        // 	id: "create-price",
        // 	path: "/prices",
        // 	method: "POST",
        // 	description: "Create a new price in Stripe",
        // 	supported_actions: ["upload"],
        // },
    ],
};
exports.StripeAdapter = StripeAdapter;
async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
function appendObject(formData, propertyValue, propertyName) {
    if (Array.isArray(propertyValue)) {
        propertyValue.forEach((item, index) => {
            appendObject(formData, item, propertyName + `[${index}]`);
        });
    }
    else if (typeof propertyValue === "object") {
        if (propertyValue) {
            Object.entries(propertyValue).forEach(([key, value]) => {
                appendObject(formData, value, propertyName + `[${key}]`);
            });
        }
    }
    else {
        if (propertyValue !== undefined && propertyValue !== null) {
            formData.append(propertyName, String(propertyValue));
        }
    }
}
function stripe(connector, auth) {
    const endpoint = StripeAdapter.endpoints.find(e => e.id === connector.endpoint_id);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in Stripe adapter`);
    }
    if (auth.type !== 'api_key' || !auth.credentials.api_key) {
        throw new Error("Stripe adapter requires an API key for authentication");
    }
    const log = (...args) => {
        if (connector.debug)
            console.log(...args);
    };
    const getRequestHeaders = function () {
        return {
            'Authorization': `Bearer ${auth.credentials.api_key}`, // Type-safe after guard
            ...connector.config?.headers,
            'Stripe-Version': '2025-02-24.acacia',
        };
    };
    function buildDownloadRequestConfig(pageOptions) {
        if (auth.type !== 'api_key' || !auth.credentials.api_key) {
            throw new Error("Stripe adapter requires an API key for authentication");
        }
        let isSearchEndpoint = endpoint.path.endsWith('/search');
        const params = {
            ...(isSearchEndpoint ? getSearchQueryParams() : getListQueryParams()),
            ...connector.config?.query_params,
        };
        if (!params.query) {
            isSearchEndpoint = false;
        }
        if (pageOptions?.limit) {
            params.limit = pageOptions.limit;
        }
        if (pageOptions?.offset) {
            params[isSearchEndpoint ? 'page' : 'starting_after'] = pageOptions.offset;
        }
        const config = {
            headers: getRequestHeaders(),
            params,
        };
        log("Request config:", JSON.stringify(config, null, 2));
        return {
            isSearchEndpoint,
            config
        };
    }
    const buildUploadRequestConfig = function (pageOptions) {
        if (auth.type !== 'api_key' || !auth.credentials.api_key) {
            throw new Error("Stripe adapter requires an API key for authentication");
        }
        const config = {
            headers: getRequestHeaders(),
        };
        log("Request config:", JSON.stringify(config, null, 2));
        return config;
    };
    function getListQueryParams() {
        const params = {};
        if (connector.filters && connector.filters.length > 0) {
            connector.filters.forEach(filter => {
                if ('field' in filter && 'value' in filter) {
                    params[filter.field] = filter.value;
                }
            });
        }
        return params;
    }
    function getSearchQueryParams() {
        if (connector.filters && connector.filters.length > 0) {
            let queries = [];
            connector.filters.forEach(filter => {
                if ('field' in filter && 'value' in filter) {
                    const value = typeof filter.value === 'string'
                        ? `"${filter.value.replace(/"/g, '\"')}"`
                        : filter.value;
                    if (filter.operator === '!=') {
                        queries.push(`-${filter.field}:${value}`);
                    }
                    else {
                        let operator = filter.operator === '=' ? ':' : filter.operator;
                        queries.push(filter.field + operator + value);
                    }
                }
            });
            if (queries.length > 0) {
                return {
                    query: queries.join(' AND '),
                };
            }
        }
        return {};
    }
    const download = async function (pageOptions) {
        if (typeof pageOptions.limit === 'undefined') {
            throw new Error('Number of items per page is required by the Stripe adapter');
        }
        if (pageOptions.limit > maxItemsPerPage) {
            throw new Error('Number of items per page exceeds Stripe maximum');
        }
        const { config, isSearchEndpoint } = buildDownloadRequestConfig(pageOptions);
        try {
            let url = `${StripeAdapter.base_url}${endpoint.path}`;
            if (url.includes('/search') && !isSearchEndpoint) {
                url = url.replace('/search', '');
            }
            const response = await axios_1.default.get(url, config);
            const { data, has_more } = response.data;
            log("API Response:", JSON.stringify(response.data, null, 2));
            if (!Array.isArray(data)) {
                console.warn("Data is not an array or is undefined:", response.data);
                return { data: [], options: { nextOffset: undefined } };
            }
            let filteredResults = connector.fields.length > 0
                ? data.map((item) => {
                    const filteredItem = {};
                    connector.fields.forEach(field => {
                        if (item[field] !== undefined && item[field] !== null) {
                            filteredItem[field] = item[field];
                        }
                    });
                    return filteredItem;
                })
                : data;
            // Set nextOffset for cursor pagination
            let nextOffset;
            if (has_more) {
                if (isSearchEndpoint) {
                    nextOffset = response.data.next_page;
                }
                else if (data.length > 0) {
                    nextOffset = data[data.length - 1].id;
                }
            }
            return {
                data: filteredResults,
                options: { nextOffset }
            };
        }
        catch (error) {
            throw handleDownloadError(error);
        }
    };
    const handleDownloadError = (error) => {
        let errorMessage;
        if ((0, axios_1.isAxiosError)(error)) {
            errorMessage = error.response?.data?.error?.message || 'Unknown error';
        }
        else {
            errorMessage = error instanceof Error ? error.message : 'Unknown error';
        }
        if (error.response && typeof error.response.status === 'number') {
            console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
        }
        throw new Error(`Download failed: ${errorMessage}`);
    };
    return {
        getConfig: () => {
            return StripeAdapter;
        },
        download: async function (pageOptions) {
            if (!endpoint.supported_actions.includes('download')) {
                throw new Error(`${endpoint.id} endpoint doesn't support download`);
            }
            try {
                return await download(pageOptions);
            }
            catch (error) {
                if (error.response?.status === 401) {
                    throw new Error("Invalid API key; please check your credentials");
                }
                else if (error.response?.status === 429) {
                    const retryAfter = error.response.headers['retry-after']
                        ? parseInt(error.response.headers['retry-after'], 10) * 1000
                        : 1000;
                    log(`Rate limit hit, waiting ${retryAfter}ms`);
                    await delay(retryAfter);
                    return await download(pageOptions);
                }
                throw handleDownloadError(error);
            }
        },
        upload: async function (data) {
            if (!endpoint.supported_actions.includes('upload')) {
                throw new Error(`${endpoint.id} endpoint doesn't support upload`);
            }
            if (data.length !== 1) {
                throw new Error('Stripe adapter only supports uploading one product at a time');
            }
            const config = buildUploadRequestConfig();
            try {
                const formData = new URLSearchParams();
                const item = data[0];
                Object.entries(item).forEach(([key, value]) => {
                    const valueType = typeof value;
                    if (valueType === "object") {
                        appendObject(formData, value, key);
                    }
                    else if (valueType === "string") {
                        formData.append(key, value);
                    }
                    else if (valueType === "number" || valueType === "boolean") {
                        formData.append(key, String(value));
                    }
                });
                const response = await axios_1.default.post(`${StripeAdapter.base_url}${endpoint.path}`, formData.toString(), {
                    ...config,
                    headers: {
                        ...config.headers,
                        'Content-Type': 'application/x-www-form-urlencoded',
                    },
                });
                log("Upload successful:", JSON.stringify(response.data, null, 2));
            }
            catch (error) {
                const errorMessage = (0, axios_1.isAxiosError)(error) && error.response?.data?.error?.message
                    ? error.response.data.error.message
                    : error instanceof Error ? error.message : 'Unknown error';
                console.error("Upload error:", errorMessage);
                throw new Error(`Upload failed: ${errorMessage}`);
            }
        },
    };
}


/***/ }),

/***/ 719:
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
/******/ 	var __webpack_exports__ = __webpack_require__(156);
/******/ 	adapter = __webpack_exports__;
/******/ 	
/******/ })()
;

    return adapter;
})));