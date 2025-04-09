
if (! axios && typeof(require) === 'function') {
    var axios = require('axios');
}

;(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
    typeof define === 'function' && define.amd ? define(factory) :
    global.xero = factory();
}(this, (function () {

var xero;
/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ 920:
/***/ (function(__unused_webpack_module, exports, __webpack_require__) {


/**
 * Xero Adapter for OpenETL
 * https://componade.com/openetl
 *
 * Xero API Reference: https://developer.xero.com/documentation/
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
exports.XeroAdapter = void 0;
exports.xero = xero;
const axios_1 = __importStar(__webpack_require__(467));
const maxItemsPerPage = 100;
const XeroAdapter = {
    id: "xero",
    name: "Xero Accounting Adapter",
    type: "http",
    category: 'Accounting & Finance',
    image: 'https://www.xero.com/content/dam/xero-refresh/global/logos/xero-logo-blue.svg', // Example logo URL
    action: ["download", "upload", "sync"],
    credential_type: "oauth2",
    base_url: "https://api.xero.com/api.xro/2.0",
    metadata: {
        provider: "xero",
        description: "Adapter for Xero Accounting API",
        version: "1.0", // Current stable API version as of 2025
    },
    endpoints: [
        // Core Accounting Endpoints
        {
            id: "contacts",
            path: "/Contacts",
            method: "GET",
            description: "Retrieve all contacts from Xero",
            supported_actions: ["download", "sync"],
            settings: {
                pagination: {
                    type: 'offset',
                    maxItemsPerPage,
                },
            }
        },
        {
            id: "create-contact",
            path: "/Contacts",
            method: "POST",
            description: "Create a new contact in Xero",
            supported_actions: ["upload"],
        },
        {
            id: "invoices",
            path: "/Invoices",
            method: "GET",
            description: "Retrieve all invoices from Xero",
            supported_actions: ["download", "sync"],
            settings: {
                pagination: {
                    type: 'offset',
                    maxItemsPerPage,
                },
            }
        },
        {
            id: "create-invoice",
            path: "/Invoices",
            method: "POST",
            description: "Create a new invoice in Xero",
            supported_actions: ["upload"],
        },
        {
            id: "accounts",
            path: "/Accounts",
            method: "GET",
            description: "Retrieve all accounts from Xero",
            supported_actions: ["download", "sync"],
        },
        {
            id: "create-account",
            path: "/Accounts",
            method: "POST",
            description: "Create a new account in Xero",
            supported_actions: ["upload"],
        },
    ],
    helpers: {
        getCode: function (redirectUrl, client_id) {
            let result = `https://login.xero.com/identity/connect/authorize?client_id=${client_id}`;
            result += `&redirect_uri=${encodeURIComponent(redirectUrl)}`;
            result += '&response_type=code';
            result += `&scope=${encodeURIComponent('offline_access accounting.contacts accounting.transactions accounting.settings')}`;
            return result;
        },
        getTokens: async function (redirectUrl, client_id, secret_id, queryParams) {
            const params = new URLSearchParams(queryParams);
            const code = params.get('code');
            if (!code) {
                throw new Error('Invalid authentication');
            }
            try {
                const tokenResponse = await (0, axios_1.default)({
                    method: 'post',
                    url: 'https://identity.xero.com/connect/token',
                    headers: {
                        'Content-Type': 'application/x-www-form-urlencoded',
                        'Authorization': 'Basic ' + Buffer.from(`${client_id}:${secret_id}`).toString('base64'),
                    },
                    data: new URLSearchParams({
                        grant_type: 'authorization_code',
                        code: code,
                        redirect_uri: redirectUrl,
                    }),
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
        },
    },
};
exports.XeroAdapter = XeroAdapter;
async function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
function xero(connector, auth) {
    const log = function (...args) {
        if (connector.debug) {
            console.log(...args);
        }
    };
    const endpoint = XeroAdapter.endpoints.find(e => e.id === connector.endpoint_id);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in Xero adapter`);
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
            const response = await axios_1.default.post('https://identity.xero.com/connect/token', new URLSearchParams({
                grant_type: 'refresh_token',
                refresh_token: auth.credentials.refresh_token,
            }).toString(), {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': 'Basic ' + Buffer.from(`${auth.credentials.client_id}:${auth.credentials.client_secret}`).toString('base64'),
                },
            });
            auth.credentials.access_token = response.data.access_token;
            auth.credentials.refresh_token = response.data.refresh_token || auth.credentials.refresh_token;
            console.log('novo refresh token');
            console.log(auth.credentials.refresh_token);
            auth.expires_at = new Date(Date.now() + response.data.expires_in * 1000).toISOString();
            log("Token refreshed successfully");
        }
        catch (error) {
            let errorMessage;
            if ((0, axios_1.isAxiosError)(error)) {
                errorMessage = error.response?.data.error;
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
            throw new Error("Xero adapter requires OAuth2 authentication");
        }
        if (!auth.credentials.access_token || !auth.expires_at || new Date(auth.expires_at) < new Date()) {
            await refreshOAuthToken();
        }
        const tenantId = await getTenantId();
        return {
            headers: {
                'Authorization': `Bearer ${auth.credentials.access_token}`,
                'Accept': 'application/json',
                'xero-tenant-id': tenantId,
                ...connector.config?.headers,
            },
            params: {
                ...connector.config?.query_params,
            },
        };
    }
    const getTenantId = async function () {
        const organisationName = connector.config?.organisationName;
        if (!organisationName) {
            throw new Error('An organisationName is required to use Xero adapter endpoints');
        }
        const { data } = await axios_1.default.get('https://api.xero.com/connections', {
            headers: {
                'Authorization': `Bearer ${auth.credentials.access_token}`
            }
        });
        const targetConnection = data.find((connection) => connection.tenantName === organisationName &&
            connection.tenantType === 'ORGANISATION');
        if (!targetConnection) {
            const tenantNames = data
                .map((connection) => '"' + connection.tenantName + '"')
                .filter((tenantName) => Boolean(tenantName));
            if (tenantNames.length === 0) {
                throw new Error('The Xero adapter was unable to access any of the organizations. Please review your connection settings.');
            }
            throw new Error(`The Xero adapter does not have access to an organization named "${organisationName}". Please use a connection that does have access to this organization, or use one of the organizations available for this connection: ${tenantNames.join(', ')}`);
        }
        return targetConnection.tenantId;
    };
    function getQueryParams(limit, offset) {
        const params = {};
        if (endpoint.settings?.pagination) {
            if (typeof limit === 'undefined') {
                throw new Error('Number of items per page is required by the Xero adapter');
            }
            if (limit > maxItemsPerPage) {
                throw new Error('Number of items per page is greater than the maximum allowed by the Xero adapter');
            }
            if (typeof offset === 'string') {
                throw new Error('Download endpoints of the Xero adapter don\'t accept a string as offset');
            }
            params.page = Math.floor((Number(offset || 0) / limit) + 1);
            params.pageSize = limit;
        }
        if (connector.filters && connector.filters.length > 0) {
            params.where = connector.filters.map(f => `${f.field}${f.operator === '=' ? '==' : f.operator}'${f.value}'`).join(' AND ');
        }
        return params;
    }
    const download = async function (pageOptions) {
        const config = await buildRequestConfig();
        const { limit, offset } = pageOptions;
        config.params = { ...config.params, ...getQueryParams(limit, offset) };
        const response = await axios_1.default.get(`${XeroAdapter.base_url}${endpoint.path}`, config);
        log("API Response:", JSON.stringify(response.data, null, 2));
        const results = response.data[endpoint.path.split('/')[1]] || [];
        let filteredResults = results;
        if (connector.fields.length > 0) {
            filteredResults = results.map((item) => {
                const filteredItem = {};
                connector.fields.forEach(field => {
                    if (item[field] !== undefined && item[field] !== null) {
                        filteredItem[field] = item[field];
                    }
                });
                return filteredItem;
            });
        }
        return {
            data: filteredResults
        };
    };
    return {
        getConfig: () => XeroAdapter,
        download: async function (pageOptions) {
            if (!endpoint.supported_actions.includes('download')) {
                throw new Error(`${endpoint.id} endpoint doesn't support download`);
            }
            try {
                return await download(pageOptions);
            }
            catch (error) {
                if ((0, axios_1.isAxiosError)(error) && error.response) {
                    const status = error.response.status;
                    if (status === 401) {
                        log('Error status 401 detected, refreshing token');
                        await refreshOAuthToken();
                        return await download(pageOptions);
                    }
                    else if (status === 429) {
                        const retryAfter = error.response.headers['retry-after'] ? parseInt(error.response.headers['retry-after'], 10) * 1000 : 1000;
                        log(`Rate limit hit, waiting ${retryAfter}ms`);
                        await delay(retryAfter);
                        return await download(pageOptions);
                    }
                }
                const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                console.error("Download error:", errorMessage);
                throw new Error(`Download failed: ${errorMessage}`);
            }
        },
        upload: async function (data) {
            if (!endpoint.supported_actions.includes('upload')) {
                throw new Error(`${endpoint.id} endpoint doesn't support upload`);
            }
            const config = await buildRequestConfig();
            try {
                await axios_1.default.post(`${XeroAdapter.base_url}${endpoint.path}`, { [endpoint.path.split('/')[1]]: data }, config);
            }
            catch (error) {
                let errorMessage;
                if ((0, axios_1.isAxiosError)(error)) {
                    if (error.response?.data.Type === 'ValidationException') {
                        errorMessage = error.response?.data.Elements[0].ValidationErrors[0].Message;
                    }
                    else if (error.response?.data.Type === 'PostDataInvalidException') {
                        errorMessage = error.response?.data.Message;
                    }
                }
                if (!errorMessage) {
                    errorMessage = error instanceof Error ? error.message : 'Unknown error';
                }
                console.error("Upload error:", errorMessage);
                throw new Error(`Upload failed: ${errorMessage}`);
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
/******/ 	xero = __webpack_exports__;
/******/ 	
/******/ })()
;

    return xero;
})));