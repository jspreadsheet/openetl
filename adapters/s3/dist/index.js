
if (! s3AwsSdk && typeof(require) === 'function') {
    var s3AwsSdk = require('@aws-sdk/client-s3');
}

;(function (global, factory) {
    typeof exports === 'object' && typeof module !== 'undefined' ? module.exports = factory() :
    typeof define === 'function' && define.amd ? define(factory) :
    global.s3 = factory();
}(this, (function () {

var s3;
/******/ (() => { // webpackBootstrap
/******/ 	"use strict";
/******/ 	var __webpack_modules__ = ({

/***/ 728:
/***/ ((module) => {

module.exports = s3AwsSdk;

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
/******/ 		__webpack_modules__[moduleId](module, module.exports, __webpack_require__);
/******/ 	
/******/ 		// Return the exports of the module
/******/ 		return module.exports;
/******/ 	}
/******/ 	
/************************************************************************/
var __webpack_exports__ = {};
// This entry need to be wrapped in an IIFE because it need to be isolated against other modules in the chunk.
(() => {
var exports = __webpack_exports__;

/**
 * Amazon S3 Adapter for OpenETL
 * https://componade.com/openetl
 */
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.S3Adapter = void 0;
exports.s3 = s3;
const client_s3_1 = __webpack_require__(728);
const maxItemsPerPage = 1000;
const S3Adapter = {
    id: "s3-adapter",
    name: "Amazon S3 Adapter",
    type: "http",
    action: ["download", "upload", "sync"],
    credential_type: "api_key",
    base_url: "https://s3.amazonaws.com", // Base URL genérica, ajustada por região
    config: [
        {
            name: 'bucket',
            required: true,
        },
    ],
    metadata: {
        provider: "aws",
        description: "Adapter for Amazon S3 storage service",
        version: "v3", // Usando AWS SDK v3
    },
    endpoints: [
        {
            id: "list-objects",
            path: "",
            method: "GET",
            description: "List objects in an S3 bucket",
            supported_actions: ["download", "sync"],
            settings: {
                pagination: {
                    type: 'cursor',
                    maxItemsPerPage,
                }
            }
        },
        {
            id: "download-object",
            path: "",
            method: "GET",
            description: "Download a specific object from S3",
            supported_actions: ["download"],
            settings: {
                pagination: false,
            }
        },
        {
            id: "upload-object",
            path: "",
            method: "PUT",
            description: "Upload an object to S3",
            supported_actions: ["upload"],
            settings: {
                pagination: {
                    type: 'offset',
                    maxItemsPerPage: 1,
                },
            }
        },
    ],
};
exports.S3Adapter = S3Adapter;
function isAWSAuth(auth) {
    return auth.type === 'api_key' &&
        auth.credentials &&
        typeof auth.credentials.api_key === 'string' &&
        typeof auth.credentials.api_secret === 'string' &&
        typeof auth.credentials.region === 'string';
}
function s3(connector, auth) {
    const log = (...args) => {
        if (connector.debug) {
            console.log(...args);
        }
    };
    const endpoint = S3Adapter.endpoints.find(e => e.id === connector.endpoint_id);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in S3 adapter`);
    }
    if (!isAWSAuth(auth)) {
        throw new Error("S3 adapter requires AWS authentication with api_key, api_secret and region");
    }
    const bucket = connector.config?.bucket;
    if (!bucket) {
        throw new Error("Bucket name must be specified in connector config");
    }
    const s3Client = new client_s3_1.S3Client({
        region: auth.credentials.region,
        credentials: {
            accessKeyId: auth.credentials.api_key,
            secretAccessKey: auth.credentials.api_secret,
        }
    });
    const download = async function (pageOptions) {
        const { limit, offset } = pageOptions;
        if (endpoint.id === "list-objects") {
            if (typeof limit === 'undefined') {
                throw new Error(`Number of items per page is required by the ${endpoint.id} endpoint of the S3 adapter`);
            }
            if (limit > maxItemsPerPage) {
                throw new Error(`Number of items per page exceeds the maximum allowed by the ${endpoint.id} endpoint of the S3 adapter (${maxItemsPerPage})`);
            }
            const prefix = connector.filters?.find(f => 'field' in f && f.field === 'prefix' && f.operator === '=')?.value;
            if (typeof prefix === 'number') {
                throw new Error('The "prefix" filter, if defined, must be a string');
            }
            const command = new client_s3_1.ListObjectsV2Command({
                Bucket: bucket,
                MaxKeys: limit,
                ContinuationToken: offset ? String(offset) : undefined,
                Prefix: prefix,
            });
            const response = await s3Client.send(command);
            const results = response.Contents?.map(obj => ({
                key: obj.Key,
                size: obj.Size,
                lastModified: obj.LastModified?.toISOString(),
                eTag: obj.ETag,
            })) || [];
            return {
                data: results,
                options: {
                    nextOffset: response.IsTruncated ? response.NextContinuationToken : undefined,
                },
            };
        }
        const key = connector.config?.id;
        if (typeof key !== 'string') {
            throw new Error('For the download-object endpoint, the id config is required');
        }
        const command = new client_s3_1.GetObjectCommand({
            Bucket: bucket,
            Key: key,
        });
        const response = await s3Client.send(command);
        const body = response.Body;
        const chunks = [];
        for await (const chunk of body) {
            chunks.push(Buffer.from(chunk));
        }
        const content = Buffer.concat(chunks);
        return {
            data: [{ key, content }],
        };
    };
    return {
        getConfig: () => S3Adapter,
        download: async function (pageOptions) {
            if (!endpoint.supported_actions.includes('download')) {
                throw new Error(`${endpoint.id} endpoint does not support download`);
            }
            try {
                return await download(pageOptions);
            }
            catch (error) {
                const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                console.error("Download error:", errorMessage);
                throw new Error(`Download failed: ${errorMessage}`);
            }
        },
        upload: async function (data) {
            if (!endpoint.supported_actions.includes('upload')) {
                throw new Error(`${endpoint.id} endpoint does not support upload`);
            }
            try {
                const { key, content, contentType } = data[0];
                if (!key) {
                    throw new Error("key must be specified in data");
                }
                if (!content) {
                    throw new Error("content must be specified in data");
                }
                if (!contentType) {
                    throw new Error("contentType must be specified in data");
                }
                await s3Client.send(new client_s3_1.PutObjectCommand({
                    Bucket: bucket,
                    Key: key,
                    Body: content,
                    ContentType: contentType,
                }));
                log(`Uploaded object: ${key}`);
            }
            catch (error) {
                const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                console.error("Upload error:", errorMessage);
                throw new Error(`Upload failed: ${errorMessage}`);
            }
        },
    };
}

})();

s3 = __webpack_exports__;
/******/ })()
;

    return s3;
})));