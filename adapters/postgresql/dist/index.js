
if (! pg && typeof(require) === 'function') {
    var pg = require('pg');
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

/***/ 920:
/***/ (function(__unused_webpack_module, exports, __webpack_require__) {


/**
 * HubSpot Adapter for OpenETL
 * https://componade.com/openetl
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", ({ value: true }));
exports.PostgresqlAdapter = void 0;
exports.postgresql = postgresql;
const pg_1 = __importDefault(__webpack_require__(645)); // Use named imports
const { Pool } = pg_1.default;
const PostgresqlAdapter = {
    id: "postgres",
    name: "PostgreSQL Database Adapter",
    type: "database",
    action: ["download", "upload", "sync"],
    config: [
        {
            name: 'schema',
            required: false,
            default: 'public',
        },
        {
            name: 'table',
            required: true,
        },
        {
            name: 'custom_query',
            required: false,
        },
    ],
    credential_type: "basic",
    metadata: {
        provider: "postgresql",
        description: "Adapter for PostgreSQL database operations",
        version: "1.0",
    },
    endpoints: [
        { id: "table_query", query_type: "table", description: "Query a specific table", supported_actions: ["download", "sync"] },
        {
            id: "custom_query",
            query_type: "custom",
            description: "Run a custom SQL query",
            supported_actions: ["download"],
            settings: {
                pagination: false,
            }
        },
        { id: "table_insert", query_type: "table", description: "Insert into a specific table", supported_actions: ["upload"] },
    ],
    pagination: {
        type: 'offset',
    }
};
exports.PostgresqlAdapter = PostgresqlAdapter;
function postgresql(connector, auth) {
    const log = function (...args) {
        if (connector.debug) {
            console.log(...arguments);
        }
    };
    const endpoint = PostgresqlAdapter.endpoints.find(e => e.id === connector.endpoint_id);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in PostgreSQL adapter`);
    }
    function isBasicAuth(auth) {
        return auth.type === 'basic';
    }
    function isFilter(filter) {
        return 'field' in filter && 'operator' in filter && 'value' in filter;
    }
    let pool;
    function buildSelectQuery(customLimit, customOffset) {
        if (endpoint.id === "custom_query" && connector.config?.custom_query) {
            return connector.config.custom_query;
        }
        if (!connector.config?.schema || !connector.config?.table) {
            throw new Error("Schema and table required for table-based endpoints");
        }
        const parts = [];
        // SELECT clause
        parts.push(`SELECT ${connector.fields.length > 0 ? connector.fields.join(', ') : '*'}`);
        // FROM clause
        parts.push(`FROM "${connector.config.schema}"."${connector.config.table}"`);
        // WHERE clause
        if (connector.filters && connector.filters.length > 0) {
            const whereClauses = connector.filters.map(filter => {
                if (!isFilter(filter)) {
                    const subClauses = filter.filters.map(f => isFilter(f) ? `${f.field} ${f.operator} '${f.value}'` : '');
                    return `(${subClauses.join(` ${filter.op} `)})`;
                }
                return `${filter.field} ${filter.operator} '${filter.value}'`;
            });
            parts.push(`WHERE ${whereClauses.join(' AND ')}`);
        }
        // ORDER BY clause
        if (connector.sort && connector.sort.length > 0) {
            const orderBy = connector.sort
                .map(sort => `${sort.field} ${sort.type.toUpperCase()}`)
                .join(', ');
            parts.push(`ORDER BY ${orderBy}`);
        }
        // LIMIT and OFFSET
        if (customLimit !== undefined) {
            parts.push(`LIMIT ${customLimit}`);
            if (customOffset !== undefined) {
                parts.push(`OFFSET ${customOffset}`);
            }
        }
        return parts.join(' ');
    }
    function buildInsertQuery(data) {
        if (!connector.config?.schema || !connector.config?.table) {
            throw new Error("Schema and table required for table_insert endpoint");
        }
        const schema = connector.config.schema;
        const table = connector.config.table;
        const fields = connector.fields.length > 0 ? connector.fields : Object.keys(data[0]);
        const values = data.map(row => {
            const rowValues = fields.map(field => {
                const value = row[field];
                if (value === null || typeof value === 'undefined') {
                    return 'NULL';
                }
                if (typeof value === 'number') {
                    return value.toString();
                }
                return `'${value.toString().replace(/'/g, "''")}'`;
            });
            return `(${rowValues.join(', ')})`;
        });
        return `INSERT INTO "${schema}"."${table}" (${fields.map(f => `"${f}"`).join(', ')}) VALUES ${values.join(', ')}`;
    }
    return {
        getConfig: function () {
            return PostgresqlAdapter;
        },
        connect: async function () {
            if (!isBasicAuth(auth)) {
                throw new Error("PostgreSQL adapter requires basic authentication");
            }
            // Define default config with port as a number
            const defaultConfig = {
                host: 'localhost',
                database: 'postgres',
                port: 5432, // Changed from "5432" (string) to 5432 (number)
            };
            // Construct config with port always as a number
            const config = {
                user: auth.credentials.username,
                password: auth.credentials.password,
                host: auth.credentials.host || defaultConfig.host,
                database: auth.credentials.database || defaultConfig.database,
                port: auth.credentials.port !== undefined
                    ? parseInt(auth.credentials.port.toString(), 10)
                    : defaultConfig.port,
            };
            pool = new Pool(config);
            try {
                log("Testing connection to PostgreSQL...");
                const client = await pool.connect();
                await client.query('SELECT 1');
                client.release();
                log("Connection successful");
            }
            catch (error) {
                console.error("Connection test failed:", error.message);
                throw new Error(`Failed to connect to PostgreSQL: ${error.message}`);
            }
        },
        disconnect: async function () {
            try {
                log("Closing PostgreSQL connection pool...");
                await pool.end();
                log("Pool closed successfully");
            }
            catch (error) {
                console.error("Error closing pool:", error.message);
                throw error;
            }
        },
        download: async function (pageOptions) {
            if (endpoint.id === "table_insert") {
                throw new Error("Table_insert endpoint only supported for upload");
            }
            if (typeof pageOptions.offset === 'string') {
                throw new Error('table_query and custom_query endpoints of the PostgreSQL adapter don\'t accept a string as offset');
            }
            const offset = typeof pageOptions.offset === 'string' ? parseInt(pageOptions.offset) : pageOptions.offset;
            const query = buildSelectQuery(pageOptions.limit, pageOptions.offset);
            log("Executing query:", query);
            try {
                const result = await pool.query(query);
                log("Downloaded rows:", result.rows.length);
                return {
                    data: result.rows
                };
            }
            catch (error) {
                console.error("Download error:", error.message);
                throw error;
            }
        },
        upload: async function (data) {
            if (endpoint.id !== "table_insert") {
                throw new Error("Upload only supported for table_insert endpoint");
            }
            const query = buildInsertQuery(data);
            log("Executing insert:", query);
            try {
                await pool.query(query);
                log("Uploaded rows:", data.length);
            }
            catch (error) {
                console.error("Upload error:", error.message);
                throw error;
            }
        }
    };
}


/***/ }),

/***/ 645:
/***/ ((module) => {

module.exports = pg;

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
/******/ 	adapter = __webpack_exports__;
/******/ 	
/******/ })()
;

    return adapter;
})));