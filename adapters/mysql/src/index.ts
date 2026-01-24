/**
 * MySQL Adapter for OpenETL
 * https://componade.com/openetl
 */

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, BasicAuth } from 'openetl';
import mysql, { RowDataPacket } from 'mysql2/promise';

// SQL Security Functions
const ALLOWED_OPERATORS = new Set([
    '=', '!=', '<>', '<', '>', '<=', '>=',
    'LIKE', 'NOT LIKE',
    'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL',
    'BETWEEN', 'NOT BETWEEN'
]);

/**
 * Escapes a MySQL identifier (table name, column name, database name)
 * by wrapping in backticks and escaping any existing backticks
 */
function escapeIdentifier(identifier: string): string {
    if (!identifier || typeof identifier !== 'string') {
        throw new Error('Invalid identifier: must be a non-empty string');
    }
    // Check for dangerous characters that shouldn't be in identifiers
    if (/[\x00-\x1f\x7f]/.test(identifier)) {
        throw new Error('Invalid identifier: contains control characters');
    }
    return `\`${identifier.replace(/`/g, '``')}\``;
}

/**
 * Validates that an operator is in the allowed list
 */
function validateOperator(operator: string): string {
    const normalized = operator.trim().toUpperCase();
    if (!ALLOWED_OPERATORS.has(normalized)) {
        throw new Error(`Invalid SQL operator: '${operator}'`);
    }
    return normalized;
}

/**
 * Validates sort direction
 */
function validateSortDirection(direction: string): 'ASC' | 'DESC' {
    const normalized = direction.trim().toUpperCase();
    if (normalized !== 'ASC' && normalized !== 'DESC') {
        throw new Error(`Invalid sort direction: '${direction}'`);
    }
    return normalized;
}

const MySQLAdapter: DatabaseAdapter = {
    id: "mysql",
    name: "MySQL Database Adapter",
    category: 'Databases & Data Warehouses',
    image: "https://static.cdnlogo.com/logos/m/10/mysql.svg",
    type: "database",
    action: ["download", "upload", "sync"],
    credential_type: "basic",
    metadata: {
        provider: "mysql",
        description: "Adapter for MySQL database operations",
        version: "1.0",
    },
    endpoints: [
        {
            id: "table_query",
            query_type: "table",
            description: "Query a specific table",
            supported_actions: ["download", "sync"],
            settings: {
                config: [
                    {
                        id: 'table',
                        name: 'table',
                        required: true,
                    },
                ]
            },
            tool: 'database_query',
        },
        {
            id: "custom_query",
            query_type: "custom",
            description: "Run a custom SQL query",
            supported_actions: ["download"],
            settings: {
                pagination: false,
                config: [
                    {
                        id: 'custom_query',
                        name: 'custom_query',
                        required: true,
                    }
                ]
            }
        },
        {
            id: "table_insert",
            query_type: "table",
            description: "Insert into a specific table",
            supported_actions: ["upload"],
            settings: {
                config: [
                    {
                        id: 'table',
                        name: 'table',
                        required: true,
                    },
                ],
            },
            tool: 'database_create',
        },
        {
            id: "table_columns",
            query_type: "table",
            description: "Query the columns of a specific table",
            supported_actions: ["download"],
            settings: {
                pagination: false,
                config: [
                    {
                        id: 'table',
                        name: 'table',
                        required: true,
                    },
                ],
            },
            tool: 'table_columns',
        },
    ],
    pagination: {
        type: 'offset',
    }
};

function mysqlAdapter(connector: Connector, auth: AuthConfig): AdapterInstance {
    const log = function (...args: any) {
        if (connector.debug) {
            console.log(...arguments);
        }
    };
    const endpoint = MySQLAdapter.endpoints.find(e => e.id === connector.endpoint_id)!;
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in MySQL adapter`);
    }

    function isBasicAuth(auth: AuthConfig): auth is BasicAuth {
        return auth.type === 'basic';
    }

    let connection: mysql.Connection;

    type PageOptions = {
        limit?: number | undefined;
        offset?: string | number | undefined;
    };

    function buildGetColumnsQuery(pageOptions: PageOptions): { sql: string; values: any[] } {
        if (!connector.config?.table) {
            throw new Error(`table property is required on the MySQL adapter's ${endpoint.id} endpoint`)
        }

        return {
            sql: `SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?`,
            values: [(auth as BasicAuth).credentials.database, connector.config.table]
        };
    }

    function buildCustomQuery(pageOptions: PageOptions): { sql: string; values: any[] } {
        if (!connector.config?.custom_query) {
            throw new Error(`custom_query property is required on the MySQL adapter's ${endpoint.id} endpoint`)
        }

        // Note: Custom queries are user-provided and should be used carefully
        return { sql: connector.config.custom_query, values: [] };
    }

    function buildSelectQuery(pageOptions: PageOptions): { sql: string; values: any[] } {
        if (!connector.config?.table) {
            throw new Error(`table property is required on the MySQL adapter's ${endpoint.id} endpoint`)
        }

        const parts: string[] = [];
        const values: any[] = [];

        // SELECT clause - escape all field identifiers
        const escapedFields = connector.fields.length > 0
            ? connector.fields.map(f => escapeIdentifier(f)).join(', ')
            : '*';
        parts.push(`SELECT ${escapedFields}`);

        // FROM clause - escape table identifier
        parts.push(`FROM ${escapeIdentifier(connector.config.table)}`);

        // WHERE clause - use parameterized values
        if (connector.filters && connector.filters.length > 0) {
            const whereClauses = connector.filters.map(filter => {
                // Skip FilterGroup for now (only process simple filters)
                if ('op' in filter) {
                    throw new Error('FilterGroup is not yet supported in MySQL adapter');
                }

                const field = escapeIdentifier(filter.field);
                const operator = validateOperator(filter.operator);

                // Handle special operators
                if (operator === 'IS NULL' || operator === 'IS NOT NULL') {
                    return `${field} ${operator}`;
                }

                if (operator === 'IN' || operator === 'NOT IN') {
                    const inValues = Array.isArray(filter.value) ? filter.value : [filter.value];
                    const placeholders = inValues.map(() => '?').join(', ');
                    values.push(...inValues);
                    return `${field} ${operator} (${placeholders})`;
                }

                if (operator === 'BETWEEN' || operator === 'NOT BETWEEN') {
                    if (!Array.isArray(filter.value) || filter.value.length !== 2) {
                        throw new Error(`${operator} requires an array of exactly 2 values`);
                    }
                    values.push(filter.value[0], filter.value[1]);
                    return `${field} ${operator} ? AND ?`;
                }

                values.push(filter.value);
                return `${field} ${operator} ?`;
            });
            parts.push(`WHERE ${whereClauses.join(' AND ')}`);
        }

        // ORDER BY clause - escape field identifiers and validate direction
        if (connector.sort && connector.sort.length > 0) {
            const orderBy = connector.sort
                .map(sort => `${escapeIdentifier(sort.field)} ${validateSortDirection(sort.type)}`)
                .join(', ');
            parts.push(`ORDER BY ${orderBy}`);
        }

        // LIMIT and OFFSET - use parameters (MySQL uses LIMIT offset, row_count syntax)
        if (pageOptions.limit !== undefined) {
            values.push(pageOptions.offset || 0, pageOptions.limit);
            parts.push(`LIMIT ?, ?`);
        }

        return { sql: parts.join(' '), values };
    }

    const queryBuilderMap: Record<string, (pageOptions: PageOptions) => { sql: string; values: any[] }> = {
        table_query: buildSelectQuery,
        custom_query: buildCustomQuery,
        table_columns: buildGetColumnsQuery,
    };

    function buildInsertQuery(data: any[]): { sql: string; values: any[] } {
        if (!connector.config?.table) {
            throw new Error(`table property is required on the MySQL adapter's ${endpoint.id} endpoint`)
        }

        if (!data || data.length === 0) {
            throw new Error("Data array cannot be empty for insert operation");
        }

        const table = escapeIdentifier(connector.config.table);
        const fields = connector.fields.length > 0 ? connector.fields : Object.keys(data[0]);

        if (fields.length === 0) {
            throw new Error("No fields specified or inferred from data for insert operation");
        }

        const escapedFields = fields.map(f => escapeIdentifier(f)).join(', ');
        const values: any[] = [];

        const valuePlaceholders = data.map(row => {
            const rowPlaceholders = fields.map(field => {
                values.push(row[field] ?? null);
                return '?';
            });
            return `(${rowPlaceholders.join(', ')})`;
        });

        const sql = `INSERT INTO ${table} (${escapedFields}) VALUES ${valuePlaceholders.join(', ')}`;

        log("Generated query:", sql, "with values count:", values.length);
        return { sql, values };
    }

    return {
        getConfig: function () {
            return MySQLAdapter;
        },
        connect: async function () {
            if (!isBasicAuth(auth)) {
                throw new Error("MySQL adapter requires basic authentication");
            }

            const defaultConfig = {
                host: 'localhost',
                database: 'mysql',
                port: 3306,
            };

            if (!auth.credentials.database) {
                auth.credentials.database = defaultConfig.database;
            }

            const config = {
                user: auth.credentials.username,
                password: auth.credentials.password,
                host: auth.credentials.host || defaultConfig.host,
                database: auth.credentials.database,
                port: auth.credentials.port !== undefined
                    ? parseInt(auth.credentials.port.toString(), 10)
                    : defaultConfig.port,
            };

            try {
                log("Connecting to MySQL...");
                connection = await mysql.createConnection(config);
                await connection.execute('SELECT 1');
                log("Connection successful");
            } catch (error: any) {
                console.error("Connection test failed:", error.message);
                throw new Error(`Failed to connect to MySQL: ${error.message}`);
            }
        },
        disconnect: async function () {
            try {
                log("Closing MySQL connection...");
                if (connection) {
                    await connection.end();
                    log("Connection closed successfully");
                }
            } catch (error: any) {
                console.error("Error closing connection:", error.message);
                throw error;
            }
        },
        download: async function (pageOptions: { limit?: number; offset?: number }) {
            const queryBuilder = queryBuilderMap[endpoint.id];
            if (!queryBuilder) {
                throw new Error(`${endpoint.id} endpoint don't support download`);
            }

            const query = queryBuilder(pageOptions);

            log("Executing query:", query.sql, "with values:", query.values);

            try {
                const [rows] = await connection.execute<RowDataPacket[]>(query.sql, query.values);
                log("Downloaded rows:", rows.length);
                return {
                    data: rows
                };
            } catch (error: any) {
                console.error("Download error:", error.message);
                throw error;
            }
        },
        upload: async function (data: any[]) {
            if (endpoint.id !== "table_insert") {
                throw new Error("Upload only supported for table_insert endpoint");
            }
            const query = buildInsertQuery(data);
            log("Executing insert:", query.sql, "with values count:", query.values.length);
            try {
                await connection.execute(query.sql, query.values);
                log("Uploaded rows:", data.length);
            } catch (error: any) {
                console.error("Upload error:", error.message);
                throw error;
            }
        }
    };
}

export { mysqlAdapter as mysql, MySQLAdapter };