/**
 * MySQL Adapter for OpenETL
 * https://componade.com/openetl
 */

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, BasicAuth } from 'openetl';
import mysql, { RowDataPacket } from 'mysql2/promise';

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

    function buildGetColumnsQuery(pageOptions: PageOptions) {
        if (!connector.config?.table) {
            throw new Error(`table property is required on the MySQL adapter's ${endpoint.id} endpoint`)
        }

        return `SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${(auth as BasicAuth).credentials.database}' AND TABLE_NAME = '${connector.config.table}'`;
    }
    
    function buildCustomQuery(pageOptions: PageOptions) {
        if (!connector.config?.custom_query) {
            throw new Error(`custom_query property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`)
        }

        return connector.config.custom_query;
    }

    function buildSelectQuery(pageOptions: PageOptions): string {
        if (!connector.config?.table) {
            throw new Error(`database property is required on the MySQL adapter's ${endpoint.id} endpoint`)
        }

        const parts = [];

        // SELECT clause
        parts.push(`SELECT ${connector.fields.length > 0 ? connector.fields.join(', ') : '*'}`);

        // FROM clause
        parts.push(`FROM \`${connector.config.table}\``);

        // WHERE clause
        if (connector.filters && connector.filters.length > 0) {
            const whereClauses = connector.filters.map(filter => {
                return `\`${filter.field}\` ${filter.operator} '${filter.value}'`; // Fixed here
            });
            parts.push(`WHERE ${whereClauses.join(' AND ')}`);
        }

        // ORDER BY clause
        if (connector.sort && connector.sort.length > 0) {
            const orderBy = connector.sort
                .map(sort => `\`${sort.field}\` ${sort.type.toUpperCase()}`)
                .join(', ');
            parts.push(`ORDER BY ${orderBy}`);
        }

        // LIMIT and OFFSET (MySQL uses LIMIT offset, row_count syntax)
        if (pageOptions.limit !== undefined) {
            parts.push(`LIMIT ${pageOptions.offset || 0}, ${pageOptions.limit}`);
        }

        return parts.join(' ');
    }

    const queryBuilderMap: Record<string, (pageOptions: PageOptions) => string> = {
        table_query: buildSelectQuery,
        custom_query: buildCustomQuery,
        table_columns: buildGetColumnsQuery,
    };

    function buildInsertQuery(data: any[]): string {
        if (!connector.config?.table) {
            throw new Error(`table property is required on the MySQL adapter's ${endpoint.id} endpoint`)
        }

        if (!data || data.length === 0) {
            throw new Error("Data array cannot be empty for insert operation");
        }

        const table = connector.config.table;
        const fields = connector.fields.length > 0 ? connector.fields : Object.keys(data[0]);

        if (fields.length === 0) {
            throw new Error("No fields specified or inferred from data for insert operation");
        }

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

        // Break down the query construction for clarity
        const fieldList = fields.map(f => `\`${f}\``).join(', ');
        const valueList = values.join(', ');
        const query = `INSERT INTO \`${table}\` (${fieldList}) VALUES ${valueList}`;

        log("Generated query:", query); // Debug output to verify the string
        return query;
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

            log("Executing query:", query);

            try {
                const [rows] = await connection.execute<RowDataPacket[]>(query);
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
            log("Executing insert:", query);
            try {
                await connection.execute(query);
                log("Uploaded rows:", data.length);
            } catch (error: any) {
                console.error("Upload error:", error.message);
                throw error;
            }
        }
    };
}

export { mysqlAdapter as mysql, MySQLAdapter };