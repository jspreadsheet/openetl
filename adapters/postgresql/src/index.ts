/**
 * HubSpot Adapter for OpenETL
 * https://componade.com/openetl
 */

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, BasicAuth } from 'openetl';
import pg from 'pg'; // Use named imports
import { QueryResult } from 'pg'; // Use named imports

const { Pool } = pg;

const schemaDefaultValue = 'public';

const PostgresqlAdapter: DatabaseAdapter = {
  id: "postgresql",
  name: "PostgreSQL Database Adapter",
  category: 'Databases & Data Warehouses',
  image: 'https://static.cdnlogo.com/logos/p/93/postgresql.svg',
  type: "database",
  action: ["download", "upload", "sync"],
  config: [
    {
      id: 'schema',
      name: 'schema',
      required: false,
      default: schemaDefaultValue,
    },
  ],
  credential_type: "basic",
  metadata: {
    provider: "postgresql",
    description: "Adapter for PostgreSQL database operations",
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
            type: 'sql',
          },
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
        ]
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
        ]
      },
      tool: 'table_columns',
    },
  ],
  pagination: {
    type: 'offset',
  }
};

function postgresql(connector: Connector, auth: AuthConfig): AdapterInstance {
  const log = function(...args: any[]) {
    if (connector.debug) {
      console.log(...arguments)
    }
  }

  const endpoint = PostgresqlAdapter.endpoints.find(e => e.id === connector.endpoint_id)!;
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in PostgreSQL adapter`);
  }

  function isBasicAuth(auth: AuthConfig): auth is BasicAuth {
    return auth.type === 'basic' && typeof auth.credentials === 'object';
  }

  let pool: pg.Pool;

  type PageOptions = {
    limit?: number | undefined;
    offset?: string | number | undefined;
  };

  function buildGetColumnsQuery(pageOptions: PageOptions) {
    if (!connector.config?.table) {
      throw new Error(`table property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`);
    }

    return `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = '${connector.config?.schema || schemaDefaultValue}' AND table_name = '${connector.config.table}';`;
  }

  function buildCustomQuery(pageOptions: PageOptions) {
    if (!connector.config?.custom_query) {
      throw new Error(`custom_query property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`)
    }

    return connector.config.custom_query;
  }

  function buildSelectQuery(pageOptions: PageOptions) {
    if (typeof pageOptions.offset === 'string') {
      throw new Error('table_query endpoint of the PostgreSQL adapter don\'t accept a string as offset');
    }

    if (!connector.config?.table) {
      throw new Error(`table property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`);
    }

    const parts = [];

    // SELECT clause
    parts.push(`SELECT ${connector.fields.length > 0 ? connector.fields.join(', ') : '*'}`);

    // FROM clause
    parts.push(`FROM "${connector.config?.schema || schemaDefaultValue}"."${connector.config.table}"`);

    // WHERE clause
    if (connector.filters && connector.filters.length > 0) {
      const whereClauses = connector.filters.map(filter => {
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
    if (pageOptions.limit !== undefined) {
      parts.push(`LIMIT ${pageOptions.limit}`);

      if (pageOptions.offset !== undefined) {
        parts.push(`OFFSET ${pageOptions.offset}`);
      }
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
      throw new Error(`table property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`)
    }

    const schema = connector.config.schema || schemaDefaultValue;
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
    getConfig: function() {
      return PostgresqlAdapter;
    },
    connect: async function() {
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
      } catch (error: any) {
        console.error("Connection test failed:", error.message);
        throw new Error(`Failed to connect to PostgreSQL: ${error.message}`);
      }
    },
    disconnect: async function() {
      try {
        log("Closing PostgreSQL connection pool...");
        await pool.end();
        log("Pool closed successfully");
      } catch (error: any) {
        console.error("Error closing pool:", error.message);
        throw error;
      }
    },
    download: async function(pageOptions) {
      const queryBuilder = queryBuilderMap[endpoint.id];
      if (!queryBuilder) {
        throw new Error(`${endpoint.id} endpoint don't support download`);
      }

      const query = queryBuilder(pageOptions);

      log("Executing query:", query);

      try {
        if (endpoint.id === "custom_query") {
          await pool.query(`SET SCHEMA '${connector.config?.schema || schemaDefaultValue}'`);
        }

        const result: QueryResult<any> = await pool.query(query);
        log("Downloaded rows:", result.rows.length);
        return {
          data: result.rows
        };
      } catch (error: any) {
        console.error("Download error:", error.message);
        throw error;
      }
    },
    upload: async function(data) {
      if (endpoint.id !== "table_insert") {
        throw new Error("Upload only supported for table_insert endpoint");
      }
      const query = buildInsertQuery(data);
      log("Executing insert:", query);
      try {
        await pool.query(query);
        log("Uploaded rows:", data.length);
      } catch (error: any) {
        console.error("Upload error:", error.message);
        throw error;
      }
    }
  };
}

export { postgresql, PostgresqlAdapter };