/**
 * HubSpot Adapter for OpenETL
 * https://componade.com/openetl
 */

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, BasicAuth, Filter, FilterGroup } from '../types';
import pg from 'pg'; // Use named imports
import { QueryResult } from 'pg'; // Use named imports

const { Pool } = pg;

export const PostgresAdapter: DatabaseAdapter = {
  id: "postgres",
  name: "PostgreSQL Database Adapter",
  type: "database",
  action: ["download", "upload", "sync"],
  credential_type: "basic",
  metadata: {
    provider: "postgresql",
    description: "Adapter for PostgreSQL database operations",
    version: "1.0",
  },
  endpoints: [
    { id: "table_query", query_type: "table", description: "Query a specific table", supported_actions: ["download", "sync"] },
    { id: "custom_query", query_type: "custom", description: "Run a custom SQL query", supported_actions: ["download"] },
    { id: "table_insert", query_type: "table", description: "Insert into a specific table", supported_actions: ["upload"] },
  ],
};

export default function Adapter(connector: Connector, auth: AuthConfig): AdapterInstance {
  const endpoint = PostgresAdapter.endpoints.find(e => e.id === connector.endpoint_id)!;
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in PostgreSQL adapter`);
  }

  function isBasicAuth(auth: AuthConfig): auth is BasicAuth {
    return auth.type === 'basic';
  }

  function isFilter(filter: Filter | FilterGroup): filter is Filter {
    return 'field' in filter && 'operator' in filter && 'value' in filter;
  }

  // Define default config with port as a number
  const defaultConfig = {
    host: 'localhost',
    database: 'postgres',
    port: 5432, // Changed from "5432" (string) to 5432 (number)
  };

  // Construct config with port always as a number
  const config = isBasicAuth(auth) ? {
    user: auth.credentials.username,
    password: auth.credentials.password,
    host: auth.credentials.host || defaultConfig.host,
    database: auth.credentials.database || defaultConfig.database,
    port: auth.credentials.port !== undefined
        ? parseInt(auth.credentials.port.toString(), 10)
        : defaultConfig.port,
  } : {
    host: defaultConfig.host,
    database: defaultConfig.database,
    port: defaultConfig.port,
  };

  const pool = new Pool(config);

  function buildSelectQuery(customLimit?: number, customOffset?: number): string {
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
          const subClauses = filter.filters.map(f =>
              isFilter(f) ? `${f.field} ${f.operator} '${f.value}'` : ''
          );
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

  function buildInsertQuery(data: any[]): string {
    if (!connector.config?.schema || !connector.config?.table) {
      throw new Error("Schema and table required for table_insert endpoint");
    }
    const schema = connector.config.schema;
    const table = connector.config.table;
    const fields = connector.fields.length > 0 ? connector.fields : Object.keys(data[0]);
    const values = data.map(row => {
      const rowValues = fields.map(field => {
        const value = row[field];
        return value === null || value === undefined ? 'NULL' : `'${value.toString().replace(/'/g, "''")}'`;
      });
      return `(${rowValues.join(', ')})`;
    });
    return `INSERT INTO "${schema}"."${table}" (${fields.map(f => `"${f}"`).join(', ')}) VALUES ${values.join(', ')}`;
  }

  return {
    connect: async function() {
      if (!isBasicAuth(auth)) {
        throw new Error("PostgreSQL adapter requires basic authentication");
      }
      try {
        console.log("Testing connection to PostgreSQL...");
        const client = await pool.connect();
        await client.query('SELECT 1');
        client.release();
        console.log("Connection successful");
      } catch (error: any) {
        console.error("Connection test failed:", error.message);
        throw new Error(`Failed to connect to PostgreSQL: ${error.message}`);
      }
    },
    disconnect: async function() {
      try {
        console.log("Closing PostgreSQL connection pool...");
        await pool.end();
        console.log("Pool closed successfully");
      } catch (error: any) {
        console.error("Error closing pool:", error.message);
        throw error;
      }
    },
    download: async function(pageOptions) {
      if (endpoint.id === "table_insert") {
        throw new Error("Table_insert endpoint only supported for upload");
      }

      const query = buildSelectQuery(pageOptions.limit, pageOptions.offset);
      console.log("Executing query:", query);

      try {
        const result: QueryResult<any> = await pool.query(query);
        console.log("Downloaded rows:", result.rows.length);
        return {
          data: result.rows,
          hasMore: result.rows.length === pageOptions.limit,
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
      console.log("Executing insert:", query);
      try {
        await pool.query(query);
        console.log("Uploaded rows:", data.length);
      } catch (error: any) {
        console.error("Upload error:", error.message);
        throw error;
      }
    }
  };
}