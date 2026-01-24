/**
 * HubSpot Adapter for OpenETL
 * https://componade.com/openetl
 */

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, BasicAuth } from 'openetl';
import pg from 'pg'; // Use named imports
import { QueryResult } from 'pg'; // Use named imports

const { Pool } = pg;

const schemaDefaultValue = 'public';

// SQL Security Functions
const ALLOWED_OPERATORS = new Set([
  '=', '!=', '<>', '<', '>', '<=', '>=',
  'LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE',
  'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL',
  'BETWEEN', 'NOT BETWEEN'
]);

/**
 * Escapes a SQL identifier (table name, column name, schema name)
 * by wrapping in double quotes and escaping any existing double quotes
 */
function escapeIdentifier(identifier: string): string {
  if (!identifier || typeof identifier !== 'string') {
    throw new Error('Invalid identifier: must be a non-empty string');
  }
  // Check for dangerous characters that shouldn't be in identifiers
  if (/[\x00-\x1f\x7f]/.test(identifier)) {
    throw new Error('Invalid identifier: contains control characters');
  }
  return `"${identifier.replace(/"/g, '""')}"`;
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

  function buildGetColumnsQuery(pageOptions: PageOptions): { text: string; values: any[] } {
    if (!connector.config?.table) {
      throw new Error(`table property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`);
    }

    return {
      text: `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
      values: [connector.config?.schema || schemaDefaultValue, connector.config.table]
    };
  }

  function buildCustomQuery(pageOptions: PageOptions): { text: string; values: any[] } {
    if (!connector.config?.custom_query) {
      throw new Error(`custom_query property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`)
    }

    // Note: Custom queries are user-provided and should be used carefully
    // We return them as-is since the user is responsible for their content
    return { text: connector.config.custom_query, values: [] };
  }

  function buildSelectQuery(pageOptions: PageOptions): { text: string; values: any[] } {
    if (typeof pageOptions.offset === 'string') {
      throw new Error('table_query endpoint of the PostgreSQL adapter don\'t accept a string as offset');
    }

    if (!connector.config?.table) {
      throw new Error(`table property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`);
    }

    const parts: string[] = [];
    const values: any[] = [];
    let paramIndex = 1;

    // SELECT clause - escape all field identifiers
    const escapedFields = connector.fields.length > 0
      ? connector.fields.map(f => escapeIdentifier(f)).join(', ')
      : '*';
    parts.push(`SELECT ${escapedFields}`);

    // FROM clause - escape schema and table identifiers
    const schema = escapeIdentifier(connector.config?.schema || schemaDefaultValue);
    const table = escapeIdentifier(connector.config.table);
    parts.push(`FROM ${schema}.${table}`);

    // WHERE clause - use parameterized values
    if (connector.filters && connector.filters.length > 0) {
      const whereClauses = connector.filters.map(filter => {
        // Skip FilterGroup for now (only process simple filters)
        if ('op' in filter) {
          throw new Error('FilterGroup is not yet supported in PostgreSQL adapter');
        }

        const field = escapeIdentifier(filter.field);
        const operator = validateOperator(filter.operator);

        // Handle special operators
        if (operator === 'IS NULL' || operator === 'IS NOT NULL') {
          return `${field} ${operator}`;
        }

        if (operator === 'IN' || operator === 'NOT IN') {
          const inValues = Array.isArray(filter.value) ? filter.value : [filter.value];
          const placeholders = inValues.map(() => `$${paramIndex++}`).join(', ');
          values.push(...inValues);
          return `${field} ${operator} (${placeholders})`;
        }

        if (operator === 'BETWEEN' || operator === 'NOT BETWEEN') {
          if (!Array.isArray(filter.value) || filter.value.length !== 2) {
            throw new Error(`${operator} requires an array of exactly 2 values`);
          }
          values.push(filter.value[0], filter.value[1]);
          return `${field} ${operator} $${paramIndex++} AND $${paramIndex++}`;
        }

        values.push(filter.value);
        return `${field} ${operator} $${paramIndex++}`;
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

    // LIMIT and OFFSET - use parameters
    if (pageOptions.limit !== undefined) {
      values.push(pageOptions.limit);
      parts.push(`LIMIT $${paramIndex++}`);

      if (pageOptions.offset !== undefined) {
        values.push(pageOptions.offset);
        parts.push(`OFFSET $${paramIndex++}`);
      }
    }

    return { text: parts.join(' '), values };
  }

  const queryBuilderMap: Record<string, (pageOptions: PageOptions) => { text: string; values: any[] }> = {
    table_query: buildSelectQuery,
    custom_query: buildCustomQuery,
    table_columns: buildGetColumnsQuery,
  };

  function buildInsertQuery(data: any[]): { text: string; values: any[] } {
    if (!connector.config?.table) {
      throw new Error(`table property is required on the PostgreSQL adapter's ${endpoint.id} endpoint`)
    }

    const schema = escapeIdentifier(connector.config.schema || schemaDefaultValue);
    const table = escapeIdentifier(connector.config.table);
    const fields = connector.fields.length > 0 ? connector.fields : Object.keys(data[0]);
    const escapedFields = fields.map(f => escapeIdentifier(f)).join(', ');

    const values: any[] = [];
    let paramIndex = 1;

    const valuePlaceholders = data.map(row => {
      const rowPlaceholders = fields.map(field => {
        values.push(row[field] ?? null);
        return `$${paramIndex++}`;
      });
      return `(${rowPlaceholders.join(', ')})`;
    });

    return {
      text: `INSERT INTO ${schema}.${table} (${escapedFields}) VALUES ${valuePlaceholders.join(', ')}`,
      values
    };
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

      log("Executing query:", query.text, "with values:", query.values);

      try {
        if (endpoint.id === "custom_query") {
          // Use parameterized query for SET SCHEMA
          await pool.query('SET search_path TO $1', [connector.config?.schema || schemaDefaultValue]);
        }

        const result: QueryResult<any> = await pool.query(query.text, query.values);
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
      log("Executing insert:", query.text, "with values count:", query.values.length);
      try {
        await pool.query(query.text, query.values);
        log("Uploaded rows:", data.length);
      } catch (error: any) {
        console.error("Upload error:", error.message);
        throw error;
      }
    }
  };
}

export { postgresql, PostgresqlAdapter };