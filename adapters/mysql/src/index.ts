/**
 * MySQL Adapter for OpenETL
 * https://componade.com/openetl
 */

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, BasicAuth, Filter, FilterGroup } from 'openetl';
import mysql, { RowDataPacket } from 'mysql2/promise';

const MySQLAdapter: DatabaseAdapter = {
  id: "mysql",
  name: "MySQL Database Adapter",
  type: "database",
  action: ["download", "upload", "sync"],
  config: [
    {
      name: 'database',
      required: true,
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
    provider: "mysql",
    description: "Adapter for MySQL database operations",
    version: "1.0",
  },
  endpoints: [
    { id: "table_query", query_type: "table", description: "Query a specific table", supported_actions: ["download", "sync"] },
    { id: "custom_query", query_type: "custom", description: "Run a custom SQL query", supported_actions: ["download"] },
    { id: "table_insert", query_type: "table", description: "Insert into a specific table", supported_actions: ["upload"] },
  ],
};

function mysqlAdapter(connector: Connector, auth: AuthConfig): AdapterInstance {
  const endpoint = MySQLAdapter.endpoints.find(e => e.id === connector.endpoint_id)!;
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in MySQL adapter`);
  }

  function isBasicAuth(auth: AuthConfig): auth is BasicAuth {
    return auth.type === 'basic';
  }

  function isFilter(filter: Filter | FilterGroup): filter is Filter {
    return 'field' in filter && 'operator' in filter && 'value' in filter;
  }

  let connection: mysql.Connection;

  function buildSelectQuery(customLimit?: number, customOffset?: number): string {
    if (endpoint.id === "custom_query" && connector.config?.custom_query) {
      return connector.config.custom_query;
    }

    if (!connector.config?.database || !connector.config?.table) {
      throw new Error("Database and table required for table-based endpoints");
    }

    const parts = [];

    // SELECT clause
    parts.push(`SELECT ${connector.fields.length > 0 ? connector.fields.join(', ') : '*'}`);

    // FROM clause
    parts.push(`FROM \`${connector.config.database}\`.\`${connector.config.table}\``);

    // WHERE clause
    if (connector.filters && connector.filters.length > 0) {
      const whereClauses = connector.filters.map(filter => {
        if (!isFilter(filter)) {
          const subClauses = filter.filters.map(f =>
              isFilter(f) ? `\`${f.field}\` ${f.operator} '${f.value}'` : ''
          );
          return `(${subClauses.join(` ${filter.op} `)})`;
        }
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
    if (customLimit !== undefined) {
      parts.push(`LIMIT ${customOffset || 0}, ${customLimit}`);
    }

    return parts.join(' ');
  }

  function buildInsertQuery(data: any[]): string {
    if (!connector.config?.database || !connector.config?.table) {
      throw new Error("Database and table required for table_insert endpoint");
    }
  
    if (!data || data.length === 0) {
      throw new Error("Data array cannot be empty for insert operation");
    }
  
    const database = connector.config.database;
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
    const query = `INSERT INTO \`${database}\`.\`${table}\` (${fieldList}) VALUES ${valueList}`;
  
    console.log("Generated query:", query); // Debug output to verify the string
    return query;
  }

  return {
    paginationType: 'offset',
    connect: async function() {
      if (!isBasicAuth(auth)) {
        throw new Error("MySQL adapter requires basic authentication");
      }

      const defaultConfig = {
        host: 'localhost',
        database: 'mysql',
        port: 3306,
      };

      const config = {
        user: auth.credentials.username,
        password: auth.credentials.password,
        host: auth.credentials.host || defaultConfig.host,
        database: auth.credentials.database || defaultConfig.database,
        port: auth.credentials.port !== undefined
            ? parseInt(auth.credentials.port.toString(), 10)
            : defaultConfig.port,
      };

      try {
        console.log("Connecting to MySQL...");
        connection = await mysql.createConnection(config);
        await connection.execute('SELECT 1');
        console.log("Connection successful");
      } catch (error: any) {
        console.error("Connection test failed:", error.message);
        throw new Error(`Failed to connect to MySQL: ${error.message}`);
      }
    },
    disconnect: async function() {
      try {
        console.log("Closing MySQL connection...");
        if (connection) {
          await connection.end();
          console.log("Connection closed successfully");
        }
      } catch (error: any) {
        console.error("Error closing connection:", error.message);
        throw error;
      }
    },
    download: async function(pageOptions: { limit?: number; offset?: number }) {
      if (endpoint.id === "table_insert") {
        throw new Error("Table_insert endpoint only supported for upload");
      }

      const query = buildSelectQuery(pageOptions.limit, pageOptions.offset);
      console.log("Executing query:", query);

      try {
        const [rows] = await connection.execute<RowDataPacket[]>(query);
        console.log("Downloaded rows:", rows.length);
        return {
          data: rows
        };
      } catch (error: any) {
        console.error("Download error:", error.message);
        throw error;
      }
    },
    upload: async function(data: any[]) {
      if (endpoint.id !== "table_insert") {
        throw new Error("Upload only supported for table_insert endpoint");
      }
      const query = buildInsertQuery(data);
      console.log("Executing insert:", query);
      try {
        await connection.execute(query);
        console.log("Uploaded rows:", data.length);
      } catch (error: any) {
        console.error("Upload error:", error.message);
        throw error;
      }
    }
  };
}

export { mysqlAdapter as mysql, MySQLAdapter };