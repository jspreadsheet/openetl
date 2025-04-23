/**
 * MongoDB Adapter for OpenETL
 * https://componade.com/openetl
 */

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, BasicAuth, Filter } from 'openetl';
import { MongoClient, Db, Collection } from 'mongodb';

const MongoDBAdapter: DatabaseAdapter = {
  id: "mongodb",
  name: "MongoDB Database Adapter",
  category: 'Databases & Data Warehouses',
  image: "https://static.cdnlogo.com/logos/m/30/mongodb-icon.svg",
  type: "database",
  hasGetColumnsRoute: false,
  action: ["download", "upload", "sync"],
  credential_type: "basic",
  metadata: {
    provider: "mongodb",
    description: "Adapter for MongoDB database operations",
    version: "1.0",
  },
  endpoints: [
    {
      id: "collection_query",
      query_type: "table",
      description: "Query a specific collection",
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
      description: "Run a custom MongoDB query",
      supported_actions: ["download"],
      settings: {
        config: [
          {
            id: 'table',
            name: 'table',
            required: true,
          },
          {
            id: 'custom_query',
            name: 'custom_query',
            required: false,
          },
        ]
      }
    },
    {
      id: "collection_insert",
      query_type: "table",
      description: "Insert into a specific collection",
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
  ],
  pagination: { type: 'offset' }
};

function mongodb(connector: Connector, auth: AuthConfig): AdapterInstance {
  const log = function (...args: any) {
    if (connector.debug) {
        console.log(...arguments);
    }
  };

  const endpoint = MongoDBAdapter.endpoints.find(e => e.id === connector.endpoint_id)!;
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in MongoDB adapter`);
  }

  function isBasicAuth(auth: AuthConfig): auth is BasicAuth {
    return auth.type === 'basic';
  }

  let client: MongoClient;
  let db: Db;

  function buildMongoQuery(): any {
    if (endpoint.id === "custom_query" && connector.config?.custom_query) {
      try {
        return JSON.parse(connector.config.custom_query);
      } catch (error: any) {
        throw new Error(`Invalid custom query JSON: ${error.message}`);
      }
    }

    const query: any = {};

    // Build filters
    if (connector.filters && connector.filters.length > 0) {
      const processFilter = (filter: Filter): any => {
        switch (filter.operator) {
          case '=': return { [filter.field]: filter.value };
          case '>': return { [filter.field]: { $gt: filter.value } };
          case '<': return { [filter.field]: { $lt: filter.value } };
          case '>=': return { [filter.field]: { $gte: filter.value } };
          case '<=': return { [filter.field]: { $lte: filter.value } };
          case '!=': return { [filter.field]: { $ne: filter.value } };
          default: return { [filter.field]: filter.value };
        }
      };

      connector.filters.forEach(filter => {
        Object.assign(query, processFilter(filter));
      });
    }

    return query;
  }

  function buildProjection(): any {
    if (connector.fields && connector.fields.length > 0) {
      const projection: any = {};
      connector.fields.forEach(field => {
        projection[field] = 1;
      });
      return projection;
    }
    return undefined;
  }

  function buildSort(): any {
    if (connector.sort && connector.sort.length > 0) {
      const sort: any = {};
      connector.sort.forEach(s => {
        sort[s.field] = s.type === 'asc' ? 1 : -1;
      });
      return sort;
    }
    return undefined;
  }

  return {
    getConfig: () => MongoDBAdapter,
    connect: async function() {
      if (!isBasicAuth(auth)) {
        throw new Error("MongoDB adapter requires basic authentication");
      }

      const defaultConfig = {
        host: 'localhost',
        port: 27017,
        database: 'test'
      };

      const config = {
        host: auth.credentials.host || defaultConfig.host,
        port: auth.credentials.port ? parseInt(auth.credentials.port) : defaultConfig.port,
        database: auth.credentials.database || defaultConfig.database,
        username: auth.credentials.username,
        password: auth.credentials.password
      };

      const url = config.username
        ? `mongodb://${config.username}:${config.password}@${config.host}:${config.port}/${config.database}?authSource=admin`
        : `mongodb://${config.host}:${config.port}/${config.database}`;

      try {
        log("Connecting to MongoDB...");
        client = new MongoClient(url);
        await client.connect();
        db = client.db(config.database);
        log("Connection successful");
      } catch (error: any) {
        console.error("Connection test failed:", error.message);
        throw new Error(`Failed to connect to MongoDB: ${error.message}`);
      }
    },
    disconnect: async function() {
      try {
        log("Closing MongoDB connection...");
        if (client) {
          await client.close();
          log("Connection closed successfully");
        }
      } catch (error: any) {
        console.error("Error closing connection:", error.message);
        throw error;
      }
    },
    download: async function(pageOptions) {
      if (endpoint.id === "collection_insert") {
        throw new Error("Collection_insert endpoint only supported for upload");
      }

      if (!connector.config?.table) {
        throw new Error(`table property is required on the MongoDB adapter's ${endpoint.id} endpoint`);
      }

      if ( pageOptions.offset && Number(pageOptions.offset) < 0 ) {
        pageOptions.offset = 0;
      }

      const query = buildMongoQuery();
      const projection = buildProjection();
      const sort = buildSort();

      log("Executing query:", JSON.stringify(query));

      try {
        const collection = db.collection(connector.config!.table);
        let cursor = collection.find(query);

        if (projection) cursor = cursor.project(projection);
        if (sort) cursor = cursor.sort(sort);
        if (pageOptions.limit) cursor = cursor.limit(pageOptions.limit);
        if (pageOptions.offset) cursor = cursor.skip(Number(pageOptions.offset));

        const results = await cursor.toArray();
        log("Downloaded documents:", results.length);

        return {
          data: results
        };
      } catch (error: any) {
        console.error("Download error:", error.message);
        throw error;
      }
    },
    upload: async function(data) {
      if (endpoint.id !== "collection_insert") {
        throw new Error("Upload only supported for collection_insert endpoint");
      }

      if (!connector.config?.table) {
        throw new Error(`table property is required on the MongoDB adapter's ${endpoint.id} endpoint`);
      }

      log("Uploading documents:", data.length);

      try {
        const collection = db.collection(connector.config!.table);
        const result = await collection.insertMany(data);
        log("Inserted documents:", result.insertedCount);
      } catch (error: any) {
        console.error("Upload error:", error.message);
        throw error;
      }
    }
  };
}

export { mongodb, MongoDBAdapter };