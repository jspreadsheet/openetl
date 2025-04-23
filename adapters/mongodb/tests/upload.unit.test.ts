import { Orchestrator } from '../../../src/index'; // Adjust path as needed
import { Connector, Pipeline, Vault, AdapterInstance, DatabaseAdapter } from '../../../src/types';

describe('Orchestrator Unit Tests - Upload Batching', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let mockAdapter: AdapterInstance;
  let connector: Connector;
  let pipeline: Pipeline;
  let vault: Vault;
  const MongoDBAdapter: DatabaseAdapter = {
    id: "mongodb",
    name: "MongoDB Database Adapter",
    type: "database",
    action: ["download", "upload", "sync"],
    config: [
      {
        id: 'collection',
        name: 'collection',
        required: true,
      },
    ],
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
              id: 'custom_query',
              name: 'custom_query',
              required: false,
            },
          ]
        }
      },
      {
        id: "custom_query",
        query_type: "custom",
        description: "Run a custom MongoDB query",
        supported_actions: ["download"]
      },
      {
        id: "collection_insert",
        query_type: "table",
        description: "Insert into a specific collection",
        supported_actions: ["upload"]
      },
    ],
    pagination: { type: 'offset' }
  };

  beforeEach(() => {
    // Mock adapter with upload method
    mockAdapter = {
      connect: jest.fn().mockResolvedValue(undefined),
      disconnect: jest.fn().mockResolvedValue(undefined),
      upload: jest.fn().mockResolvedValue(undefined),
      download: jest.fn().mockResolvedValue({ data: [] }), // Required but not used here
      getConfig: () => MongoDBAdapter
    };

    // Mock adapter factory
    const adapters = {
      mongodb: jest.fn().mockReturnValue(mockAdapter),
    };

    // Vault setup
    vault = {
      'mongo-auth': {
        id: 'mongo-auth',
        type: 'basic',
        credentials: {
          username: 'test',
          password: 'test',
          host: 'localhost',
          database: 'admin',
          port: '27000',
        },
      },
    };

    orchestrator = Orchestrator(vault, adapters);

    // Connector setup
    connector = {
      id: 'mongo-users-connector',
      adapter_id: 'mongodb',
      endpoint_id: 'collection_insert',
      credential_id: 'mongo-auth',
      config: { table: 'users' },
      fields: ['name', 'email'],
      filters: [],
      sort: [],
      pagination: { itemsPerPage: 1 }, // 1 item per batch
    };

    // Pipeline setup
    pipeline = {
      id: 'test-upload-batching',
      data: [
        { name: 'Alice', email: 'alice@example.com' },
        { name: 'Bob', email: 'bob@example.com' },
        { name: 'Charlie', email: 'charlie@example.com' },
        { name: 'David', email: 'david@example.com' },
        { name: 'Eve', email: 'eve@example.com' },
      ],
      target: connector,
      error_handling: {
        max_retries: 0,
        retry_interval: 300,
      },
      rate_limiting: {
        requests_per_second: 1,
        max_retries_on_rate_limit: 0,
      },
    };
  });

  it('calls upload 5 times with 1 item per batch when itemsPerPage=1', async () => {
    // Run the pipeline
    await orchestrator.runPipeline(pipeline);

    // Verify upload was called 5 times
    expect(mockAdapter.upload).toHaveBeenCalledTimes(5);

    // Check each call had exactly 1 item
    expect(mockAdapter.upload).toHaveBeenNthCalledWith(1, [{ name: 'Alice', email: 'alice@example.com' }]);
    expect(mockAdapter.upload).toHaveBeenNthCalledWith(2, [{ name: 'Bob', email: 'bob@example.com' }]);
    expect(mockAdapter.upload).toHaveBeenNthCalledWith(3, [{ name: 'Charlie', email: 'charlie@example.com' }]);
    expect(mockAdapter.upload).toHaveBeenNthCalledWith(4, [{ name: 'David', email: 'david@example.com' }]);
    expect(mockAdapter.upload).toHaveBeenNthCalledWith(5, [{ name: 'Eve', email: 'eve@example.com' }]);
  });
});