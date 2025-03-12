// Unit Tests
import { mongodb } from './../src/index'; // Adjust path as needed
import { MongoClient, Collection } from 'mongodb';
import { Connector, AuthConfig, AdapterInstance, Vault } from '../../../src/types';
import { FilterGroup } from 'openetl';

jest.mock('mongodb');

describe('MongoDB Adapter Unit Tests', () => {
  let connector: Connector;
  let auth: AuthConfig;
  let adapter: AdapterInstance;
  let mockCollection: any;
  let mockDb: any;
  let mockClient: any;

  beforeEach(() => {
    jest.clearAllMocks();

    // Mock connector configuration
    connector = {
      id: 'mongodb-connector',
      adapter_id: 'mongodb',
      endpoint_id: 'collection_query',
      credential_id: 'mongo-auth',
      config: {
        database: 'admin',
        collection: 'users',
      },
      fields: ['_id', 'name', 'email'],
      filters: [{
        field: 'status',
        operator: '=',
        value: 'active',
      }],
      sort: [{ type: 'asc', field: 'name' }],
      transform: [],
      pagination: { itemsPerPage: 10 },
    };

    // Mock basic authentication
    auth = {
      id: 'mongo-auth',
      type: 'basic',
      credentials: {
        username: 'test',
        password: 'test',
        host: 'localhost',
        database: 'admin',
        port: '27000',
      },
    };

    // Mock MongoDB client
    mockCollection = {
      find: jest.fn().mockReturnThis(),
      project: jest.fn().mockReturnThis(),
      sort: jest.fn().mockReturnThis(),
      limit: jest.fn().mockReturnThis(),
      skip: jest.fn().mockReturnThis(),
      toArray: jest.fn().mockResolvedValue([]),
      insertMany: jest.fn().mockResolvedValue({ insertedCount: 0 }),
    };

    mockDb = {
      collection: jest.fn().mockReturnValue(mockCollection),
    };

    mockClient = {
      connect: jest.fn().mockResolvedValue(undefined),
      db: jest.fn().mockReturnValue(mockDb),
      close: jest.fn().mockResolvedValue(undefined),
    };

    (MongoClient as jest.MockedClass<typeof MongoClient>).mockImplementation(() => mockClient as any);

    // Create adapter instance
    adapter = mongodb(connector, auth);
  });

  it('connects successfully with valid credentials', async () => {
    await expect(adapter.connect!()).resolves.toBeUndefined();
    expect(mockClient.connect).toHaveBeenCalled();
    expect(mockDb.collection).toHaveBeenCalledWith('users');
  });

  it('throws error on connection failure', async () => {
    mockClient.connect.mockRejectedValueOnce(new Error('Connection refused'));
    await expect(adapter.connect!()).rejects.toThrow('Failed to connect to MongoDB: Connection refused');
  });

  it('downloads data with filters, sorting, and pagination', async () => {
    await adapter.connect!();

    const mockDocs = [
      { _id: '1', name: 'Alice', email: 'alice@example.com' },
      { _id: '2', name: 'Bob', email: 'bob@example.com' },
    ];
    mockCollection.toArray.mockResolvedValueOnce(mockDocs);

    const result = await adapter.download({ limit: 2, offset: 1 });
    expect(mockCollection.find).toHaveBeenCalledWith({ status: 'active' });
    expect(mockCollection.project).toHaveBeenCalledWith({ _id: 1, name: 1, email: 1 });
    expect(mockCollection.sort).toHaveBeenCalledWith({ name: 1 });
    expect(mockCollection.limit).toHaveBeenCalledWith(2);
    expect(mockCollection.skip).toHaveBeenCalledWith(1);

    expect(result.data).toEqual(mockDocs);
  });

  it('downloads all fields when none specified', async () => {
    const connectorNoFields = { ...connector, fields: [] };
    const adapterNoFields = mongodb(connectorNoFields, auth);
    await adapterNoFields.connect!();

    const mockDocs = [{ _id: '1', name: 'Alice', email: 'alice@example.com' }];
    mockCollection.toArray.mockResolvedValueOnce(mockDocs);

    const result = await adapterNoFields.download({ limit: 1, offset: 0 });
    expect(mockCollection.project).not.toHaveBeenCalled();
    expect(result.data).toEqual(mockDocs);
  });

  it('handles custom query for custom_query endpoint', async () => {
    const customConnector = {
      ...connector,
      endpoint_id: 'custom_query',
      config: { ...connector.config, custom_query: '{"age": {"$gt": 25}}' },
    };
    const customAdapter = mongodb(customConnector, auth);
    await customAdapter.connect!();

    const mockDocs = [{ _id: '1', name: 'Charlie' }];
    mockCollection.toArray.mockResolvedValueOnce(mockDocs);

    const result = await customAdapter.download({ limit: 10, offset: 0 });
    expect(mockCollection.find).toHaveBeenCalledWith({ age: { $gt: 25 } });
    expect(result.data).toEqual(mockDocs);
  });

  it('uploads data successfully', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'collection_insert' };
    const uploadAdapter = mongodb(uploadConnector, auth);
    await uploadAdapter.connect!();

    const data = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];

    mockCollection.insertMany.mockResolvedValueOnce({ insertedCount: 2 });

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();
    expect(mockCollection.insertMany).toHaveBeenCalledWith(data);
  });

  it('throws error when downloading with collection_insert endpoint', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'collection_insert' };
    const uploadAdapter = mongodb(uploadConnector, auth);

    await expect(uploadAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      'Collection_insert endpoint only supported for upload'
    );
  });

  it('throws error when database or collection is missing', async () => {
    const invalidConnector = { ...connector, config: {} };
    const invalidAdapter = mongodb(invalidConnector, auth);

    await expect(invalidAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      'Database and collection required for collection-based endpoints'
    );
  });

  it('disconnects successfully', async () => {
    await adapter.connect!();
    await expect(adapter.disconnect!()).resolves.toBeUndefined();
    expect(mockClient.close).toHaveBeenCalled();
  });

  it('builds query with filter groups', async () => {
    const filters: FilterGroup[] = [{
      op: 'OR',
      filters: [
        { field: 'status', operator: '=', value: 'active' },
        { field: 'role', operator: '=', value: 'admin' },
      ],
    }];

    const connectorWithFilterGroup = { ...connector, filters };
    const adapterWithFilterGroup = mongodb(connectorWithFilterGroup, auth);
    await adapterWithFilterGroup.connect!();

    const mockDocs = [{ _id: '1', name: 'Admin', email: 'admin@example.com' }];
    mockCollection.toArray.mockResolvedValueOnce(mockDocs);

    const result = await adapterWithFilterGroup.download({ limit: 1, offset: 0 });
    expect(mockCollection.find).toHaveBeenCalledWith({
      $or: [{ status: 'active' }, { role: 'admin' }]
    });
    expect(result.data).toEqual(mockDocs);
  });
});

// Integration Tests
import { Orchestrator } from '../../../src/index';
import { Pipeline } from '../../../src/types';

xdescribe('MongoDB Adapter Integration Tests', () => {
  let connector: Connector;
  let pipeline: Pipeline;
  let realClient: MongoClient;

  const adapters = { mongodb };

  const mongoUser = "mongo_user";
  const mongoPassword = "mongo_pass";
  const mongoDatabase = "test_db";

  let vault: Vault = {
    'mongo-auth': {
      id: 'mongo-auth',
      name: 'MongoDB',
      environment: 'production',
      type: 'basic',
      credentials: {
        host: "localhost",
        username: mongoUser,
        password: mongoPassword,
        database: mongoDatabase,
        port: "27017",
      }
    }
  };

  let orchestrator = Orchestrator(vault, adapters);

  beforeEach(async () => {
    realClient = new MongoClient(`mongodb://${mongoUser}:${mongoPassword}@localhost:27017/${mongoDatabase}?authSource=admin`);
    await realClient.connect();
    const db = realClient.db(mongoDatabase);

    await db.dropCollection('users').catch(() => {});
    await db.createCollection('users');

    connector = {
      id: "mongo-users-connector",
      adapter_id: "mongodb",
      endpoint_id: "collection_query",
      credential_id: "mongo-auth",
      config: { database: mongoDatabase, collection: "users" },
      fields: ['name', 'email'],
      filters: [{
        field: 'status',
        operator: '=',
        value: 'active',
      }],
      sort: [{ type: 'asc', field: 'name' }],
      transform: [],
      pagination: { itemsPerPage: 10 },
    };

    pipeline = {
      id: "test-download",
      source: connector,
      error_handling: {
        max_retries: 3,
        retry_interval: 300,
        fail_on_error: false,
      },
      rate_limiting: {
        requests_per_second: 1,
        max_retries_on_rate_limit: 3,
      },
    };
  });

  afterAll(async () => {
    if (realClient) {
      await realClient.close();
    }
  });

  async function insertUsers(users: {email: string; name: string; status?: string}[]) {
    const db = realClient.db(mongoDatabase);
    await db.collection('users').insertMany(users.map(u => ({
      ...u,
      status: u.status || 'active'
    })));
  }

  it('downloads data with filters, sorting, and pagination', async () => {
    const expectedResult = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];

    await insertUsers(expectedResult);

    let result;
    pipeline.onload = (data) => {
      result = data;
    };

    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(
      expect.arrayContaining([
        expect.objectContaining(expectedResult[0]),
        expect.objectContaining(expectedResult[1]),
      ])
    );
  });

  it('downloads all fields when none specified', async () => {
    const expectedResult = [{ name: 'Alice', email: 'alice@example.com' }];
    await insertUsers(expectedResult);

    connector.fields = [];

    let result;
    pipeline.onload = (data) => {
      result = data;
    };

    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(
      expect.arrayContaining([
        expect.objectContaining(expectedResult[0]),
      ])
    );
  });

  it('handles custom query for custom_query endpoint', async () => {
    const db = realClient.db(mongoDatabase);
    await db.dropCollection('temptable').catch(() => {});
    await db.createCollection('temptable');

    const expectedResult = [{ name: 'Charlie' }];
    await db.collection('temptable').insertOne({ name: 'Charlie', age: 30 });

    connector.endpoint_id = 'custom_query';
    connector.config!.custom_query = '{"age": {"$gt": 25}}';
    connector.config!.collection = 'temptable';

    let result;
    pipeline.onload = (data) => {
      result = data;
    };

    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(
      expect.arrayContaining([
        expect.objectContaining(expectedResult[0]),
      ])
    );
  });

  it('uploads data successfully', async () => {
    connector.endpoint_id = 'collection_insert';

    const data = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];

    pipeline.data = data;
    pipeline.target = connector;
    delete pipeline.source;

    await orchestrator.runPipeline(pipeline);

    const db = realClient.db(mongoDatabase);
    const result = await db.collection('users').find({}).toArray();

    expect(result).toEqual(
      expect.arrayContaining([
        expect.objectContaining(data[0]),
        expect.objectContaining(data[1]),
      ])
    );
  });

  it('download and upload data successfully', async () => {
    const expectedResult = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];

    await insertUsers(expectedResult);

    const db = realClient.db(mongoDatabase);
    await db.dropCollection('test').catch(() => {});
    await db.createCollection('test');

    pipeline.target = {
      id: "mongo-users-connector2",
      adapter_id: "mongodb",
      endpoint_id: "collection_insert",
      credential_id: "mongo-auth",
      config: { database: mongoDatabase, collection: "test" },
      fields: ['name', 'email'],
      transform: [],
      pagination: { itemsPerPage: 10 },
    };

    await orchestrator.runPipeline(pipeline);

    const sourceDocs = await db.collection('users').find({}).toArray();
    const targetDocs = await db.collection('test').find({}).toArray();

    expect(targetDocs).toEqual(
      expect.arrayContaining(sourceDocs.map(doc =>
        expect.objectContaining({ name: doc.name, email: doc.email })
      ))
    );
  });
});