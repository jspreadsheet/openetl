import { mongodb } from './../src/index';
import { MongoClient } from 'mongodb';
import { Orchestrator } from '../../../src/index';
import { Connector, Pipeline, Vault } from '../../../src/types';

describe('MongoDB Adapter Integration Tests', () => {
  let connector: Connector;
  let pipeline: Pipeline;
  let realClient: MongoClient;

  const adapters = { mongodb };

  const mongoUser = "test";
  const mongoPassword = "test";
  const mongoDatabase = "admin";
  const mongoPort = "27000";
  const host = "localhost";

  let vault: Vault = {
    'mongo-auth': {
      id: 'mongo-auth',
      name: 'MongoDB',
      environment: 'production',
      type: 'basic',
      credentials: {
        host,
        username: mongoUser,
        password: mongoPassword,
        database: mongoDatabase,
        port: mongoPort
      }
    }
  };

  let orchestrator = Orchestrator(vault, adapters);

  beforeEach(async () => {
    realClient = new MongoClient(`mongodb://${mongoUser}:${mongoPassword}@${host}:${mongoPort}/${mongoDatabase}`);
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