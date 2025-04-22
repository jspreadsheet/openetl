import { mongodb } from './../src/index';
import { MongoClient, ObjectId } from "mongodb";
import { Connector, Orchestrator } from "../../../dist";
import { Pipeline, PipelineEvent, Vault } from "../../../dist/types";

describe("MongoDBAdapter Upload Method", () => {
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
		"mongo-auth": {
			id: "mongo-auth",
			name: "MongoDB",
			environment: "production",
			type: "basic",
			credentials: {
				host,
				username: mongoUser,
				password: mongoPassword,
				database: mongoDatabase,
				port: mongoPort,
			},
		},
	};

  let orchestrator = Orchestrator(vault, adapters);

	beforeEach(async () => {
    realClient = new MongoClient(`mongodb://${mongoUser}:${mongoPassword}@${host}:${mongoPort}/${mongoDatabase}`);
		const db = realClient.db(mongoDatabase);
		await db.dropCollection("users").catch(() => {});
		await db.createCollection("users");

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
      pagination: { itemsPerPage: 10 }
    };



		// Configure connector for upload
		connector.endpoint_id = "collection_insert";
		connector.config = { database: mongoDatabase, collection: "users" };
		connector.fields = ["name", "email"];
		connector.filters = []; // Reset filters
		connector.sort = []; // Reset sort
		connector.pagination = { itemsPerPage: 10 }; // Small page size


    pipeline = {
      id: "test-upload",
      target: connector,
      error_handling: {
        max_retries: 3,
        retry_interval: 300,
      },
      rate_limiting: {
        requests_per_second: 1,
        max_retries_on_rate_limit: 3,
      },
    };

	});

	afterEach(async () => {
		const db = realClient.db(mongoDatabase);
		await db.dropCollection("users").catch(() => {});
    await db.dropCollection('nonexistent_collection').catch(() => {});
	});

  afterEach(async () => {
    if (realClient) {
      await realClient.close();
    }
  })

	it("Upload: uploads an array of data objects successfully", async () => {
		const arrayData = [
			{ name: "Alice", email: "alice@example.com" },
			{ name: "Bob", email: "bob@example.com" },
			{ name: "Charlie", email: "charlie@example.com" },
		];
		pipeline.data = arrayData;

		await orchestrator.runPipeline(pipeline);

		const db = realClient.db(mongoDatabase);
		const result = await db.collection("users").find({}).toArray();

		expect(result.length).toBe(3);
		expect(result).toEqual(
			expect.arrayContaining([
				expect.objectContaining(arrayData[0]),
				expect.objectContaining(arrayData[1]),
				expect.objectContaining(arrayData[2]),
			])
		);
	});

  
	it("Upload: handles empty array upload without error", async () => {
		pipeline.data = [];

		await orchestrator.runPipeline(pipeline);

		const db = realClient.db(mongoDatabase);
		const result = await db.collection("users").find({}).toArray();

		expect(result.length).toBe(0); // No documents inserted
	});

  

	it("Upload: uploads single object with nested fields", async () => {
		const nestedData = {
			name: "David",
			email: "david@example.com",
			address: { city: "Tokyo" },
		};
		pipeline.data = [nestedData];

		await orchestrator.runPipeline(pipeline);

		const db = realClient.db(mongoDatabase);
		const result = await db.collection("users").find({}).toArray();

		expect(result.length).toBe(1);
		expect(result).toEqual(
			expect.arrayContaining([
				expect.objectContaining({
					name: "David",
					email: "david@example.com",
					address: { city: "Tokyo" },
				}),
			])
		);
	});

	it("Upload: uploads array with mixed data (nested and flat)", async () => {
		const mixedData = [
			{ name: "Eve", email: "eve@example.com" },
			{
				name: "Frank",
				email: "frank@example.com",
				address: { city: "Paris" },
			},
		];
		pipeline.data = mixedData;

		await orchestrator.runPipeline(pipeline);

		const db = realClient.db(mongoDatabase);
		const result = await db.collection("users").find({}).toArray();

		expect(result.length).toBe(2);
		expect(result).toEqual(
			expect.arrayContaining([
				expect.objectContaining(mixedData[0]),
				expect.objectContaining(mixedData[1]),
			])
		);
	});

	it("Upload: upload works even with non existent collection", async () => {
		connector.config = {
			database: mongoDatabase,
			collection: "nonexistent_collection",
		};
		pipeline.data = [{ name: "Hank", email: "hank@example.com" }];

    await orchestrator.runPipeline(pipeline);

    const db = realClient.db(mongoDatabase);
		const result = await db.collection("nonexistent_collection").find({}).toArray();

    expect(result.length).toBe(1);
		expect(result).toEqual(
			expect.arrayContaining([
				expect.objectContaining({ name: "Hank", email: "hank@example.com" })
			])
		);
	});

  it('Upload: fails on duplicate _id', async () => {
    const db = realClient.db(mongoDatabase);
    const testId = new ObjectId('1234567890abcdef12345678'); // 24-char hex string
    await db.collection('users').insertOne({ _id: testId, name: 'PreExisting' });

    pipeline.data = [{ _id: testId, name: 'Alice', email: 'alice@example.com' }];
    pipeline.error_handling = { max_retries: 0, retry_interval: 1000 };

    await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(/duplicate key/i);

    const result = await db.collection('users').find({}).toArray();
    expect(result.length).toBe(1); // Original remains
    expect(result[0].name).toBe('PreExisting');
  });

  it('Upload: fails on duplicate unique field (email)', async () => {
    const db = realClient.db(mongoDatabase);
    await db.collection('users').createIndex({ email: 1 }, { unique: true });
    await db.collection('users').insertOne({ name: 'PreExisting', email: 'alice@example.com' });
  
    pipeline.data = [{ name: 'Alice', email: 'alice@example.com' }];
    pipeline.error_handling = { max_retries: 0, retry_interval: 1000 };
  
    await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(/duplicate key/i);
  
    const result = await db.collection('users').find({}).toArray();
    expect(result.length).toBe(1);
    expect(result[0].name).toBe('PreExisting');
  });

  it('Upload: successfully inserts 5 items with itemsPerPage=1', async () => {
    // Prepare data: 5 items to upload
    const uploadData = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
      { name: 'Charlie', email: 'charlie@example.com' },
      { name: 'David', email: 'david@example.com' },
      { name: 'Eve', email: 'eve@example.com' },
    ];
    pipeline.data = uploadData;
  
    // Configure connector with itemsPerPage = 1
    connector.pagination = { itemsPerPage: 1 };
    connector.endpoint_id = 'collection_insert';
    connector.config = { database: mongoDatabase, collection: 'users' };
    connector.fields = ['name', 'email'];
    connector.filters = [];
    connector.sort = [];
  
    // Optional: Capture log events to verify batching (for extra assurance)
    const logEvents: PipelineEvent[] = [];
    pipeline.logging = (event) => logEvents.push(event);
  
    // Run the pipeline
    await orchestrator.runPipeline(pipeline);
  
    // Verify all 5 items are in the collection
    const db = realClient.db(mongoDatabase);
    const result = await db.collection('users').find({}).toArray();
  
    expect(result.length).toBe(5);
    expect(result).toEqual(
      expect.arrayContaining([
        expect.objectContaining(uploadData[0]),
        expect.objectContaining(uploadData[1]),
        expect.objectContaining(uploadData[2]),
        expect.objectContaining(uploadData[3]),
        expect.objectContaining(uploadData[4]),
      ])
    );
  
    // Optional: Verify 5 upload events occurred
    const uploadEvents = logEvents.filter(event => event.type === 'load');
    expect(uploadEvents.length).toBe(5);
    uploadEvents.forEach((event, index) => {
      expect(event.message).toMatch(`Uploaded batch at offset ${index}`);
      expect(event.dataCount).toBe(1);
    });
  });
});
