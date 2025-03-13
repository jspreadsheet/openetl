import { mongodb } from './../src/index';
import { MongoClient } from "mongodb";
import { Connector, Orchestrator } from "../../../dist";
import { Pipeline, Vault } from "../../../dist/types";

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
        fail_on_error: false,
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
	});

  afterEach(async () => {
    if (realClient) {
      await realClient.close();
    }
  })

	it("uploads an array of data objects successfully", async () => {
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

  /*
	it("handles empty array upload without error", async () => {
		pipeline.data = [];

		await orchestrator.runPipeline(pipeline);

		const db = realClient.db(mongoDatabase);
		const result = await db.collection("users").find({}).toArray();

		expect(result.length).toBe(0); // No documents inserted
	}, 10000);

	it("uploads single object with nested fields", async () => {
		const nestedData = {
			name: "David",
			email: "david@example.com",
			address: { city: "Tokyo" },
		};
		pipeline.data = nestedData;

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
	}, 10000);

	it("uploads array with mixed data (nested and flat)", async () => {
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
	}, 10000);

	it("respects fields configuration for single upload", async () => {
		connector.fields = ["name"]; // Only include name
		const dataWithExtra = {
			name: "Grace",
			email: "grace@example.com",
			extra: "ignored",
		};
		pipeline.data = dataWithExtra;

		await orchestrator.runPipeline(pipeline);

		const db = realClient.db(mongoDatabase);
		const result = await db.collection("users").find({}).toArray();

		expect(result.length).toBe(1);
		expect(result[0]).toHaveProperty("name", "Grace");
		expect(result[0]).not.toHaveProperty("email");
		expect(result[0]).not.toHaveProperty("extra");
	}, 10000);

	it("handles upload failure with error handling disabled", async () => {
		// Simulate failure by using an invalid collection name
		connector.config = {
			database: mongoDatabase,
			collection: "nonexistent_collection",
		};
		pipeline.data = { name: "Hank", email: "hank@example.com" };
		pipeline.error_handling = {
			max_retries: 0,
			retry_interval: 1000,
			fail_on_error: true,
		};

		await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow();
	}, 10000);
  */
});
