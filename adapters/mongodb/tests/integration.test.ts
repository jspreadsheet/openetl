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
      pagination: { itemsPerPage: 10 }
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

  afterEach(async () => {
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

  it('Download: data with filters, sorting, and pagination', async () => {
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

  it('Download: all fields when none specified', async () => {
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

  it('Download: handles custom query for custom_query endpoint', async () => {
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

  it('Download: uploads data successfully', async () => {
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

  it('Download: and upload data successfully', async () => {
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


  describe('Download: Filtering with Simple Conditions', () => {
    const sampleUsers = [
      { name: 'Alice', email: 'alice@example.com', age: 20 },
      { name: 'Bob', email: 'bob@example.com', age: 25 },
      { name: 'Charlie', email: 'charlie@example.com', age: 30 },
      { name: 'David', email: 'david@example.com', age: 35 },
    ];

    it('Download: filters data with equality condition on numerical field', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'age', operator: '=', value: 25 }];

      let result;
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com' })
        ])
      );
    });

    it('Download: filters data with greater than condition', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'age', operator: '>', value: 25 }];

      let result;
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Charlie', email: 'charlie@example.com' }),
          expect.objectContaining({ name: 'David', email: 'david@example.com' })
        ])
      );
    });

    it('Download: filters data with less than or equal condition', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'age', operator: '<=', value: 25 }];

      let result;
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com' }),
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com' })
        ])
      );
    });

    it('Download: filters data with not equal condition', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'age', operator: '!=', value: 25 }];

      let result;
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com' }),
          expect.objectContaining({ name: 'Charlie', email: 'charlie@example.com' }),
          expect.objectContaining({ name: 'David', email: 'david@example.com' })
        ])
      );
    });

    it('Download: filters data with equality condition on string field', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'name', operator: '=', value: 'Bob' }];

      let result;
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com' })
        ])
      );
    });
  });

  describe('Download: Filtering with Filter Groups', () => {
    const sampleUsers = [
      { name: 'Alice', email: 'alice@example.com', age: 20, status: 'active' },
      { name: 'Bob', email: 'bob@example.com', age: 25, status: 'inactive' },
      { name: 'Charlie', email: 'charlie@example.com', age: 30, status: 'active' },
      { name: 'David', email: 'david@example.com', age: 35, status: 'inactive' },
    ];

    it('Download: filters data with AND operation in filter group', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{
        op: 'AND',
        filters: [
          { field: 'age', operator: '>', value: 25 },
          { field: 'status', operator: '=', value: 'inactive' }
        ]
      }];

      let result: any[] = [];
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([  
          expect.objectContaining({ name: 'David', email: 'david@example.com' })
        ])
      );
      expect(result.length).toBe(1); // Ensure only one match
    });

    it('Download: filters data with OR operation in filter group', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{
        op: 'OR',
        filters: [
          { field: 'age', operator: '<=', value: 20 },
          { field: 'status', operator: '=', value: 'inactive' }
        ]
      }];

      let result: any[] = [];
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com' }),
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com' }),
          expect.objectContaining({ name: 'David', email: 'david@example.com' })
        ])
      );
      expect(result.length).toBe(3); // Ensure three matches
    });

    it('Download: filters data with nested AND within OR operation', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{
        op: 'OR',
        filters: [
          { field: 'age', operator: '<=', value: 20 },
          {
            op: 'AND',
            filters: [
              { field: 'age', operator: '>', value: 25 },
              { field: 'status', operator: '=', value: 'active' }
            ]
          }
        ]
      }];

      let result: any[] = [];
      pipeline.onload = (data) => {
        result = data;
      };

      await orchestrator.runPipeline(pipeline);

      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com' }),
          expect.objectContaining({ name: 'Charlie', email: 'charlie@example.com' })
        ])
      );
      expect(result.length).toBe(2); // Ensure two matches
    });
  });

  describe('Download: Projection', () => {
    const sampleUsers = [
      { name: 'Alice', email: 'alice@example.com', age: 20, status: 'active' },
      { name: 'Bob', email: 'bob@example.com', age: 25, status: 'inactive' },
    ];
  
    it('Download: returns only specified fields when fields are provided', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'status', operator: '=', value: 'active' }];
      connector.fields = ['name']; // Project only the 'name' field
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Alice' })
        ])
      );
      expect(result!.length).toBe(1);
      expect(result![0]).toHaveProperty('name');
      // it should be possible to remove the _id from the response?
      // if so, we will need to change buildProjection implementation
      // expect(result![0]).not.toHaveProperty('_id');
      expect(result![0]).not.toHaveProperty('email'); // Ensure 'email' is excluded
      expect(result![0]).not.toHaveProperty('age');   // Ensure 'age' is excluded
      expect(result![0]).not.toHaveProperty('status'); // Ensure 'status' is excluded
    });
  
    it('Download: returns all fields when no fields are specified', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'status', operator: '=', value: 'active' }];
      connector.fields = []; // No projection
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            name: 'Alice',
            email: 'alice@example.com',
            age: 20,
            status: 'active'
          })
        ])
      );
      expect(result!.length).toBe(1);
      expect(result![0]).toHaveProperty('name');
      expect(result![0]).toHaveProperty('email'); // Ensure 'email' is included
      expect(result![0]).toHaveProperty('age');   // Ensure 'age' is included
      expect(result![0]).toHaveProperty('status'); // Ensure 'status' is included
    });
  
    it('Download: returns multiple specified fields correctly', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [{ field: 'status', operator: '=', value: 'active' }];
      connector.fields = ['name', 'email']; // Project 'name' and 'email'

      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            name: 'Alice',
            email: 'alice@example.com'
          })
        ])
      );
      expect(result!.length).toBe(1);
      expect(result![0]).toHaveProperty('name');   // Ensure 'name' is included
      expect(result![0]).toHaveProperty('email');  // Ensure 'email' is included
      expect(result![0]).not.toHaveProperty('age');    // Ensure 'age' is excluded
      expect(result![0]).not.toHaveProperty('status'); // Ensure 'status' is excluded
    });
  });

  describe('Download: Sorting', () => {
    const sampleUsers = [
      { name: 'Charlie', email: 'charlie@example.com', age: 30 },
      { name: 'Alice', email: 'alice@example.com', age: 20 },
      { name: 'Bob', email: 'bob@example.com', age: 25 },
    ];
  
    it('Download: sorts data in ascending order by name', async () => {
      await insertUsers(sampleUsers);
      connector.filters = []; // No filters to get all records
      connector.fields = ['name', 'email']; // Project name and email
      connector.sort = [{ field: 'name', type: 'asc' }]; // Sort by name ascending
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com' }),
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com' }),
          expect.objectContaining({ name: 'Charlie', email: 'charlie@example.com' }),
        ])
      );
      expect(result!.length).toBe(3);
      // Verify exact order since arrayContaining doesn't guarantee it
      expect(result![0].name).toBe('Alice');
      expect(result![1].name).toBe('Bob');
      expect(result![2].name).toBe('Charlie');
    });
  
    it('Download: sorts data in descending order by name', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'desc' }]; // Sort by name descending
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Charlie', email: 'charlie@example.com' }),
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com' }),
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com' }),
        ])
      );
      expect(result!.length).toBe(3);
      expect(result![0].name).toBe('Charlie');
      expect(result![1].name).toBe('Bob');
      expect(result![2].name).toBe('Alice');
    });
  
    it('Download: sorts data in ascending order by age', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [];
      connector.fields = ['name', 'email', 'age']; // Include age in projection
      connector.sort = [{ field: 'age', type: 'asc' }]; // Sort by age ascending
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com', age: 20 }),
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com', age: 25 }),
          expect.objectContaining({ name: 'Charlie', email: 'charlie@example.com', age: 30 }),
        ])
      );
      expect(result!.length).toBe(3);
      expect(result![0].age).toBe(20);
      expect(result![1].age).toBe(25);
      expect(result![2].age).toBe(30);
    });
  
    it('Download: sorts data in descending order by age', async () => {
      await insertUsers(sampleUsers);
      connector.filters = [];
      connector.fields = ['name', 'email', 'age'];
      connector.sort = [{ field: 'age', type: 'desc' }]; // Sort by age descending
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'Charlie', email: 'charlie@example.com', age: 30 }),
          expect.objectContaining({ name: 'Bob', email: 'bob@example.com', age: 25 }),
          expect.objectContaining({ name: 'Alice', email: 'alice@example.com', age: 20 }),
        ])
      );
      expect(result!.length).toBe(3);
      expect(result![0].age).toBe(30);
      expect(result![1].age).toBe(25);
      expect(result![2].age).toBe(20);
    });
  });

  describe('Download: Pagination', () => {
    jest.setTimeout(10000);
    beforeEach(async () => {
      // Insert 100 documents with padded names for correct alphabetical sorting
      const users = Array.from({ length: 100 }, (_, i) => {
        const paddedIndex = String(i + 1).padStart(3, '0'); // e.g., 001, 002, ..., 100
        return {
          name: `User${paddedIndex}`,
          email: `user${paddedIndex}@example.com`,
          age: 20 + (i % 80),
        };
      });
      await insertUsers(users);
    });
  
    it('Download: returns first page of 20 documents with offset 0', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20 };
      connector.limit = 20;
      let result: any[] = [];
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline); // Offset defaults to 0 in getDataSerially
  
      expect(result!.length).toBe(20);
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'User001', email: 'user001@example.com' }),
          expect.objectContaining({ name: 'User020', email: 'user020@example.com' }),
        ])
      );
      expect(result![0].name).toBe('User001');
      expect(result![19].name).toBe('User020');
    });
  
    it('Download: returns second page of 20 documents with offset 20', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: '20' };
      connector.limit = 20;
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result!.length).toBe(20);
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'User021', email: 'user021@example.com' }),
          expect.objectContaining({ name: 'User040', email: 'user040@example.com' }),
        ])
      );
      expect(result![0].name).toBe('User021');  // First item of second page
      expect(result![19].name).toBe('User040'); // Last item of second page
    });
  
    it('Download: returns third page of 20 documents with offset 40', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: '40' };
      connector.limit = 20;
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result!.length).toBe(20);
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'User041', email: 'user041@example.com' }),
          expect.objectContaining({ name: 'User060', email: 'user060@example.com' }),
        ])
      );
      expect(result![0].name).toBe('User041');  // First item of third page
      expect(result![19].name).toBe('User060'); // Last item of third page
    });
  
    it('Download: returns last partial page with offset 80', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: '80' };
      connector.limit = 20;
  
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
  
      await orchestrator.runPipeline(pipeline);
  
      expect(result!.length).toBe(20); // Still 20 items, as 100 total fits 5 full pages
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'User081', email: 'user081@example.com' }),
          expect.objectContaining({ name: 'User100', email: 'user100@example.com' }),
        ])
      );
      expect(result![0].name).toBe('User081');   // First item of last page
      expect(result![19].name).toBe('User100'); // Last item of last page
    });

    it('Download: returns all items starting from page 3 (offset 40)', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: '40' };
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(60);
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'User041', email: 'user041@example.com' }),
          expect.objectContaining({ name: 'User100', email: 'user100@example.com' }),
        ])
      );
      expect(result![0].name).toBe('User041');
      expect(result![59].name).toBe('User100');
    });

    it('Download: returns empty result when offset exceeds total items', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: '100' };
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(0);
    });

    it('Download: returns all items when itemsPerPage exceeds dataset size', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 200 };
      connector.limit = 200;
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(100);
      expect(result![0].name).toBe('User001');
      expect(result![99].name).toBe('User100');
    });

    it('Download: handles zero items per page', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 0 };
      // No connector.limit to test default behavior
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      // Expect all items since limit: 0 means no limit in MongoDB
      expect(result!.length).toBe(100);
      expect(result![0].name).toBe('User001');
      expect(result![99].name).toBe('User100');
    });

    it('Download: returns partial last page with offset 90', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: '90' };
      connector.limit = 20;
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(10);
      expect(result).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: 'User091', email: 'user091@example.com' }),
          expect.objectContaining({ name: 'User100', email: 'user100@example.com' }),
        ])
      );
      expect(result![0].name).toBe('User091');
      expect(result![9].name).toBe('User100');
    });

    it('Download: respects total items limit smaller than page size', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20 };
      connector.limit = 5; // Fetch only 5 items
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(5);
      expect(result![0].name).toBe('User001');
      expect(result![4].name).toBe('User005');
    });

    it('Download: handles invalid offset format by defaulting to 0', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: 'invalid' };
      connector.limit = 20;
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(20);
      expect(result![0].name).toBe('User001');
      expect(result![19].name).toBe('User020');
    });

    it('Download: treats negative offset as offset 0', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20, pageOffsetKey: '-20' };
      connector.limit = 20;
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(20);
      expect(result![0].name).toBe('User001');
      expect(result![19].name).toBe('User020');
    });

  });


  describe('Download: Pagination - Empty Collection', () => {
    it('Download: handles empty dataset with offset 0', async () => {
      connector.filters = [];
      connector.fields = ['name', 'email'];
      connector.sort = [{ field: 'name', type: 'asc' }];
      connector.pagination = { itemsPerPage: 20 };
      connector.limit = 20;
    
      let result: any[] | null = null;
      pipeline.onload = (data) => {
        result = data;
      };
    
      await orchestrator.runPipeline(pipeline);
    
      expect(result!.length).toBe(0);
    });
  });
  
});