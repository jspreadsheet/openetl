import { postgresql } from '../src/index'; // Adjust path as needed
import pg from 'pg';
import { Connector, Vault, Pipeline } from '../../../src/types';
import { Orchestrator } from '../../../src/index'

const { Pool } = pg;

describe('PostgreSQL Adapter integration tests', () => {
  let connector: Connector;
  let pipeline: Pipeline;

  const adapters = { postgresql };

  const postgreUser = "postgres1";
  const postgrePassword = "postgres2";
  const postgreDatabase = "postgres";

  let vault: Vault = {
      'pg-auth': {
          id: 'pg-auth',
          name: 'Postgresql',
          environment: 'production',
          type: 'basic',
          credentials: {
              host: "localhost",
              username: postgreUser,
              password: postgrePassword,
              database: postgreDatabase,
              port: "5403",
          }
      }
  };

  let orchestrator = Orchestrator(vault, adapters);

  const insertUsers = async function(newUsers: {email: string; name: string; status?: string}[]) {
    let query = 'INSERT INTO users(email, name, status) VALUES ';

    const substitutions: any[] = []

    const texts = newUsers.map((newUser) => {
      let text = '('

      substitutions.push(newUser.email);
      text += '$' + substitutions.length;

      substitutions.push(newUser.name);
      text += ', $' + substitutions.length;

      substitutions.push(newUser.status || 'active');
      text += ', $' + substitutions.length;

      return text + ')';
    });

    query += texts.join(',') + ';';

    await pool.query(query, substitutions);
  }

  let pool = new Pool({
    host: "localhost",
    user: postgreUser,
    password: postgrePassword,
    database: postgreDatabase,
    port: 5403,
  });

  beforeEach(async () => {
    try {
      await pool.query('DROP TABLE users;');
    } catch (error) {}

    await pool.query(`
      CREATE TABLE public.users
      (
          id serial NOT NULL,
          email text NOT NULL,
          name text NOT NULL,
          status text DEFAULT 'active',
          CONSTRAINT users_pkey PRIMARY KEY (id),
          CONSTRAINT users_email_key UNIQUE (email)
      );
    `);

    connector = {
      id: "pg-customers-connector",
      adapter_id: "postgresql",
      endpoint_id: "table_query",
      credential_id: "pg-auth",
      config: { schema: "public", table: "users" },
      fields: ['name', 'email'],
      filters: [{
        field: 'status',
        operator: '=',
        value: 'active',
      }],
      sort: [{ type: 'asc', field: 'name' }],
      transform: [],
      pagination: { type: 'offset', itemsPerPage: 10 },
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
    await pool.end()
  })

  it('downloads data with filters, sorting, and pagination', async () => {
    const expectedResult = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];

    await insertUsers(expectedResult);

    let result;
    pipeline.onload = (data) => {
      result = data;
    }

    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(
      [
        expect.objectContaining(expectedResult[0]),
        expect.objectContaining(expectedResult[1]),
      ]
    );
  });

  it('downloads all fields when none specified', async () => {
    const expectedResult = [{ name: 'Alice', email: 'alice@example.com' }];
    await insertUsers(expectedResult);

    connector.fields = [];

    let result;
    pipeline.onload = (data) => {
      result = data;
    }

    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(
      [
        expect.objectContaining(expectedResult[0]),
      ]
    );
  });

  it('handles custom query for custom_query endpoint', async () => {
    try {
      await pool.query('DROP TABLE temptable;');
    } catch (error) {}

    await pool.query(`
      CREATE TABLE temptable
      (
          id serial NOT NULL,
          name text NOT NULL,
          CONSTRAINT temptable_pkey PRIMARY KEY (id)
      );
    `);

    const expectedResult = [{ name: 'Charlie' }];

    await pool.query(`
      INSERT INTO temptable(name) VALUES ('${expectedResult[0].name}')
    `);

    connector.endpoint_id = 'custom_query';
    connector.config!.custom_query = 'SELECT * FROM temptable';

    let result;
    pipeline.onload = (data) => {
      result = data;
    }

    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(
      [
        expect.objectContaining(expectedResult[0]),
      ]
    );
  });

  it('uploads data successfully', async () => {
    connector.endpoint_id = 'table_insert';

    const data = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];

    pipeline.data = data;
    pipeline.target = connector;
    delete pipeline.source;

    await orchestrator.runPipeline(pipeline);

    const result = await pool.query(`
      SELECT *
      FROM users
    `);

    expect(result.rows).toEqual(
      [
        expect.objectContaining(data[0]),
        expect.objectContaining(data[1]),
      ]
    );
  });
});