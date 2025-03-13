import { mysql as mysqlAdapter } from '../src/index';
import mysql from 'mysql2/promise';
import { Connector, Vault, Pipeline } from '../../../src/types';
import { Orchestrator } from '../../../src/index';

describe('MySQL Adapter Integration Tests', () => {
  let connector: Connector;
  let pipeline: Pipeline;

  const adapters = { mysql: mysqlAdapter };

  const mysqlUser = "mysql1";
  const mysqlPassword = "mysql2";
  const mysqlDatabase = "mysql";

  let vault: Vault = {
    'mysql-auth': {
      id: 'mysql-auth',
      name: 'MySQL',
      environment: 'development',
      type: 'basic',
      credentials: {
        host: 'localhost',
        username: mysqlUser,
        password: mysqlPassword,
        database: mysqlDatabase,
        port: '3307',
      },
    },
  };

  let orchestrator = Orchestrator(vault, adapters);

  let pool = mysql.createPool({
    host: 'localhost',
    user: mysqlUser,
    password: mysqlPassword,
    database: mysqlDatabase,
    port: 3307,
  });

  const insertUsers = async (newUsers: { email: string; name: string; status?: string }[]) => {
    const query = 'INSERT INTO users (email, name, status) VALUES ' +
      newUsers.map(() => '(?, ?, ?)').join(', ');
    const values = newUsers.flatMap(u => [u.email, u.name, u.status || 'active']);
    await pool.execute(query, values);
  };

  beforeEach(async () => {
    try {
      await pool.execute('DROP TABLE IF EXISTS users');
      await pool.execute('DROP TABLE IF EXISTS temptable');
      await pool.execute('DROP TABLE IF EXISTS test');
    } catch (error) {}

    await pool.execute(`
      CREATE TABLE users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255) NOT NULL,
        status VARCHAR(50) DEFAULT 'active'
      )
    `);

    connector = {
      id: 'mysql-users-connector',
      adapter_id: 'mysql',
      endpoint_id: 'table_query',
      credential_id: 'mysql-auth',
      config: { database: mysqlDatabase, table: 'users' },
      fields: ['id', 'name', 'email'],
      filters: [{ field: 'status', operator: '=', value: 'active' }],
      sort: [{ type: 'asc', field: 'name' }],
      transform: [],
      pagination: { itemsPerPage: 10 },
    };

    pipeline = {
      id: 'test-download',
      source: connector,
      error_handling: { max_retries: 3, retry_interval: 300, fail_on_error: false },
      rate_limiting: { requests_per_second: 1, max_retries_on_rate_limit: 3 },
    };
  });

  afterAll(async () => {
    try {
      await pool.execute('DROP TABLE IF EXISTS users');
      await pool.execute('DROP TABLE IF EXISTS temptable');
      await pool.execute('DROP TABLE IF EXISTS test');
    } catch (error) {}
    await pool.end();
  });

  it('downloads data with filters, sorting, and pagination', async () => {
    const expectedResult = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];
    await insertUsers(expectedResult);

    let result: any;
    pipeline.onload = (data) => { result = data; };
    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(expect.arrayContaining([
      expect.objectContaining(expectedResult[0]),
      expect.objectContaining(expectedResult[1]),
    ]));
  });

  it('downloads all fields when none specified', async () => {
    const expectedResult = [{ name: 'Alice', email: 'alice@example.com' }];
    await insertUsers(expectedResult);

    connector.fields = [];
    let result: any;
    pipeline.onload = (data) => { result = data; };
    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(expect.arrayContaining([
      expect.objectContaining(expectedResult[0]),
    ]));
  });

  it('handles custom query for custom_query endpoint', async () => {
    await pool.execute('CREATE TABLE temptable (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255))');
    await pool.execute("INSERT INTO temptable (name) VALUES ('Charlie')");

    connector.endpoint_id = 'custom_query';
    connector.config!.custom_query = 'SELECT * FROM temptable';

    let result: any;
    pipeline.onload = (data) => { result = data; };
    await orchestrator.runPipeline(pipeline);

    expect(result).toEqual(expect.arrayContaining([
      expect.objectContaining({ name: 'Charlie' }),
    ]));
  });

  it('uploads data successfully', async () => {
    connector.endpoint_id = 'table_insert';
    connector.fields.shift();

    const data = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];
    pipeline.data = data;
    pipeline.target = connector;
    delete pipeline.source;

    await orchestrator.runPipeline(pipeline);

    const result = await pool.execute('SELECT name, email FROM users');
    expect((result[0] as any)).toEqual(expect.arrayContaining([
      expect.objectContaining(data[0]),
      expect.objectContaining(data[1]),
    ]));
  });

  it('downloads and uploads data successfully', async () => {
    const expectedResult = [
      { name: 'Alice', email: 'alice@example.com' },
      { name: 'Bob', email: 'bob@example.com' },
    ];
    await insertUsers(expectedResult);

    await pool.execute(`
      CREATE TABLE test (
        id INT AUTO_INCREMENT PRIMARY KEY,
        email VARCHAR(255) NOT NULL UNIQUE,
        name VARCHAR(255) NOT NULL,
        status VARCHAR(50) DEFAULT 'active'
      )
    `);

    pipeline.target = {
      id: 'mysql-users-connector2',
      adapter_id: 'mysql',
      endpoint_id: 'table_insert',
      credential_id: 'mysql-auth',
      config: { database: mysqlDatabase, table: 'test' },
      fields: ['id', 'name', 'email'],
      transform: [],
      pagination: { itemsPerPage: 10 },
    };

    await orchestrator.runPipeline(pipeline);

    const usersRows = (await pool.execute('SELECT * FROM users'))[0];
    const testRows = (await pool.execute('SELECT * FROM test'))[0];
    expect(usersRows).toEqual(testRows);
  });
});