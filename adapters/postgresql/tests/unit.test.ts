import { postgresql } from '../src/index'; // Adjust path as needed
import pg from 'pg';
import { Connector, AuthConfig, AdapterInstance } from '../../../src/types';
import { Filter } from 'openetl';

jest.mock('pg');

const { Pool } = pg as jest.Mocked<typeof pg>;

describe('PostgreSQL Adapter', () => {
  let connector: Connector;
  let auth: AuthConfig;
  let adapter: AdapterInstance;
  let mockPool: any;

  beforeEach(() => {
    jest.clearAllMocks();

    // Mock connector configuration
    connector = {
      id: 'postgres-table-connector',
      adapter_id: 'postgres',
      endpoint_id: 'table_query',
      credential_id: 'pg-auth',
      config: {
        schema: 'public',
        table: 'users',
      },
      fields: ['id', 'name', 'email'],
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
      id: 'pg-auth',
      type: 'basic',
      credentials: {
        username: 'test_user',
        password: 'test_pass',
        host: 'localhost',
        database: 'test_db',
        port: '5432',
      },
    };

    // Mock Pool instance
    mockPool = {
      connect: jest.fn().mockResolvedValue({
        query: jest.fn().mockResolvedValue({ rows: [] }),
        release: jest.fn(),
      }),
      query: jest.fn(),
      end: jest.fn().mockResolvedValue(undefined),
    };
    (Pool as unknown as jest.Mock).mockImplementation(() => mockPool);

    // Create adapter instance
    adapter = postgresql(connector, auth);
  });

  /**
   * Test 1: Connection
   * Ensures the adapter can connect to the PostgreSQL database.
   */
  it('connects successfully with valid credentials', async () => {
    const mockResult = {
      query: jest.fn().mockResolvedValue({ rows: [1] }),
      release: jest.fn(),
    };

    mockPool.connect.mockResolvedValueOnce(mockResult);

    await expect(adapter.connect!()).resolves.toBeUndefined();
    expect(mockPool.connect).toHaveBeenCalled();
    expect(mockResult.query).toHaveBeenCalledWith('SELECT 1');
    expect(mockResult.release).toHaveBeenCalled();
  });

  it('throws error on connection failure', async () => {
    mockPool.connect.mockRejectedValueOnce(new Error('Connection refused'));
    await expect(adapter.connect!()).rejects.toThrow('Failed to connect to PostgreSQL: Connection refused');
  });

  /**
   * Test 2: Query Construction and Download with Parameterized Queries
   * Tests SQL query building with filters, sorting, and pagination using secure parameterized queries.
   */
  it('downloads data with filters, sorting, and pagination using parameterized queries', async () => {
    adapter.connect!();

    const mockRows = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await adapter.download({ limit: 2, offset: 0 });

    // Verify parameterized query format
    expect(mockPool.query).toHaveBeenCalledWith(
      `SELECT "id", "name", "email" FROM "public"."users" WHERE "status" = $1 ORDER BY "name" ASC LIMIT $2 OFFSET $3`,
      ['active', 2, 0]
    );
    expect(result.data).toEqual(mockRows);
  });

  it('downloads all fields when none specified', async () => {
    const connectorNoFields = { ...connector, fields: [] };
    const adapterNoFields = postgresql(connectorNoFields, auth);

    adapterNoFields.connect!();

    const mockRows = [{ id: 1, name: 'Alice', email: 'alice@example.com' }];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await adapterNoFields.download({ limit: 1, offset: 0 });

    // Verify parameterized query format with SELECT *
    expect(mockPool.query).toHaveBeenCalledWith(
      `SELECT * FROM "public"."users" WHERE "status" = $1 ORDER BY "name" ASC LIMIT $2 OFFSET $3`,
      ['active', 1, 0]
    );
    expect(result.data).toEqual(mockRows);
  });

  it('handles custom query for custom_query endpoint', async () => {
    const customConnector = {
      ...connector,
      endpoint_id: 'custom_query',
      config: { ...connector.config, custom_query: 'SELECT * FROM custom_table WHERE id > 5' },
    };
    const customAdapter = postgresql(customConnector, auth);

    customAdapter.connect!();

    const mockRows = [{ id: 6, name: 'Charlie' }];
    mockPool.query.mockResolvedValueOnce({}); // SET search_path
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await customAdapter.download({ limit: 10, offset: 0 });

    // First call should be SET search_path with parameterized value
    expect(mockPool.query).toHaveBeenNthCalledWith(1, 'SET search_path TO $1', ['public']);
    // Second call should be the custom query
    expect(mockPool.query).toHaveBeenNthCalledWith(2, 'SELECT * FROM custom_table WHERE id > 5', []);
    expect(result.data).toEqual(mockRows);
  });

  /**
   * Test 3: Upload with Parameterized Queries
   * Tests inserting data into a table using secure parameterized queries.
   */
  it('uploads data successfully with parameterized queries', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = postgresql(uploadConnector, auth);

    uploadAdapter.connect!();

    const data = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];

    mockPool.query.mockResolvedValueOnce({ rowCount: 2 });

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();

    // Verify parameterized insert query
    expect(mockPool.query).toHaveBeenCalledWith(
      `INSERT INTO "public"."users" ("id", "name", "email") VALUES ($1, $2, $3), ($4, $5, $6)`,
      [1, 'Alice', 'alice@example.com', 2, 'Bob', 'bob@example.com']
    );
  });

  it('handles null values in upload with parameterized queries', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = postgresql(uploadConnector, auth);

    uploadAdapter.connect!();

    const data = [{ id: 1, name: null, email: 'alice@example.com' }];

    mockPool.query.mockResolvedValueOnce({ rowCount: 1 });

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();

    // Verify parameterized insert with null value
    expect(mockPool.query).toHaveBeenCalledWith(
      `INSERT INTO "public"."users" ("id", "name", "email") VALUES ($1, $2, $3)`,
      [1, null, 'alice@example.com']
    );
  });

  /**
   * Test 4: Error Handling
   * Tests invalid endpoint usage and missing configuration.
   */
  it('throws error when downloading with table_insert endpoint', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = postgresql(uploadConnector, auth);

    await expect(uploadAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      'table_insert endpoint don\'t support download'
    );
  });

  it('throws error when table is missing', async () => {
    const invalidConnector = { ...connector, config: {} };
    const invalidAdapter = postgresql(invalidConnector, auth);

    await expect(invalidAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "table property is required on the PostgreSQL adapter's table_query endpoint"
    );
  });

  /**
   * Test 5: Disconnect
   * Ensures the connection pool is closed properly.
   */
  it('disconnects successfully', async () => {
    adapter.connect!();

    await expect(adapter.disconnect!()).resolves.toBeUndefined();
    expect(mockPool.end).toHaveBeenCalled();
  });

  it('throws error on disconnect failure', async () => {
    adapter.connect!();

    mockPool.end.mockRejectedValueOnce(new Error('Pool closure failed'));
    await expect(adapter.disconnect!()).rejects.toThrow('Pool closure failed');
  });

  /**
   * Test 6: Complex Filters with Parameterized Queries
   * Tests handling of multiple filters with secure parameterized queries.
   */
  it('builds query with multiple filters using parameterized queries', async () => {
    const filters: Filter[] = [
      { field: 'status', operator: '=', value: 'active' },
      { field: 'role', operator: '=', value: 'admin' },
    ];

    const connectorWithFilterGroup = {
      ...connector,
      filters,
    };
    const adapterWithFilterGroup = postgresql(connectorWithFilterGroup, auth);

    adapterWithFilterGroup.connect!();

    const mockRows = [{ id: 1, name: 'Admin', email: 'admin@example.com' }];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await adapterWithFilterGroup.download({ limit: 1, offset: 0 });

    // Verify parameterized query with multiple filters
    expect(mockPool.query).toHaveBeenCalledWith(
      `SELECT "id", "name", "email" FROM "public"."users" WHERE "status" = $1 AND "role" = $2 ORDER BY "name" ASC LIMIT $3 OFFSET $4`,
      ['active', 'admin', 1, 0]
    );
    expect(result.data).toEqual(mockRows);
  });

  /**
   * Test 7: SQL Injection Protection
   * Tests that identifiers are properly escaped to prevent SQL injection.
   */
  it('escapes identifiers to prevent SQL injection', async () => {
    const maliciousConnector = {
      ...connector,
      config: {
        schema: 'public"; DROP TABLE users; --',
        table: 'users',
      },
    };
    const maliciousAdapter = postgresql(maliciousConnector, auth);

    maliciousAdapter.connect!();

    const mockRows = [{ id: 1 }];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    await maliciousAdapter.download({ limit: 1, offset: 0 });

    // Verify that double quotes in identifier are escaped
    expect(mockPool.query).toHaveBeenCalledWith(
      expect.stringContaining('"public""; DROP TABLE users; --"'),
      expect.any(Array)
    );
  });

  it('validates operators to prevent injection', async () => {
    const maliciousConnector = {
      ...connector,
      filters: [{
        field: 'status',
        operator: '; DROP TABLE users; --',
        value: 'active',
      }],
    };
    const maliciousAdapter = postgresql(maliciousConnector, auth);

    await expect(maliciousAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "Invalid SQL operator: '; DROP TABLE users; --'"
    );
  });

  it('validates sort direction', async () => {
    const maliciousConnector = {
      ...connector,
      sort: [{ field: 'name', type: '; DROP TABLE users; --' as any }],
    };
    const maliciousAdapter = postgresql(maliciousConnector, auth);

    await expect(maliciousAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "Invalid sort direction: '; DROP TABLE users; --'"
    );
  });

  /**
   * Test 8: IN operator with array values
   */
  it('handles IN operator with array values', async () => {
    const connectorWithIn = {
      ...connector,
      filters: [{
        field: 'status',
        operator: 'IN',
        value: ['active', 'pending'],
      }],
    };
    const adapterWithIn = postgresql(connectorWithIn, auth);

    adapterWithIn.connect!();

    const mockRows = [{ id: 1, name: 'Alice' }];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    await adapterWithIn.download({ limit: 10, offset: 0 });

    expect(mockPool.query).toHaveBeenCalledWith(
      expect.stringContaining('IN ($1, $2)'),
      expect.arrayContaining(['active', 'pending'])
    );
  });

  /**
   * Test 9: Table columns endpoint with parameterized query
   */
  it('uses parameterized query for table_columns endpoint', async () => {
    const columnsConnector = {
      ...connector,
      endpoint_id: 'table_columns',
    };
    const columnsAdapter = postgresql(columnsConnector, auth);

    columnsAdapter.connect!();

    const mockRows = [
      { column_name: 'id', data_type: 'integer' },
      { column_name: 'name', data_type: 'varchar' },
    ];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await columnsAdapter.download({ limit: 100, offset: 0 });

    expect(mockPool.query).toHaveBeenCalledWith(
      'SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2',
      ['public', 'users']
    );
    expect(result.data).toEqual(mockRows);
  });
});
