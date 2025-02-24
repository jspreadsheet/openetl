import { postgresql, PostgresqlAdapter } from '../src/index'; // Adjust path as needed
import pg from 'pg';
import { Connector, AuthConfig, AdapterInstance } from '../../../src/types';

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
      pagination: { type: 'offset', itemsPerPage: 10 },
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
    (Pool as jest.Mock).mockImplementation(() => mockPool);

    // Create adapter instance
    adapter = postgresql(connector, auth);
  });

  /**
   * Test 1: Connection
   * Ensures the adapter can connect to the PostgreSQL database.
   */
  it('connects successfully with valid credentials', async () => {
    mockPool.connect.mockResolvedValueOnce({
      query: jest.fn().mockResolvedValue({ rows: [1] }),
      release: jest.fn(),
    });

    await expect(adapter.connect()).resolves.toBeUndefined();
    expect(mockPool.connect).toHaveBeenCalled();
    expect(mockPool.connect().query).toHaveBeenCalledWith('SELECT 1');
  });

  it('throws error on connection failure', async () => {
    mockPool.connect.mockRejectedValueOnce(new Error('Connection refused'));
    await expect(adapter.connect()).rejects.toThrow('Failed to connect to PostgreSQL: Connection refused');
  });

  /**
   * Test 2: Query Construction and Download
   * Tests SQL query building with filters, sorting, and pagination.
   */
  it('downloads data with filters, sorting, and pagination', async () => {
    const mockRows = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await adapter.download({ limit: 2, offset: 0 });
    expect(mockPool.query).toHaveBeenCalledWith(
      `SELECT id, name, email FROM "public"."users" WHERE status = 'active' ORDER BY name ASC LIMIT 2 OFFSET 0`
    );
    expect(result.data).toEqual(mockRows);
  });

  it('downloads all fields when none specified', async () => {
    const connectorNoFields = { ...connector, fields: [] };
    const adapterNoFields = postgresql(connectorNoFields, auth);
    const mockRows = [{ id: 1, name: 'Alice', email: 'alice@example.com' }];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await adapterNoFields.download({ limit: 1, offset: 0 });
    expect(mockPool.query).toHaveBeenCalledWith(
      `SELECT * FROM "public"."users" WHERE status = 'active' ORDER BY name ASC LIMIT 1 OFFSET 0`
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
    const mockRows = [{ id: 6, name: 'Charlie' }];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await customAdapter.download({ limit: 10, offset: 0 });
    expect(mockPool.query).toHaveBeenCalledWith('SELECT * FROM custom_table WHERE id > 5');
    expect(result.data).toEqual(mockRows);
  });

  /**
   * Test 3: Upload
   * Tests inserting data into a table.
   */
  it('uploads data successfully', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = postgresql(uploadConnector, auth);
    const data = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];

    mockPool.query.mockResolvedValueOnce({ rowCount: 2 });

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();
    expect(mockPool.query).toHaveBeenCalledWith(
      `INSERT INTO "public"."users" ("id", "name", "email") VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')`
    );
  });

  it('handles null values in upload', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = postgresql(uploadConnector, auth);
    const data = [{ id: 1, name: null, email: 'alice@example.com' }];

    mockPool.query.mockResolvedValueOnce({ rowCount: 1 });

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();
    expect(mockPool.query).toHaveBeenCalledWith(
      `INSERT INTO "public"."users" ("id", "name", "email") VALUES (1, NULL, 'alice@example.com')`
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
      'Table_insert endpoint only supported for upload'
    );
  });

  it('throws error when schema or table is missing', async () => {
    const invalidConnector = { ...connector, config: {} };
    const invalidAdapter = postgresql(invalidConnector, auth);

    await expect(invalidAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      'Schema and table required for table-based endpoints'
    );
  });

  /**
   * Test 5: Disconnect
   * Ensures the connection pool is closed properly.
   */
  it('disconnects successfully', async () => {
    await expect(adapter.disconnect!()).resolves.toBeUndefined();
    expect(mockPool.end).toHaveBeenCalled();
  });

  it('throws error on disconnect failure', async () => {
    mockPool.end.mockRejectedValueOnce(new Error('Pool closure failed'));
    await expect(adapter.disconnect!()).rejects.toThrow('Pool closure failed');
  });

  /**
   * Test 6: Complex Filters
   * Tests handling of filter groups.
   */
  it('builds query with filter groups', async () => {
    const connectorWithFilterGroup = {
      ...connector,
      filters: [{
        op: 'OR',
        filters: [
          { field: 'status', operator: '=', value: 'active' },
          { field: 'role', operator: '=', value: 'admin' },
        ],
      }],
    };
    const adapterWithFilterGroup = postgresql(connectorWithFilterGroup, auth);
    const mockRows = [{ id: 1, name: 'Admin', email: 'admin@example.com' }];
    mockPool.query.mockResolvedValueOnce({ rows: mockRows });

    const result = await adapterWithFilterGroup.download({ limit: 1, offset: 0 });
    expect(mockPool.query).toHaveBeenCalledWith(
      `SELECT id, name, email FROM "public"."users" WHERE (status = 'active' OR role = 'admin') ORDER BY name ASC LIMIT 1 OFFSET 0`
    );
    expect(result.data).toEqual(mockRows);
  });
});