import { mysql as mysqlAdapter, MySQLAdapter } from '../src/index'; // Rename adapter import
import mysql from 'mysql2/promise'; // Keep library as mysql
import { Connector, AuthConfig, AdapterInstance } from 'openetl';

jest.mock('mysql2/promise');

describe('MySQL Adapter', () => {
  let connector: Connector;
  let auth: AuthConfig;
  let adapter: AdapterInstance;
  let mockConnection: any;

  beforeEach(() => {
    jest.clearAllMocks();

    connector = {
      id: 'mysql-table-connector',
      adapter_id: 'mysql',
      endpoint_id: 'table_query',
      credential_id: 'mysql-auth',
      config: {
        database: 'test_db',
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

    auth = {
      id: 'mysql-auth',
      type: 'basic',
      credentials: {
        username: 'test_user',
        password: 'test_pass',
        host: 'localhost',
        database: 'test_db',
        port: '3306',
      },
    };

    mockConnection = {
      execute: jest.fn().mockResolvedValue([[]]),
      end: jest.fn().mockResolvedValue(undefined),
    };
    (mysql.createConnection as jest.Mock).mockResolvedValue(mockConnection);

    adapter = mysqlAdapter(connector, auth); // Use renamed adapter
  });

  it('connects successfully with valid credentials', async () => {
    mockConnection.execute.mockResolvedValueOnce([[{ 1: 1 }]]);
    await adapter.connect!();
    expect(mysql.createConnection).toHaveBeenCalledWith({
      user: 'test_user',
      password: 'test_pass',
      host: 'localhost',
      database: 'test_db',
      port: 3306,
    });
    expect(mockConnection.execute).toHaveBeenCalledWith('SELECT 1');
  });

  it('throws error on connection failure', async () => {
    (mysql.createConnection as jest.Mock).mockRejectedValueOnce(new Error('Connection refused'));
    await expect(adapter.connect!()).rejects.toThrow('Failed to connect to MySQL: Connection refused');
  });

  /**
   * Test: Query Construction and Download with Parameterized Queries
   */
  it('downloads data with filters, sorting, and pagination using parameterized queries', async () => {
    await adapter.connect!();
    const mockRows = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    const result = await adapter.download({ limit: 2, offset: 0 });

    // Verify parameterized query format
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "SELECT `id`, `name`, `email` FROM `users` WHERE `status` = ? ORDER BY `name` ASC LIMIT ?, ?",
      ['active', 0, 2]
    );
    expect(result.data).toEqual(mockRows);
  });

  it('downloads all fields when none specified', async () => {
    const connectorNoFields = { ...connector, fields: [] };
    const adapterNoFields = mysqlAdapter(connectorNoFields, auth);
    await adapterNoFields.connect!();

    const mockRows = [{ id: 1, name: 'Alice', email: 'alice@example.com' }];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    const result = await adapterNoFields.download({ limit: 1, offset: 0 });

    // Verify parameterized query with SELECT *
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "SELECT * FROM `users` WHERE `status` = ? ORDER BY `name` ASC LIMIT ?, ?",
      ['active', 0, 1]
    );
    expect(result.data).toEqual(mockRows);
  });

  it('handles custom query for custom_query endpoint', async () => {
    const customConnector = {
      ...connector,
      endpoint_id: 'custom_query',
      config: { ...connector.config, custom_query: 'SELECT * FROM custom_table WHERE id > 5' },
    };
    const customAdapter = mysqlAdapter(customConnector, auth);
    await customAdapter.connect!();

    const mockRows = [{ id: 6, name: 'Charlie' }];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    const result = await customAdapter.download({ limit: 10, offset: 0 });

    // Custom query should be passed as-is with empty values array
    expect(mockConnection.execute).toHaveBeenCalledWith(
      'SELECT * FROM custom_table WHERE id > 5',
      []
    );
    expect(result.data).toEqual(mockRows);
  });

  /**
   * Test: Upload with Parameterized Queries
   */
  it('uploads data successfully with parameterized queries', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = mysqlAdapter(uploadConnector, auth);
    await uploadAdapter.connect!();

    const data = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];
    mockConnection.execute.mockResolvedValueOnce([{ affectedRows: 2 }]);

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();

    // Verify parameterized insert query
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?), (?, ?, ?)",
      [1, 'Alice', 'alice@example.com', 2, 'Bob', 'bob@example.com']
    );
  });

  it('handles null values in upload with parameterized queries', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = mysqlAdapter(uploadConnector, auth);
    await uploadAdapter.connect!();

    const data = [{ id: 1, name: null, email: 'alice@example.com' }];
    mockConnection.execute.mockResolvedValueOnce([{ affectedRows: 1 }]);

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();

    // Verify parameterized insert with null value
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "INSERT INTO `users` (`id`, `name`, `email`) VALUES (?, ?, ?)",
      [1, null, 'alice@example.com']
    );
  });

  it('throws error when downloading with table_insert endpoint', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = mysqlAdapter(uploadConnector, auth);
    await uploadAdapter.connect!();

    await expect(uploadAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "table_insert endpoint don't support download"
    );
  });

  it('throws error when table is missing', async () => {
    const invalidConnector = { ...connector, config: {} };
    const invalidAdapter = mysqlAdapter(invalidConnector, auth);
    await invalidAdapter.connect!();

    await expect(invalidAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "table property is required on the MySQL adapter's table_query endpoint"
    );
  });

  it('disconnects successfully', async () => {
    await adapter.connect!();
    await expect(adapter.disconnect!()).resolves.toBeUndefined();
    expect(mockConnection.end).toHaveBeenCalled();
  });

  it('throws error on disconnect failure', async () => {
    await adapter.connect!();
    mockConnection.end.mockRejectedValueOnce(new Error('Connection closure failed'));
    await expect(adapter.disconnect!()).rejects.toThrow('Connection closure failed');
  });

  /**
   * Test: SQL Injection Protection
   */
  it('escapes identifiers to prevent SQL injection', async () => {
    const maliciousConnector = {
      ...connector,
      config: {
        ...connector.config,
        table: 'users`; DROP TABLE users; --',
      },
    };
    const maliciousAdapter = mysqlAdapter(maliciousConnector, auth);

    await maliciousAdapter.connect!();

    const mockRows = [{ id: 1 }];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    await maliciousAdapter.download({ limit: 1, offset: 0 });

    // Verify that backticks in identifier are escaped
    expect(mockConnection.execute).toHaveBeenCalledWith(
      expect.stringContaining('`users``; DROP TABLE users; --`'),
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
    const maliciousAdapter = mysqlAdapter(maliciousConnector, auth);

    await maliciousAdapter.connect!();

    await expect(maliciousAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "Invalid SQL operator: '; DROP TABLE users; --'"
    );
  });

  it('validates sort direction', async () => {
    const maliciousConnector = {
      ...connector,
      sort: [{ field: 'name', type: '; DROP TABLE users; --' as any }],
    };
    const maliciousAdapter = mysqlAdapter(maliciousConnector, auth);

    await maliciousAdapter.connect!();

    await expect(maliciousAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "Invalid sort direction: '; DROP TABLE users; --'"
    );
  });

  /**
   * Test: IN operator with array values
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
    const adapterWithIn = mysqlAdapter(connectorWithIn, auth);

    await adapterWithIn.connect!();

    const mockRows = [{ id: 1, name: 'Alice' }];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    await adapterWithIn.download({ limit: 10, offset: 0 });

    expect(mockConnection.execute).toHaveBeenCalledWith(
      expect.stringContaining('IN (?, ?)'),
      expect.arrayContaining(['active', 'pending'])
    );
  });

  /**
   * Test: table_columns endpoint with parameterized query
   */
  it('uses parameterized query for table_columns endpoint', async () => {
    const columnsConnector = {
      ...connector,
      endpoint_id: 'table_columns',
    };
    const columnsAdapter = mysqlAdapter(columnsConnector, auth);

    await columnsAdapter.connect!();

    const mockRows = [
      { column_name: 'id', data_type: 'int' },
      { column_name: 'name', data_type: 'varchar' },
    ];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    const result = await columnsAdapter.download({ limit: 100, offset: 0 });

    expect(mockConnection.execute).toHaveBeenCalledWith(
      'SELECT COLUMN_NAME as column_name, DATA_TYPE as data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?',
      ['test_db', 'users']
    );
    expect(result.data).toEqual(mockRows);
  });

  /**
   * Test: Multiple filters with parameterized queries
   */
  it('builds query with multiple filters using parameterized queries', async () => {
    const connectorMultiFilter = {
      ...connector,
      filters: [
        { field: 'status', operator: '=', value: 'active' },
        { field: 'role', operator: '=', value: 'admin' },
      ],
    };
    const adapterMultiFilter = mysqlAdapter(connectorMultiFilter, auth);

    await adapterMultiFilter.connect!();

    const mockRows = [{ id: 1, name: 'Admin' }];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    await adapterMultiFilter.download({ limit: 1, offset: 0 });

    expect(mockConnection.execute).toHaveBeenCalledWith(
      "SELECT `id`, `name`, `email` FROM `users` WHERE `status` = ? AND `role` = ? ORDER BY `name` ASC LIMIT ?, ?",
      ['active', 'admin', 0, 1]
    );
  });
});
