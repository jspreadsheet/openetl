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

  it('downloads data with filters, sorting, and pagination', async () => {
    await adapter.connect!();
    const mockRows = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    const result = await adapter.download({ limit: 2, offset: 0 });
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "SELECT id, name, email FROM `users` WHERE `status` = 'active' ORDER BY `name` ASC LIMIT 0, 2"
    );
    expect(result.data).toEqual(mockRows);
  });

  it('downloads all fields when none specified', async () => {
    const connectorNoFields = { ...connector, fields: [] };
    const adapterNoFields = mysqlAdapter(connectorNoFields, auth); // Use renamed adapter
    await adapterNoFields.connect!();

    const mockRows = [{ id: 1, name: 'Alice', email: 'alice@example.com' }];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    const result = await adapterNoFields.download({ limit: 1, offset: 0 });
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "SELECT * FROM `users` WHERE `status` = 'active' ORDER BY `name` ASC LIMIT 0, 1"
    );
    expect(result.data).toEqual(mockRows);
  });

  it('handles custom query for custom_query endpoint', async () => {
    const customConnector = {
      ...connector,
      endpoint_id: 'custom_query',
      config: { ...connector.config, custom_query: 'SELECT * FROM custom_table WHERE id > 5' },
    };
    const customAdapter = mysqlAdapter(customConnector, auth); // Use renamed adapter
    await customAdapter.connect!();

    const mockRows = [{ id: 6, name: 'Charlie' }];
    mockConnection.execute.mockResolvedValueOnce([mockRows]);

    const result = await customAdapter.download({ limit: 10, offset: 0 });
    expect(mockConnection.execute).toHaveBeenCalledWith('SELECT * FROM custom_table WHERE id > 5');
    expect(result.data).toEqual(mockRows);
  });

  it('uploads data successfully', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = mysqlAdapter(uploadConnector, auth); // Use renamed adapter
    await uploadAdapter.connect!();

    const data = [
      { id: 1, name: 'Alice', email: 'alice@example.com' },
      { id: 2, name: 'Bob', email: 'bob@example.com' },
    ];
    mockConnection.execute.mockResolvedValueOnce([{ affectedRows: 2 }]);

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, 'Alice', 'alice@example.com'), (2, 'Bob', 'bob@example.com')"
    );
  });

  it('handles null values in upload', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = mysqlAdapter(uploadConnector, auth); // Use renamed adapter
    await uploadAdapter.connect!();

    const data = [{ id: 1, name: null, email: 'alice@example.com' }];
    mockConnection.execute.mockResolvedValueOnce([{ affectedRows: 1 }]);

    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();
    expect(mockConnection.execute).toHaveBeenCalledWith(
      "INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, NULL, 'alice@example.com')"
    );
  });

  it('throws error when downloading with table_insert endpoint', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'table_insert' };
    const uploadAdapter = mysqlAdapter(uploadConnector, auth); // Use renamed adapter
    await uploadAdapter.connect!();

    await expect(uploadAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "table_insert endpoint don't support download"
    );
  });

  it('throws error when database or table is missing', async () => {
    const invalidConnector = { ...connector, config: {} };
    const invalidAdapter = mysqlAdapter(invalidConnector, auth); // Use renamed adapter
    await invalidAdapter.connect!();

    await expect(invalidAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
      "database property is required on the MySQL adapter's table_query endpoint"
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
});