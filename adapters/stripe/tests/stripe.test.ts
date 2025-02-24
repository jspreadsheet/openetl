import { stripe, StripeAdapter } from '../src/index'; // Adjust path as needed
import axios from 'axios';
import { Connector, AuthConfig, AdapterInstance } from '../../../src/types';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('Stripe Adapter', () => {
  let connector: Connector;
  let auth: AuthConfig;
  let adapter: AdapterInstance;

  beforeEach(() => {
    jest.clearAllMocks();

    // Mock connector configuration
    connector = {
      id: 'stripe-customers-connector',
      adapter_id: 'stripe',
      endpoint_id: 'customers',
      credential_id: 'stripe-auth',
      fields: ['id', 'email', 'name'],
      filters: [{
        field: 'status',
        operator: '=',
        value: 'active',
      }],
      transform: [],
      pagination: { type: 'cursor', itemsPerPage: 10 },
    };

    // Mock API key authentication
    auth = {
      id: 'stripe-auth',
      type: 'api_key',
      credentials: {
        api_key: 'sk_test_mock_key',
      },
    };

    // Create adapter instance
    adapter = stripe(connector, auth);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  /**
   * Test 1: Connecting to Stripe
   * Ensures the adapter can establish a connection using the provided API key.
   */
  it('connects successfully with valid credentials', async () => {
    mockedAxios.get.mockResolvedValueOnce({
      status: 200,
      data: { data: [] },
    });

    await expect(adapter.connect()).resolves.toBeUndefined();
    expect(mockedAxios.get).toHaveBeenCalledWith(
      'https://api.stripe.com/v1/customers',
      expect.objectContaining({
        headers: {
          Authorization: `Bearer ${auth.credentials.api_key}`,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        params: expect.objectContaining({
          limit: 1,
          expand: ['data.id', 'data.email', 'data.name'],
          status: 'active',
        }),
      })
    );
  });

  it('throws error on connection failure', async () => {
    mockedAxios.get.mockRejectedValueOnce(new Error('Network error'));
    await expect(adapter.connect()).rejects.toThrow('Failed to connect to Stripe: Network error');
  });

  /**
   * Test 2: Downloading Data
   * Tests cursor-based pagination and field filtering.
   */
  it('downloads data with cursor-based pagination', async () => {
    mockedAxios.get
      .mockResolvedValueOnce({
        status: 200,
        data: {
          data: [
            { id: 'cus_1', email: 'john@example.com', name: 'John Doe' },
            { id: 'cus_2', email: 'jane@example.com', name: 'Jane Smith' },
          ],
          has_more: true,
        },
      })
      .mockResolvedValueOnce({
        status: 200,
        data: {
          data: [{ id: 'cus_3', email: 'bob@example.com', name: 'Bob Jones' }],
          has_more: false,
        },
      });

    const firstPage = await adapter.download({ limit: 2, offset: 0 });
    expect(firstPage.data).toEqual([
      { id: 'cus_1', email: 'john@example.com', name: 'John Doe' },
      { id: 'cus_2', email: 'jane@example.com', name: 'Jane Smith' },
    ]);
    expect(firstPage.options?.nextOffset).toBe('cus_2');

    const secondPage = await adapter.download({ limit: 2, offset: 'cus_2' as any });
    expect(secondPage.data).toEqual([{ id: 'cus_3', email: 'bob@example.com', name: 'Bob Jones' }]);
    expect(secondPage.options?.nextOffset).toBeUndefined();
  });

  it('filters response data to requested fields', async () => {
    mockedAxios.get.mockResolvedValueOnce({
      status: 200,
      data: {
        data: [
          {
            id: 'cus_1',
            email: 'john@example.com',
            name: 'John Doe',
            extra_field: 'should be ignored',
          },
        ],
      },
    });

    const result = await adapter.download({ limit: 1, offset: 0 });
    expect(result.data).toEqual([{ id: 'cus_1', email: 'john@example.com', name: 'John Doe' }]);
  });

  /**
   * Test 3: Error Handling
   * Tests handling of 401 (invalid API key) and 429 (rate limit) errors.
   */
  it('throws error on 401 invalid API key', async () => {
    const unauthorizedError = {
      response: {
        data: 'Invalid API Key',
        status: 401,
        statusText: 'Unauthorized',
        headers: {},
        config: {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          method: 'get',
          url: 'https://api.stripe.com/v1/customers',
        },
      },
    };

    mockedAxios.get.mockRejectedValueOnce(unauthorizedError);

    await expect(adapter.download({ limit: 1, offset: 0 })).rejects.toThrow('Invalid API key provided');
    expect(mockedAxios.get).toHaveBeenCalledTimes(1); // No retry for 401
  });

  it('handles rate limiting with retry-after', async () => {
    jest.useFakeTimers();

    const rateLimitError = {
      response: {
        data: 'Too Many Requests',
        status: 429,
        statusText: 'Too Many Requests',
        headers: { 'retry-after': '2' },
        config: {
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          method: 'get',
          url: 'https://api.stripe.com/v1/customers',
        },
      },
    };

    mockedAxios.get
      .mockRejectedValueOnce(rateLimitError)
      .mockResolvedValueOnce({
        status: 200,
        data: { data: [{ id: 'cus_1', email: 'john@example.com', name: 'John Doe' }] },
      });

    const downloadPromise = adapter.download({ limit: 1, offset: 0 });
    await new Promise(resolve => setImmediate(resolve)); // Flush initial call
    expect(mockedAxios.get).toHaveBeenCalledTimes(1); // First call (429)

    jest.advanceTimersByTime(2000); // Advance 2 seconds for retry-after
    await new Promise(resolve => setImmediate(resolve)); // Flush retry
    await downloadPromise;

    expect(mockedAxios.get).toHaveBeenCalledTimes(2); // After retry
    const result = await downloadPromise;
    expect(result.data).toEqual([{ id: 'cus_1', email: 'john@example.com', name: 'John Doe' }]);
  });

  /**
   * Test 4: Uploading Data
   * Tests uploading a customer with form-urlencoded data.
   */
  it('uploads data successfully', async () => {
    const data = [
      { email: 'alice@example.com', name: 'Alice Brown' },
    ];

    mockedAxios.post.mockResolvedValueOnce({ status: 201, data: { id: 'cus_123' } });

    await expect(adapter.upload!(data)).resolves.toBeUndefined();
    expect(mockedAxios.post).toHaveBeenCalledWith(
      'https://api.stripe.com/v1/customers',
      expect.stringContaining('email=alice%40example.com&name=Alice%20Brown'),
      expect.objectContaining({
        headers: {
          Authorization: `Bearer ${auth.credentials.api_key}`,
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        params: {
          expand: ['data.id', 'data.email', 'data.name'],
          status: 'active',
        },
      })
    );
  });

  /**
   * Test 5: Disconnect
   * Ensures disconnect is a no-op without errors.
   */
  it('disconnects without errors', async () => {
    const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
    await expect(adapter.disconnect!()).resolves.toBeUndefined();
    expect(consoleLogSpy).toHaveBeenCalledWith('Disconnecting from Stripe adapter (no-op)');
    consoleLogSpy.mockRestore();
  });

  /**
   * Test 6: Invalid Endpoint
   * Ensures an error is thrown for an invalid endpoint.
   */
  it('throws error for invalid endpoint', () => {
    const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
    expect(() => stripe(invalidConnector, auth)).toThrow(
      'Endpoint invalid-endpoint not found in Stripe adapter'
    );
  });

  /**
   * Test 7: Query Parameters
   * Ensures filters are correctly applied as query params.
   */
  it('builds correct query params with filters', async () => {
    mockedAxios.get.mockResolvedValueOnce({
      status: 200,
      data: { data: [] },
    });

    await adapter.download({ limit: 1, offset: 0 });
    expect(mockedAxios.get).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        params: expect.objectContaining({
          expand: ['data.id', 'data.email', 'data.name'],
          status: 'active',
          limit: 1,
        }),
      })
    );
  });
});