import { hubspot, HubSpotAdapter } from '../src/index'; // Adjust path as needed
import axios, { AxiosResponse, AxiosError } from 'axios';
import { Connector, AuthConfig, OAuth2Auth, AdapterInstance } from '../../../src/types';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('HubSpot Adapter', () => {
    let connector: Connector;
    let auth: OAuth2Auth;
    let adapter: AdapterInstance;

    beforeEach(() => {
        jest.clearAllMocks();

        // Mock connector configuration
        connector = {
            id: 'hubspot-contacts-connector',
            adapter_id: 'hubspot',
            endpoint_id: 'contacts',
            credential_id: 'hs-auth',
            fields: ['firstname', 'lastname', 'email'],
            filters: [{
                field: 'lifecyclestage',
                operator: '=',
                value: 'customer',
            }],
            transform: [],
            pagination: { type: 'cursor', itemsPerPage: 10 },
        };

        // Mock OAuth2 authentication
        auth = {
            id: 'hs-auth',
            type: 'oauth2',
            credentials: {
                client_id: 'mock-client-id',
                client_secret: 'mock-client-secret',
                refresh_token: 'mock-refresh-token',
                access_token: 'mock-access-token',
                token_url: 'https://api.hubapi.com/oauth/v1/token',
            },
            expires_at: new Date(Date.now() + 3600 * 1000).toISOString(), // Valid for 1 hour
        };

        // Create adapter instance
        adapter = hubspot(connector, auth);
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    /**
     * Test 1: Connecting to HubSpot
     * Ensures the adapter can establish a connection using the provided credentials.
     */
    it('connects successfully with valid credentials', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { results: [] },
        });

        await expect(adapter.connect()).resolves.toBeUndefined();
        expect(mockedAxios.get).toHaveBeenCalledWith(
            'https://api.hubapi.com/crm/v3/objects/contacts',
            expect.objectContaining({
                headers: {
                    Authorization: `Bearer ${auth.credentials.access_token}`,
                    'Content-Type': 'application/json',
                },
                params: expect.objectContaining({
                    limit: 1,
                    properties: 'firstname,lastname,email',
                    filterGroups: [{
                        filters: [{
                            propertyName: 'lifecyclestage',
                            operator: 'EQ',
                            value: 'customer',
                        }],
                    }],
                }),
            })
        );
    });

    it('throws error on connection failure', async () => {
        mockedAxios.get.mockRejectedValueOnce(new Error('Network error'));
        await expect(adapter.connect()).rejects.toThrow('Failed to connect to HubSpot: Network error');
    });

    it('downloads data with cursor-based pagination', async () => {
        mockedAxios.get
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    results: [
                        { properties: { firstname: 'John', lastname: 'Doe', email: 'john.doe@example.com' } },
                        { properties: { firstname: 'Jane', lastname: 'Smith', email: 'jane.smith@example.com' } },
                    ],
                    paging: { next: { after: 'next-cursor' } },
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    results: [{ properties: { firstname: 'Bob', lastname: 'Jones', email: 'bob.jones@example.com' } }],
                    paging: undefined,
                },
            });

        const firstPage = await adapter.download({ limit: 2, offset: 0 });
        expect(firstPage.data).toEqual([
            { firstname: 'John', lastname: 'Doe', email: 'john.doe@example.com' },
            { firstname: 'Jane', lastname: 'Smith', email: 'jane.smith@example.com' },
        ]);
        expect(firstPage.options?.nextOffset).toBe('next-cursor');

        const secondPage = await adapter.download({ limit: 2, offset: parseInt('next-cursor') });
        expect(secondPage.data).toEqual([{ firstname: 'Bob', lastname: 'Jones', email: 'bob.jones@example.com' }]);
        expect(secondPage.options?.nextOffset).toBeUndefined();
    });

    it('filters response data to requested fields', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: {
                results: [
                    {
                        properties: {
                            firstname: 'John',
                            lastname: 'Doe',
                            email: 'john.doe@example.com',
                            extra_field: 'should be ignored',
                        },
                    },
                ],
            },
        });

        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(result.data).toEqual([{ firstname: 'John', lastname: 'Doe', email: 'john.doe@example.com' }]);
    });

    it('refreshes token on 401 error and retries download', async () => {
        // Mock error with a simple structure
        const unauthorizedError = {
            response: {
                data: 'Unauthorized',
                status: 401,
                statusText: 'Unauthorized',
                headers: {},
                config: {
                    headers: { 'Content-Type': 'application/json' },
                    method: 'get',
                    url: 'https://api.hubapi.com/crm/v3/objects/contacts',
                },
            },
        };

        // Mock axios.get: 401 error first, then successful response
        mockedAxios.get
            .mockRejectedValueOnce(unauthorizedError)
            .mockResolvedValueOnce({
                status: 200,
                data: { results: [{ properties: { firstname: 'John' } }] },
            });

        // Mock axios.post for token refresh
        mockedAxios.post.mockResolvedValueOnce({
            status: 200,
            data: {
                access_token: 'new-access-token',
                refresh_token: 'new-refresh-token',
                expires_in: 3600,
            },
        });

        // Execute the download and verify results
        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://api.hubapi.com/oauth/v1/token',
            expect.any(String),
            expect.objectContaining({ headers: { 'Content-Type': 'application/x-www-form-urlencoded' } })
        );
        expect(auth.credentials.access_token).toBe('new-access-token');
        expect(mockedAxios.get).toHaveBeenCalledTimes(2); // Ensure retry happens
        expect(result.data).toEqual([{ firstname: 'John' }]);
    });

    it('handles rate limiting with retry-after', async () => {
        // Mock error with a simple structure for 429
        const rateLimitError = {
            response: {
                data: 'Too Many Requests',
                status: 429,
                statusText: 'Too Many Requests',
                headers: { 'retry-after': '2' },
                config: {
                    headers: { 'Content-Type': 'application/json' },
                    method: 'get',
                    url: 'https://api.hubapi.com/crm/v3/objects/contacts',
                },
            },
        };

        // Mock axios.get: 429 error first, then successful response
        mockedAxios.get
            .mockRejectedValueOnce(rateLimitError)
            .mockResolvedValueOnce({
                status: 200,
                data: { results: [{ properties: { firstname: 'John' } }] },
            });

        // Start download and verify initial call
        const downloadPromise = adapter.download({ limit: 1, offset: 0 });
        await new Promise(resolve => setImmediate(resolve)); // Flush initial call
        expect(mockedAxios.get).toHaveBeenCalledTimes(1); // First call (429)

        // Advance timers for retry-after (2 seconds = 2000ms)
        jest.advanceTimersByTime(2000);
        await new Promise(resolve => setImmediate(resolve)); // Flush retry
        await downloadPromise;

        // Verify retry and result
        expect(mockedAxios.get).toHaveBeenCalledTimes(2); // After retry
        const result = await downloadPromise;
        expect(result.data).toEqual([{ firstname: 'John' }]);
    });

    it('uploads data successfully', async () => {
        const data = [
            { properties: { firstname: 'Alice', lastname: 'Brown', email: 'alice.brown@example.com' } },
        ];

        mockedAxios.post.mockResolvedValueOnce({ status: 201, data: { id: '123' } });

        await expect(adapter.upload!(data)).resolves.toBeUndefined();
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://api.hubapi.com/crm/v3/objects/contacts',
            data[0],
            expect.objectContaining({
                headers: {
                    Authorization: `Bearer ${auth.credentials.access_token}`,
                    'Content-Type': 'application/json',
                },
                params: {
                    properties: 'firstname,lastname,email',
                    filterGroups: [{
                        filters: [{
                            propertyName: 'lifecyclestage',
                            operator: 'EQ',
                            value: 'customer',
                        }],
                    }],
                },
            })
        );
    });

    it('disconnects without errors', async () => {
        const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
        await expect(adapter.disconnect!()).resolves.toBeUndefined();
        expect(consoleLogSpy).toHaveBeenCalledWith('Disconnecting from HubSpot adapter (no-op)');
        consoleLogSpy.mockRestore();
    });

    it('throws error for invalid endpoint', () => {
        const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
        expect(() => hubspot(invalidConnector, auth)).toThrow(
            'Endpoint invalid-endpoint not found in HubSpot adapter'
        );
    });

    it('builds correct query params with filters', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { results: [] },
        });

        await adapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.get).toHaveBeenCalledWith(
            expect.any(String),
            expect.objectContaining({
                params: expect.objectContaining({
                    properties: 'firstname,lastname,email',
                    filterGroups: [{
                        filters: [{
                            propertyName: 'lifecyclestage',
                            operator: 'EQ',
                            value: 'customer',
                        }],
                    }],
                }),
            })
        );
    });
});