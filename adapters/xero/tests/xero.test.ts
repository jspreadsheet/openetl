import { xero } from '../src/index'; // Adjust path as needed
import axios from 'axios';
import { Connector, OAuth2Auth, AdapterInstance } from '../../../src/types';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('Xero Adapter', () => {
    let connector: Connector;
    let auth: OAuth2Auth;
    let adapter: AdapterInstance;

    beforeEach(() => {
        jest.clearAllMocks();

        // Mock connector configuration
        connector = {
            id: 'xero-contacts-connector',
            adapter_id: 'xero',
            endpoint_id: 'contacts',
            credential_id: 'xero-auth',
            fields: ['ContactID', 'Name', 'EmailAddress'],
            filters: [{
                field: 'ContactStatus',
                operator: '=',
                value: 'ACTIVE',
            }],
            config: { organisationName: 'TestOrg' },
            pagination: { itemsPerPage: 10 },
        };

        // Mock OAuth2 authentication
        auth = {
            id: 'xero-auth',
            type: 'oauth2',
            credentials: {
                client_id: 'mock-client-id',
                client_secret: 'mock-client-secret',
                refresh_token: 'mock-refresh-token',
                access_token: 'mock-access-token',
            },
            expires_at: new Date(Date.now() + 3600 * 1000).toISOString(), // Valid for 1 hour
        };

        // Mock tenant ID retrieval
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: [
                { tenantId: 'mock-tenant-id', tenantName: 'TestOrg', tenantType: 'ORGANISATION' },
            ],
        });

        // Create adapter instance
        adapter = xero(connector, auth);
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it('downloads data with offset-based pagination', async () => {
        mockedAxios.get
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    Contacts: [
                        { ContactID: '1', Name: 'John Doe', EmailAddress: 'john.doe@example.com' },
                        { ContactID: '2', Name: 'Jane Smith', EmailAddress: 'jane.smith@example.com' },
                    ],
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    Contacts: [
                        { ContactID: '3', Name: 'Bob Jones', EmailAddress: 'bob.jones@example.com' },
                    ],
                },
            });

        const firstPage = await adapter.download({ limit: 2, offset: 0 });
        expect(firstPage.data).toEqual([
            { ContactID: '1', Name: 'John Doe', EmailAddress: 'john.doe@example.com' },
            { ContactID: '2', Name: 'Jane Smith', EmailAddress: 'jane.smith@example.com' },
        ]);

        const secondPage = await adapter.download({ limit: 2, offset: 2 });
        expect(secondPage.data).toEqual([
            { ContactID: '3', Name: 'Bob Jones', EmailAddress: 'bob.jones@example.com' },
        ]);
    });

    it('filters response data to requested fields', async () => {
        mockedAxios.get
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    Contacts: [
                        {
                            ContactID: '1',
                            Name: 'John Doe',
                            EmailAddress: 'john.doe@example.com',
                            ExtraField: 'should be ignored',
                        },
                    ],
                },
            });

        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(result.data).toEqual([
            { ContactID: '1', Name: 'John Doe', EmailAddress: 'john.doe@example.com' },
        ]);
    });

    it('refreshes token on 401 error and retries download', async () => {
        const unauthorizedError = {
            response: {
                data: { error: 'Unauthorized' },
                status: 401,
                statusText: 'Unauthorized',
                headers: {},
                config: {
                    headers: { 'Content-Type': 'application/json' },
                    method: 'get',
                    url: 'https://api.xero.com/api.xro/2.0/Contacts',
                },
            },
        };

        mockedAxios.get
            .mockRejectedValueOnce(unauthorizedError)
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    Contacts: [{ ContactID: '1', Name: 'John' }],
                },
            });

        mockedAxios.post.mockResolvedValueOnce({
            status: 200,
            data: {
                access_token: 'new-access-token',
                refresh_token: 'new-refresh-token',
                expires_in: 3600,
            },
        });

        mockedAxios.isAxiosError.mockReturnValueOnce(true);

        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://identity.xero.com/connect/token',
            expect.any(String),
            expect.objectContaining({
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': expect.stringContaining('Basic'),
                },
            })
        );
        expect(auth.credentials.access_token).toBe('new-access-token');
        expect(mockedAxios.get).toHaveBeenCalledTimes(3);
        expect(result.data).toEqual([{ ContactID: '1', Name: 'John' }]);
    });

    it('uploads data successfully', async () => {
        const localConnector = {
            ...connector,
            endpoint_id: 'create-contact',
        };

        const localAdapter = xero(localConnector, auth);

        const data = [
            { Name: 'Alice Brown', EmailAddress: 'alice.brown@example.com' },
        ];

        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: [
                { tenantId: 'mock-tenant-id', tenantName: 'TestOrg', tenantType: 'ORGANISATION' },
            ],
        });

        mockedAxios.post.mockResolvedValueOnce({
            status: 200,
            data: { Contacts: [{ ContactID: '123' }] },
        });

        await expect(localAdapter.upload!(data)).resolves.toBeUndefined();
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://api.xero.com/api.xro/2.0/Contacts',
            { Contacts: data },
            expect.objectContaining({
                headers: {
                    Authorization: `Bearer ${auth.credentials.access_token}`,
                    'Accept': 'application/json',
                    'xero-tenant-id': 'mock-tenant-id',
                },
            })
        );
    });

    it('throws error for invalid endpoint', () => {
        const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
        expect(() => xero(invalidConnector, auth)).toThrow(
            'Endpoint invalid-endpoint not found in Xero adapter'
        );
    });

    it('builds correct query params with filters', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { Contacts: [] },
        });

        await adapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.get).toHaveBeenCalledWith(
            'https://api.xero.com/api.xro/2.0/Contacts',
            expect.objectContaining({
                headers: expect.objectContaining({
                    Authorization: 'Bearer mock-access-token',
                    'xero-tenant-id': 'mock-tenant-id',
                }),
                params: expect.objectContaining({
                    page: 1,
                    pageSize: 1,
                    where: "ContactStatus='ACTIVE'",
                }),
            })
        );
    });

    it('handles special filters like includeArchived', async () => {
        const localConnector = {
            ...connector,
            filters: [{
                field: 'includeArchived',
                operator: '=',
                value: 'true',
            }],
        };

        const localAdapter = xero(localConnector, auth);

        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { Contacts: [] },
        });

        await localAdapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.get).toHaveBeenCalledWith(
            'https://api.xero.com/api.xro/2.0/Contacts',
            expect.objectContaining({
                params: expect.objectContaining({
                    page: 1,
                    pageSize: 1,
                    includeArchived: 'true',
                }),
            })
        );
    });

    it('handles Modified After filter with If-Modified-Since header', async () => {
        const localConnector = {
            ...connector,
            filters: [{
                field: 'Modified After',
                operator: '=',
                value: '2023-01-01T00:00:00Z',
            }],
        };

        const localAdapter = xero(localConnector, auth);

        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { Contacts: [] },
        });

        await localAdapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.get).toHaveBeenCalledWith(
            'https://api.xero.com/api.xro/2.0/Contacts',
            expect.objectContaining({
                headers: expect.objectContaining({
                    'If-Modified-Since': '2023-01-01T00:00:00Z',
                }),
                params: expect.objectContaining({
                    page: 1,
                    pageSize: 1,
                }),
            })
        );
    });

    it('throws error for missing organisationName', async () => {
        const invalidConnector = { ...connector, config: {} };
        const invalidAdapter = xero(invalidConnector, auth);
        await expect(invalidAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
            'An organisationName is required to use Xero adapter endpoints'
        );
    });

    it('throws error for invalid organisationName', async () => {
        mockedAxios.get.mockRestore();

        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: [
                { tenantId: 'other-tenant-id', tenantName: 'OtherOrg', tenantType: 'ORGANISATION' },
            ],
        });

        const invalidAdapter = xero(connector, auth);
        await expect(invalidAdapter.download({ limit: 1, offset: 0 })).rejects.toThrow(
            'The Xero adapter does not have access to an organization named "TestOrg". Please use a connection that does have access to this organization, or use one of the organizations available for this connection: "OtherOrg"'
        );
    });

    it('handles upload validation error', async () => {
        const localConnector = {
            ...connector,
            endpoint_id: 'create-contact',
        };

        const localAdapter = xero(localConnector, auth);

        const data = [{ Name: '' }]; // Invalid data

        mockedAxios.post.mockRejectedValueOnce({
            response: {
                status: 400,
                data: {
                    Type: 'ValidationException',
                    Elements: [{
                        ValidationErrors: [{ Message: 'Name is required' }],
                    }],
                },
            },
        });

        mockedAxios.isAxiosError.mockReturnValueOnce(true);

        await expect(localAdapter.upload!(data)).rejects.toThrow('Upload failed: Name is required');
    });

    it('handles rate limiting with retry-after', async () => {
        const rateLimitError = {
            response: {
                data: 'Too Many Requests',
                status: 429,
                statusText: 'Too Many Requests',
                headers: { 'retry-after': '1' },
                config: {
                    headers: { 'Content-Type': 'application/json' },
                    method: 'get',
                    url: 'https://api.xero.com/api.xro/2.0/Contacts',
                },
            },
        };

        mockedAxios.get
            .mockRejectedValueOnce(rateLimitError)
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    Contacts: [{ ContactID: '1', Name: 'John' }],
                },
            });

        mockedAxios.isAxiosError.mockReturnValueOnce(true);

        const downloadPromise = adapter.download({ limit: 1, offset: 0 });

        await new Promise(resolve => setImmediate(resolve));

        expect(mockedAxios.get).toHaveBeenCalledTimes(2); // Tenant ID, 429
        
        jest.advanceTimersByTime(2000);

        await new Promise(resolve => setImmediate(resolve));

        await downloadPromise;

        expect(mockedAxios.get).toHaveBeenCalledTimes(3); // Tenant ID, 429, retry
        const result = await downloadPromise;
        expect(result.data).toEqual([{ ContactID: '1', Name: 'John' }]);
    });
});