import { zoho, ZohoAdapter } from '../src/index'; // Adjust path as needed
import axios, { AxiosResponse, AxiosError } from 'axios';
import { Connector, AuthConfig, OAuth2Auth, AdapterInstance } from '../../../src/types';

jest.mock('axios');

// Mock the delay function to resolve immediately when timers are advanced
jest.mock('../src/index', () => {
    const originalModule = jest.requireActual('../src/index');
    return {
        ...originalModule,
        delay: jest.fn().mockImplementation((ms) => new Promise(resolve => setTimeout(resolve, ms))),
    };
});

const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('Zoho Adapter', () => {
    let connector: Connector;
    let auth: OAuth2Auth;
    let adapter: AdapterInstance;

    beforeEach(() => {
        jest.clearAllMocks();

        // Mock connector configuration for leads
        connector = {
            id: 'zoho-leads-connector',
            adapter_id: 'zoho',
            endpoint_id: 'leads',
            credential_id: 'zoho-auth',
            fields: ['Last_Name', 'First_Name', 'Email'],
            filters: [{
                field: 'Lead_Status',
                operator: '=',
                value: 'Contacted',
            }],
            transform: [],
            pagination: { itemsPerPage: 10 },
        };

        // Mock OAuth2 authentication
        auth = {
            id: 'zoho-auth',
            type: 'oauth2',
            credentials: {
                client_id: 'mock-client-id',
                client_secret: 'mock-client-secret',
                refresh_token: 'mock-refresh-token',
                access_token: 'mock-access-token',
            },
            expires_at: new Date(Date.now() + 3600 * 1000).toISOString(), // Valid for 1 hour
        };

        // Create adapter instance
        adapter = zoho(connector, auth);
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it('downloads data with offset-based pagination', async () => {
        mockedAxios.get
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    data: [
                        { Last_Name: 'Doe', First_Name: 'John', Email: 'john.doe@example.com' },
                        { Last_Name: 'Smith', First_Name: 'Jane', Email: 'jane.smith@example.com' },
                    ],
                    info: { more_records: true },
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    data: [{ Last_Name: 'Jones', First_Name: 'Bob', Email: 'bob.jones@example.com' }],
                    info: { more_records: false },
                },
            });

        const firstPage = await adapter.download({ limit: 2, offset: 0 });
        expect(firstPage.data).toEqual([
            { Last_Name: 'Doe', First_Name: 'John', Email: 'john.doe@example.com' },
            { Last_Name: 'Smith', First_Name: 'Jane', Email: 'jane.smith@example.com' },
        ]);
        expect(firstPage.options?.nextOffset).toBe(2);

        const secondPage = await adapter.download({ limit: 2, offset: 2 });
        expect(secondPage.data).toEqual([{ Last_Name: 'Jones', First_Name: 'Bob', Email: 'bob.jones@example.com' }]);
        expect(secondPage.options?.nextOffset).toBeUndefined();
    });

    it('filters response data to requested fields', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: {
                data: [{
                    Last_Name: 'Doe',
                    First_Name: 'John',
                    Email: 'john.doe@example.com',
                    Extra_Field: 'should be ignored',
                }],
            },
        });

        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(result.data).toEqual([{ Last_Name: 'Doe', First_Name: 'John', Email: 'john.doe@example.com' }]);
    });

    it('refreshes token on 401 error and retries download', async () => {
        const unauthorizedError = {
            response: {
                data: 'Unauthorized',
                status: 401,
                statusText: 'Unauthorized',
                headers: {},
                config: {
                    headers: { 'Content-Type': 'application/json' },
                    method: 'get',
                    url: 'https://www.zohoapis.com/crm/v3/Leads',
                },
            },
        };

        mockedAxios.get
            .mockRejectedValueOnce(unauthorizedError)
            .mockResolvedValueOnce({
                status: 200,
                data: { data: [{ Last_Name: 'Doe' }] },
            });

        mockedAxios.post.mockResolvedValueOnce({
            status: 200,
            data: {
                access_token: 'new-access-token',
                refresh_token: 'new-refresh-token',
                expires_in: 3600,
            },
        });

        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://accounts.zoho.com/oauth/v2/token',
            expect.any(String),
            expect.objectContaining({ headers: { 'Content-Type': 'application/x-www-form-urlencoded' } })
        );
        expect(auth.credentials.access_token).toBe('new-access-token');
        expect(mockedAxios.get).toHaveBeenCalledTimes(2);
        expect(result.data).toEqual([{ Last_Name: 'Doe' }]);
    });

    xit('handles rate limiting with retry-after', async () => {
        jest.useFakeTimers();
    
        const rateLimitError = {
            response: {
                data: 'Too Many Requests',
                status: 429,
                statusText: 'Too Many Requests',
                headers: { 'retry-after': '2' },
                config: {
                    headers: { 'Content-Type': 'application/json' },
                    method: 'get',
                    url: 'https://www.zohoapis.com/crm/v3/Leads',
                },
            },
        };
    
        mockedAxios.get
            .mockRejectedValueOnce(rateLimitError)
            .mockResolvedValueOnce({
                status: 200,
                data: { data: [{ Last_Name: 'Doe' }] },
            });
    
        const downloadPromise = adapter.download({ limit: 1, offset: 0 });
        jest.advanceTimersByTime(2000);
        jest.runAllTimers(); // Run all timers, not just pending ones
    
        const result = await downloadPromise;
        expect(mockedAxios.get).toHaveBeenCalledTimes(2);
        expect(result.data).toEqual([{ Last_Name: 'Doe' }]);
    });

    /**
     * Tests the upload functionality for creating leads.
     */
    it('uploads data successfully', async () => {
        const localConnector = {
            ...connector,
            endpoint_id: 'create-lead',
        };

        const localAdapter = zoho(localConnector, auth);

        const data = [
            { Last_Name: 'Brown', First_Name: 'Alice', Email: 'alice.brown@example.com' },
        ];

        mockedAxios.post.mockResolvedValueOnce({ status: 201, data: { data: [{ id: '123' }] } });

        await expect(localAdapter.upload!(data)).resolves.toBeUndefined();
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://www.zohoapis.com/crm/v3/Leads',
            { data },
            expect.objectContaining({
                headers: {
                    Authorization: `Zoho-oauthtoken ${auth.credentials.access_token}`,
                    'Content-Type': 'application/json',
                },
                params: {
                    fields: 'Last_Name,First_Name,Email',
                    criteria: '(Lead_Status:equals:Contacted)',
                },
            })
        );
    });

    /**
     * Ensures invalid endpoint_id throws an error.
     */
    it('throws error for invalid endpoint', () => {
        const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
        expect(() => zoho(invalidConnector, auth)).toThrow(
            'Endpoint invalid-endpoint not found in Zoho adapter'
        );
    });

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
                    fields: 'Last_Name,First_Name,Email',
                    criteria: '(Lead_Status:equals:Contacted)',
                    per_page: 1,
                    page: 1,
                }),
            })
        );
    });
});