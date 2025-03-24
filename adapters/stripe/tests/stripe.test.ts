import { stripe, StripeAdapter } from '../src/index';
import axios, { AxiosError } from 'axios';
import { Connector, AuthConfig, ApiKeyAuth, AdapterInstance } from '../../../src/types';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('Stripe Adapter', () => {
    let connector: Connector;
    let auth: ApiKeyAuth;
    let adapter: AdapterInstance;

    beforeEach(() => {
        jest.clearAllMocks();

        // Mock connector configuration for downloading charges
        connector = {
            id: 'stripe-charges-connector',
            adapter_id: 'stripe',
            endpoint_id: 'charges',
            credential_id: 'stripe-auth',
            fields: ['id', 'amount'],
            filters: [{ field: 'status', operator: '=', value: 'succeeded' }],
            debug: true,
            pagination: { itemsPerPage: 10 },
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

    /**
     * Test 1: Connecting to Stripe
     */
    it('connects successfully with valid API key', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { data: [] },
        });

        await expect(adapter.connect!()).resolves.toBeUndefined();
        expect(mockedAxios.get).toHaveBeenCalledWith(
            'https://api.stripe.com/v1/charges',
            expect.objectContaining({
                headers: { Authorization: `Bearer ${auth.credentials.api_key}` },
                params: { limit: 1 },
            })
        );
    });

    it('throws error on connection failure', async () => {
        mockedAxios.get.mockRejectedValueOnce(new Error('Network error'));
        await expect(adapter.connect!()).rejects.toThrow('Failed to connect to Stripe: Network error');
    });

    /**
     * Test 2: Downloading Data
     */
    it('downloads data with cursor-based pagination', async () => {
        mockedAxios.get
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    data: [
                        { id: 'ch_1', amount: 1000 },
                        { id: 'ch_2', amount: 2000 },
                    ],
                    has_more: true,
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    data: [{ id: 'ch_3', amount: 3000 }],
                    has_more: false,
                },
            });

        const firstPage = await adapter.download({ limit: 2 });
        expect(firstPage.data).toEqual([
            { id: 'ch_1', amount: 1000 },
            { id: 'ch_2', amount: 2000 },
        ]);
        expect(firstPage.options?.nextOffset).toBe('ch_2');

        const secondPage = await adapter.download({ limit: 2, offset: 'ch_2' });
        expect(secondPage.data).toEqual([{ id: 'ch_3', amount: 3000 }]);
        expect(secondPage.options?.nextOffset).toBeUndefined();
    });

    it('filters response data to requested fields', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: {
                data: [
                    {
                        id: 'ch_1',
                        amount: 1000,
                        currency: 'usd', // Extra field to be ignored
                    },
                ],
                has_more: false,
            },
        });

        const result = await adapter.download({ limit: 1 });
        expect(result.data).toEqual([{ id: 'ch_1', amount: 1000 }]);
    });

    it('throws error when limit exceeds maximum', async () => {
        await expect(adapter.download({ limit: 101 })).rejects.toThrow(
            'Number of items per page exceeds Stripe maximum'
        );
    });

    /**
     * Test 3: Uploading Data
     */
    it('uploads a single product successfully', async () => {
        const uploadConnector = {
            ...connector,
            endpoint_id: 'create-product',
            fields: ['name'],
        };
        const uploadAdapter = stripe(uploadConnector, auth);

        const data = [{ name: 'Test Product' }];

        mockedAxios.post.mockResolvedValueOnce({
            status: 201,
            data: { id: 'prod_123', name: 'Test Product' },
        });

        await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://api.stripe.com/v1/products',
            'name=Test+Product',
            expect.objectContaining({
                headers: {
                    Authorization: `Bearer ${auth.credentials.api_key}`,
                    'Content-Type': 'application/x-www-form-urlencoded',
                },
            })
        );
    });

    it('throws error when uploading multiple products', async () => {
        const uploadConnector = {
            ...connector,
            endpoint_id: 'create-product',
        };
        const uploadAdapter = stripe(uploadConnector, auth);

        const data = [{ name: 'Product 1' }, { name: 'Product 2' }];

        await expect(uploadAdapter.upload!(data)).rejects.toThrow(
            'Stripe adapter only supports uploading one product at a time'
        );
    });

    it('throws error on upload failure', async () => {
        const uploadConnector = {
            ...connector,
            endpoint_id: 'create-product',
        };
        const uploadAdapter = stripe(uploadConnector, auth);

        const data = [{ name: 'Test Product' }];

        mockedAxios.post.mockRejectedValueOnce({
            response: { status: 400, data: { error: { message: 'Invalid parameter' } } },
        });

        await expect(uploadAdapter.upload!(data)).rejects.toThrow(
            'Upload failed: Unknown error'
        );
    });

    /**
     * Test 4: Disconnecting
     */
    it('disconnects without errors', async () => {
        const consoleLogSpy = jest.spyOn(console, 'log').mockImplementation(() => {});
        await expect(adapter.disconnect!()).resolves.toBeUndefined();
        expect(consoleLogSpy).toHaveBeenCalledWith('Disconnecting from Stripe adapter (no-op)');
        consoleLogSpy.mockRestore();
    });

    /**
     * Test 5: Error Handling
     */
    it('throws error for invalid endpoint', () => {
        const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
        expect(() => stripe(invalidConnector, auth)).toThrow(
            'Endpoint invalid-endpoint not found in Stripe adapter'
        );
    });

    it('throws error for missing API key', () => {
        const invalidAuth = { ...auth, credentials: { api_key: '' } };
        expect(() => stripe(connector, invalidAuth)).toThrow(
            'Stripe adapter requires an API key for authentication'
        );
    });

    /**
     * Test 6: Query Parameter Building
     */
    it('builds correct query params with filters', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { data: [], has_more: false },
        });

        await adapter.download({ limit: 1 });
        expect(mockedAxios.get).toHaveBeenCalledWith(
            expect.any(String),
            expect.objectContaining({
                params: expect.objectContaining({
                    limit: 1,
                    status: 'succeeded',
                }),
            })
        );
    });
});