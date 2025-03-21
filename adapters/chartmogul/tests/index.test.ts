import { chartmogul } from '../src/index';
import axios from 'axios';
import { Connector, ApiKeyAuth, AdapterInstance } from '../../../src/types';
import { FilterGroup } from '../../../dist';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('ChartMogul Adapter', () => {
    let connector: Connector;
    let auth: ApiKeyAuth;
    let adapter: AdapterInstance;

    beforeEach(() => {
        jest.clearAllMocks();

        connector = {
            id: 'chartmogul-customers-connector',
            adapter_id: 'chartmogul',
            endpoint_id: 'customers',
            credential_id: 'cm-auth',
            fields: ['id', 'name', 'email'],
            filters: [{
                field: 'status',
                operator: '=',
                value: 'active',
            }],
            transform: [],
            pagination: { itemsPerPage: 10 },
        };

        auth = {
            id: 'cm-auth',
            type: 'api_key',
            credentials: {
                api_key: 'mock-api-key',
                api_secret: 'mock-api-secret',
            },
        };

        adapter = chartmogul(connector, auth);
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it('downloads data successfully with valid credentials', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: {
                entries: [
                    { id: '1', name: 'John Doe', email: 'john@example.com', status: 'active' },
                ],
                has_more: false,
                cursor: undefined,
            },
        });

        const result = await adapter.download({ limit: 10, offset: undefined });
        expect(result.data).toEqual([{ id: '1', name: 'John Doe', email: 'john@example.com' }]);
        expect(mockedAxios.get).toHaveBeenCalledWith(
            'https://api.chartmogul.com/v1/customers',
            expect.objectContaining({
                auth: { username: 'mock-api-key' },
                headers: { 'Content-Type': 'application/json' },
                params: expect.objectContaining({
                    per_page: 10,
                    status: 'active',
                }),
            })
        );
    });

    it('throws error on download failure', async () => {
        mockedAxios.get.mockRejectedValueOnce(new Error('Network error'));
        await expect(adapter.download({ limit: 10, offset: undefined })).rejects.toThrow('Download failed: Network error');
    });

    it('downloads data with cursor-based pagination', async () => {
        mockedAxios.get
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    entries: [
                        { id: '1', name: 'John', email: 'john@example.com' },
                        { id: '2', name: 'Jane', email: 'jane@example.com' },
                    ],
                    has_more: true,
                    cursor: 'next-cursor',
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    entries: [{ id: '3', name: 'Bob', email: 'bob@example.com' }],
                    has_more: false,
                    cursor: undefined,
                },
            });

        const firstPage = await adapter.download({ limit: 2, offset: undefined });
        expect(firstPage.data).toEqual([
            { id: '1', name: 'John', email: 'john@example.com' },
            { id: '2', name: 'Jane', email: 'jane@example.com' },
        ]);
        expect(firstPage.options?.nextOffset).toBe('next-cursor');

        const secondPage = await adapter.download({ limit: 2, offset: 'next-cursor' });
        expect(secondPage.data).toEqual([{ id: '3', name: 'Bob', email: 'bob@example.com' }]);
        expect(secondPage.options?.nextOffset).toBeUndefined();
    });

    it('filters response data to requested fields', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: {
                entries: [
                    {
                        id: '1',
                        name: 'John',
                        email: 'john@example.com',
                        extra_field: 'ignored',
                    },
                ],
                has_more: false,
            },
        });

        const result = await adapter.download({ limit: 1, offset: undefined });
        expect(result.data).toEqual([{ id: '1', name: 'John', email: 'john@example.com' }]);
    });

    it('uploads data successfully', async () => {
        const localConnector = {
            ...connector,
            endpoint_id: 'create-customer',
        };
        const localAdapter = chartmogul(localConnector, auth);

        const data = [{ name: 'Alice', email: 'alice@example.com' }];

        mockedAxios.post.mockResolvedValueOnce({ status: 201, data: { id: '123' } });

        await expect(localAdapter.upload!(data)).resolves.toBeUndefined();
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://api.chartmogul.com/v1/customers',
            data[0],
            expect.objectContaining({
                auth: { username: 'mock-api-key' },
                headers: { 'Content-Type': 'application/json' },
            })
        );
    });

    it('throws error when uploading too many items for endpoint with maxItemsPerPage=1', async () => {
        const localConnector = {
            ...connector,
            endpoint_id: 'create-customer',
        };
        const localAdapter = chartmogul(localConnector, auth);

        const data = [
            { name: 'Alice', email: 'alice@example.com' },
            { name: 'Bob', email: 'bob@example.com' },
        ];

        await expect(localAdapter.upload!(data)).rejects.toThrow(
            'Number of items per page (2), exceeds the maximum number allowed for the create-customer endpoint of the ChartMogul adapter'
        );
    });

    it('uploads invoices with customer_uuid successfully', async () => {
        const localConnector = {
            ...connector,
            endpoint_id: 'create-invoice',
            config: { customer_uuid: 'cust_123' },
        };
        const localAdapter = chartmogul(localConnector, auth);

        const data = [
            { external_id: 'inv_001', date: '2023-01-01', currency: 'USD', line_items: [] },
        ];

        mockedAxios.post.mockResolvedValueOnce({ status: 201, data: { invoices: [data[0]] } });

        await expect(localAdapter.upload!(data)).resolves.toBeUndefined();
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://api.chartmogul.com/v1/import/customers/cust_123/invoices',
            { invoices: data },
            expect.objectContaining({
                auth: { username: 'mock-api-key' },
                headers: { 'Content-Type': 'application/json' },
            })
        );
    });

    it('throws error when customer_uuid is missing for required endpoint', async () => {
        const localConnector = {
            ...connector,
            endpoint_id: 'subscriptions',
            config: {},
        };
        const localAdapter = chartmogul(localConnector, auth);

        await expect(localAdapter.download({ limit: 10, offset: undefined })).rejects.toThrow(
            'subscriptions endpoint of the ChartMogul adapter requires a customer_uuid property in the config property'
        );
    });

    it('throws error for invalid endpoint', () => {
        const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
        expect(() => chartmogul(invalidConnector, auth)).toThrow(
            'Endpoint invalid-endpoint not found in ChartMogul adapter'
        );
    });

    it('builds correct query params with filters', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { entries: [] },
        });

        await adapter.download({ limit: 1, offset: undefined });
        expect(mockedAxios.get).toHaveBeenCalledWith(
            expect.any(String),
            expect.objectContaining({
                params: expect.objectContaining({
                    per_page: 1,
                    status: 'active',
                }),
            })
        );
    });

    it('throws error when using filter groups', async () => {
        const filters: FilterGroup[] = [{ op: 'AND', filters: [{ field: 'status', operator: '=', value: 'active' }] }];

        const connectorWithFilterGroup = {
            ...connector,
            filters: filters,
        };

        const localAdapter = chartmogul(connectorWithFilterGroup, auth);

        await expect(localAdapter.download({ limit: 10, offset: undefined })).rejects.toThrow('Filter groups are not supported in ChartMogul adapter; use flat filters');
    });
});