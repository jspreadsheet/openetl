import { googleAds } from '../src/index';
import axios from 'axios';
import { Connector, OAuth2Auth, AdapterInstance } from '../../../src/types';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

const customerId = '1111111111';
const loginCustomerId = '2222222222';
const developerToken = '1111111111111111111111';

describe('Google ads Adapter', () => {
    let connector: Connector;
    let auth: OAuth2Auth;
    let adapter: AdapterInstance;

    beforeEach(() => {
        jest.clearAllMocks();

        // Mock connector configuration
        connector = {
            id: "google-ads-connector",
            adapter_id: "googleAds",
            endpoint_id: "table_query",
            credential_id: "hs-auth",
            config: {
                table: "campaign",
                customerId,
                loginCustomerId,
                developerToken,
            },
            fields: [
                'campaign.id',
                'campaign.name',
                'campaign.status',
                'campaign.start_date',
                'campaign.end_date',
                'metrics.impressions'
            ],
            transform: [],
            limit: 10
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
            },
            expires_at: new Date(Date.now() + 3600 * 1000).toISOString(), // Valid for 1 hour
        };

        // Create adapter instance
        adapter = googleAds(connector, auth);
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it('downloads data', async () => {
        const expectedResult = [
            {
                campaign: {
                    status: "PAUSED",
                    name: "Sales-Search-1",
                    id: "33333333333",
                },
                metrics: {
                    impressions: "0"
                }
            },
            {
                campaign: {
                    status: "PAUSED",
                    name: "Sales-Search-2",
                    id: "44444444444",
                },
                metrics: {
                    impressions: "0"
                }
            },
        ];

        mockedAxios.post
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    results: [
                        {
                            metrics: expectedResult[0].metrics,
                            campaign: {
                                ...expectedResult[0].campaign,
                                resourceName: "customers/1111111111/campaigns/33333333333",
                                startDate: "2025-03-07",
                                endDate: "2037-12-30"
                            },
                        },
                        {
                            metrics: expectedResult[1].metrics,
                            campaign: {
                                ...expectedResult[1].campaign,
                                resourceName: "customers/1111111111/campaigns/44444444444",
                                startDate: "2025-03-07",
                                endDate: "2037-12-30"
                            },
                        }
                    ],
                },
            });

        const result = await adapter.download({ limit: 100, offset: 0 });

        expect(mockedAxios.post).toHaveBeenCalledTimes(1);
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://googleads.googleapis.com/v19/customers/' + customerId + '/googleAds:search',
            {
                query: 'SELECT campaign.id, campaign.name, campaign.status, campaign.start_date, campaign.end_date, metrics.impressions FROM campaign LIMIT 100'
            },
            expect.objectContaining({
                headers: expect.objectContaining({
                    "Authorization": "Bearer mock-access-token",
                    "developer-token": "1111111111111111111111",
                    "login-customer-id": "2222222222"
                })
            })
        );
        expect(result.data).toEqual(expectedResult);
    });

    it('downloads data without specifying any fields', async () => {
        connector.fields = [];

        await expect(adapter.download({ limit: 100, offset: 0 })).rejects.toThrow('At least one field name must be informed');
    });

    it('refreshes token on 401 error and retries download', async () => {
        const expectedResult = [
            {
                campaign: {
                    status: "PAUSED",
                    name: "Sales-Search-1",
                    id: "33333333333",
                },
                metrics: {
                    impressions: "0"
                }
            },
        ];

        mockedAxios.post
            .mockRejectedValueOnce({
                response: {
                    data: 'Unauthorized',
                    status: 401,
                    statusText: 'Unauthorized',
                    headers: {},
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    access_token: 'new-access-token',
                    refresh_token: 'new-refresh-token',
                    expires_in: 3600,
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    results: [
                        {
                            metrics: expectedResult[0].metrics,
                            campaign: {
                                ...expectedResult[0].campaign,
                                resourceName: "customers/1111111111/campaigns/33333333333",
                                startDate: "2025-03-07",
                                endDate: "2037-12-30"
                            },
                        }
                    ],
                },
            });

        // Execute the download and verify results
        const result = await adapter.download({ limit: 1, offset: 0 });

        expect(mockedAxios.post).toHaveBeenCalledTimes(3);
        expect(mockedAxios.post).toHaveBeenNthCalledWith(
            2,
            'https://oauth2.googleapis.com/token',
            expect.anything(),
        );
        expect(auth.credentials.access_token).toBe('new-access-token');
        expect(result.data).toEqual(expectedResult);
    });

    it('throws error for invalid endpoint', () => {
        const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
        expect(() => googleAds(invalidConnector, auth)).toThrow(
            'Endpoint invalid-endpoint not found in Google Ads API adapter'
        );
    });
});