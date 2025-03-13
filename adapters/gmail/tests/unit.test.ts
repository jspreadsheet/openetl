import { gmail } from '../src/index';
import axios from 'axios';
import { Connector, OAuth2Auth, AdapterInstance } from '../../../src/types';

jest.mock('axios');

const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('Gmail Adapter', () => {
    let connector: Connector;
    let auth: OAuth2Auth;
    let adapter: AdapterInstance;

    beforeEach(() => {
        jest.clearAllMocks();

        connector = {
            id: 'gmail-connector',
            adapter_id: 'gmail',
            endpoint_id: 'list-messages',
            credential_id: 'gmail-auth',
            fields: ['id', 'snippet'],
            filters: [{
                field: 'from',
                operator: '=',
                value: 'test@example.com',
            }],
            transform: [],
            pagination: { itemsPerPage: 10 },
            debug: false,
        };

        auth = {
            id: 'gmail-auth',
            type: 'oauth2',
            credentials: {
                client_id: 'mock-client-id',
                client_secret: 'mock-client-secret',
                refresh_token: 'mock-refresh-token',
                access_token: 'mock-access-token',
            },
            expires_at: new Date(Date.now() + 3600 * 1000).toISOString(),
        };

        adapter = gmail(connector, auth);
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it('downloads data with pagination', async () => {
        mockedAxios.get
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    messages: [
                        { id: 'msg1', snippet: 'First message' },
                        { id: 'msg2', snippet: 'Second message' },
                    ],
                    nextPageToken: 'next-page-token',
                },
            })
            .mockResolvedValueOnce({
                status: 200,
                data: {
                    messages: [{ id: 'msg3', snippet: 'Third message' }],
                },
            });

        const firstPage = await adapter.download({ limit: 2, offset: 0 });
        expect(firstPage.data).toEqual([
            { id: 'msg1', snippet: 'First message' },
            { id: 'msg2', snippet: 'Second message' },
        ]);
        expect(firstPage.options?.nextOffset).toBe('next-page-token');

        const secondPage = await adapter.download({ limit: 2, offset: 'next-page-token' });
        expect(secondPage.data).toEqual([{ id: 'msg3', snippet: 'Third message' }]);
        expect(secondPage.options?.nextOffset).toBeUndefined();
    });

    it('filters response data to requested fields', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: {
                messages: [
                    {
                        id: 'msg1',
                        snippet: 'Test message',
                        threadId: 'thread1',
                    },
                ],
            },
        });

        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(result.data).toEqual([{ id: 'msg1', snippet: 'Test message' }]);
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
                    url: 'https://gmail.googleapis.com/gmail/v1/users/me/messages',
                },
            },
        };

        mockedAxios.get
            .mockRejectedValueOnce(unauthorizedError)
            .mockResolvedValueOnce({
                status: 200,
                data: { messages: [{ id: 'msg1', snippet: 'Test' }] },
            });

        mockedAxios.post.mockResolvedValueOnce({
            status: 200,
            data: {
                access_token: 'new-access-token',
                expires_in: 3600,
            },
        });

        const result = await adapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://oauth2.googleapis.com/token',
            expect.any(String),
            expect.objectContaining({ headers: { 'Content-Type': 'application/x-www-form-urlencoded' } })
        );
        expect(auth.credentials.access_token).toBe('new-access-token');
        expect(mockedAxios.get).toHaveBeenCalledTimes(2);
        expect(result.data).toEqual([{ id: 'msg1', snippet: 'Test' }]);
    });

    it('uploads data successfully', async () => {
        const sendConnector = {
            ...connector,
            endpoint_id: 'send-message',
        };

        const sendAdapter = gmail(sendConnector, auth);

        const data = [
            { to: 'recipient@example.com', subject: 'Test Email', body: 'Hello, this is a test!' },
        ];

        mockedAxios.post.mockResolvedValueOnce({ status: 200, data: { id: 'sent-msg1' } });

        await expect(sendAdapter.upload!(data)).resolves.toBeUndefined();
        expect(mockedAxios.post).toHaveBeenCalledWith(
            'https://gmail.googleapis.com/gmail/v1/users/me/messages/send',
            expect.objectContaining({
                raw: expect.any(String),
            }),
            expect.objectContaining({
                headers: {
                    Authorization: `Bearer ${auth.credentials.access_token}`,
                    'Content-Type': 'application/json',
                },
            })
        );
    });

    it('throws error for invalid endpoint', () => {
        const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
        expect(() => gmail(invalidConnector, auth)).toThrow(
            'Endpoint invalid-endpoint not found in Gmail adapter'
        );
    });

    it('builds correct query params with filters', async () => {
        mockedAxios.get.mockResolvedValueOnce({
            status: 200,
            data: { messages: [] },
        });

        await adapter.download({ limit: 1, offset: 0 });
        expect(mockedAxios.get).toHaveBeenCalledWith(
            expect.any(String),
            expect.objectContaining({
                params: expect.objectContaining({
                    maxResults: 1,
                    q: 'from:test@example.com',
                }),
            })
        );
    });
});