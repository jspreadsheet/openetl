import Orchestrator from '../src/index';
import { Pipeline, Adapter, AuthConfig, Vault, Connector } from '../src/types';

let attemptCount = 0;

const transformMock = jest.fn(async (data: any[]) =>
    data.map(item => ({ ...item, transformed: true }))
);

const cursorAdapter: jest.Mock<ReturnType<Adapter>> = jest.fn(() => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    download: jest.fn(async ({ limit, offset }) => {
        if (offset === 0) {
            return { data: [{ id: 1 }, { id: 2 }], options: { nextOffset: 2 } };
        } else if (offset === 2) {
            return { data: [{ id: 3 }], options: { nextOffset: undefined } };
        }
        return { data: [] };
    }),
    upload: jest.fn().mockResolvedValue(undefined),
}));

const mockAdapter: jest.Mock<ReturnType<Adapter>> = jest.fn((connector: Connector, auth: AuthConfig) => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    download: jest.fn(async ({ limit, offset }) => {
        const startId = offset + 1;
        const totalItems = 6;
        const allData = Array.from(
            { length: Math.min(limit, totalItems - offset) },
            (_, i) => ({
                id: startId + i,
                name: `Item${startId + i}`,
                created_at: `2025-02-21T${String(i).padStart(2, '0')}:00:00Z`,
            })
        );
        return { data: offset < totalItems ? allData : [] };
    }),
    upload: jest.fn().mockResolvedValue(undefined),
    transform: transformMock
}));

const failingAdapter: jest.Mock<ReturnType<Adapter>> = jest.fn(() => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),  // Add disconnect here
    download: jest.fn(async ({ limit, offset }) => {
        attemptCount++;
        if (attemptCount <= 2) {
            throw new Error(`Attempt ${attemptCount} failed`);
        }
        return { data: offset === 0 ? [{ id: 1, name: 'Success after retry' }] : [] };
    }),
    upload: jest.fn().mockResolvedValue(undefined),
}));

const mockAuth: AuthConfig = {
    id: 'mock-auth',
    type: 'api_key',
    credentials: { api_key: 'test' },
};

const mockVault: Vault = {
    'mock-auth': mockAuth,
};

const mockConnector: Connector = {
    id: 'mock-source',
    adapter_id: 'mockAdapter',
    endpoint_id: 'test',
    credential_id: 'mock-auth',
    fields: ['id', 'name'],
    pagination: { type: 'offset', itemsPerPage: 5, pageOffsetKey: '0' },
};

const failingConnector: Connector = {
    id: 'mock-source',
    adapter_id: 'failingAdapter',
    endpoint_id: 'test',
    credential_id: 'mock-auth',
    fields: ['id', 'name'],
    pagination: { type: 'offset', itemsPerPage: 5, pageOffsetKey: '0' },
};

const cursorConnector: Connector = {
    id: 'mock-source',
    adapter_id: 'cursorAdapter',
    endpoint_id: 'test',
    credential_id: 'mock-auth',
    fields: ['id', 'name'],
    pagination: { type: 'cursor', itemsPerPage: 2 },
};


describe('Orchestrator', () => {
    let orchestrator: ReturnType<typeof Orchestrator>;
    let logging: jest.Mock;

    beforeEach(() => {
        jest.useFakeTimers();
        orchestrator = Orchestrator(mockVault, { mockAdapter: mockAdapter, failingAdapter: failingAdapter, cursorAdapter: cursorAdapter });
        logging = jest.fn();
        jest.clearAllMocks();
        attemptCount = 0;
    });

    afterEach(() => {
        jest.useRealTimers();
    });

    it('downloads data with pagination from source', async () => {
        const pipeline: Pipeline<any> = {
            id: 'extract-test',
            source: mockConnector,
            logging,
        };

        const promise = orchestrator.runPipeline(pipeline);
        await jest.runAllTimersAsync();
        await promise;

        const adapterInstance = mockAdapter.mock.results[0].value;
        expect(adapterInstance.connect).toHaveBeenCalled();
        expect(adapterInstance.download).toHaveBeenCalledTimes(3);
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 0 });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 5 });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 10 });
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({ type: 'extract', dataCount: 5 }));
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({ type: 'extract', dataCount: 1 }));
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({ type: 'complete' }));
    });

    it('downloads data with pagination and rate limiting from source', async () => {
        const pipeline: Pipeline<any> = {
            id: 'rate-limit-test',
            source: mockConnector,
            rate_limiting: { requests_per_second: 10, concurrent_requests: 1, max_retries_on_rate_limit: 0 },
            logging,
        };

        const promise = orchestrator.runPipeline(pipeline);
        await jest.runAllTimersAsync();
        await promise;

        const adapterInstance = mockAdapter.mock.results[0].value;
        expect(adapterInstance.connect).toHaveBeenCalled();
        expect(adapterInstance.download).toHaveBeenCalledTimes(3);
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 0 });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 5 });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 10 });

        const extractLogs = logging.mock.calls.filter(call => call[0].type === 'extract');
        expect(extractLogs).toHaveLength(3);
        expect(extractLogs[0][0]).toMatchObject({
            type: 'extract',
            message: 'Extracted page at offset 0',
            dataCount: 5
        });
        expect(extractLogs[1][0]).toMatchObject({
            type: 'extract',
            message: 'Extracted page at offset 5',
            dataCount: 1
        });
        expect(extractLogs[2][0]).toMatchObject({
            type: 'extract',
            message: 'Data extraction complete',
            dataCount: 6
        });

        const rateLimitLogs = logging.mock.calls.filter(
            call => call[0].type === 'info' && call[0].message.includes('Rate limiting')
        );
        expect(rateLimitLogs.length).toBeGreaterThan(0);
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({ type: 'complete' }));
    });

    it('handles errors and retries failed operations', async () => {

        const pipeline: Pipeline<any> = {
            id: 'retry-test',
            source: failingConnector,
            error_handling: {
                max_retries: 3,
                retry_interval: 1000,
                fail_on_error: false
            },
            logging,
        };

        const promise = orchestrator.runPipeline(pipeline);
        await jest.runAllTimersAsync();
        await promise;

        const adapterInstance = failingAdapter.mock.results[0].value;
        expect(adapterInstance.download).toHaveBeenCalledTimes(4); // 2 failures + 1 success + 1 empty
        expect(adapterInstance.download).toHaveBeenNthCalledWith(1, { limit: 5, offset: 0 });
        expect(adapterInstance.download).toHaveBeenNthCalledWith(2, { limit: 5, offset: 0 });
        expect(adapterInstance.download).toHaveBeenNthCalledWith(3, { limit: 5, offset: 0 });
        expect(adapterInstance.download).toHaveBeenNthCalledWith(4, { limit: 5, offset: 5 });

        const errorLogs = logging.mock.calls.filter(call => call[0].type === 'error');
        expect(errorLogs).toHaveLength(2);
        expect(errorLogs[0][0].message).toBe('Attempt 1 failed in download: Attempt 1 failed');
        expect(errorLogs[1][0].message).toBe('Attempt 2 failed in download: Attempt 2 failed');

        const extractLogs = logging.mock.calls.filter(
            call => call[0].type === 'extract' && call[0].message === 'Data extraction complete'
        );
        expect(extractLogs).toHaveLength(1);
        expect(extractLogs[0][0].dataCount).toBe(1);

        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'complete',
            message: 'Pipeline finished'
        }));
    });

    it('properly disconnects adapters after completion', async () => {
        const pipeline: Pipeline<any> = {
            id: 'disconnect-test',
            source: mockConnector,
            logging,
        };

        const promise = orchestrator.runPipeline(pipeline);
        await jest.runAllTimersAsync();
        await promise;

        const adapterInstance = mockAdapter.mock.results[0].value;
        expect(adapterInstance.disconnect).toHaveBeenCalled();
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'info',
            message: 'Source adapter disconnected'
        }));
    });

    it('disconnects adapters even when pipeline fails', async () => {
        const pipeline: Pipeline<any> = {
            id: 'disconnect-error-test',
            source: failingConnector,
            error_handling: {
                max_retries: 0,
                retry_interval: 1000,  // Add the required retry_interval
                fail_on_error: true
            },
            logging,
        };

        await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow();

        const adapterInstance = failingAdapter.mock.results[0].value;
        expect(adapterInstance.disconnect).toHaveBeenCalled();
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'info',
            message: 'Source adapter disconnected'
        }));
    });

    it('successfully uploads data to target', async () => {
        const pipeline: Pipeline<any> = {
            id: 'upload-test',
            source: mockConnector,
            target: {
                ...mockConnector,
                id: 'mock-target'
            },
            logging,
        };

        await orchestrator.runPipeline(pipeline);

        const targetAdapter = mockAdapter.mock.results[1].value;
        expect(targetAdapter.connect).toHaveBeenCalled();
        expect(targetAdapter.upload).toHaveBeenCalled();
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'load',
            message: expect.stringContaining('Uploaded batch')
        }));
    });


    /*it('applies transformation to extracted data', async () => {
        const pipeline: Pipeline<any> = {
            id: 'transform-test',
            source: {
                ...mockConnector,
                transform: []
            },
            logging,
        };

        await orchestrator.runPipeline(pipeline);
        expect(transformMock).toHaveBeenCalled();
    });*/

    it('handles timeout correctly', async () => {

        const pipeline: Pipeline<any> = {
            id: 'timeout-test',
            source: {
                ...mockConnector,
                timeout: 1000
            },
            rate_limiting: {
                requests_per_second: 1,
                concurrent_requests: 5,
                max_retries_on_rate_limit: 3,
            },
            logging,
        };

        const promise = orchestrator.runPipeline(pipeline);
        await jest.runAllTimersAsync();
        await promise;

        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'error',
            message: expect.stringContaining('timeout exceeded')
        }));
    });


    it('handles invalid credentials', async () => {
        const pipeline: Pipeline<any> = {
            id: 'invalid-auth-test',
            source: {
                ...mockConnector,
                credential_id: 'non-existent'
            },
            logging,
        };

        await expect(orchestrator.runPipeline(pipeline))
            .rejects
            .toThrow('Credentials not found');
    });

    it('handles missing adapter', async () => {
        const pipeline: Pipeline<any> = {
            id: 'missing-adapter-test',
            source: {
                ...mockConnector,
                adapter_id: 'non-existent'
            },
            logging,
        };

        await expect(orchestrator.runPipeline(pipeline))
            .rejects
            .toThrow('Adapter non-existent not found');
    });

    it('handles cursor-based pagination with HubSpot', async () => {
        const pipeline: Pipeline<any> = {
            id: 'cursor-test',
            source: cursorConnector,
            logging,
        };

        await orchestrator.runPipeline(pipeline);
        const adapterInstance = cursorAdapter.mock.results[0].value;
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 2, offset: 0 });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 2, offset: 2 });
        expect(adapterInstance.download).toHaveBeenCalledTimes(3);
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'extract',
            message: 'Extracted page with cursor 2', // Still works as string "2" is logged
            dataCount: 2
        }));
    });
});