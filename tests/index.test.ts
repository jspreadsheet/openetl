import { Orchestrator } from '../src/index';
import { Pipeline, Adapter, AuthConfig, Vault, Connector, AdapterInstance } from '../src/types';

let attemptCount = 0;

const transformMock = jest.fn(async (data: any[]) =>
    data.map(item => ({ ...item, transformed: true }))
);

const cursorAdapter: jest.Mock<ReturnType<Adapter>> = jest.fn(() => ({
    paginationType: 'cursor',
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    download: jest.fn(async ({ limit, offset }) => {
        if (offset === undefined) {
            return { data: [{ id: 1 }, { id: 2 }], options: { nextOffset: 2 } };
        } else if (offset === 2) {
            return { data: [{ id: 3 }], options: { nextOffset: undefined } };
        }
        return { data: [] };
    }),
    upload: jest.fn().mockResolvedValue(undefined),
    getConfig: () => {
        return {
            id: '',
            name: '',
            type: "database",
            action: [],
            credential_type: "basic",
            config: [],
            pagination: {
                type: 'cursor',
            },
            endpoints: [],
        }
    }
}));

const downloadFunction: AdapterInstance['download'] = async ({ limit, offset }) => {
    offset = (typeof offset === 'string' ? parseInt(offset) : (offset || 0));
    const startId = offset + 1;
    const totalItems = 6;
    const allData = Array.from(
        { length: Math.min(limit || Infinity, totalItems - offset) },
        (_, i) => ({
            id: startId + i,
            name: `Item${startId + i}`,
            created_at: `2025-02-21T${String(i).padStart(2, '0')}:00:00Z`,
        })
    );
    return { data: offset < totalItems ? allData : [] };
}

const mockAdapter: jest.Mock<ReturnType<Adapter>> = jest.fn((connector: Connector, auth: AuthConfig) => ({
    paginationType: 'offset',
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    download: jest.fn(downloadFunction),
    upload: jest.fn().mockResolvedValue(undefined),
    transform: transformMock,
    getConfig: () => {
        return {
            id: '',
            name: '',
            type: "database",
            action: [],
            credential_type: "basic",
            config: [],
            pagination: {
                type: 'offset',
            },
            endpoints: [],
        }
    }
}));

const adapterWithoutUpload: jest.Mock<ReturnType<Adapter>> = jest.fn((connector: Connector, auth: AuthConfig) => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    download: jest.fn(downloadFunction),
    transform: transformMock,
    getConfig: () => {
        return {
            id: '',
            name: '',
            type: "database",
            action: [],
            credential_type: "basic",
            config: [],
            pagination: {
                type: 'offset',
            },
            endpoints: [],
        }
    }
}));

const failingAdapter: jest.Mock<ReturnType<Adapter>> = jest.fn(() => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),  // Add disconnect here
    download: jest.fn(async ({ limit, offset }) => {
        attemptCount++;
        if (attemptCount <= 2) {
            throw new Error(`Attempt ${attemptCount} failed`);
        }
        return { data: offset === undefined ? [{ id: 1, name: 'Success after retry' }] : [] };
    }),
    upload: jest.fn().mockResolvedValue(undefined),
    getConfig: () => {
        return {
            id: '',
            name: '',
            type: "database",
            action: [],
            credential_type: "basic",
            config: [],
            pagination: {
                type: 'offset',
            },
            endpoints: [],
        }
    }
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
    pagination: { itemsPerPage: 5 },
};

const failingConnector: Connector = {
    id: 'mock-source',
    adapter_id: 'failingAdapter',
    endpoint_id: 'test',
    credential_id: 'mock-auth',
    fields: ['id', 'name'],
    pagination: { itemsPerPage: 5 },
};

const cursorConnector: Connector = {
    id: 'mock-source',
    adapter_id: 'cursorAdapter',
    endpoint_id: 'test',
    credential_id: 'mock-auth',
    fields: ['id', 'name'],
    pagination: { itemsPerPage: 2 },
};


describe('Orchestrator', () => {
    let orchestrator: ReturnType<typeof Orchestrator>;
    let logging: jest.Mock;

    beforeEach(() => {
        jest.useFakeTimers();
        orchestrator = Orchestrator(mockVault, { mockAdapter: mockAdapter, failingAdapter: failingAdapter, cursorAdapter: cursorAdapter, adapterWithoutUpload });
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
        expect(adapterInstance.download).toHaveBeenCalledTimes(2);
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: undefined });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 5 });
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({ type: 'extract', dataCount: 5 }));
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({ type: 'extract', dataCount: 1 }));
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({ type: 'complete' }));
    });

    it('downloads data with pagination and rate limiting from source', async () => {
        const pipeline: Pipeline<any> = {
            id: 'rate-limit-test',
            source: mockConnector,
            rate_limiting: { requests_per_second: 10, max_retries_on_rate_limit: 0 },
            logging,
        };

        const promise = orchestrator.runPipeline(pipeline);
        await jest.runAllTimersAsync();
        await promise;

        const adapterInstance = mockAdapter.mock.results[0].value;
        expect(adapterInstance.connect).toHaveBeenCalled();
        expect(adapterInstance.download).toHaveBeenCalledTimes(2);
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: undefined });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 5, offset: 5 });

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
        expect(adapterInstance.download).toHaveBeenCalledTimes(3); // 2 failures + 1 success
        expect(adapterInstance.download).toHaveBeenNthCalledWith(2, { limit: 5, offset: undefined });
        expect(adapterInstance.download).toHaveBeenNthCalledWith(1, { limit: 5, offset: undefined });
        expect(adapterInstance.download).toHaveBeenNthCalledWith(3, { limit: 5, offset: undefined });

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
        let requestedData;

        const pipeline: Pipeline<any> = {
            id: 'timeout-test',
            source: {
                ...mockConnector,
                timeout: 1000
            },
            rate_limiting: {
                requests_per_second: 1,
                max_retries_on_rate_limit: 3,
            },
            onload: (data) => {
                requestedData = data;
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

        const expectedResult: any[] = [];
        for (let i = 0; i < 5; i++) {
            expectedResult.push(
                expect.objectContaining({
                    id: i + 1,
                    name: 'Item' + (i + 1)
                })
            );
        }

        expect(requestedData).toEqual(expectedResult);
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
            .toThrow('Credentials not found for id: non-existent');
    });

    it('handles missing source adapter', async () => {
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

    it('handles missing target adapter', async () => {
        const pipeline: Pipeline<any> = {
            id: 'missing-adapter-test',
            data: [{
                id: 1,
                name: 'Item1'
            }],
            target: {
                ...mockConnector,
                adapter_id: 'non-existent'
            },
            logging,
        };

        await expect(orchestrator.runPipeline(pipeline))
            .rejects
            .toThrow('Target adapter non-existent not found');
    });

    it('handles cursor-based pagination with HubSpot', async () => {
        const pipeline: Pipeline<any> = {
            id: 'cursor-test',
            source: cursorConnector,
            logging,
        };

        await orchestrator.runPipeline(pipeline);
        const adapterInstance = cursorAdapter.mock.results[0].value;
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 2, offset: undefined });
        expect(adapterInstance.download).toHaveBeenCalledWith({ limit: 2, offset: 2 });
        expect(adapterInstance.download).toHaveBeenCalledTimes(2);
        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'extract',
            message: 'Extracted page with cursor 2', // Still works as string "2" is logged
            dataCount: 2
        }));
    });

    it('handles pipeline without source or data', async () => {
        const pipeline: Pipeline<any> = {
            id: 'invalid-auth-test',
        };

        await expect(orchestrator.runPipeline(pipeline))
            .rejects
            .toThrow('Pipeline must have either a source or data');
    });

    it('item limit must be respected', async () => {
        let requestedData;

        const itemsLimit = 5;

        const pipeline: Pipeline<any> = {
            id: 'timeout-test',
            source: {
                ...mockConnector,
                limit: itemsLimit,
                pagination: {
                    itemsPerPage: 2,
                }
            },
            onload: (data) => {
                requestedData = data;
            },
            logging,
        };

        await orchestrator.runPipeline(pipeline);

        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'info',
            message: 'Reached total items limit of ' + itemsLimit,
        }));

        const expectedResult: any[] = [];
        for (let i = 0; i < itemsLimit; i++) {
            expectedResult.push(
                expect.objectContaining({
                    id: i + 1,
                    name: 'Item' + (i + 1)
                })
            );
        }

        expect(requestedData).toEqual(expectedResult);
    });

    it('stop searching when the number of items returned is less than expected', async () => {
        let requestedData;

        const itemsPerPage = 7;

        const pipeline: Pipeline<any> = {
            id: 'timeout-test',
            source: {
                ...mockConnector,
                pagination: {
                    itemsPerPage,
                }
            },
            onload: (data) => {
                requestedData = data;
            },
            logging,
        };

        await orchestrator.runPipeline(pipeline);

        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'info',
            message: `Received 6 items, less than ${itemsPerPage}, so it's the last page`,
        }));

        const expectedResult: any[] = [];
        for (let i = 0; i < 6; i++) {
            expectedResult.push(
                expect.objectContaining({
                    id: i + 1,
                    name: 'Item' + (i + 1)
                })
            );
        }

        expect(requestedData).toEqual(expectedResult);
    });

    it('target does not upload data', async () => {
        const pipeline: Pipeline<any> = {
            id: 'target-without-upload-test',
            data: [{
                id: 1,
                name: 'Item1'
            }],
            target: {
                ...mockConnector,
                adapter_id: 'adapterWithoutUpload'
            },
            logging,
        };

        await expect(orchestrator.runPipeline(pipeline))
            .rejects
            .toThrow('Upload not supported by adapter adapterWithoutUpload');
    });

    it('stop upload with onbeforesend event', async () => {
        const pipeline: Pipeline<any> = {
            id: 'upload-test',
            source: mockConnector,
            target: {
                ...mockConnector,
                id: 'mock-target'
            },
            onbeforesend: () => false,
            logging,
        };

        await orchestrator.runPipeline(pipeline);

        expect(logging).toHaveBeenCalledWith(expect.objectContaining({
            type: 'complete',
            message: 'Pipeline halted by onbeforesend'
        }));
    });
});