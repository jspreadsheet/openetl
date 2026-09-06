import {
    Orchestrator,
    runStreamingPipeline,
    runStreamingPipelinesInParallel,
    chainStreamingPipelines,
    StreamingPipelineRunner,
    createSourceStream,
    applyTransformers,
    uploadBatchWithRetry,
    StreamingPipeline,
    StreamingAdapter,
    StreamingAdapterInstance,
    Adapter,
    Connector,
    Vault,
    AuthConfig
} from '../src/index';

describe('Critical Functions Unit Tests', () => {
    const mockConnector: Connector = {
        id: 'conn1',
        adapter_id: 'adapter1',
        endpoint_id: 'ep1',
        credential_id: 'cred1',
        fields: ['id', 'name']
    };

    test('createSourceStream with regular paginated adapter', async () => {
        const downloadMock = jest.fn(async ({ limit, offset }) => {
            const off = typeof offset === 'string' ? parseInt(offset) : (offset || 0);
            if (off >= 4) return { data: [] };
            return {
                data: [
                    { id: off + 1, name: `Item ${off + 1}` },
                    { id: off + 2, name: `Item ${off + 2}` }
                ]
            };
        });

        const mockAdapter = {
            getConfig: () => ({
                id: 'adapter1',
                name: 'Test Adapter',
                type: 'http' as const,
                base_url: 'http://localhost',
                action: ['download' as const],
                credential_type: 'basic' as const,
                pagination: { type: 'offset', itemsPerPage: 2 },
                endpoints: [{
                    id: 'ep1',
                    path: '/items',
                    method: 'GET' as const,
                    supported_actions: ['download' as const]
                }]
            }),
            download: downloadMock
        };

        const stream = createSourceStream(mockConnector, mockAdapter, { chunkSize: 2, limit: 10 });
        const chunks: any[][] = [];
        for await (const chunk of stream) {
            chunks.push(chunk);
        }

        expect(chunks.length).toBe(2);
        expect(chunks[0]).toEqual([{ id: 1, name: 'Item 1' }, { id: 2, name: 'Item 2' }]);
        expect(chunks[1]).toEqual([{ id: 3, name: 'Item 3' }, { id: 4, name: 'Item 4' }]);
        expect(downloadMock).toHaveBeenCalled();
    });

    test('createSourceStream with streaming API adapter (downloadStream)', async () => {
        const downloadStreamMock = jest.fn(async function* ({ limit, chunkSize }) {
            yield [{ id: 1, name: 'Stream 1' }, { id: 2, name: 'Stream 2' }];
            yield [{ id: 3, name: 'Stream 3' }];
        });

        const mockStreamingAdapter: StreamingAdapterInstance = {
            getConfig: () => ({
                id: 'adapter1',
                name: 'Streaming Adapter',
                type: 'http' as const,
                base_url: 'http://localhost',
                action: ['download' as const],
                credential_type: 'basic' as const,
                endpoints: [{
                    id: 'ep1',
                    path: '/stream',
                    method: 'GET' as const,
                    supported_actions: ['download' as const]
                }]
            }),
            download: jest.fn(),
            downloadStream: downloadStreamMock
        };

        const stream = createSourceStream(mockConnector, mockStreamingAdapter, { chunkSize: 2, limit: 10 });
        const chunks: any[][] = [];
        for await (const chunk of stream) {
            chunks.push(chunk);
        }

        expect(chunks.length).toBe(2);
        expect(chunks[0]).toEqual([{ id: 1, name: 'Stream 1' }, { id: 2, name: 'Stream 2' }]);
        expect(chunks[1]).toEqual([{ id: 3, name: 'Stream 3' }]);
        expect(downloadStreamMock).toHaveBeenCalledWith(expect.objectContaining({ limit: 1000000, chunkSize: 2 }));
    });

    test('applyTransformers with existing transformer steps and custom functions', async () => {
        const chunk = [
            { id: 1, name: '  john doe  ', score: '10.5' },
            { id: 2, name: '  jane smith  ', score: '20' }
        ];

        const connectorWithTransform: Connector = {
            ...mockConnector,
            transform: [
                { type: 'trim', options: { field: 'name' } },
                { type: 'uppercase', options: { field: 'name' } }
            ]
        };

        const customTransformer = async (data: any[]) => {
            return data.map(item => ({ ...item, processed: true }));
        };

        const result = await applyTransformers(chunk, [customTransformer], connectorWithTransform);

        expect(result).toEqual([
            { id: 1, name: 'JOHN DOE', score: '10.5', processed: true },
            { id: 2, name: 'JANE SMITH', score: '20', processed: true }
        ]);
    });

    test('uploadBatchWithRetry handles retry logic', async () => {
        let attempts = 0;
        const uploadMock = jest.fn(async (batch: any[]) => {
            attempts++;
            if (attempts < 2) {
                throw new Error('Temporary failure');
            }
        });

        const mockAdapter = {
            getConfig: () => ({ id: 'a', name: 'a', action: [], credential_type: 'basic' as const, endpoints: [] }),
            download: jest.fn(),
            upload: uploadMock
        };

        await uploadBatchWithRetry(mockAdapter, [{ id: 1 }], { max_retries: 2, retry_interval: 10 });
        expect(uploadMock).toHaveBeenCalledTimes(2);
    });
});

describe('Streaming Mode Pipeline Runner', () => {
    const vault: Vault = {
        'cred1': { id: 'cred1', type: 'basic', credentials: { username: 'user', password: 'pwd' } }
    };

    const createMockAdapter = (downloadData: any[][], uploadMock = jest.fn()): Adapter => {
        return jest.fn(() => {
            let callIndex = 0;
            return {
                connect: jest.fn().mockResolvedValue(undefined),
                disconnect: jest.fn().mockResolvedValue(undefined),
                download: jest.fn(async () => {
                    const data = downloadData[callIndex] || [];
                    callIndex++;
                    return { data };
                }),
                upload: uploadMock,
                getConfig: () => ({
                    id: 'adapter1',
                    name: 'Test',
                    type: 'http' as const,
                    base_url: 'http://localhost',
                    action: ['download' as const, 'upload' as const],
                    credential_type: 'basic' as const,
                    pagination: { type: 'offset', itemsPerPage: 2 },
                    endpoints: [{ id: 'ep1', path: '/test', method: 'GET' as const, supported_actions: ['download' as const, 'upload' as const] }]
                })
            };
        });
    };

    test('runs pipeline with existing regular Adapter interface', async () => {
        const uploadMock = jest.fn().mockResolvedValue(undefined);
        const adapter = createMockAdapter([
            [{ id: 1, val: 'a' }, { id: 2, val: 'b' }],
            [{ id: 3, val: 'c' }],
            []
        ], uploadMock);

        const adapters: Record<string, Adapter> = { 'adapter1': adapter };

        const pipeline: StreamingPipeline = {
            id: 'pipe1',
            chunkSize: 2,
            source: { id: 's', adapter_id: 'adapter1', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] },
            target: { id: 't', adapter_id: 'adapter1', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] }
        };

        const res = await runStreamingPipeline(pipeline, vault, adapters);

        expect(res.chunksCount).toBe(2);
        expect(res.totalProcessed).toBe(3);
        expect(uploadMock).toHaveBeenCalledTimes(2);
        expect(res.data).toEqual([
            { id: 1, val: 'a' }, { id: 2, val: 'b' }, { id: 3, val: 'c' }
        ]);
    });

    test('supports multiple transformer steps in streaming mode', async () => {
        const uploadMock = jest.fn().mockResolvedValue(undefined);
        const adapter = createMockAdapter([
            [{ id: 1, text: ' hello ' }, { id: 2, text: ' world ' }],
            []
        ], uploadMock);

        const adapters: Record<string, Adapter> = { 'adapter1': adapter };

        const pipeline: StreamingPipeline = {
            id: 'pipe-transform',
            chunkSize: 2,
            source: {
                id: 's',
                adapter_id: 'adapter1',
                endpoint_id: 'ep1',
                credential_id: 'cred1',
                fields: [],
                transform: [{ type: 'trim', options: { field: 'text' } }]
            },
            target: { id: 't', adapter_id: 'adapter1', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] },
            transformers: [
                { type: 'uppercase', options: { field: 'text' } },
                async (data: any[]) => data.map(item => ({ ...item, step3: true }))
            ]
        };

        const res = await runStreamingPipeline(pipeline, vault, adapters);

        expect(res.data).toEqual([
            { id: 1, text: 'HELLO', step3: true },
            { id: 2, text: 'WORLD', step3: true }
        ]);
        expect(uploadMock).toHaveBeenCalledWith([
            { id: 1, text: 'HELLO', step3: true },
            { id: 2, text: 'WORLD', step3: true }
        ]);
    });

    test('runs in dry mode without calling target upload', async () => {
        const uploadMock = jest.fn();
        const adapter = createMockAdapter([
            [{ id: 1 }, { id: 2 }],
            []
        ], uploadMock);

        const adapters: Record<string, Adapter> = { 'adapter1': adapter };

        const pipeline: StreamingPipeline = {
            id: 'pipe-dry',
            dryRun: true,
            source: { id: 's', adapter_id: 'adapter1', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] },
            target: { id: 't', adapter_id: 'adapter1', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] }
        };

        const res = await runStreamingPipeline(pipeline, vault, adapters);

        expect(res.dryRun).toBe(true);
        expect(res.data).toEqual([{ id: 1 }, { id: 2 }]);
        expect(uploadMock).not.toHaveBeenCalled();
    });

    test('supports adapter with streaming API (downloadStream and uploadStream)', async () => {
        const uploadStreamMock = jest.fn(async (stream: AsyncIterable<any[]>) => {
            const received: any[] = [];
            for await (const chunk of stream) {
                received.push(...chunk);
            }
            expect(received).toEqual([{ id: 10, st: true }, { id: 20, st: true }]);
        });

        const streamingAdapterFactory: StreamingAdapter = jest.fn(() => ({
            connect: jest.fn().mockResolvedValue(undefined),
            disconnect: jest.fn().mockResolvedValue(undefined),
            download: jest.fn(),
            downloadStream: async function* () {
                yield [{ id: 10 }];
                yield [{ id: 20 }];
            },
            upload: jest.fn(),
            uploadStream: uploadStreamMock,
            getConfig: () => ({
                id: 'stream-adapter',
                name: 'Stream Adapter',
                type: 'http' as const,
                base_url: 'http://localhost',
                action: ['download' as const, 'upload' as const],
                credential_type: 'basic' as const,
                endpoints: [{ id: 'ep1', path: '/s', method: 'GET' as const, supported_actions: ['download' as const, 'upload' as const] }]
            })
        }));

        const adapters: Record<string, any> = { 'stream-adapter': streamingAdapterFactory };

        const pipeline: StreamingPipeline = {
            id: 'pipe-streaming-api',
            source: { id: 's', adapter_id: 'stream-adapter', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] },
            target: { id: 't', adapter_id: 'stream-adapter', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] },
            transform: [
                async (data: any[]) => data.map(item => ({ ...item, st: true }))
            ]
        };

        const res = await runStreamingPipeline(pipeline, vault, adapters);

        expect(res.totalProcessed).toBe(2);
        expect(uploadStreamMock).toHaveBeenCalled();
        expect(res.data).toEqual([{ id: 10, st: true }, { id: 20, st: true }]);
    });
});

describe('Parallel and Chained Execution', () => {
    const vault: Vault = {
        'cred1': { id: 'cred1', type: 'basic', credentials: { username: 'user', password: 'pwd' } }
    };

    const createMockAdapter = (id: string, data: any[]) => jest.fn(() => ({
        download: jest.fn(async () => ({ data })),
        upload: jest.fn().mockResolvedValue(undefined),
        getConfig: () => ({
            id, name: id, type: 'http' as const, base_url: '', action: ['download' as const, 'upload' as const],
            credential_type: 'basic' as const, endpoints: [{ id: 'ep1', path: '', method: 'GET' as const, supported_actions: ['download' as const, 'upload' as const] }]
        })
    }));

    test('runs multiple streaming pipelines in parallel', async () => {
        const adapterA = createMockAdapter('adapterA', [{ id: 1 }]);
        const adapterB = createMockAdapter('adapterB', [{ id: 2 }]);
        const adapters: Record<string, any> = { 'adapterA': adapterA, 'adapterB': adapterB };

        const pipe1: StreamingPipeline = {
            id: 'p1',
            source: { id: 's1', adapter_id: 'adapterA', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] }
        };
        const pipe2: StreamingPipeline = {
            id: 'p2',
            source: { id: 's2', adapter_id: 'adapterB', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] }
        };

        const results = await runStreamingPipelinesInParallel([pipe1, pipe2], vault, adapters);

        expect(results.length).toBe(2);
        expect(results[0].success).toBe(true);
        expect(results[0].data).toEqual([{ id: 1 }]);
        expect(results[1].success).toBe(true);
        expect(results[1].data).toEqual([{ id: 2 }]);
    });

    test('chains multiple streaming pipelines one after the other', async () => {
        const uploadTarget = jest.fn().mockResolvedValue(undefined);
        const adapterA = createMockAdapter('adapterA', [{ id: 100, status: 'raw' }]);
        const adapterTarget = jest.fn(() => ({
            download: jest.fn(),
            upload: uploadTarget,
            getConfig: () => ({
                id: 'adapterTarget', name: 'target', type: 'http' as const, base_url: '', action: ['upload' as const],
                credential_type: 'basic' as const, endpoints: [{ id: 'ep1', path: '', method: 'POST' as const, supported_actions: ['upload' as const] }]
            })
        }));

        const adapters: Record<string, any> = { 'adapterA': adapterA, 'adapterTarget': adapterTarget };

        // Pipeline 1 extracts from adapterA and transforms
        const pipe1: StreamingPipeline = {
            id: 'step1',
            source: { id: 's1', adapter_id: 'adapterA', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] },
            transform: [
                async (data: any[]) => data.map(item => ({ ...item, status: 'step1-done' }))
            ]
        };

        // Pipeline 2 receives step1 output data and uploads to adapterTarget
        const pipe2: StreamingPipeline = {
            id: 'step2',
            target: { id: 't2', adapter_id: 'adapterTarget', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] },
            transform: [
                async (data: any[]) => data.map(item => ({ ...item, status: 'step2-done' }))
            ]
        };

        const chainedRes = await chainStreamingPipelines([pipe1, pipe2], vault, adapters, { passData: true });

        expect(chainedRes.results.length).toBe(2);
        expect(chainedRes.finalData).toEqual([{ id: 100, status: 'step2-done' }]);
        expect(uploadTarget).toHaveBeenCalledWith([{ id: 100, status: 'step2-done' }]);
    });
});

describe('StreamingPipelineRunner Class & Orchestrator integration', () => {
    test('StreamingPipelineRunner builder pattern and runDry', async () => {
        const pipeline: StreamingPipeline = {
            id: 'builder-pipe',
            data: [{ x: 1 }, { x: 2 }]
        };

        const runner = new StreamingPipelineRunner(pipeline);
        runner.setChunkSize(1)
              .addTransformer(async (data: any[]) => data.map(item => ({ ...item, y: item.x * 10 })));

        const res = await runner.runDry();
        expect(res.dryRun).toBe(true);
        expect(res.chunksCount).toBe(2);
        expect(res.data).toEqual([{ x: 1, y: 10 }, { x: 2, y: 20 }]);
    });

    test('Orchestrator integration with streaming methods', async () => {
        const vault: Vault = {
            'cred1': { id: 'cred1', type: 'basic', credentials: { username: 'u', password: 'p' } }
        };
        const mockAdapter: Adapter = jest.fn(() => ({
            download: jest.fn(async () => ({ data: [{ a: '1' }] })),
            upload: jest.fn(),
            getConfig: () => ({
                id: 'ad1', name: 'ad1', type: 'http' as const, base_url: '', action: ['download' as const],
                credential_type: 'basic' as const, endpoints: [{ id: 'ep1', path: '', method: 'GET' as const, supported_actions: ['download' as const] }]
            })
        }));

        const orchestrator = Orchestrator(vault, { 'ad1': mockAdapter });
        const res = await orchestrator.runStreamingPipeline({
            id: 'orch-pipe',
            source: { id: 's', adapter_id: 'ad1', endpoint_id: 'ep1', credential_id: 'cred1', fields: [] }
        }, { dryRun: true });

        expect(res.data).toEqual([{ a: '1' }]);
        expect(res.dryRun).toBe(true);
    });
});
