import {
    StreamingOrchestrator,
    chunkIterableFromPages,
    applyTransformers,
    transformStream,
    collectTransformers,
    hasDownloadStream,
    hasUploadStream,
    buildSourceStream,
    consumeToTarget,
} from '../src/index';
import {
    Adapter,
    Adapters,
    Vault,
    StreamChunk,
    StreamingAdapterInstance,
} from '../src/types';
import type { StreamingPipeline } from '../src/streaming';

/** Collects an async iterable of chunks into a flat array. */
async function collect(stream: AsyncIterable<StreamChunk>): Promise<any[]> {
    const out: any[] = [];
    for await (const chunk of stream) out.push(...chunk.data);
    return out;
}

// ---------------------------------------------------------------------------
// Adapters used by the tests
// ---------------------------------------------------------------------------

const baseConfig = {
    id: '',
    name: '',
    type: 'http' as const,
    base_url: '',
    action: [],
    credential_type: 'basic' as const,
    config: [],
    endpoints: [
        {
            id: 'create',
            path: '',
            method: 'GET' as const,
            supported_actions: ['download', 'upload'] as Array<'download' | 'upload'>,
        },
    ],
};

/** Paginated (offset) source adapter with 5 records. */
const offsetSourceAdapter: Adapter = () => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    download: jest.fn(async ({ limit, offset }) => {
        const off = typeof offset === 'string' ? parseInt(offset, 10) : (offset || 0);
        const total = 5;
        const lim = limit || 2;
        const data = [];
        for (let i = off; i < Math.min(off + lim, total); i++) data.push({ id: i, name: `n${i}` });
        return { data };
    }),
    getConfig: () => ({
        ...baseConfig,
        pagination: { type: 'offset', maxItemsPerPage: 2 },
    }),
});

/** Target adapter that records uploaded batches. */
function makeTargetAdapter(store: any[][]): Adapter {
    return () => ({
        connect: jest.fn().mockResolvedValue(undefined),
        disconnect: jest.fn().mockResolvedValue(undefined),
        download: jest.fn(async () => ({ data: [] })),
        upload: jest.fn(async (data: any[]) => { store.push(data); }),
        getConfig: () => ({ ...baseConfig, pagination: { type: 'offset', maxItemsPerPage: 2 } }),
    });
}

/** Native streaming source adapter. */
const streamingSourceAdapter: Adapter = () => ({
    connect: jest.fn().mockResolvedValue(undefined),
    disconnect: jest.fn().mockResolvedValue(undefined),
    download: jest.fn(async () => ({ data: [] })),
    downloadStream: async function* () {
        yield { data: [{ id: 1 }, { id: 2 }] };
        yield { data: [{ id: 3 }] };
    },
    getConfig: () => ({ ...baseConfig }),
} as StreamingAdapterInstance);

const vault: Vault = {
    cred: {
        id: 'cred',
        type: 'basic',
        credentials: { username: 'u', password: 'p' },
    },
};

function sourceConnector(overrides: Partial<any> = {}) {
    return {
        id: 'src',
        adapter_id: 'source',
        endpoint_id: 'create',
        credential_id: 'cred',
        fields: [],
        ...overrides,
    };
}

function targetConnector(overrides: Partial<any> = {}) {
    return {
        id: 'tgt',
        adapter_id: 'target',
        endpoint_id: 'create',
        credential_id: 'cred',
        fields: [],
        ...overrides,
    };
}

// ---------------------------------------------------------------------------
// Unit tests for critical helper functions
// ---------------------------------------------------------------------------

describe('chunkIterableFromPages', () => {
    it('iterates offset pagination to completion', async () => {
        const stream = chunkIterableFromPages(
            async (offset) => {
                const off = typeof offset === 'string' ? parseInt(offset, 10) : (offset || 0) as number;
                const data = [];
                for (let i = off; i < Math.min(off + 2, 5); i++) data.push({ id: i });
                return { data };
            },
            { startOffset: 0, itemsPerPage: 2, paginationType: 'offset' }
        );
        expect(await collect(stream)).toHaveLength(5);
    });

    it('iterates cursor pagination to completion', async () => {
        const stream = chunkIterableFromPages(
            async (offset) => {
                if (offset === undefined) return { data: [{ id: 1 }, { id: 2 }], options: { nextOffset: 2 } };
                if (offset === 2) return { data: [{ id: 3 }], options: { nextOffset: undefined } };
                return { data: [] };
            },
            { paginationType: 'cursor' }
        );
        expect(await collect(stream)).toHaveLength(3);
    });

    it('respects the total limit', async () => {
        const stream = chunkIterableFromPages(
            async (offset) => {
                const off = (offset as number) || 0;
                return { data: [{ id: off }, { id: off + 1 }] };
            },
            { startOffset: 0, itemsPerPage: 2, totalLimit: 3, paginationType: 'offset' }
        );
        expect(await collect(stream)).toHaveLength(3);
    });
});

describe('applyTransformers', () => {
    it('applies multiple transformer steps in order', async () => {
        const out = await applyTransformers(
            [{ name: 'bob' }],
            [
                [{ type: 'uppercase', options: { field: 'name' } }],
                [{ type: 'addPrefix', options: { field: 'name', prefix: 'X-' } }],
            ]
        );
        expect(out[0].name).toBe('X-BOB');
    });

    it('is a no-op with no transformer steps', async () => {
        const out = await applyTransformers([{ a: 1 }], []);
        expect(out).toEqual([{ a: 1 }]);
    });
});

describe('transformStream', () => {
    it('lazily transforms each chunk', async () => {
        async function* single(): AsyncIterable<StreamChunk> {
            yield { data: [{ name: 'a' }] };
            yield { data: [{ name: 'b' }] };
        }
        const stream = transformStream(single(), [[{ type: 'uppercase', options: { field: 'name' } }]]);
        const out = await collect(stream);
        expect(out.map(x => x.name)).toEqual(['A', 'B']);
    });
});

describe('collectTransformers', () => {
    it('combines source.transform and transformers', () => {
        const steps = collectTransformers({
            id: 'p',
            source: sourceConnector({ transform: [{ type: 'uppercase', options: { field: 'name' } }] }),
            transformers: [[{ type: 'addPrefix', options: { field: 'name', prefix: 'Y-' } }]],
        } as StreamingPipeline);
        expect(steps).toHaveLength(2);
    });
});

describe('hasDownloadStream / hasUploadStream', () => {
    it('detects native streaming APIs', () => {
        const s = streamingSourceAdapter(sourceConnector() as any, vault.cred) as StreamingAdapterInstance;
        expect(hasDownloadStream(s)).toBe(true);
        expect(hasUploadStream(s)).toBe(false);
    });
});

describe('consumeToTarget', () => {
    it('uploads chunks in batches', async () => {
        const store: any[][] = [];
        const adapter = makeTargetAdapter(store)(targetConnector() as any, vault.cred) as StreamingAdapterInstance;
        async function* s(): AsyncIterable<StreamChunk> {
            yield { data: [1, 2] };
            yield { data: [3] };
        }
        const written = await consumeToTarget(s, adapter);
        expect(written).toBe(3);
        expect(store).toHaveLength(2);
    });

    it('uses native uploadStream when available', async () => {
        const received: any[] = [];
        const adapter: StreamingAdapterInstance = {
            getConfig: () => ({ ...baseConfig }),
            download: async () => ({ data: [] }),
            uploadStream: async (stream) => {
                for await (const c of stream) received.push(...c.data);
            },
        };
        async function* s(): AsyncIterable<StreamChunk> {
            yield { data: [1, 2, 3] };
        }
        const written = await consumeToTarget(s, adapter);
        expect(written).toBe(3);
        expect(received).toHaveLength(3);
    });
});

describe('buildSourceStream', () => {
    it('uses native downloadStream when present', async () => {
        const adapter = streamingSourceAdapter(sourceConnector() as any, vault.cred) as StreamingAdapterInstance;
        const stream = buildSourceStream(sourceConnector(), adapter);
        expect(await collect(stream)).toHaveLength(3);
    });

    it('falls back to paginated download', async () => {
        const adapter = offsetSourceAdapter(sourceConnector() as any, vault.cred) as StreamingAdapterInstance;
        const stream = buildSourceStream(sourceConnector(), adapter);
        expect(await collect(stream)).toHaveLength(5);
    });
});

// ---------------------------------------------------------------------------
// Integration tests for the orchestrator
// ---------------------------------------------------------------------------

describe('StreamingOrchestrator.runStreamingPipeline', () => {
    it('streams from source to target with transforms', async () => {
        const store: any[][] = [];
        const adapters: Adapters = {
            source: offsetSourceAdapter,
            target: makeTargetAdapter(store),
        };
        const orch = StreamingOrchestrator(vault, adapters);
        const result = await orch.runStreamingPipeline({
            id: 'p1',
            source: sourceConnector({ transform: [{ type: 'uppercase', options: { field: 'name' } }] }),
            target: targetConnector(),
        } as StreamingPipeline);

        expect(result.read).toBe(5);
        expect(result.written).toBe(5);
        expect(store.flat()).toHaveLength(5);
        expect(store.flat()[0].name).toBe('N0');
    });

    it('does not upload in dry run mode', async () => {
        const store: any[][] = [];
        const adapters: Adapters = { source: offsetSourceAdapter, target: makeTargetAdapter(store) };
        const orch = StreamingOrchestrator(vault, adapters);
        const result = await orch.runStreamingPipeline({
            id: 'p-dry',
            source: sourceConnector(),
            target: targetConnector(),
            dryRun: true,
        } as StreamingPipeline);

        expect(result.dryRun).toBe(true);
        expect(result.written).toBe(0);
        expect(result.read).toBe(5);
        expect(store).toHaveLength(0);
    });

    it('supports multiple transformers', async () => {
        const store: any[][] = [];
        const adapters: Adapters = { source: offsetSourceAdapter, target: makeTargetAdapter(store) };
        const orch = StreamingOrchestrator(vault, adapters);
        await orch.runStreamingPipeline({
            id: 'p-mt',
            source: sourceConnector(),
            target: targetConnector(),
            transformers: [
                [{ type: 'uppercase', options: { field: 'name' } }],
                [{ type: 'addPrefix', options: { field: 'name', prefix: 'X-' } }],
            ],
        } as StreamingPipeline);
        expect(store.flat()[0].name).toBe('X-N0');
    });

    it('supports adapters with a native streaming API', async () => {
        const store: any[][] = [];
        const adapters: Adapters = { source: streamingSourceAdapter, target: makeTargetAdapter(store) };
        const orch = StreamingOrchestrator(vault, adapters);
        const result = await orch.runStreamingPipeline({
            id: 'p-stream',
            source: sourceConnector(),
            target: targetConnector(),
        } as StreamingPipeline);
        expect(result.read).toBe(3);
        expect(result.written).toBe(3);
    });
});

describe('StreamingOrchestrator parallel & chain', () => {
    it('runs multiple pipelines in parallel', async () => {
        const storeA: any[][] = [];
        const storeB: any[][] = [];
        const adapters: Adapters = {
            source: offsetSourceAdapter,
            targetA: makeTargetAdapter(storeA),
            targetB: makeTargetAdapter(storeB),
        };
        const orch = StreamingOrchestrator(vault, adapters);
        const results = await orch.runStreamingPipelines([
            { id: 'pa', source: sourceConnector(), target: targetConnector({ adapter_id: 'targetA' }) } as StreamingPipeline,
            { id: 'pb', source: sourceConnector(), target: targetConnector({ adapter_id: 'targetB' }) } as StreamingPipeline,
        ]);
        expect(results).toHaveLength(2);
        expect(storeA.flat()).toHaveLength(5);
        expect(storeB.flat()).toHaveLength(5);
    });

    it('chains pipelines feeding output to the next', async () => {
        const store: any[][] = [];
        const adapters: Adapters = { source: offsetSourceAdapter, target: makeTargetAdapter(store) };
        const orch = StreamingOrchestrator(vault, adapters);
        const results = await orch.runStreamingPipelineChain([
            {
                id: 'c1',
                source: sourceConnector({ transform: [{ type: 'uppercase', options: { field: 'name' } }] }),
            } as StreamingPipeline,
            {
                id: 'c2',
                target: targetConnector(),
                transformers: [[{ type: 'addPrefix', options: { field: 'name', prefix: 'Z-' } }]],
            } as StreamingPipeline,
        ]);
        expect(results).toHaveLength(2);
        expect(store.flat()[0].name).toBe('Z-N0');
    });
});
