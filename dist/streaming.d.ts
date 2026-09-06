import { Connector, PipelineEvent, AdapterInstance, Adapters, Vault, StreamingAdapterInstance, StreamingPipeline, StreamingPipelineResult, ParallelStreamingPipelineResult, ChainedStreamingPipelineResult, TransformerStep } from './types';
/**
 * Creates an AsyncIterable stream of data chunks from a source connector and adapter.
 * Supports both streaming APIs (downloadStream) and paginated/batch APIs (download).
 */
export declare function createSourceStream(connector: Connector, adapter: AdapterInstance | StreamingAdapterInstance, options?: {
    chunkSize?: number;
    limit?: number;
    pageOffsetKey?: number | string;
}, errorHandling?: {
    max_retries: number;
    retry_interval: number;
    use_exponential_backoff?: boolean;
}, log?: (event: PipelineEvent) => void): AsyncGenerator<any[], void, unknown>;
/**
 * Applies transformations to a chunk of data.
 * Executes both existing adapter transformations (connector.transform) and pipeline-level transformer steps.
 */
export declare function applyTransformers(chunk: any[], transformers?: TransformerStep[], connector?: Connector): Promise<any[]>;
/**
 * Uploads a batch of data with retry support
 */
export declare function uploadBatchWithRetry(adapter: StreamingAdapterInstance | AdapterInstance, batch: any[], errorHandling?: {
    max_retries: number;
    retry_interval: number;
    use_exponential_backoff?: boolean;
}, log?: (event: PipelineEvent) => void): Promise<void>;
/**
 * Executes a single streaming mode pipeline
 */
export declare function runStreamingPipeline<T = any>(pipeline: StreamingPipeline<T>, vault?: Vault, adapters?: Adapters, options?: {
    dryRun?: boolean;
}): Promise<StreamingPipelineResult<T>>;
/**
 * Executes multiple streaming pipelines in parallel
 */
export declare function runStreamingPipelinesInParallel<T = any>(pipelines: Array<StreamingPipeline<T>>, vault?: Vault, adapters?: Adapters, options?: {
    dryRun?: boolean;
    stopOnError?: boolean;
}): Promise<Array<ParallelStreamingPipelineResult<T>>>;
/**
 * Chains multiple streaming pipelines to run sequentially.
 * If passData is true or a subsequent pipeline has no source, the output data of the previous pipeline is passed as input data.
 */
export declare function chainStreamingPipelines<T = any>(pipelines: Array<StreamingPipeline<T>>, vault?: Vault, adapters?: Adapters, options?: {
    dryRun?: boolean;
    passData?: boolean;
}): Promise<ChainedStreamingPipelineResult<T>>;
/**
 * Builder/Runner class for managing and executing a streaming pipeline
 */
export declare class StreamingPipelineRunner<T = any> {
    private pipeline;
    private vault;
    private adapters;
    constructor(pipeline: StreamingPipeline<T>, vault?: Vault, adapters?: Adapters);
    addTransformer(step: TransformerStep): this;
    setChunkSize(size: number): this;
    run(options?: {
        dryRun?: boolean;
    }): Promise<StreamingPipelineResult<T>>;
    runDry(): Promise<StreamingPipelineResult<T>>;
}
