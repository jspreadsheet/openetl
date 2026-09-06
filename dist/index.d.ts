/**
 * Core orchestration module for managing data pipelines
 * @module Orchestrator
 */
import { Pipeline, Adapter, Adapters, Vault } from './types';
import { runStreamingPipeline as _runStreamingPipeline, runStreamingPipelinesInParallel as _runStreamingPipelinesInParallel, chainStreamingPipelines as _chainStreamingPipelines, StreamingPipelineRunner as _StreamingPipelineRunner, createSourceStream, applyTransformers, uploadBatchWithRetry } from './streaming';
import { StreamingPipeline, StreamingPipelineResult, ParallelStreamingPipelineResult, ChainedStreamingPipelineResult } from './types';
/**
 * Default configuration values for the orchestrator
 */
export declare const DEFAULT_CONFIG: {
    readonly ITEMS_PER_PAGE: 100;
    readonly TOTAL_ITEMS_LIMIT: 1000000;
    readonly TIMEOUT_MS: 30000;
    readonly RETRY_INTERVAL_MS: 1000;
    readonly MAX_RETRIES: 0;
    readonly MAX_BACKOFF_MS: 30000;
};
/**
 * Configuration options for the Orchestrator
 */
export interface OrchestratorConfig {
    /** Default number of items per page for pagination */
    itemsPerPage?: number;
    /** Maximum total items to fetch (default: 1,000,000) */
    totalItemsLimit?: number;
    /** Download timeout in milliseconds (default: 30,000) */
    timeoutMs?: number;
    /** Base retry interval in milliseconds (default: 1,000) */
    retryIntervalMs?: number;
    /** Maximum number of retries (default: 0) */
    maxRetries?: number;
    /** Maximum backoff delay in milliseconds (default: 30,000) */
    maxBackoffMs?: number;
}
/**
 * Creates an orchestrator instance for managing data pipelines
 * @param vault - Credential vault containing authentication configurations
 * @param availableAdapters - Map of available adapter implementations
 * @returns Object containing pipeline management methods
 */
declare function Orchestrator(vault: Vault, availableAdapters: Adapters): {
    registerAdapter: (id: string, adapter: Adapter) => void;
    runPipeline: <T>(pipeline: Pipeline<T>) => Promise<{
        data: T[];
    }>;
    runStreamingPipeline: <T>(pipeline: StreamingPipeline<T>, options?: {
        dryRun?: boolean;
    }) => Promise<StreamingPipelineResult<T>>;
    runParallelStreamingPipelines: <T>(pipelines: Array<StreamingPipeline<T>>, options?: {
        dryRun?: boolean;
        stopOnError?: boolean;
    }) => Promise<Array<ParallelStreamingPipelineResult<T>>>;
    runStreamingPipelinesInParallel: <T>(pipelines: Array<StreamingPipeline<T>>, options?: {
        dryRun?: boolean;
        stopOnError?: boolean;
    }) => Promise<Array<ParallelStreamingPipelineResult<T>>>;
    chainStreamingPipelines: <T>(pipelines: Array<StreamingPipeline<T>>, options?: {
        dryRun?: boolean;
        passData?: boolean;
    }) => Promise<ChainedStreamingPipelineResult<T>>;
    createStreamingRunner: <T>(pipeline: StreamingPipeline<T>) => _StreamingPipelineRunner<T>;
};
export { Orchestrator };
export { DatabaseAdapter, BasicAuth, HttpAdapter, Connector, AuthConfig, OAuth2Auth, ApiKeyAuth, AdapterInstance, Filter, FilterGroup, AdapterError, AdapterErrorCode, ValidationResult, SchemaInfo, PipelineValidationResult, validatePipeline, StreamingAdapterOptions, StreamingAdapterInstance, StreamingAdapter, TransformerStep, StreamingPipeline, StreamingPipelineResult, ParallelStreamingPipelineResult, ChainedStreamingPipelineResult } from './types';
export { _runStreamingPipeline as runStreamingPipeline, _runStreamingPipelinesInParallel as runStreamingPipelinesInParallel, _chainStreamingPipelines as chainStreamingPipelines, _StreamingPipelineRunner as StreamingPipelineRunner, createSourceStream, applyTransformers, uploadBatchWithRetry };
