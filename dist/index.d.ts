/**
 * Core orchestration module for managing data pipelines
 * @module Orchestrator
 */
import { Pipeline, Adapter, Adapters, Vault } from './types';
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
};
export { Orchestrator };
export { DatabaseAdapter, BasicAuth, HttpAdapter, Connector, AuthConfig, OAuth2Auth, ApiKeyAuth, AdapterInstance, Filter, FilterGroup, AdapterError, AdapterErrorCode, ValidationResult, SchemaInfo, PipelineValidationResult, validatePipeline } from './types';
