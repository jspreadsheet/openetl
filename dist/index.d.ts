/**
 * Core orchestration module for managing data pipelines
 * @module Orchestrator
 */
import { Pipeline, Adapter, Adapters, Vault } from './types';
/**
 * Creates an orchestrator instance for managing data pipelines
 * @param vault - Credential vault containing authentication configurations
 * @param availableAdapters - Map of available adapter implementations
 * @returns Object containing pipeline management methods
 */
declare function Orchestrator(vault: Vault, availableAdapters: Adapters): {
    registerAdapter: (id: string, adapter: Adapter) => void;
    runPipeline: <T>(pipeline: Pipeline<T>) => Promise<void>;
};
export { Orchestrator };
export { DatabaseAdapter, BasicAuth, HttpAdapter, Connector, AuthConfig, OAuth2Auth, AdapterInstance, FilterGroup, Filter } from './types';
