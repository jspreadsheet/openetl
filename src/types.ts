interface AxiosError {
    response?: {
        status: number;
        data?: unknown;
    };
}

function isAxiosError(error: unknown): error is AxiosError {
    return (
        typeof error === 'object' &&
        error !== null &&
        'response' in error &&
        typeof (error as any).response === 'object'
    );
}

// Error handling types
export type AdapterErrorCode =
    | 'CONNECTION_FAILED'
    | 'AUTHENTICATION_FAILED'
    | 'VALIDATION_FAILED'
    | 'DOWNLOAD_FAILED'
    | 'UPLOAD_FAILED'
    | 'TIMEOUT'
    | 'RATE_LIMITED'
    | 'NOT_FOUND'
    | 'INVALID_CONFIG'
    | 'UNKNOWN';

/**
 * Standardized error class for adapter operations with retry support and context
 */
export class AdapterError extends Error {
    public readonly code: AdapterErrorCode;
    public readonly retryable: boolean;
    public readonly context?: Record<string, unknown>;
    public readonly cause?: Error;

    constructor(
        message: string,
        code: AdapterErrorCode = 'UNKNOWN',
        retryable: boolean = false,
        context?: Record<string, unknown>,
        cause?: Error
    ) {
        super(message);
        this.name = 'AdapterError';
        this.code = code;
        this.retryable = retryable;
        this.context = context;
        this.cause = cause;

        // Maintains proper stack trace for where error was thrown (V8 engines)
        if (Error.captureStackTrace) {
            Error.captureStackTrace(this, AdapterError);
        }
    }

    /**
     * Creates an AdapterError from an unknown error type
     */
    static from(
        error: unknown,
        code: AdapterErrorCode = 'UNKNOWN',
        retryable: boolean = false,
        context?: Record<string, unknown>
    ): AdapterError {
        if (error instanceof AdapterError) {
            return error;
        }

        const message = error instanceof Error ? error.message : String(error);
        const cause = error instanceof Error ? error : undefined;

        // Detect specific error types from axios or other sources
        if (isAxiosError(error)) {
            const status = error.response?.status;
            if (status === 401 || status === 403) {
                return new AdapterError(message, 'AUTHENTICATION_FAILED', false, context, cause);
            }
            if (status === 404) {
                return new AdapterError(message, 'NOT_FOUND', false, context, cause);
            }
            if (status === 429) {
                return new AdapterError(message, 'RATE_LIMITED', true, context, cause);
            }
            if (status && status >= 500) {
                return new AdapterError(message, code, true, context, cause);
            }
        }

        return new AdapterError(message, code, retryable, context, cause);
    }

    /**
     * Serializes the error for logging or transmission
     */
    toJSON(): Record<string, unknown> {
        return {
            name: this.name,
            message: this.message,
            code: this.code,
            retryable: this.retryable,
            context: this.context,
            cause: this.cause ? {
                name: this.cause.name,
                message: this.cause.message
            } : undefined,
            stack: this.stack
        };
    }
}

// Auth Types
export type BaseAuth = {
    id: string;
    type?: "api_key" | "oauth2" | "basic";
    name?: string;
    provider?: string;
    environment?: "production" | "staging" | "development";
    metadata?: {
        created_by: string;
        created_at: string;
        updated_at: string;
    };
    retry?: {
        attempts: number;
        delay: number;
    };
    timeout?: number;
    expires_at?: string | number;
};

export type ApiKeyAuth = BaseAuth & {
    type: "api_key";
    credentials: {
        api_key: string;
        api_secret?: string;
        [key: string]: string | undefined;
    };
};

export type OAuth2Auth = BaseAuth & {
    type: "oauth2";
    credentials: {
        client_id: string;
        client_secret: string;
        refresh_token?: string;
        access_token?: string;
    };
    scopes?: string[];
};

export type BasicAuth = BaseAuth & {
    type: "basic";
    credentials: {
        username: string;
        password: string;
        host?: string | undefined;
        port?: string | undefined;
        database?: string | undefined;
        [key: string]: string | undefined;  // Allow additional string properties
    };
};

export type AuthConfig = ApiKeyAuth | OAuth2Auth | BasicAuth;

// Add Vault type
export type Vault = Record<string, AuthConfig>;

export type AdapterPagination = {
    type: string;
    maxItemsPerPage?: number;
};

export interface ConfigItem {
    id: string;
    name: string;
    required: boolean;
    type?: string;
    default?: any;
};

export interface EndpointSettings {
    pagination?: AdapterPagination | false;
    config?: ConfigItem[],
}

export type Helpers = {
    getCode: (redirectUrl: string, client_id: string) => string;
    getTokens: (redirectUrl: string, client_id: string, secret_id: string, queryParams: string) => object;
}

// Adapter Types
export interface BaseAdapter {
    id: string;
    name: string;
    type?: "http" | "database" | "file";
    category?: string;
    image?: string;
    action: Array<"download" | "upload" | "sync">;
    credential_type: "api_key" | "oauth2" | "basic";
    config?: ConfigItem[];
    metadata?: {
        description?: string;
        provider?: string;
        version?: string;
        [key: string]: any;
    };
    pagination?: AdapterPagination;
    helpers?: Helpers;
}

export interface Endpoint {
    id: string;
    tool?: string,
    description?: string;
    supported_actions: Array<"download" | "upload" | "sync">;
    settings?: EndpointSettings;
}

export interface HttpEndpoint extends Endpoint {
    path: string;
    method: "GET" | "POST" | "PUT" | "DELETE";
    defaultFields?: string[];
}

export interface HttpAdapter extends BaseAdapter {
    type: "http";
    base_url: string;
    endpoints: HttpEndpoint[];
}

export interface DatabaseEndpoint extends Endpoint {
    query_type: "table" | "custom";
}

export interface DatabaseAdapter extends BaseAdapter {
    type: "database";

    /**
     * @default true
     */
    hasGetColumnsRoute?: boolean;

    endpoints: DatabaseEndpoint[];
}

// Connector Types
export type Filter = {
    field: string;
    operator: string;
    value: string | number | boolean | null | Array<string | number>;
};

/**
 * Complex filter conditions with AND/OR logic for nested filtering
 */
export type FilterGroup = {
    op: "AND" | "OR";
    filters: Array<Filter | FilterGroup>;
};

export type BaseTransformationOption = {
    to?: string;
};

export type ConcatOptions = BaseTransformationOption & {
    properties: string[];
    glue?: string;
};

export type RenameKeyOptions = BaseTransformationOption & {
    from: string;
};

export type FieldTransformationOptions = BaseTransformationOption & {
    field: string;
};

export type SplitOptions = FieldTransformationOptions & {
    delimiter: string;
};

export type ReplaceOptions = FieldTransformationOptions & {
    search: string;
    replace: string;
};

export type PrefixOptions = FieldTransformationOptions & {
    prefix: string;
};

export type SuffixOptions = FieldTransformationOptions & {
    suffix: string;
};

export type ExtractOptions = FieldTransformationOptions & {
    pattern?: string;
    start?: number;
    end?: number;
};

export type MergeObjectsOptions = BaseTransformationOption & {
    fields: string[];
};

export type TransformationType =
    | 'concat'
    | 'renameKey'
    | 'uppercase'
    | 'lowercase'
    | 'trim'
    | 'split'
    | 'replace'
    | 'addPrefix'
    | 'addSuffix'
    | 'toNumber'
    | 'extract'
    | 'mergeObjects'
    | Function

export type TransformationOption =
    | ConcatOptions
    | RenameKeyOptions
    | FieldTransformationOptions
    | SplitOptions
    | ReplaceOptions
    | PrefixOptions
    | SuffixOptions
    | ExtractOptions
    | MergeObjectsOptions;

export type Transformation = {
    type: TransformationType;
    options: TransformationOption;
};

export type Sort = {
    type: 'asc' | 'desc';
    field: string;
};

export type Pagination = {
    itemsPerPage?: number;       // Number of items per page
    pageOffsetKey?: number | string;
};

export interface Connector {
    id: string;
    adapter_id: string;
    endpoint_id: string;
    credential_id: string;
    config?: {
        headers?: Record<string, string>;
        query_params?: Record<string, string>;
        schema?: string;
        table?: string;
        custom_query?: string;

        [key: string]: any;
    };
    fields: string[];
    filters?: Array<Filter | FilterGroup>;
    transform?: Transformation[];
    sort?: Sort[];
    limit?: number;                    // Total items to fetch, defaults to 1M in core.ts if unset
    pagination?: Pagination;
    timeout?: number;                  // Maximum time in milliseconds for download process, defaults to 30s in core.ts if unset
    debug?: boolean;
}

// Pipeline Types
export type PipelineEvent = {
    type: "start" | "extract" | "transform" | "load" | "error" | "complete" | "info";
    message: string;
    timestamp?: string;
    dataCount?: number;
};

export interface Pipeline<T = object> {
    id: string;
    data?: T[];
    source?: Connector;
    target?: Connector;
    schedule?: {
        frequency: "hourly" | "daily" | "weekly";
        at: string;
    };
    logging?: (event: PipelineEvent) => void;
    onload?: (data: T[]) => void;
    onbeforesend?: (data: T[]) => T[] | boolean | void;
    onupload?: () => void;
    error_handling?: {
        max_retries: number;
        retry_interval: number;
        /** Use exponential backoff with jitter for retries */
        use_exponential_backoff?: boolean;
    };
    rate_limiting?: {
        requests_per_second: number;
        // To do
        // concurrent_requests: number;
        max_retries_on_rate_limit: number;
    };
}

/**
 * Validation result returned by adapter's validate method
 */
export interface ValidationResult {
    valid: boolean;
    message?: string;
    details?: Record<string, unknown>;
}

/**
 * Schema information returned by adapter's getSchema method
 */
export interface SchemaInfo {
    fields: string[];
    types?: Record<string, string>;
    primaryKey?: string | string[];
    required?: string[];
}

export interface AdapterInstance {
    getConfig: () => HttpAdapter | DatabaseAdapter;
    connect?(): Promise<void>;
    disconnect?: () => Promise<void>;
    download(pageOptions: { limit?: number; offset?: number | string }): Promise<{
        data: any[];
        options?: {
            nextOffset?: string | number;
            totalCount?: number;
            [key: string]: any;
        };
    }>;
    upload?(data: any[]): Promise<void>;
    getOauthPermissionUrl?: (redirectUrl?: string) => string;

    /**
     * Validates the adapter configuration and connection
     */
    validate?(): Promise<ValidationResult>;

    /**
     * Returns schema information for the current endpoint/table
     */
    getSchema?(): Promise<SchemaInfo>;

    /**
     * Returns the total count of records for the current query
     */
    count?(): Promise<number>;
}

export type Adapter = (connector: Connector, auth: AuthConfig) => AdapterInstance;

export type Adapters = {
    [key: string]: Adapter;
};

/**
 * Result of pipeline validation
 */
export interface PipelineValidationResult {
    valid: boolean;
    errors: string[];
    warnings: string[];
}

/**
 * Validates a pipeline configuration before execution
 * @param pipeline - Pipeline configuration to validate
 * @param adapters - Available adapters
 * @param vault - Credential vault
 * @returns Validation result with errors and warnings
 */
export function validatePipeline<T>(
    pipeline: Pipeline<T>,
    adapters: Adapters,
    vault: Vault
): PipelineValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Validate pipeline has source or data
    if (!pipeline.source && !pipeline.data) {
        errors.push('Pipeline must have either a source or data');
    }

    // Validate source configuration
    if (pipeline.source) {
        const { adapter_id, endpoint_id, credential_id } = pipeline.source;

        // Check adapter exists
        if (!adapters[adapter_id]) {
            errors.push(`Source adapter '${adapter_id}' not found`);
        }

        // Check credentials exist
        if (!vault[credential_id]) {
            errors.push(`Source credentials '${credential_id}' not found in vault`);
        }

        // Validate endpoint_id is provided
        if (!endpoint_id) {
            errors.push('Source endpoint_id is required');
        }

        // Warn if no fields specified
        if (!pipeline.source.fields || pipeline.source.fields.length === 0) {
            warnings.push('No fields specified for source - will use defaults if available');
        }

        // Validate filters if present
        if (pipeline.source.filters) {
            pipeline.source.filters.forEach((filter, index) => {
                if ('field' in filter) {
                    if (!filter.field) {
                        errors.push(`Filter at index ${index} has empty field`);
                    }
                    if (!filter.operator) {
                        errors.push(`Filter at index ${index} has empty operator`);
                    }
                }
            });
        }
    }

    // Validate target configuration
    if (pipeline.target) {
        const { adapter_id, endpoint_id, credential_id } = pipeline.target;

        // Check adapter exists
        if (!adapters[adapter_id]) {
            errors.push(`Target adapter '${adapter_id}' not found`);
        }

        // Check credentials exist
        if (!vault[credential_id]) {
            errors.push(`Target credentials '${credential_id}' not found in vault`);
        }

        // Validate endpoint_id is provided
        if (!endpoint_id) {
            errors.push('Target endpoint_id is required');
        }
    }

    // Validate error_handling if present
    if (pipeline.error_handling) {
        if (pipeline.error_handling.max_retries < 0) {
            errors.push('max_retries must be non-negative');
        }
        if (pipeline.error_handling.retry_interval < 0) {
            errors.push('retry_interval must be non-negative');
        }
    }

    // Validate rate_limiting if present
    if (pipeline.rate_limiting) {
        if (pipeline.rate_limiting.requests_per_second <= 0) {
            errors.push('requests_per_second must be positive');
        }
    }

    return {
        valid: errors.length === 0,
        errors,
        warnings
    };
}

// Streaming Pipeline Types
export interface StreamingAdapterOptions {
    limit?: number;
    offset?: number | string;
    chunkSize?: number;
    [key: string]: any;
}

export interface StreamingAdapterInstance extends AdapterInstance {
    /**
     * Download data as an asynchronous stream of chunks/batches
     */
    downloadStream?(options: StreamingAdapterOptions): AsyncIterable<any[]>;
    
    /**
     * Upload data from an asynchronous stream of chunks/batches
     */
    uploadStream?(stream: AsyncIterable<any[]>, options?: Record<string, any>): Promise<void>;
}

export type StreamingAdapter = (connector: Connector, auth: AuthConfig) => StreamingAdapterInstance;

export type TransformerStep = Transformation | ((data: any[], connector?: Connector) => Promise<any[]> | any[]);

export interface StreamingPipeline<T = any> extends Pipeline<T> {
    /** Number of items per chunk/batch during streaming (default: 1000) */
    chunkSize?: number;
    /** If true, runs the pipeline without executing target upload operations */
    dryRun?: boolean;
    /** Additional transformer steps to apply to each chunk */
    transformers?: TransformerStep[];
    /** Alias for transformers */
    transform?: TransformerStep[];
    /** Callback invoked after each chunk is extracted and transformed */
    onChunk?: (chunk: T[], chunkIndex: number) => Promise<void> | void;
}

export interface StreamingPipelineResult<T = any> {
    data: T[];
    chunksCount: number;
    totalProcessed: number;
    dryRun?: boolean;
}

export interface ParallelStreamingPipelineResult<T = any> {
    pipelineId: string;
    data: T[];
    chunksCount: number;
    totalProcessed: number;
    success: boolean;
    error?: Error;
}

export interface ChainedStreamingPipelineResult<T = any> {
    results: Array<{
        pipelineId: string;
        data: T[];
        chunksCount: number;
        totalProcessed: number;
    }>;
    finalData: T[];
}
