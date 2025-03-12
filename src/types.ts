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
        token_url?: string;
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

// Adapter Types
export interface BaseAdapter {
    id: string;
    name: string;
    type: "http" | "database" | "file";
    action: Array<"download" | "upload" | "sync">;
    credential_type: "api_key" | "oauth2" | "basic";
    config: {
        name: string,
        required: boolean,
        default?: any,
    }[],
    metadata?: {
        description?: string;
        provider?: string;
        [key: string]: any;
    };
}

export interface HttpAdapter extends BaseAdapter {
    type: "http";
    base_url: string;
    endpoints: Array<{
        id: string;
        path: string;
        method: "GET" | "POST" | "PUT" | "DELETE";
        description?: string;
        supported_actions: Array<"download" | "upload" | "sync">;
    }>;
}

export interface DatabaseAdapter extends BaseAdapter {
    type: "database";
    endpoints: Array<{
        id: string;
        query_type: "table" | "custom";
        description?: string;
        supported_actions: Array<"download" | "upload" | "sync">;  // Added "upload"
        pagination?: boolean;
    }>;
}

// Connector Types
export type Filter = {
    field: string;
    operator: string;
    value: string;
};

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
    pageOffsetKey?: string;      // Initial offset as string
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
        fail_on_error: boolean;
    };
    rate_limiting?: {
        requests_per_second: number;
        // To do
        // concurrent_requests: number;
        max_retries_on_rate_limit: number;
    };
}

export interface AdapterInstance {
    paginationType?: string,
    maxItemsPerPage?: number;
    connect?(): Promise<void>;
    disconnect?: () => Promise<void>;
    download(pageOptions: { limit?: number; offset?: number }): Promise<{
        data: any[];
        options?: {
            [key: string]: any; // Allow future extensions
        };
    }>;
    upload?(data: any[]): Promise<void>;
}

export type Adapter = (connector: Connector, auth: AuthConfig) => AdapterInstance;

export type Adapters = {
    [key: string]: Adapter;
};
