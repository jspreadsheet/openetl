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
        [key: string]: string | undefined;
    };
};
export type AuthConfig = ApiKeyAuth | OAuth2Auth | BasicAuth;
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
}
export interface EndpointSettings {
    pagination?: AdapterPagination | false;
    config?: ConfigItem[];
}
export type Helpers = {
    getCode: (redirectUrl: string, client_id: string) => string;
    getTokens: (redirectUrl: string, client_id: string, secret_id: string, queryParams: string) => object;
};
export interface BaseAdapter {
    id: string;
    name: string;
    category?: string;
    image?: string;
    action: Array<"download" | "upload" | "sync">;
    credential_type: "api_key" | "oauth2" | "basic";
    config?: ConfigItem[];
    metadata?: {
        description?: string;
        provider?: string;
        [key: string]: any;
    };
    pagination?: AdapterPagination;
    helpers?: Helpers;
}
export interface Endpoint {
    id: string;
    tool?: string;
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
export type Filter = {
    field: string;
    operator: string;
    value: string | number;
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
export type TransformationType = 'concat' | 'renameKey' | 'uppercase' | 'lowercase' | 'trim' | 'split' | 'replace' | 'addPrefix' | 'addSuffix' | 'toNumber' | 'extract' | 'mergeObjects' | Function;
export type TransformationOption = ConcatOptions | RenameKeyOptions | FieldTransformationOptions | SplitOptions | ReplaceOptions | PrefixOptions | SuffixOptions | ExtractOptions | MergeObjectsOptions;
export type Transformation = {
    type: TransformationType;
    options: TransformationOption;
};
export type Sort = {
    type: 'asc' | 'desc';
    field: string;
};
export type Pagination = {
    itemsPerPage?: number;
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
    filters?: Array<Filter>;
    transform?: Transformation[];
    sort?: Sort[];
    limit?: number;
    pagination?: Pagination;
    timeout?: number;
    debug?: boolean;
}
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
    };
    rate_limiting?: {
        requests_per_second: number;
        max_retries_on_rate_limit: number;
    };
}
export interface AdapterInstance {
    getConfig: () => HttpAdapter | DatabaseAdapter;
    connect?(): Promise<void>;
    disconnect?: () => Promise<void>;
    download(pageOptions: {
        limit?: number;
        offset?: number | string;
    }): Promise<{
        data: any[];
        options?: {
            [key: string]: any;
        };
    }>;
    upload?(data: any[]): Promise<void>;
    getOauthPermissionUrl?: (redirectUrl?: string) => string;
}
export type Adapter = (connector: Connector, auth: AuthConfig) => AdapterInstance;
export type Adapters = {
    [key: string]: Adapter;
};
