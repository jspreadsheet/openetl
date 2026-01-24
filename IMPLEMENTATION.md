# OpenETL Implementation Improvements

This document captures all improvements identified and partially implemented during the January 2026 review session. These should be implemented systematically in future development cycles.

---

## 1. Core Type Improvements (src/types.ts)

### 1.1 AdapterError Class
Standardized error handling with retry support and context.

```typescript
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
    }

    static from(error: unknown, code?: AdapterErrorCode, retryable?: boolean, context?: Record<string, unknown>): AdapterError;
    toJSON(): Record<string, unknown>;
}
```

### 1.2 ConfigItem Interface
Structured configuration for adapter settings.

```typescript
export interface ConfigItem {
    id: string;
    name: string;
    required: boolean;
    type?: string;
    default?: any;
}
```

### 1.3 EndpointSettings Interface
Settings for adapter endpoints.

```typescript
export interface EndpointSettings {
    pagination?: AdapterPagination | false;
    config?: ConfigItem[];
}
```

### 1.4 Helpers Type
OAuth flow utilities.

```typescript
export type Helpers = {
    getCode: (redirectUrl: string, client_id: string) => string;
    getTokens: (redirectUrl: string, client_id: string, secret_id: string, queryParams: string) => object;
}
```

### 1.5 Enhanced BaseAdapter Interface
```typescript
export interface BaseAdapter {
    id: string;
    name: string;
    type?: "http" | "database" | "file";
    category?: string;  // e.g., "Databases & Data Warehouses"
    image?: string;     // URL to adapter logo
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
```

### 1.6 Enhanced Endpoint Interfaces
```typescript
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

export interface DatabaseEndpoint extends Endpoint {
    query_type: "table" | "custom";
}
```

### 1.7 FilterGroup Support
Complex filter conditions with AND/OR logic.

```typescript
export type FilterGroup = {
    op: "AND" | "OR";
    filters: Array<Filter | FilterGroup>;
};

// In Connector:
filters?: Array<Filter | FilterGroup>;
```

### 1.8 Enhanced AdapterInstance
```typescript
export interface AdapterInstance {
    connect?(): Promise<void>;
    disconnect?(): Promise<void>;
    download(pageOptions: { limit: number; offset: number }): Promise<{
        data: any[];
        options?: { nextOffset?: string | number; totalCount?: number; [key: string]: any; };
    }>;
    upload?(data: any[]): Promise<void>;
    validate?(): Promise<{ valid: boolean; message?: string; details?: Record<string, unknown> }>;
    getSchema?(): Promise<{ fields: string[]; types?: Record<string, string>; }>;
    count?(): Promise<number>;
    getConfig?(): BaseAdapter | HttpAdapter | DatabaseAdapter;
}
```

### 1.9 Pipeline Validation Function
```typescript
export function validatePipeline<T>(
    pipeline: Pipeline<T>,
    adapters: Adapters,
    vault: Vault
): { valid: boolean; errors: string[]; warnings: string[] };
```

### 1.10 Connector Debug Flag
```typescript
export interface Connector {
    // ... existing fields
    debug?: boolean;  // Enable debug logging
}
```

---

## 2. Core Orchestrator Improvements (src/index.ts)

### 2.1 Exponential Backoff with Jitter
```typescript
function calculateBackoff(
    attempt: number,
    baseDelay: number = 1000,
    maxDelay: number = 30000
): number {
    const exponentialDelay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
    const jitter = Math.random() * 0.3 * exponentialDelay;
    return exponentialDelay + jitter;
}
```

### 2.2 Configurable Constants
```typescript
export const DEFAULT_CONFIG = {
    ITEMS_PER_PAGE: 100,
    TOTAL_ITEMS_LIMIT: 1000000,
    TIMEOUT_MS: 30000,
    RETRY_INTERVAL_MS: 1000,
    MAX_RETRIES: 0,
    MAX_BACKOFF_MS: 30000,
} as const;

export interface OrchestratorConfig {
    itemsPerPage?: number;
    totalItemsLimit?: number;
    timeoutMs?: number;
    retryIntervalMs?: number;
    maxRetries?: number;
    maxBackoffMs?: number;
}
```

---

## 3. SQL Injection Protection (Database Adapters)

### 3.1 PostgreSQL Security Functions
```typescript
const ALLOWED_OPERATORS = new Set([
    '=', '!=', '<>', '<', '>', '<=', '>=',
    'LIKE', 'ILIKE', 'NOT LIKE', 'NOT ILIKE',
    'IN', 'NOT IN', 'IS NULL', 'IS NOT NULL',
    'BETWEEN', 'NOT BETWEEN'
]);

function escapeIdentifier(identifier: string): string {
    if (!identifier || typeof identifier !== 'string') {
        throw new Error('Invalid identifier: must be a non-empty string');
    }
    return `"${identifier.replace(/"/g, '""')}"`;
}

function validateOperator(operator: string): string {
    const normalized = operator.trim().toUpperCase();
    if (!ALLOWED_OPERATORS.has(normalized)) {
        throw new Error(`Invalid SQL operator: '${operator}'`);
    }
    return normalized;
}
```

### 3.2 MySQL Security Functions
```typescript
function escapeIdentifier(identifier: string): string {
    if (!identifier || typeof identifier !== 'string') {
        throw new Error('Invalid identifier: must be a non-empty string');
    }
    return `\`${identifier.replace(/`/g, '``')}\``;
}
```

### 3.3 Parameterized Queries
Instead of:
```typescript
// VULNERABLE
`SELECT ${fields} FROM ${table} WHERE status = '${value}'`
```

Use:
```typescript
// SECURE
const query = {
    text: `SELECT "id", "name" FROM "public"."users" WHERE "status" = $1 LIMIT $2 OFFSET $3`,
    values: [value, limit, offset]
};
await pool.query(query.text, query.values);
```

---

## 4. Adapter Metadata Standards

All adapters should include:

```typescript
const Adapter = {
    id: "adapter-name",           // Consistent, lowercase, hyphenated
    name: "Human Readable Name",
    type: "http" | "database",
    category: "Category Name",    // e.g., "Databases & Data Warehouses"
    image: "https://...",         // URL to logo SVG
    // ...
};
```

### Category Standards
- `"Databases & Data Warehouses"` - PostgreSQL, MySQL, MongoDB
- `"SaaS & CRM Applications"` - HubSpot, Zoho
- `"E-commerce & Payment Platforms"` - Stripe
- `"Accounting & Finance"` - Xero

---

## 5. Adapter List with Required IDs

| Adapter | ID | Category |
|---------|-----|----------|
| PostgreSQL | `postgresql` | Databases & Data Warehouses |
| MySQL | `mysql` | Databases & Data Warehouses |
| MongoDB | `mongodb` | Databases & Data Warehouses |
| HubSpot | `hubspot` | SaaS & CRM Applications |
| Stripe | `stripe` | E-commerce & Payment Platforms |
| Xero | `xero` | Accounting & Finance |
| Google Ads | `google-ads` | Advertising & Marketing |
| GitHub | `github` | Developer Tools |
| Gmail | `gmail` | Communication |
| Twitter | `twitter` | Social Media |
| ChartMogul | `chartmogul` | Analytics |
| S3 | `s3` | Cloud Storage |
| Zoho | `zoho` | SaaS & CRM Applications |

---

## 6. Webpack Configuration Standard

All adapters should use consistent webpack configuration:

```javascript
const path = require('path');

module.exports = {
    mode: 'production',
    entry: './src/index.ts',
    output: {
        filename: 'index.js',
        path: path.resolve(__dirname, 'dist'),
        library: {
            type: 'commonjs2',
        },
    },
    resolve: {
        extensions: ['.ts', '.js'],
    },
    module: {
        rules: [
            {
                test: /\.ts$/,
                use: 'ts-loader',
                exclude: /node_modules/,
            },
        ],
    },
    externals: {
        // List adapter-specific externals
    },
    target: 'node',
};
```

---

## 7. Package.json Standards

All adapter package.json files should include:

```json
{
    "name": "@openetl/adapter-name",
    "version": "x.x.x",
    "description": "Descriptive text with key features",
    "main": "dist/index.js",
    "types": "dist/index.d.ts",
    "scripts": {
        "build": "npx webpack --config webpack.config.js",
        "test": "jest",
        "lint": "eslint src --ext .ts"
    },
    "keywords": ["etl", "adapter-name", "openetl", "data-pipeline"],
    "license": "MIT",
    "repository": {
        "type": "git",
        "url": "https://github.com/openetl/openetl.git",
        "directory": "adapters/adapter-name"
    },
    "peerDependencies": {
        "openetl": "^1.0.0"
    }
}
```

---

## 8. Test Standards

### 8.1 Unit Tests Should Cover
- Adapter metadata validation
- Query building with parameterized queries
- Filter handling (including FilterGroup)
- Error handling
- Authentication validation

### 8.2 Test for Parameterized Queries
```typescript
expect(mockPool.query).toHaveBeenCalledWith(
    `SELECT "id", "name" FROM "public"."users" WHERE "status" = $1 LIMIT $2 OFFSET $3`,
    ['active', 10, 0]
);
```

---

## 9. Documentation Requirements

Each adapter should have:
1. **README.md** (300+ lines) covering:
   - Installation
   - Quick start
   - All endpoints
   - Filtering options
   - Security features
   - Configuration
   - Complete examples
   - Error handling

2. **Website docs** at `docs/openetl/docs/adapters/[name].md`

---

## 10. Future Improvements (from TODO.md)

### High Priority
- Debug mode pattern via `connector.debug` flag
- Streaming support for large datasets
- Webhook/event hooks system

### Medium Priority
- Per-endpoint pagination settings
- Schema validation before upload
- Data type coercion
- Flatten nested objects utility

### Low Priority
- CLI tool
- Monorepo restructuring
- Connection pooling improvements
- Dry run mode
- Resume/checkpoint support

---

## Implementation Order

1. **Phase 1**: Core types (AdapterError, ConfigItem, etc.)
2. **Phase 2**: Orchestrator improvements (backoff, validation)
3. **Phase 3**: SQL injection protection for all database adapters
4. **Phase 4**: Standardize webpack/package.json across adapters
5. **Phase 5**: Update tests for parameterized queries
6. **Phase 6**: Documentation updates

---

## 11. Missing Features for Competitive FREE ETL Project

### Critical Missing Features (High Priority)

#### 11.1 Streaming/Chunked Processing
Currently, all data is loaded into memory. For large datasets, implement streaming:

```typescript
export interface StreamingOptions {
    chunkSize?: number;
    onChunk?: (chunk: T[], chunkIndex: number) => Promise<void>;
    backpressure?: boolean;
}

// Usage in pipeline
const pipeline = {
    source: connector,
    streaming: {
        chunkSize: 1000,
        onChunk: async (chunk, index) => {
            // Process each chunk without loading all into memory
        }
    }
};
```

#### 11.2 Data Validation Before Upload
Validate data schema before sending to target:

```typescript
export interface SchemaValidator {
    validate(data: any[], schema: SchemaInfo): ValidationResult;
}

// In Connector:
validation?: {
    schema?: SchemaInfo;
    strict?: boolean;  // Fail on first error
    coerce?: boolean;  // Attempt type coercion
}
```

#### 11.3 Dry Run Mode
Test pipeline without executing:

```typescript
export interface DryRunResult {
    wouldExtract: number;
    wouldTransform: { before: any; after: any }[];
    wouldLoad: number;
    errors: string[];
    warnings: string[];
}

// In pipeline execution:
orchestrator.runPipeline(pipeline, { dryRun: true });
```

#### 11.4 Resume/Checkpoint Support
Continue from failure point:

```typescript
export interface Checkpoint {
    pipelineId: string;
    lastOffset: number | string;
    processedCount: number;
    timestamp: string;
    state?: Record<string, unknown>;
}

// In Pipeline:
checkpoint?: {
    enabled: boolean;
    storage: 'memory' | 'file' | 'redis';
    path?: string;
}
```

#### 11.5 Parallel/Concurrent Processing
Process multiple batches concurrently:

```typescript
export interface ConcurrencyOptions {
    maxConcurrent?: number;  // Default: 1 (sequential)
    batchSize?: number;
    preserveOrder?: boolean;
}

// In Pipeline rate_limiting:
rate_limiting?: {
    requests_per_second: number;
    concurrent_requests: number;  // Enable this
    max_retries_on_rate_limit: number;
}
```

### Medium Priority Features

#### 11.6 Upsert Support
Insert or update based on key:

```typescript
export interface UpsertOptions {
    keys: string[];              // Unique key fields
    updateFields?: string[];     // Fields to update (default: all)
    onConflict?: 'update' | 'ignore' | 'error';
}

// In DatabaseEndpoint:
export interface DatabaseEndpoint extends Endpoint {
    query_type: "table" | "custom" | "upsert";
    upsert?: UpsertOptions;
}
```

#### 11.7 CLI Tool
Command-line interface for running pipelines:

```bash
# Run a pipeline from JSON config
openetl run --config pipeline.json --vault vault.json

# Validate configuration
openetl validate --config pipeline.json

# List available adapters
openetl adapters list

# Test connection
openetl test-connection --adapter postgresql --vault vault.json
```

#### 11.8 Change Data Capture (CDC)
Track and sync only changed records:

```typescript
export interface CDCOptions {
    trackingColumn: string;      // e.g., 'updated_at'
    lastSync?: string | number;  // Last sync timestamp/version
    includeDeletes?: boolean;
    softDeleteColumn?: string;
}
```

#### 11.9 Data Masking/PII Protection
Built-in anonymization:

```typescript
export type MaskingType =
    | 'hash'         // SHA-256 hash
    | 'redact'       // Replace with ***
    | 'partial'      // Show last 4 chars
    | 'fake'         // Replace with fake data
    | 'encrypt';     // Reversible encryption

export interface MaskingRule {
    field: string;
    type: MaskingType;
    options?: Record<string, unknown>;
}

// In Connector:
masking?: MaskingRule[];
```

#### 11.10 Metrics/Observability
Built-in metrics collection:

```typescript
export interface PipelineMetrics {
    extractedCount: number;
    transformedCount: number;
    loadedCount: number;
    errorCount: number;
    durationMs: number;
    bytesProcessed: number;
    avgRecordsPerSecond: number;
}

export interface MetricsCollector {
    onExtract(count: number, durationMs: number): void;
    onTransform(count: number, durationMs: number): void;
    onLoad(count: number, durationMs: number): void;
    onError(error: AdapterError): void;
    getMetrics(): PipelineMetrics;
}
```

### Low Priority (Future Roadmap)

#### 11.11 Built-in Scheduling
Actual scheduler implementation (not just configuration):

```typescript
import { schedule } from 'openetl/scheduler';

schedule(pipeline, {
    cron: '0 0 * * *',  // Daily at midnight
    timezone: 'UTC',
    retryOnFailure: true,
});
```

#### 11.12 Web UI (Future)
Admin interface for pipeline management - consider separate package.

#### 11.13 Distributed Processing (Future)
Support for running across multiple workers/nodes.

---

## 12. Comparison with Popular ETL Tools

| Feature | OpenETL | Airbyte | Singer | Apache NiFi |
|---------|---------|---------|--------|-------------|
| TypeScript Native | ✅ | ❌ | ❌ | ❌ |
| Lightweight | ✅ | ❌ | ✅ | ❌ |
| MIT License | ✅ | ELVE2 | ✅ | Apache 2.0 |
| Streaming | ❌ TODO | ✅ | ✅ | ✅ |
| Schema Validation | ❌ TODO | ✅ | ✅ | ✅ |
| CDC Support | ❌ TODO | ✅ | Partial | ✅ |
| CLI Tool | ❌ TODO | ✅ | ✅ | ✅ |
| Web UI | ❌ | ✅ | ❌ | ✅ |
| Custom Transforms | ✅ | Limited | ✅ | ✅ |
| SQL Injection Protection | ✅ | ✅ | Varies | ✅ |
| Parameterized Queries | ✅ | ✅ | Varies | ✅ |

---

## Notes

- Always use parameterized queries for database adapters
- Never interpolate user values directly into SQL strings
- Escape all identifiers (table names, column names)
- Validate operators against whitelist
- Include category and image in all adapters for UI display
- Use consistent adapter IDs across the codebase
