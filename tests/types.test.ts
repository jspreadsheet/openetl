import { AdapterError, validatePipeline, Pipeline, Adapters, Vault, Filter, FilterGroup } from '../src/types';

describe('AdapterError', () => {
    it('creates error with default values', () => {
        const error = new AdapterError('Test error');

        expect(error.message).toBe('Test error');
        expect(error.code).toBe('UNKNOWN');
        expect(error.retryable).toBe(false);
        expect(error.context).toBeUndefined();
        expect(error.cause).toBeUndefined();
        expect(error.name).toBe('AdapterError');
    });

    it('creates error with all parameters', () => {
        const cause = new Error('Original error');
        const context = { endpoint: '/api/users', method: 'GET' };

        const error = new AdapterError(
            'Connection failed',
            'CONNECTION_FAILED',
            true,
            context,
            cause
        );

        expect(error.message).toBe('Connection failed');
        expect(error.code).toBe('CONNECTION_FAILED');
        expect(error.retryable).toBe(true);
        expect(error.context).toEqual(context);
        expect(error.cause).toBe(cause);
    });

    it('creates error from unknown error type using from()', () => {
        const error = AdapterError.from('String error', 'DOWNLOAD_FAILED');

        expect(error.message).toBe('String error');
        expect(error.code).toBe('DOWNLOAD_FAILED');
        expect(error.cause).toBeUndefined();
    });

    it('creates error from Error instance using from()', () => {
        const originalError = new Error('Original message');
        const error = AdapterError.from(originalError, 'UPLOAD_FAILED', true);

        expect(error.message).toBe('Original message');
        expect(error.code).toBe('UPLOAD_FAILED');
        expect(error.retryable).toBe(true);
        expect(error.cause).toBe(originalError);
    });

    it('returns same instance when from() receives AdapterError', () => {
        const original = new AdapterError('Test', 'TIMEOUT', true);
        const result = AdapterError.from(original);

        expect(result).toBe(original);
    });

    it('detects authentication errors from axios-like errors', () => {
        const axiosError = {
            response: { status: 401, data: 'Unauthorized' }
        };

        const error = AdapterError.from(axiosError);

        expect(error.code).toBe('AUTHENTICATION_FAILED');
        expect(error.retryable).toBe(false);
    });

    it('detects rate limiting from axios-like errors', () => {
        const axiosError = {
            response: { status: 429, data: 'Too many requests' }
        };

        const error = AdapterError.from(axiosError);

        expect(error.code).toBe('RATE_LIMITED');
        expect(error.retryable).toBe(true);
    });

    it('detects not found from axios-like errors', () => {
        const axiosError = {
            response: { status: 404, data: 'Not found' }
        };

        const error = AdapterError.from(axiosError);

        expect(error.code).toBe('NOT_FOUND');
        expect(error.retryable).toBe(false);
    });

    it('marks server errors as retryable', () => {
        const axiosError = {
            response: { status: 503, data: 'Service unavailable' }
        };

        const error = AdapterError.from(axiosError, 'CONNECTION_FAILED');

        expect(error.code).toBe('CONNECTION_FAILED');
        expect(error.retryable).toBe(true);
    });

    it('serializes to JSON correctly', () => {
        const cause = new Error('Cause');
        const error = new AdapterError(
            'Test error',
            'TIMEOUT',
            true,
            { key: 'value' },
            cause
        );

        const json = error.toJSON();

        expect(json.name).toBe('AdapterError');
        expect(json.message).toBe('Test error');
        expect(json.code).toBe('TIMEOUT');
        expect(json.retryable).toBe(true);
        expect(json.context).toEqual({ key: 'value' });
        expect(json.cause).toEqual({ name: 'Error', message: 'Cause' });
        expect(json.stack).toBeDefined();
    });

    it('serializes without cause when not present', () => {
        const error = new AdapterError('Test');
        const json = error.toJSON();

        expect(json.cause).toBeUndefined();
    });
});

describe('FilterGroup', () => {
    it('supports simple AND group', () => {
        const filterGroup: FilterGroup = {
            op: 'AND',
            filters: [
                { field: 'status', operator: '=', value: 'active' },
                { field: 'age', operator: '>=', value: 18 }
            ]
        };

        expect(filterGroup.op).toBe('AND');
        expect(filterGroup.filters).toHaveLength(2);
    });

    it('supports simple OR group', () => {
        const filterGroup: FilterGroup = {
            op: 'OR',
            filters: [
                { field: 'role', operator: '=', value: 'admin' },
                { field: 'role', operator: '=', value: 'moderator' }
            ]
        };

        expect(filterGroup.op).toBe('OR');
        expect(filterGroup.filters).toHaveLength(2);
    });

    it('supports nested filter groups', () => {
        const filterGroup: FilterGroup = {
            op: 'AND',
            filters: [
                { field: 'status', operator: '=', value: 'active' },
                {
                    op: 'OR',
                    filters: [
                        { field: 'role', operator: '=', value: 'admin' },
                        { field: 'role', operator: '=', value: 'moderator' }
                    ]
                }
            ]
        };

        expect(filterGroup.op).toBe('AND');
        expect(filterGroup.filters).toHaveLength(2);
        expect((filterGroup.filters[1] as FilterGroup).op).toBe('OR');
    });

    it('supports array values in filters', () => {
        const filter: Filter = {
            field: 'id',
            operator: 'IN',
            value: [1, 2, 3]
        };

        expect(filter.value).toEqual([1, 2, 3]);
    });

    it('supports boolean values in filters', () => {
        const filter: Filter = {
            field: 'active',
            operator: '=',
            value: true
        };

        expect(filter.value).toBe(true);
    });

    it('supports null values in filters', () => {
        const filter: Filter = {
            field: 'deleted_at',
            operator: 'IS',
            value: null
        };

        expect(filter.value).toBeNull();
    });
});

describe('validatePipeline', () => {
    const mockAdapter = jest.fn();
    const mockAdapters: Adapters = {
        'test-adapter': mockAdapter
    };

    const mockVault: Vault = {
        'test-cred': {
            id: 'test-cred',
            type: 'api_key',
            credentials: { api_key: 'test' }
        }
    };

    it('validates pipeline with source successfully', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            source: {
                id: 'source',
                adapter_id: 'test-adapter',
                endpoint_id: 'users',
                credential_id: 'test-cred',
                fields: ['id', 'name']
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(true);
        expect(result.errors).toHaveLength(0);
    });

    it('validates pipeline with data successfully', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            data: [{ id: 1 }]
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(true);
        expect(result.errors).toHaveLength(0);
    });

    it('fails when pipeline has no source or data', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline'
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain('Pipeline must have either a source or data');
    });

    it('fails when source adapter not found', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            source: {
                id: 'source',
                adapter_id: 'non-existent',
                endpoint_id: 'users',
                credential_id: 'test-cred',
                fields: ['id']
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain("Source adapter 'non-existent' not found");
    });

    it('fails when source credentials not found', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            source: {
                id: 'source',
                adapter_id: 'test-adapter',
                endpoint_id: 'users',
                credential_id: 'non-existent',
                fields: ['id']
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain("Source credentials 'non-existent' not found in vault");
    });

    it('warns when no fields specified', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            source: {
                id: 'source',
                adapter_id: 'test-adapter',
                endpoint_id: 'users',
                credential_id: 'test-cred',
                fields: []
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(true);
        expect(result.warnings).toContain('No fields specified for source - will use defaults if available');
    });

    it('validates target configuration', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            data: [{ id: 1 }],
            target: {
                id: 'target',
                adapter_id: 'non-existent',
                endpoint_id: 'users',
                credential_id: 'non-existent',
                fields: ['id']
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain("Target adapter 'non-existent' not found");
        expect(result.errors).toContain("Target credentials 'non-existent' not found in vault");
    });

    it('validates error_handling configuration', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            data: [{ id: 1 }],
            error_handling: {
                max_retries: -1,
                retry_interval: -100
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain('max_retries must be non-negative');
        expect(result.errors).toContain('retry_interval must be non-negative');
    });

    it('validates rate_limiting configuration', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            data: [{ id: 1 }],
            rate_limiting: {
                requests_per_second: 0,
                max_retries_on_rate_limit: 3
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain('requests_per_second must be positive');
    });

    it('validates filters with empty field', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            source: {
                id: 'source',
                adapter_id: 'test-adapter',
                endpoint_id: 'users',
                credential_id: 'test-cred',
                fields: ['id'],
                filters: [
                    { field: '', operator: '=', value: 'test' }
                ]
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain('Filter at index 0 has empty field');
    });

    it('validates filters with empty operator', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            source: {
                id: 'source',
                adapter_id: 'test-adapter',
                endpoint_id: 'users',
                credential_id: 'test-cred',
                fields: ['id'],
                filters: [
                    { field: 'status', operator: '', value: 'test' }
                ]
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(false);
        expect(result.errors).toContain('Filter at index 0 has empty operator');
    });

    it('handles FilterGroup in filters array', () => {
        const pipeline: Pipeline<any> = {
            id: 'test-pipeline',
            source: {
                id: 'source',
                adapter_id: 'test-adapter',
                endpoint_id: 'users',
                credential_id: 'test-cred',
                fields: ['id'],
                filters: [
                    { field: 'status', operator: '=', value: 'active' },
                    {
                        op: 'OR',
                        filters: [
                            { field: 'role', operator: '=', value: 'admin' },
                            { field: 'role', operator: '=', value: 'user' }
                        ]
                    }
                ]
            }
        };

        const result = validatePipeline(pipeline, mockAdapters, mockVault);

        expect(result.valid).toBe(true);
        expect(result.errors).toHaveLength(0);
    });
});
