import { s3 } from '../src/index';
import { S3Client, PutObjectCommand, GetObjectCommand, ListObjectsV2Command } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { Connector, AuthConfig, ApiKeyAuth } from 'openetl';

jest.mock('@aws-sdk/client-s3');

const mockedS3Client = S3Client as jest.MockedClass<typeof S3Client>;
const mockedPutObjectCommand = PutObjectCommand as jest.MockedClass<typeof PutObjectCommand>;
const mockedGetObjectCommand = GetObjectCommand as jest.MockedClass<typeof GetObjectCommand>;
const mockedListObjectsV2Command = ListObjectsV2Command as jest.MockedClass<typeof ListObjectsV2Command>;

describe('S3 Adapter', () => {
  let connector: Connector;
  let auth: AuthConfig;
  let adapter: ReturnType<typeof s3>;

  beforeEach(() => {
    jest.clearAllMocks();

    // Mock connector configuration para list-objects
    connector = {
      id: 's3-objects-connector',
      adapter_id: 's3',
      endpoint_id: 'list-objects',
      credential_id: 's3-auth',
      config: {
        bucket: 'my-bucket',
      },
      fields: ['key', 'size'],
      filters: [{ field: 'prefix', operator: '=', value: 'data/' }],
      pagination: { itemsPerPage: 100 },
    };

    // Mock AWS authentication
    auth = {
      id: 's3-auth',
      type: 'api_key',
      credentials: {
        api_key: 'mock-access-key',
        api_secret: 'mock-secret-key',
        region: 'us-west-2',
      },
    };

    // Create adapter instance
    adapter = s3(connector, auth);

    // Mock S3Client send method
    mockedS3Client.prototype.send = jest.fn();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('initializes successfully with valid credentials and bucket', () => {
    expect(adapter).toBeDefined();
    expect(mockedS3Client).toHaveBeenCalledWith({
      region: 'us-west-2',
      credentials: {
        accessKeyId: 'mock-access-key',
        secretAccessKey: 'mock-secret-key',
      },
    });
  });

  it('throws error if bucket is missing', () => {
    const invalidConnector = { ...connector, config: {} };
    expect(() => s3(invalidConnector, auth)).toThrow('Bucket name must be specified in connector config');
  });

  it('throws error if auth is invalid', () => {
    const invalidAuth: ApiKeyAuth = { id: 's3-auth', type: 'api_key', credentials: { api_key: 'only-key' } };
    expect(() => s3(connector, invalidAuth)).toThrow(
      'S3 adapter requires AWS authentication with api_key, api_secret and region'
    );
  });

  it('downloads object list with cursor-based pagination', async () => {
    (mockedS3Client.prototype.send as jest.Mock)
      .mockResolvedValueOnce({
        Contents: [
          { Key: 'data/file1.txt', Size: 123, LastModified: new Date('2023-01-01'), ETag: '"abc123"' },
          { Key: 'data/file2.txt', Size: 456, LastModified: new Date('2023-01-02'), ETag: '"def456"' },
        ],
        IsTruncated: true,
        NextContinuationToken: 'next-token',
      })
      .mockResolvedValueOnce({
        Contents: [{ Key: 'data/file3.txt', Size: 789, LastModified: new Date('2023-01-03'), ETag: '"ghi789"' }],
        IsTruncated: false,
      });

    const firstPage = await adapter.download({ limit: 2, offset: undefined });
    expect(firstPage.data).toEqual([
      { key: 'data/file1.txt', size: 123, lastModified: '2023-01-01T00:00:00.000Z', eTag: '"abc123"' },
      { key: 'data/file2.txt', size: 456, lastModified: '2023-01-02T00:00:00.000Z', eTag: '"def456"' },
    ]);
    expect(firstPage.options?.nextOffset).toBe('next-token');

    const secondPage = await adapter.download({ limit: 2, offset: 'next-token' });
    expect(secondPage.data).toEqual([
      { key: 'data/file3.txt', size: 789, lastModified: '2023-01-03T00:00:00.000Z', eTag: '"ghi789"' },
    ]);
    expect(secondPage.options?.nextOffset).toBeUndefined();

    expect(mockedListObjectsV2Command).toHaveBeenCalledWith(
      expect.objectContaining({
        Bucket: 'my-bucket',
        MaxKeys: 2,
        Prefix: 'data/',
      })
    );
  });

  it('throws error if limit is undefined for list-objects', async () => {
    await expect(adapter.download({ offset: undefined })).rejects.toThrow(
      'Number of items per page is required by the list-objects endpoint of the S3 adapter'
    );
  });

  it('throws error if limit exceeds maximum for list-objects', async () => {
    await expect(adapter.download({ limit: 1001, offset: undefined })).rejects.toThrow(
      'Number of items per page exceeds the maximum allowed by the list-objects endpoint of the S3 adapter (1000)'
    );
  });

  it('throws error if prefix filter is a number', async () => {
    const invalidConnector = {
      ...connector,
      filters: [{ field: 'prefix', operator: '=', value: 123 }],
    };
    const invalidAdapter = s3(invalidConnector, auth);
    await expect(invalidAdapter.download({ limit: 10, offset: undefined })).rejects.toThrow(
      'The "prefix" filter, if defined, must be a string'
    );
  });

  it('downloads single object content with config.id', async () => {
    const downloadConnector = {
      ...connector,
      endpoint_id: 'download-object',
      config: { bucket: 'my-bucket', id: 'data/report.json' },
    };
    const downloadAdapter = s3(downloadConnector, auth);

    const mockStream = Readable.from(['{"message": "Hello"}']);
    (mockedS3Client.prototype.send as jest.Mock).mockResolvedValueOnce({
      Body: mockStream,
    });

    const result = await downloadAdapter.download({ limit: 1, offset: undefined });
    expect(result.data).toEqual([{ key: 'data/report.json', content: expect.any(Buffer) }]);
    expect(result.data[0].content.toString('utf-8')).toBe('{"message": "Hello"}');
    expect(mockedGetObjectCommand).toHaveBeenCalledWith(
      expect.objectContaining({
        Bucket: 'my-bucket',
        Key: 'data/report.json',
      })
    );
  });

  it('throws error if config.id is missing for download-object', async () => {
    const downloadConnector = {
      ...connector,
      endpoint_id: 'download-object',
      config: { bucket: 'my-bucket' },
    };
    const downloadAdapter = s3(downloadConnector, auth);

    await expect(downloadAdapter.download({ limit: 1, offset: undefined })).rejects.toThrow(
      'For the download-object endpoint, the id config is required'
    );
  });

  it('uploads data successfully', async () => {
    const uploadConnector = {
      ...connector,
      endpoint_id: 'upload-object',
    };
    const uploadAdapter = s3(uploadConnector, auth);

    (mockedS3Client.prototype.send as jest.Mock).mockResolvedValueOnce({});

    const data = [{ key: 'uploads/file1.txt', content: 'Hello', contentType: 'text/plain' }];
    await expect(uploadAdapter.upload!(data)).resolves.toBeUndefined();
    expect(mockedPutObjectCommand).toHaveBeenCalledWith(
      expect.objectContaining({
        Bucket: 'my-bucket',
        Key: 'uploads/file1.txt',
        Body: 'Hello',
        ContentType: 'text/plain',
      })
    );
  });

  it('throws error if key is missing in upload data', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'upload-object' };
    const uploadAdapter = s3(uploadConnector, auth);

    const data = [{ content: 'Hello', contentType: 'text/plain' }];
    await expect(uploadAdapter.upload!(data)).rejects.toThrow('key must be specified in data');
  });

  it('throws error if content is missing in upload data', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'upload-object' };
    const uploadAdapter = s3(uploadConnector, auth);

    const data = [{ key: 'uploads/file1.txt', contentType: 'text/plain' }];
    await expect(uploadAdapter.upload!(data)).rejects.toThrow('content must be specified in data');
  });

  it('throws error if contentType is missing in upload data', async () => {
    const uploadConnector = { ...connector, endpoint_id: 'upload-object' };
    const uploadAdapter = s3(uploadConnector, auth);

    const data = [{ key: 'uploads/file1.txt', content: 'Hello' }];
    await expect(uploadAdapter.upload!(data)).rejects.toThrow('contentType must be specified in data');
  });

  it('throws error for invalid endpoint', () => {
    const invalidConnector = { ...connector, endpoint_id: 'invalid-endpoint' };
    expect(() => s3(invalidConnector, auth)).toThrow(
      'Endpoint invalid-endpoint not found in S3 adapter'
    );
  });
});