/**
 * Amazon S3 Adapter for OpenETL
 * https://componade.com/openetl
 */

import { HttpAdapter, Connector, AuthConfig, AdapterInstance, ApiKeyAuth, Filter } from 'openetl';
import { S3Client, ListObjectsV2Command, GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { Readable } from 'stream';

const maxItemsPerPage = 1000;

const S3Adapter: HttpAdapter = {
  id: "s3-adapter",
  name: "Amazon S3 Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  credential_type: "api_key",
  base_url: "https://s3.amazonaws.com",
  config: [
    {
      name: 'bucket',
      required: true,
    },
  ],
  metadata: {
    provider: "aws",
    description: "Adapter for Amazon S3 storage service",
    version: "v3", // Usando AWS SDK v3
  },
  endpoints: [
    {
      id: "list-objects",
      path: "",
      method: "GET",
      description: "List objects in an S3 bucket",
      supported_actions: ["download", "sync"],
      settings: {
        pagination: {
            type: 'cursor',
            maxItemsPerPage,
        }
      }
    },
    {
      id: "download-object",
      path: "",
      method: "GET",
      description: "Download a specific object from S3",
      supported_actions: ["download"],
      settings: {
        pagination: false,
      }
    },
    {
      id: "upload-object",
      path: "",
      method: "PUT",
      description: "Upload an object to S3",
      supported_actions: ["upload"],
      settings: {
        pagination: {
            type: 'offset',
            maxItemsPerPage: 1,
        },
      }
    },
  ],
};

interface AWSCredentials extends ApiKeyAuth {
    credentials: {
        api_key: string;
        api_secret: string;
        region: string;
    }
}

function isAWSAuth(auth: AuthConfig): auth is AWSCredentials {
    return auth.type === 'api_key' &&
        auth.credentials &&
        typeof auth.credentials.api_key === 'string' &&
        typeof auth.credentials.api_secret === 'string' &&
        typeof auth.credentials.region === 'string';
}

function s3(connector: Connector, auth: AuthConfig): AdapterInstance {
  const log = (...args: any[]) => {
    if (connector.debug) {
      console.log(...args);
    }
  };

  const endpoint = S3Adapter.endpoints.find(e => e.id === connector.endpoint_id);
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in S3 adapter`);
  }

  if (!isAWSAuth(auth)) {
    throw new Error("S3 adapter requires AWS authentication with api_key, api_secret and region");
  }

  const bucket = connector.config?.bucket;
  if (!bucket) {
    throw new Error("Bucket name must be specified in connector config");
  }

  const s3Client = new S3Client({
    region: auth.credentials.region,
    credentials: {
        accessKeyId: auth.credentials.api_key,
        secretAccessKey: auth.credentials.api_secret,
    }
  });

  const download: AdapterInstance['download'] = async function(pageOptions) {
    const { limit, offset } = pageOptions;

    if (endpoint.id === "list-objects") {
      if (typeof limit === 'undefined') {
        throw new Error(`Number of items per page is required by the ${endpoint.id} endpoint of the S3 adapter`);
      }

      if (limit > maxItemsPerPage) {
        throw new Error(`Number of items per page exceeds the maximum allowed by the ${endpoint.id} endpoint of the S3 adapter (${maxItemsPerPage})`);
      }

      const prefix = (connector.filters?.find(f => 'field' in f && f.field === 'prefix' && f.operator === '=') as Filter | undefined)?.value;

      if (typeof prefix === 'number') {
        throw new Error('The "prefix" filter, if defined, must be a string');
      }

      const command = new ListObjectsV2Command({
        Bucket: bucket,
        MaxKeys: limit,
        ContinuationToken: offset ? String(offset) : undefined,
        Prefix: prefix,
      });

      const response = await s3Client.send(command);
      const results = response.Contents?.map(obj => ({
        key: obj.Key,
        size: obj.Size,
        lastModified: obj.LastModified?.toISOString(),
        eTag: obj.ETag,
      })) || [];

      return {
        data: results,
        options: {
          nextOffset: response.IsTruncated ? response.NextContinuationToken : undefined,
        },
      };
    }

    const key = connector.config?.id;

    if (typeof key !== 'string') {
      throw new Error('For the download-object endpoint, the id config is required');
    }

    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
    });

    const response = await s3Client.send(command);
    const body = response.Body as Readable;
    const chunks: Buffer[] = [];
    for await (const chunk of body) {
      chunks.push(Buffer.from(chunk));
    }
    const content = Buffer.concat(chunks);

    return {
      data: [{ key, content }],
    };
  };

  return {
    getConfig: () => S3Adapter,

    download: async function(pageOptions) {
      if (!endpoint.supported_actions.includes('download')) {
        throw new Error(`${endpoint.id} endpoint does not support download`);
      }

      try {
        return await download(pageOptions);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        console.error("Download error:", errorMessage);
        throw new Error(`Download failed: ${errorMessage}`);
      }
    },

    upload: async function(data: any[]): Promise<void> {
      if (!endpoint.supported_actions.includes('upload')) {
        throw new Error(`${endpoint.id} endpoint does not support upload`);
      }

      try {
        const { key, content, contentType } = data[0];

        if (!key) {
          throw new Error("key must be specified in data");
        }

        if (!content) {
          throw new Error("content must be specified in data");
        }

        if (!contentType) {
          throw new Error("contentType must be specified in data");
        }

        await s3Client.send(new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: content,
          ContentType: contentType,
        }));
        log(`Uploaded object: ${key}`);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        console.error("Upload error:", errorMessage);
        throw new Error(`Upload failed: ${errorMessage}`);
      }
    },
  };
}

export { s3, S3Adapter };