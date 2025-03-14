import { Orchestrator } from '../../../src/index'; // Adjust path to your OpenETL source
import { twitter } from './../src/index'; // Adjust path to your adapter
import axios from 'axios';

describe('TwitterAdapter Download Integration Tests', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let connector: any;

  async function getTwitterTokens(): Promise<{ access_token: string; refresh_token?: string; expires_at: number }> {
    try {
      const response = await axios.get('http://localhost:2301/tokens');
      return response.data;
    } catch (error: any) {
      throw new Error(`Failed to fetch tokens from OAuth server: ${error.message}`);
    }
  }

  beforeAll(async () => {
    // Get tokens from the OAuth server
    const tokens = await getTwitterTokens();

    // Set up vault with Twitter credentials
    vault = {
      'twitter-auth': {
        id: 'twitter-auth',
        type: 'oauth2',
        credentials: {
          access_token: tokens.access_token,
          refresh_token: tokens.refresh_token || null,
          expires_at: tokens.expires_at,
        },
      },
    };

    // Initialize Orchestrator with Twitter adapter
    const adapters = { twitter };
    orchestrator = Orchestrator(vault, adapters);

    // Base connector configuration for tweets_search endpoint
    connector = {
      id: 'twitter-search',
      adapter_id: 'twitter',
      endpoint_id: 'tweets_search',
      credential_id: 'twitter-auth',
      config: { headers: {} }, // Optional custom headers
      fields: ['id', 'text', 'created_at'], // Default fields for consistency
      pagination: { itemsPerPage: 10 },
    };
  });

  it('downloads tweets successfully with query from:elonmusk tesla', async () => {
    // Pipeline with minimal configuration (query required by API)
    const pipeline = {
      id: 'twitter-download-default',
      source: {
        ...connector,
        filters: [{ field: 'query', operator: '=', value: 'from:elonmusk' }], // Simple query
      },
    };

    const result: any = await orchestrator.runPipeline(pipeline);

    // Check the resolved object structure
    expect(result).toHaveProperty('data');
    expect(Array.isArray(result.data)).toBe(true);
    expect(result.data.length).toBeGreaterThanOrEqual(0); // Could be 0 if no matches
    if (result.data.length > 0) {
      expect(result.data[0]).toHaveProperty('id');
      expect(result.data[0]).toHaveProperty('text');
      expect(result.data[0]).toHaveProperty('created_at');
    }
    // Check for pagination token if present
    if (result.options?.nextOffset) {
      expect(typeof result.options.nextOffset).toBe('string');
    }
  });

  /*
  it('downloads tweets with query "from:elonmusk tesla"', async () => {
    // Pipeline with specific query
    const pipeline = {
      id: 'twitter-download-elon',
      source: {
        ...connector,
        filters: [{ field: 'query', operator: '=', value: 'from:elonmusk tesla' }],
      },
    };

    const result = await orchestrator.runPipeline(pipeline);

    // Verify structure and content
    expect(result).toHaveProperty('data');
    expect(Array.isArray(result.data)).toBe(true);
    expect(result.data.length).toBeGreaterThan(0); // Expect at least one tweet
    expect(result.data[0]).toHaveProperty('id');
    expect(result.data[0]).toHaveProperty('text');
    expect(result.data[0]).toHaveProperty('created_at');
    // Check that text contains "tesla" (case-insensitive)
    expect(result.data.some(tweet => tweet.text.toLowerCase().includes('tesla'))).toBe(true);
  });

  it('downloads tweets with max_results and pagination', async () => {
    // Pipeline with max_results and offset
    const pipeline = {
      id: 'twitter-download-paginated',
      source: {
        ...connector,
        filters: [{ field: 'query', operator: '=', value: '#javascript' }],
        pagination: { itemsPerPage: 5 }, // Smaller page size
      },
    };

    const firstPage = await orchestrator.runPipeline(pipeline);

    expect(firstPage.data.length).toBeLessThanOrEqual(5); // Respect max_results
    expect(firstPage.options?.nextOffset).toBeDefined(); // Expect pagination token

    // Fetch next page using nextOffset
    const nextPipeline = {
      ...pipeline,
      source: {
        ...pipeline.source,
        pagination: { ...pipeline.source.pagination, nextOffset: firstPage.options.nextOffset },
      },
    };

    const secondPage = await orchestrator.runPipeline(nextPipeline);

    expect(secondPage.data.length).toBeGreaterThanOrEqual(0);
    expect(secondPage.data[0].id).not.toEqual(firstPage.data[0].id); // Different tweets
  });

  it('fails to download with invalid endpoint', async () => {
    // Pipeline with wrong endpoint
    const pipeline = {
      id: 'twitter-download-invalid',
      source: {
        ...connector,
        endpoint_id: 'tweet_post', // Should only support upload
        filters: [{ field: 'query', operator: '=', value: 'test' }],
      },
    };

    await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(
      /tweet_post endpoint does not support download/
    );
  });
  */
});