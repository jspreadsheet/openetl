import { Orchestrator } from '../../../src/index'; // Adjust path
import { twitter, TwitterAdapter, TwitterResponse, TwitterTweet } from '../src/index'; // Adjust path
import axios from 'axios';

// Mock axios
jest.mock('axios');
const mockedAxios = axios as jest.Mocked<typeof axios>;

describe('TwitterAdapter with Orchestrator Unit Tests', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;

  // Sample tweet data for mocking
  const mockTweet: TwitterTweet = {
    id: '123456789',
    text: 'Test tweet',
    edit_history_tweet_ids: ['123456789'],
    created_at: '2023-01-01T00:00:00Z',
    author_id: '987654321',
  };

  const mockResponse: TwitterResponse = {
    data: [mockTweet],
    meta: {
      newest_id: '123456789',
      oldest_id: '123456789',
      result_count: 1,
      // next_token: 'next123',
    },
  };

  beforeEach(() => {
    // Reset mocks
    mockedAxios.get.mockReset();
    mockedAxios.post.mockReset();

    // Set up vault
    vault = {
      'twitter-auth': {
        id: 'twitter-auth',
        type: 'oauth2',
        credentials: {
          access_token: 'valid-token',
        },
      },
    };

    // Initialize orchestrator with Twitter adapter
    orchestrator = Orchestrator(vault, { twitter });
  });

  describe('download scenarios', () => {
    it('downloads single page with basic configuration', async () => {
      const mockArray = Array(10).fill(mockTweet);
      const page1Response: TwitterResponse = {
        data: mockArray,
        meta: { newest_id: '123', oldest_id: '113', result_count: 10, next_token: 'page2' },
      };

      mockedAxios.get
        .mockResolvedValueOnce({ data: page1Response });

      const pipeline = {
        id: 'twitter-basic-download',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text', 'created_at', 'author_id'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
          pagination: { itemsPerPage: 10 },
          limit: 10
        },
      };

      const result = await orchestrator.runPipeline(pipeline);
      expect(result.data).toEqual(mockArray);
      expect(mockedAxios.get).toHaveBeenCalledTimes(1);
      expect(mockedAxios.get).toHaveBeenCalledWith(
        'https://api.twitter.com/2/tweets/search/recent',
        {"headers": {"Content-Type": "application/json"}, "params": {"max_results": 10, "query": "test", "tweet.fields": "id,text,created_at,author_id"}}
      );
    });

    it('handles multi-page pagination', async () => {
      const page1Response: TwitterResponse = {
        data: Array(10).fill(mockTweet),
        meta: { newest_id: '123', oldest_id: '113', result_count: 10, next_token: 'page2' },
      };
      const page2Response: TwitterResponse = {
        data: Array(5).fill(mockTweet),
        meta: { newest_id: '112', oldest_id: '108', result_count: 5 },
      };

      mockedAxios.get
        .mockResolvedValueOnce({ data: page1Response })
        .mockResolvedValueOnce({ data: page2Response });

      const pipeline = {
        id: 'twitter-paginated-download',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
          pagination: { itemsPerPage: 10 },
          limit: 15,
        },
      };

      const result = await orchestrator.runPipeline(pipeline);
      expect(result.data.length).toBe(15);
      expect(mockedAxios.get).toHaveBeenCalledTimes(2);
      expect(mockedAxios.get).toHaveBeenNthCalledWith(
        1,
        expect.any(String),
        {"headers": {"Content-Type": "application/json"}, "params": {"max_results": 10, "query": "test", "tweet.fields": "id,text"}}
      );
      expect(mockedAxios.get).toHaveBeenNthCalledWith(
        2,
        expect.any(String),
        {"headers": {"Content-Type": "application/json"}, "params": {"max_results": 10, "next_token": "page2", "query": "test", "tweet.fields": "id,text"}}
      );
    });

    it('respects rate limiting', async () => {
      const page1Response: TwitterResponse = {
        data: Array(10).fill(mockTweet),
        meta: { newest_id: '123', oldest_id: '113', result_count: 10, next_token: 'page2' },
      };
      const page2Response: TwitterResponse = {
        data: Array(5).fill(mockTweet),
        meta: { newest_id: '112', oldest_id: '108', result_count: 5 },
      };

      mockedAxios.get
        .mockResolvedValueOnce({ data: page1Response })
        .mockResolvedValueOnce({ data: page2Response });

      const pipeline = {
        id: 'twitter-rate-limited-download',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
          pagination: { itemsPerPage: 10 },
          limit: 15,
        },
        rate_limiting: {
          requests_per_second: 1, // 1 request per second for testing
          max_retries_on_rate_limit: 0,
        },
      };

      const startTime = Date.now();
      const result = await orchestrator.runPipeline(pipeline);
      const endTime = Date.now();

      expect(result.data.length).toBe(15);
      expect(mockedAxios.get).toHaveBeenCalledTimes(2);
      expect(endTime - startTime).toBeGreaterThanOrEqual(1000); // At least 1 second between requests
    }, 10000);

    it('handles invalid max_results', async () => {
      const pipeline = {
        id: 'twitter-invalid-max-results',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
          pagination: { itemsPerPage: 5 }, // Below Twitter's minimum
        },
      };

      await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(
        'max_results must be between 10 and 100, got 5'
      );
      expect(mockedAxios.get).not.toHaveBeenCalled();
    });

    it('handles API error with retries', async () => {
      mockedAxios.get
        .mockRejectedValueOnce({ response: { status: 429, data: { detail: 'Rate limit' } } })
        .mockResolvedValueOnce({ data: mockResponse });

      const pipeline = {
        id: 'twitter-retry-download',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
          pagination: { itemsPerPage: 10 },
        },
        error_handling: {
          max_retries: 1,
          retry_interval: 100,
          fail_on_error: false,
        },
      };

      const result = await orchestrator.runPipeline(pipeline);
      expect(result.data).toEqual([mockTweet]);
      expect(mockedAxios.get).toHaveBeenCalledTimes(2);
    });
  });

  describe('edge cases', () => {

    it('handles no pagination config', async () => {
      mockedAxios.get.mockResolvedValue({ data: mockResponse });

      const pipeline = {
        id: 'twitter-no-pagination',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
          // No pagination specified
        },
      };

      const result = await orchestrator.runPipeline(pipeline);
      expect(result.data).toEqual([mockTweet]);
      expect(mockedAxios.get).toHaveBeenCalledWith(
        expect.any(String),
        expect.objectContaining({
          params: { query: 'test', 'tweet.fields': 'id,text' },
        })
      );
    });

    it('handles invalid auth', async () => {
      vault['twitter-auth'] = { id: 'twitter-auth', type: 'api_key', credentials: { api_key: 'key' } };
      orchestrator = Orchestrator(vault, { twitter });

      const pipeline = {
        id: 'twitter-invalid-auth',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
        },
      };

      await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(
        'Twitter adapter requires OAuth2 authentication with access_token'
      );
      expect(mockedAxios.get).not.toHaveBeenCalled();
    });

    it('stops at total items limit', async () => {
      const page1Response: TwitterResponse = {
        data: Array(10).fill(mockTweet),
        meta: { newest_id: '123', oldest_id: '113', result_count: 10, next_token: 'page2' },
      };
      const page2Response: TwitterResponse = {
        data: Array(10).fill(mockTweet),
        meta: { newest_id: '112', oldest_id: '102', result_count: 10 },
      };

      mockedAxios.get
        .mockResolvedValueOnce({ data: page1Response })
        .mockResolvedValueOnce({ data: page2Response });

      const pipeline = {
        id: 'twitter-limit-download',
        source: {
          id: 'twitter-search',
          adapter_id: 'twitter',
          endpoint_id: 'tweets_search',
          credential_id: 'twitter-auth',
          config: { headers: {} },
          fields: ['id', 'text'],
          filters: [{ field: 'query', operator: '=', value: 'test' }],
          pagination: { itemsPerPage: 10 },
          limit: 12, // Should stop after 12 items
        },
      };

      const result = await orchestrator.runPipeline(pipeline);
      expect(result.data.length).toBe(12); // Cuts off at limit
      expect(mockedAxios.get).toHaveBeenCalledTimes(2);
    });
  });
});