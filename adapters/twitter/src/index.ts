/**
 * Twitter/X Adapter for OpenETL using BaseAdapter
 * https://componade.com/openetl
 */

import { Connector, AdapterInstance, AuthConfig, HttpAdapter } from 'openetl';
import axios, { isAxiosError } from 'axios';

export interface TwitterTweet {
  id: string;
  text: string;
  edit_history_tweet_ids: string[];
  // Add other fields you might request via tweet.fields
  created_at?: string;
  author_id?: string;
}

export interface TwitterMeta {
  newest_id: string;
  oldest_id: string;
  result_count: number;
  next_token?: string;
}

export interface TwitterResponse {
  data: TwitterTweet[];
  meta: TwitterMeta;
}

const TwitterAdapter: HttpAdapter = {
  id: "twitter",
  name: "Twitter/X API Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  base_url: "https://api.twitter.com/2",
  config: [
    {
      name: 'headers',
      required: false,
      default: {},
    },
  ],
  credential_type: "oauth2",
  metadata: {
    provider: "twitter",
    description: "HTTP Adapter for Twitter/X API operations",
    version: "1.0",
  },
  endpoints: [
    {
      id: "tweets_search",
      path: "/tweets/search/recent",
      method: "GET",
      description: "Search recent tweets",
      supported_actions: ["download", "sync"],
      settings: {
        pagination: {
          type: 'cursor',
        },
      },
    },
    {
      id: "user_lookup",
      path: "/users",
      method: "GET",
      description: "Lookup users by ID or username",
      supported_actions: ["download"],
      settings: {
        pagination: {
          type: 'cursor'
        },
      },
    },
    {
      id: "tweet_post",
      path: "/tweets",
      method: "POST",
      description: "Post a tweet",
      supported_actions: ["upload"],
      settings: {
        pagination: false, // No pagination for posting
      },
    },
  ],
  pagination: { type: 'cursor' }
};

function twitter(connector: Connector, auth: AuthConfig): AdapterInstance {
  const log = function (...args: any) {
    if (connector.debug) {
        log(...arguments);
    }
  };
  const endpoint = TwitterAdapter.endpoints.find(e => e.id === connector.endpoint_id);
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in Twitter adapter`);
  }

  function isOAuth2Auth(auth: AuthConfig) {
    return auth.type === 'oauth2' && !!auth.credentials.access_token;
  }

  let headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(connector.config?.headers || {}),
  };

  // Build query parameters based on endpoint
  function buildQueryParams(pageOptions: { limit?: number; offset?: number | string }): Record<string, string | number> {
    const params: Record<string, string | number> = {};

    if (pageOptions.limit) {
      const requestedLimit = pageOptions.limit;
      if (requestedLimit < 10 || requestedLimit > 100) {
        throw new Error(`max_results must be between 10 and 100, got ${requestedLimit}`);
      }
      params.max_results = String(requestedLimit);
    }
    // Handle filters
    if (connector.filters?.length) {
      connector.filters.forEach(filter => {
        if ('field' in filter) {
          switch (endpoint?.id) {
            case 'tweets_search':
              if (filter.field === 'query') params['query'] = String(filter.value);
              else if (['since_id', 'until_id', 'start_time', 'end_time'].includes(filter.field)) {
                params[filter.field] = String(filter.value);
              }
              break;
            case 'user_lookup':
              if (filter.field === 'ids' || filter.field === 'usernames') {
                params[filter.field] = String(filter.value);
              }
              break;
          }
        }
      });
    }

    // Handle fields
    if (connector.fields?.length) {
      params[`${endpoint?.id === 'user_lookup' ? 'user' : 'tweet'}.fields`] = connector.fields.join(',');
    }

    // Handle pagination
    if (pageOptions.limit) {
      params.max_results = Number(Math.min(pageOptions.limit, 100)); // Twitter API max is 100
    }
    if (pageOptions.offset) {
      params.next_token = String(pageOptions.offset);
    }

    return params;
  }

  return {
    getConfig: () => TwitterAdapter,
    connect: async function() {
      if (!isOAuth2Auth(auth)) {
        throw new Error("Twitter adapter requires OAuth2 authentication with access_token");
      }
      headers.Authorization = `Bearer ${auth.credentials.access_token}`;
      log("Twitter API adapter initialized");
    },
    disconnect: async function() {
      delete headers.Authorization;
      log("Twitter API adapter disconnected");
    },
    download: async function(pageOptions) {
      if (!endpoint.supported_actions.includes('download')) {
        throw new Error(`${endpoint.id} endpoint does not support download`);
      }
      if (!headers.Authorization) {
        throw new Error("Not connected to Twitter API");
      }

      const params = buildQueryParams(pageOptions);
      const url = `${TwitterAdapter.base_url}${endpoint.path}`;

      try {
        const response = await axios.get(url, { headers, params });
        const data = response.data.data || [];
        const nextOffset = response.data.meta?.next_token;
        return {
          data:  data as TwitterTweet[],
          options: nextOffset ? { nextOffset } : undefined,
        };
      } catch (error) {
        if (isAxiosError(error)) {
          throw new Error(`Twitter API error ${error.response?.status}: ${JSON.stringify(error.response?.data)}`);
        }
        throw error;
      }
    },
    upload: async function(data) {
      if (!endpoint.supported_actions.includes('upload')) {
        throw new Error(`${endpoint.id} endpoint does not support upload`);
      }
      if (!headers.Authorization) {
        throw new Error("Not connected to Twitter API");
      }

      log("Uploading tweets:", data.length);
      try {
        for (const item of data) {
          if (!item.text || typeof item.text !== 'string') {
            throw new Error("Each upload item must have a 'text' string field");
          }
          const url = `${TwitterAdapter.base_url}${endpoint.path}`;
          const response = await axios.post(url, { text: item.text }, { headers });
          log(`Tweet posted: ${response.data.data.id}`);
        }
      } catch (error: any) {
        throw new Error(`Failed to upload to Twitter: ${error.message}`);
      }
    },
  };
}

export { twitter, TwitterAdapter };