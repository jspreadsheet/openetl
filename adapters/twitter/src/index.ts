/**
 * Twitter/X Adapter for OpenETL using BaseAdapter
 * https://componade.com/openetl
 */

import { Connector, AdapterInstance, AuthConfig, HttpAdapter } from 'openetl';
import axios from 'axios';

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
  function buildQueryParams(): Record<string, string> {
    const params: Record<string, string> = {};

    if (connector.filters?.length) {
      connector.filters.forEach(filter => {
        if ('field' in filter) { // Simple Filter
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

    if (connector.fields?.length) {
      params[`${endpoint?.id === 'user_lookup' ? 'user' : 'tweet'}.fields`] = connector.fields.join(',');
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
      console.log("Twitter API adapter initialized");
    },
    disconnect: async function() {
      delete headers.Authorization;
      console.log("Twitter API adapter disconnected");
    },
    download: async function(pageOptions) {
      if (!endpoint.supported_actions.includes('download')) {
        throw new Error(`${endpoint.id} endpoint does not support download`);
      }
      if (!headers.Authorization) {
        throw new Error("Not connected to Twitter API");
      }

      const params = buildQueryParams();
      if (pageOptions.limit && endpoint.settings?.pagination) {
        // check this
        params.limitKey = String(pageOptions.limit);
      }
      if (pageOptions.offset && endpoint.settings?.pagination) {
         // check this
        params.offset = String(pageOptions.offset);
      }

      const url = `${TwitterAdapter.base_url}${endpoint.path}`;

      try {
        console.log(`Fetching from ${url} with params:`, params);
        const response = await axios.get(url, { headers, params });
        const data = response.data.data || [];
        const nextOffset = endpoint.settings?.pagination ? response.data.meta?.next_token : undefined;
        console.log(`Downloaded ${data.length} items`);
        return {
          data,
          options: nextOffset ? { nextOffset } : undefined,
        };
      } catch (error: any) {
        console.error("Twitter download error:", error.message);
        throw new Error(`Failed to download from Twitter: ${error.message}`);
      }
    },
    upload: async function(data) {
      if (!endpoint.supported_actions.includes('upload')) {
        throw new Error(`${endpoint.id} endpoint does not support upload`);
      }
      if (!headers.Authorization) {
        throw new Error("Not connected to Twitter API");
      }

      console.log("Uploading tweets:", data.length);
      try {
        for (const item of data) {
          if (!item.text || typeof item.text !== 'string') {
            throw new Error("Each upload item must have a 'text' string field");
          }
          const url = `${TwitterAdapter.base_url}${endpoint.path}`;
          const response = await axios.post(url, { text: item.text }, { headers });
          console.log(`Tweet posted: ${response.data.data.id}`);
        }
      } catch (error: any) {
        console.error("Twitter upload error:", error.message);
        throw new Error(`Failed to upload to Twitter: ${error.message}`);
      }
    },
  };
}

export { twitter, TwitterAdapter };