import { Connector, AdapterInstance, AuthConfig, HttpAdapter } from 'openetl';
import axios, { isAxiosError } from 'axios';

const GitHubAdapter: HttpAdapter = {
  id: "github",
  name: "GitHub API Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  base_url: "https://api.github.com",
  config: [
    { name: 'headers', required: false, default: {} },
    { name: 'owner', required: false },
    { name: 'repo', required: false },
  ],
  credential_type: "api_key",
  metadata: {
    provider: "github",
    description: "HTTP Adapter for GitHub API operations",
    version: "1.0",
  },
  endpoints: [
    {
      id: "repo_issues",
      path: "/repos/{owner}/{repo}/issues",
      method: "GET",
      description: "Fetch repository issues",
      supported_actions: ["download", "sync"],
      settings: {
        pagination: { type: "offset", maxItemsPerPage: 100 },
      },
    },
    {
      id: "create_issue",
      path: "/repos/{owner}/{repo}/issues",
      method: "POST",
      description: "Create an issue",
      supported_actions: ["upload"],
      settings: { pagination: false },
    },
    {
      id: "user_repos",
      path: "/user/repos",
      method: "GET",
      description: "List user repositories",
      supported_actions: ["download"],
      settings: {
        pagination: { type: "offset", maxItemsPerPage: 100 },
      },
    },
    {
      id: 'user_public_repos',
      path: '/users/{owner}/repos',
      method: 'GET',
      description: 'Get user repositories',
      supported_actions: ['download'],
      settings: {
        pagination: { type: "offset", maxItemsPerPage: 100 },
      }
    },
    {
      id: "repo_commits",
      path: "/repos/{owner}/{repo}/commits",
      method: "GET",
      description: "Fetch repository commits",
      supported_actions: ["download"],
      settings: {
        pagination: { type: "offset", maxItemsPerPage: 100 },
      },
    },
    {
      id: "user_profile",
      path: "/user",
      method: "GET",
      description: "Fetch authenticated user profile",
      supported_actions: ["download"],
      settings: { pagination: false },
    },
    {
      id: "user_info",
      path: "/users/{username}",
      method: "GET",
      description: "Fetch any user public data",
      supported_actions: ["download"],
      settings: { pagination: false },
    },
    {
      id: "user_emails",
      path: "/user/emails",
      method: "GET",
      description: "Fetch authenticated user emails",
      supported_actions: ["download"],
      settings: { pagination: false },
    },
  ],
  pagination: { type: "offset" },
};

function github(connector: Connector, auth: AuthConfig): AdapterInstance {
  const endpoint = GitHubAdapter.endpoints.find(e => e.id === connector.endpoint_id);
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in GitHub adapter`);
  }

  let headers: Record<string, string> = {
    'Content-Type': 'application/json',
    'Accept': 'application/vnd.github+json',
    ...(connector.config?.headers || {}),
  };

  function buildQueryParams(pageOptions: { limit?: number; offset?: number | string }): Record<string, string> {
    const params: Record<string, string> = {};
    if (connector.filters?.length) {
      connector.filters.forEach(filter => {
        if ('field' in filter) {
          params[filter.field] = String(filter.value);
        }
      });
    }
    if (pageOptions.limit) {
      const limit = Math.min(pageOptions.limit, 100);
      params.per_page = String(limit);
    }
    if (pageOptions.offset) {
      const offset = typeof pageOptions.offset === 'string' ? parseInt(pageOptions.offset, 10) : pageOptions.offset;
      const itemsPerPage = pageOptions.limit || 100;
      params.page = String(Math.floor(offset / itemsPerPage) + 1);
    } else {
      params.page = '1';
    }
    return params;
  }

  function replacePathParams(url: string): string {
    if ( endpoint?.path.includes('{owner}') ) {
      if ( !connector.config?.owner ) {
        throw new Error("Connector config must include owner for repo-specific endpoints");
      }
    }
    if ( endpoint?.path.includes('{repo}') ) {
      if ( !connector.config?.repo ) {
        throw new Error("Connector config must include repo for repo-specific endpoints");
      }
    }

    if ( endpoint?.path.includes('{username}')) {
      if ( !connector.config?.username ) {
        throw new Error("Connector config must include username for users endpoints");
      }
    }
    return url.replace('{owner}', connector.config?.owner)
                .replace('{repo}', connector.config?.repo)
                .replace('{username}', connector.config?.username);
  }

  return {
    getConfig: () => GitHubAdapter,
    connect: async function() {
      if (auth.type === 'api_key' && auth.credentials.api_key) {
        headers.Authorization = `Bearer ${auth.credentials.api_key}`;
      } else if (auth.type === 'oauth2' && auth.credentials.access_token) {
        headers.Authorization = `Bearer ${auth.credentials.access_token}`;
      } else {
        throw new Error("GitHub adapter requires api_key or oauth2 authentication with a valid token");
      }
    },
    disconnect: async function() {
      delete headers.Authorization;
    },
    download: async function(pageOptions) {
      if (!endpoint.supported_actions.includes('download')) {
        throw new Error(`${endpoint.id} does not support download`);
      }
      if (!headers.Authorization) {
        throw new Error("Not connected to GitHub API");
      }

      const params = buildQueryParams(pageOptions);
      const url = replacePathParams(`${GitHubAdapter.base_url}${endpoint.path}`);

      try {
        const response = await axios.get<any[]>(url, { headers, params });
        let data = response?.data;
        if (!Array.isArray(data)) {
          data = data ? [data] : [];
        }
        const linkHeader = response?.headers?.link;
        let nextOffset: number | undefined;
        if (linkHeader) {
          const nextMatch = linkHeader.match(/<[^>]+page=(\d+)[^>]*>; rel="next"/);
          if (nextMatch) {
            nextOffset = parseInt(nextMatch[1], 10);
          }
        }
        return {
          data,
          options: nextOffset ? { nextOffset } : undefined,
        };
      } catch (error) {
        if (isAxiosError(error)) {
          throw new Error(`GitHub API error ${error.response?.status}: ${JSON.stringify(error.response?.data)}`);
        }
        throw error;
      }
    },
    upload: async function(data) {
      if (!endpoint.supported_actions.includes('upload')) {
        throw new Error(`${endpoint.id} does not support upload`);
      }
      if (!headers.Authorization) {
        throw new Error("Not connected to GitHub API");
      }

      const url = replacePathParams(`${GitHubAdapter.base_url}${endpoint.path}`);

      try {
        for (const item of data) {
          if (!item.title || typeof item.title !== 'string') {
            throw new Error("Each upload item must have a 'title' string field");
          }
          await axios.post(url, item, { headers });
        }
      } catch (error) {
        if (isAxiosError(error)) {
          throw new Error(`GitHub API error ${error.response?.status}: ${JSON.stringify(error.response?.data)}`);
        }
        throw error;
      }
    },
  };
}

export { github, GitHubAdapter };
