/**
 * Zoho CRM Adapter for OpenETL
 * https://componade.com/openetl
 *
 * @TODO:
 * - Add batch upload support (/crm/v3/{module}/upsert)
 * - Implement rate limit handling based on Zoho's limits (varies by plan)
 */

import { HttpAdapter, Connector, AuthConfig, OAuth2Auth, AdapterInstance, FilterGroup, Filter } from 'openetl';
import axios, { isAxiosError } from 'axios';

const ZohoAdapter: HttpAdapter = {
  id: "zoho-adapter",
  name: "Zoho CRM Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  credential_type: "oauth2",
  base_url: "https://www.zohoapis.com",
  config: [
    {
      name: 'headers',
      required: false,
    },
    {
      name: 'query_params',
      required: false,
    },
  ],
  metadata: {
    provider: "zoho",
    description: "Adapter for Zoho CRM API",
    version: "v3", // Using Zoho CRM API v3 as of March 2025
  },
  endpoints: [
    // CRM Modules
    {
      id: "leads",
      path: "/crm/v3/Leads",
      method: "GET",
      description: "Retrieve all leads from Zoho CRM",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-lead",
      path: "/crm/v3/Leads",
      method: "POST",
      description: "Create a new lead in Zoho CRM",
      supported_actions: ["upload"],
    },
    {
      id: "contacts",
      path: "/crm/v3/Contacts",
      method: "GET",
      description: "Retrieve all contacts from Zoho CRM",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-contact",
      path: "/crm/v3/Contacts",
      method: "POST",
      description: "Create a new contact in Zoho CRM",
      supported_actions: ["upload"],
    },
    {
      id: "accounts",
      path: "/crm/v3/Accounts",
      method: "GET",
      description: "Retrieve all accounts from Zoho CRM",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-account",
      path: "/crm/v3/Accounts",
      method: "POST",
      description: "Create a new account in Zoho CRM",
      supported_actions: ["upload"],
    },
    {
      id: "deals",
      path: "/crm/v3/Deals",
      method: "GET",
      description: "Retrieve all deals from Zoho CRM",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-deal",
      path: "/crm/v3/Deals",
      method: "POST",
      description: "Create a new deal in Zoho CRM",
      supported_actions: ["upload"],
    },
    {
      id: "products",
      path: "/crm/v3/Products",
      method: "GET",
      description: "Retrieve all products from Zoho CRM",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-product",
      path: "/crm/v3/Products",
      method: "POST",
      description: "Create a new product in Zoho CRM",
      supported_actions: ["upload"],
    },
    // Users
    {
      id: "users",
      path: "/crm/v3/users",
      method: "GET",
      description: "Retrieve all users in Zoho CRM",
      supported_actions: ["download", "sync"],
    },
  ],
};

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function zoho(connector: Connector, auth: AuthConfig): AdapterInstance {
  const endpoint = ZohoAdapter.endpoints.find(e => e.id === connector.endpoint_id);
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in Zoho adapter`);
  }

  function isOAuth2Auth(auth: AuthConfig): auth is OAuth2Auth {
    return auth.type === 'oauth2';
  }

  async function refreshOAuthToken(): Promise<void> {
    if (!isOAuth2Auth(auth)) {
      throw new Error("Not an OAuth2 authentication");
    }
    if (!auth.credentials.refresh_token) {
      throw new Error("Refresh token missing; obtain initial tokens manually and update vault");
    }
    console.log("Refreshing OAuth token...");
    try {
      const response = await axios.post(
        auth.credentials.token_url || 'https://accounts.zoho.com/oauth/v2/token',
        new URLSearchParams({
          grant_type: 'refresh_token',
          client_id: auth.credentials.client_id,
          client_secret: auth.credentials.client_secret,
          refresh_token: auth.credentials.refresh_token,
        }).toString(),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
      );
      auth.credentials.access_token = response.data.access_token;
      auth.credentials.refresh_token = response.data.refresh_token || auth.credentials.refresh_token;
      auth.expires_at = new Date(Date.now() + response.data.expires_in * 1000).toISOString();
      console.log("Token refreshed successfully");
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      console.error("Token refresh failed:", errorMessage);
      throw new Error(`OAuth token refresh failed: ${errorMessage}`);
    }
  }

  async function buildRequestConfig(): Promise<any> {
    if (!isOAuth2Auth(auth)) {
      throw new Error("Zoho adapter requires OAuth2 authentication");
    }
    if (!auth.credentials.access_token || (auth.expires_at && new Date(auth.expires_at) < new Date())) {
      await refreshOAuthToken();
    }
    return {
      headers: {
        'Authorization': `Zoho-oauthtoken ${auth.credentials.access_token}`,
        'Content-Type': 'application/json',
        ...connector.config?.headers,
      },
      params: {
        ...buildQueryParams(),
        ...connector.config?.query_params,
      },
    };
  }

  function buildQueryParams(): Record<string, any> {
    const params: Record<string, any> = {};
    if (connector.fields.length > 0) params.fields = connector.fields.join(',');
    if (connector.filters && connector.filters.length > 0) {
      // Zoho uses a different filtering approach with criteria
      params.criteria = connector.filters.map(filter => {
        if ('op' in filter) {
          return (filter as FilterGroup).filters.map((f: Filter | FilterGroup) => {
            if ('field' in f && 'operator' in f && 'value' in f) {
                return `(${f.field}:${mapOperator(f.operator)}:${f.value})`;
              }
              return ''; 
          });
        }
        return `(${filter.field}:${mapOperator(filter.operator)}:${filter.value})`;
      }).join(' and ');
    }
    if (connector.sort && connector.sort.length > 0) {
      params.sort_by = connector.sort[0].field;
      params.sort_order = connector.sort[0].type;
    }
    return params;
  }

  function mapOperator(operator: string): string {
    const operatorMap: Record<string, string> = {
      '=': 'equals',
      '!=': 'not_equals',
      '>': 'greater_than',
      '>=': 'greater_equal',
      '<': 'less_than',
      '<=': 'less_equal',
      'contains': 'contains',
      'not_contains': 'not_contains',
      'in': 'in',
      'not_in': 'not_in',
    };
    return operatorMap[operator] || operator;
  }

  const maxItemsPerPage = 200; // Zoho's max page size

  const download: AdapterInstance['download'] = async function(pageOptions) {
    const config = await buildRequestConfig();
    const { limit, offset } = pageOptions;

    if (typeof limit === 'undefined') {
      throw new Error('Number of items per page is required by the Zoho adapter');
    }

    if (limit > maxItemsPerPage) {
      throw new Error('Number of items per page exceeds Zoho maximum');
    }

    config.params.per_page = limit;
    config.params.page = offset ? Math.floor(offset / limit) + 1 : 1;

    const response = await axios.get(`${ZohoAdapter.base_url}${endpoint.path}`, config);
    const { data, info } = response.data;

    if (!Array.isArray(data)) {
      console.warn("Data is not an array or is undefined:", response.data);
      return { data: [], options: { nextOffset: info?.more_records ? (config.params.page * limit) : undefined } };
    }

    let filteredResults = connector.fields.length > 0
      ? data.map((item: any) => {
          const filteredItem: Record<string, any> = {};
          connector.fields.forEach(field => {
            if (item[field] !== undefined && item[field] !== null) {
              filteredItem[field] = item[field];
            }
          });
          return filteredItem;
        })
      : data;

    return {
      data: filteredResults,
      options: {
        nextOffset: info?.more_records ? (config.params.page * limit) : undefined,
      },
    };
  }

  const handleDownloadError = (error: any) => {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    if (error.response && typeof error.response.status === 'number') {
      console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
    }
    throw new Error(`Download failed: ${errorMessage}`);
  }

  return {
    paginationType: 'offset',
    maxItemsPerPage,
    connect: async function(): Promise<void> {
      const config = await buildRequestConfig();
      try {
        console.log("Testing connection to Zoho...");
        await axios.get(`${ZohoAdapter.base_url}/crm/v3/users`, {
          ...config,
          params: { type: 'CurrentUser', ...config.params },
        });
        console.log("Connection successful");
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        throw new Error(`Failed to connect to Zoho: ${errorMessage}`);
      }
    },

    download: async function(pageOptions) {
      try {
        return await download(pageOptions);
      } catch (error: any) {
        if (error.response?.status === 401) {
          await refreshOAuthToken();
          return await download(pageOptions);
        } else if (error.response?.status === 429) {
          const retryAfter = error.response.headers['retry-after'] 
            ? parseInt(error.response.headers['retry-after'], 10) * 1000 
            : 1000;
          await delay(retryAfter);
          return await download(pageOptions);
        }
        throw handleDownloadError(error);
      }
    },

    upload: async function(data: any[]): Promise<void> {
      const config = await buildRequestConfig();
      try {
        await axios.post(
          `${ZohoAdapter.base_url}${endpoint.path}`,
          { data },
          config
        );
      } catch (error) {
        const errorMessage = isAxiosError(error) && error.response?.data?.message
          ? error.response.data.message
          : error instanceof Error ? error.message : 'Unknown error';
        console.error("Upload error:", errorMessage);
        throw new Error(`Upload failed: ${errorMessage}`);
      }
    },

    disconnect: async function(): Promise<void> {
      console.log("Disconnecting from Zoho adapter (no-op)");
    },
  };
}

export { zoho, ZohoAdapter };