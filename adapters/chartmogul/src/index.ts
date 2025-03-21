/**
 * ChartMogul Adapter for OpenETL
 * https://componade.com/openetl
 */

import { HttpAdapter, Connector, AuthConfig, ApiKeyAuth, AdapterInstance } from 'openetl';
import axios from 'axios';

const maxItemsPerPage = 200;

const ChartMogulAdapter: HttpAdapter = {
  id: "chartmogul-adapter",
  name: "ChartMogul Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  credential_type: "api_key",
  base_url: "https://api.chartmogul.com",
  config: [
    {
      id: 'headers',
      name: 'headers',
      required: false,
    },
    {
      id: 'query_params',
      name: 'query_params',
      required: false,
    },
  ],
  metadata: {
    provider: "chartmogul",
    description: "Adapter for ChartMogul",
    version: "v1",
  },
  pagination: {
    type: 'cursor',
    maxItemsPerPage,
  },
  endpoints: [
    // Source Endpoints
    {
      id: "sources",
      path: "/v1/data_sources",
      method: "GET",
      description: "Retrieve sources from ChartMogul",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-source",
      path: "/v1/data_sources",
      method: "POST",
      description: "Create a new source in ChartMogul",
      supported_actions: ["upload"],
      settings: {
        pagination: {
          type: 'offset',
          maxItemsPerPage: 1,
        }
      }
    },

    // Customer Endpoints
    {
      id: "customers",
      path: "/v1/customers",
      method: "GET",
      description: "Retrieve customers from ChartMogul",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-customer",
      path: "/v1/customers",
      method: "POST",
      description: "Create a new customer in ChartMogul",
      supported_actions: ["upload"],
      settings: {
        pagination: {
          type: 'offset',
          maxItemsPerPage: 1,
        }
      }
    },

    // Plan Endpoints
    {
      id: "plans",
      path: "/v1/plans",
      method: "GET",
      description: "Retrieve plans from ChartMogul",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-plan",
      path: "/v1/plans",
      method: "POST",
      description: "Create a new plan in ChartMogul",
      supported_actions: ["upload"],
      settings: {
        pagination: {
          type: 'offset',
          maxItemsPerPage: 1,
        }
      }
    },

    // Subscription Endpoints
    {
      id: "subscriptions",
      path: "/v1/customers/{customer_uuid}/subscriptions",
      method: "GET",
      description: "Retrieve subscriptions from ChartMogul",
      supported_actions: ["download", "sync"],
      settings: {
        config: [
          {
            id: 'customer_uuid',
            name: 'customer_uuid',
            required: true,
          },
        ]
      }
    },

    // Invoice Endpoints
    {
      id: "invoices",
      path: "/v1/invoices",
      method: "GET",
      description: "Retrieve invoices from ChartMogul",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-invoice",
      path: "/v1/import/customers/{customer_uuid}/invoices",
      method: "POST",
      description: "Create new invoices in ChartMogul",
      supported_actions: ["upload"],
      settings: {
        config: [
          {
            id: 'customer_uuid',
            name: 'customer_uuid',
            required: true,
          }
        ],
        pagination: {
          type: 'offset',
        }
      }
    },
  ],
};

function chartmogul(connector: Connector, auth: AuthConfig): AdapterInstance {
  const log = (...args: any[]) => {
    if (connector.debug) {
      console.log(...args);
    }
  };

  const endpoint = ChartMogulAdapter.endpoints.find(e => e.id === connector.endpoint_id);
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in ChartMogul adapter`);
  }

  function isApiKeyAuth(auth: AuthConfig): auth is ApiKeyAuth {
    return auth.type === 'api_key';
  }

  async function buildRequestConfig(): Promise<any> {
    if (!isApiKeyAuth(auth)) {
      throw new Error("ChartMogul adapter requires API Key authentication");
    }
    if (!auth.credentials.api_key) {
      throw new Error("API Key is required for ChartMogul authentication");
    }

    return {
      auth: {
        username: auth.credentials.api_key,
      },
      headers: {
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
    if (connector.filters && connector.filters.length > 0) {
      connector.filters.forEach((filter) => {
        if ('op' in filter) {
            throw new Error('Filter groups are not supported in ChartMogul adapter; use flat filters');
        }

        if (filter.operator === '=') {
          params[filter.field] = filter.value;
        }
      });
    }
    return params;
  }

  const endpointResponseMap: Record<string, string> = {
    sources: 'data_sources',
    customers: 'entries',
    plans: 'plans',
    invoices: 'invoices',
    subscriptions: 'entries',
  };

  const endpointPropertyMap: Record<string, string> = {
    'create-invoice': 'invoices',
  }

  const getEndpointUrl = function() {
    const url = ChartMogulAdapter.base_url + endpoint.path;
    if (!endpoint.path.includes('{customer_uuid}')) {
      return url;
    }

    const customerUuid = connector.config?.customer_uuid;
    if (!customerUuid) {
      throw new Error(`${endpoint.id} endpoint of the ChartMogul adapter requires a customer_uuid property in the config property`);
    }

    return url.replace('{customer_uuid}', customerUuid);
  }

  const download: AdapterInstance['download'] = async function(pageOptions) {
    const config = await buildRequestConfig();
    const { limit, offset } = pageOptions;

    if (typeof limit === 'undefined') {
      throw new Error('Number of items per page is required by the ChartMogul adapter');
    }
    if (limit > maxItemsPerPage) {
      throw new Error('Number of items per page exceeds ChartMogul maximum');
    }

    config.params.per_page = limit;

    if (typeof offset !== 'undefined') {
      config.params.cursor = offset;
    }

    const url = getEndpointUrl();

    const response = await axios.get(url, config);
    log("API Response:", JSON.stringify(response.data, null, 2));

    const data = response.data[endpointResponseMap[endpoint.id]];

    let filteredResults = Array.isArray(data) ? data : [data];
    if (connector.fields.length > 0) {
      filteredResults = filteredResults.map((item: any) => {
        const filteredItem: Record<string, any> = {};
        connector.fields.forEach(field => {
          if (item[field] !== undefined && item[field] !== null) {
            filteredItem[field] = item[field];
          }
        });
        return filteredItem;
      });
    }

    return {
      data: filteredResults,
      options: {
        nextOffset: response.data.has_more ? response.data.cursor : undefined,
      },
    };
  };

  return {
    getConfig: () => ChartMogulAdapter,

    download: async function(pageOptions) {
      if (!endpoint.supported_actions.includes('download')) {
        throw new Error(`${endpoint.id} endpoint doesn't support download`);
      }
      try {
        return await download(pageOptions);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        throw new Error(`Download failed: ${errorMessage}`);
      }
    },

    upload: async function(data: any[]): Promise<void> {
      const config = await buildRequestConfig();
      const url = getEndpointUrl();

      const maxItemsPerPage = endpoint.settings?.pagination && endpoint.settings?.pagination.maxItemsPerPage;

      let axiosData: any;

      if (typeof maxItemsPerPage === 'number') {
        if (data.length > maxItemsPerPage) {
          throw new Error(`Number of items per page (${data.length}), exceeds the maximum number allowed for the ${endpoint.id} endpoint of the ChartMogul adapter`);
        }

        if (maxItemsPerPage === 1) {
          axiosData = data[0];
        }
      }

      if (typeof axiosData === 'undefined') {
        const propertyName = endpointPropertyMap[endpoint.id];

        axiosData = {
          [propertyName]: data,
        };
      }

      try {
        await axios.post(url, axiosData, config);
        log("Upload successful");
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        throw new Error(`Upload failed: ${errorMessage}`);
      }
    },
  };
}

export { chartmogul, ChartMogulAdapter };