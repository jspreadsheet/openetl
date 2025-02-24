/**
 * Stripe Adapter for OpenETL
 * https://componade.com/openetl
 *
 * @TODO:
 * Performance Optimization
 * Issue: The upload function processes items sequentially with individual POST requests.
 * Fix: Implement batch processing where Stripe supports it (e.g., for idempotent operations).
 */

import axios from 'axios';
import { HttpAdapter, Connector, AuthConfig, AdapterInstance, FilterGroup, Filter } from './types';

const StripeAdapter: HttpAdapter = {
  id: "stripe-adapter",
  name: "Stripe Payments Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  credential_type: "api_key",
  base_url: "https://api.stripe.com/v1",
  metadata: {
    provider: "stripe",
    description: "Adapter for Stripe Payments API",
    version: "v1", // Stripe API version as of Feb 2025
  },
  endpoints: [
    // Core Payment Entities
    {
      id: "customers",
      path: "/customers",
      method: "GET",
      description: "Retrieve all customers from Stripe",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-customer",
      path: "/customers",
      method: "POST",
      description: "Create a new customer in Stripe",
      supported_actions: ["upload"],
    },
    {
      id: "charges",
      path: "/charges",
      method: "GET",
      description: "Retrieve all charges from Stripe",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-charge",
      path: "/charges",
      method: "POST",
      description: "Create a new charge in Stripe",
      supported_actions: ["upload"],
    },
    {
      id: "invoices",
      path: "/invoices",
      method: "GET",
      description: "Retrieve all invoices from Stripe",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-invoice",
      path: "/invoices",
      method: "POST",
      description: "Create a new invoice in Stripe",
      supported_actions: ["upload"],
    },
    {
      id: "subscriptions",
      path: "/subscriptions",
      method: "GET",
      description: "Retrieve all subscriptions from Stripe",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-subscription",
      path: "/subscriptions",
      method: "POST",
      description: "Create a new subscription in Stripe",
      supported_actions: ["upload"],
    },
    // Additional Entities
    {
      id: "products",
      path: "/products",
      method: "GET",
      description: "Retrieve all products from Stripe",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-product",
      path: "/products",
      method: "POST",
      description: "Create a new product in Stripe",
      supported_actions: ["upload"],
    },
    {
      id: "prices",
      path: "/prices",
      method: "GET",
      description: "Retrieve all prices from Stripe",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-price",
      path: "/prices",
      method: "POST",
      description: "Create a new price in Stripe",
      supported_actions: ["upload"],
    },
    {
      id: "refunds",
      path: "/refunds",
      method: "GET",
      description: "Retrieve all refunds from Stripe",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-refund",
      path: "/refunds",
      method: "POST",
      description: "Create a new refund in Stripe",
      supported_actions: ["upload"],
    },
  ],
};

async function delay(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function stripe(connector: Connector, auth: AuthConfig): AdapterInstance {
  const endpoint = StripeAdapter.endpoints.find(e => e.id === connector.endpoint_id);
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in Stripe adapter`);
  }

  let totalFetched = 0;

  function isApiKeyAuth(auth: AuthConfig): auth is AuthConfig & { credentials: { api_key: string } } {
    return auth.type === 'api_key';
  }

  async function buildRequestConfig(): Promise<any> {
    if (!isApiKeyAuth(auth)) {
      throw new Error("Stripe adapter requires API key authentication");
    }
    if (!auth.credentials.api_key) {
      throw new Error("API key is missing in credentials");
    }
    return {
      headers: {
        'Authorization': `Bearer ${auth.credentials.api_key}`,
        'Content-Type': 'application/x-www-form-urlencoded',
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
    if (connector.fields.length > 0) params.expand = connector.fields.map(f => `data.${f}`);
    if (connector.filters && connector.filters.length > 0) {
      connector.filters.forEach(filter => {
        if ('op' in filter) {
          throw new Error('Filter groups are not natively supported by Stripe; use individual filters');
        }
        const f = filter as Filter;
        params[f.field] = f.value; // Stripe uses direct field-value pairs for filtering
      });
    }
    if (connector.sort && connector.sort.length > 0) {
      // Stripe doesn't support sorting natively in API calls; handle post-fetch if needed
      console.warn("Sorting not supported natively by Stripe API; apply sorting post-fetch.");
    }
    return params;
  }

  return {
    connect: async function(): Promise<void> {
      const config = await buildRequestConfig();
      try {
        console.log("Testing connection to Stripe...");
        await axios.get(`${StripeAdapter.base_url}/customers`, {
          ...config,
          params: { limit: 1, ...config.params },
        });
        console.log("Connection successful");
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        console.error("Connection test failed:", errorMessage);
        throw new Error(`Failed to connect to Stripe: ${errorMessage}`);
      }
    },

    download: async function(pageOptions: { limit: number; offset: number }): Promise<{ data: any[]; options?: { [key: string]: any } }> {
      const config = await buildRequestConfig();
      const pageLimit = Math.min(pageOptions.limit, 100); // Stripe max limit is 100
      const totalLimit = connector.limit || Number.MAX_SAFE_INTEGER;
      const remainingLimit = totalLimit - totalFetched;
      const effectiveLimit = Math.min(pageLimit, remainingLimit);
      let startingAfter: string | undefined = pageOptions.offset > 0 ? pageOptions.offset.toString() : undefined;

      if (effectiveLimit <= 0) {
        console.log("Effective limit reached, returning empty result");
        return { data: [], options: { nextOffset: undefined } };
      }

      config.params.limit = effectiveLimit;
      if (startingAfter) {
        config.params.starting_after = startingAfter;
      }

      try {
        const response = await axios.get(`${StripeAdapter.base_url}${endpoint.path}`, config);
        console.log("API Response:", JSON.stringify(response.data, null, 2));

        const { data, has_more } = response.data;

        if (!Array.isArray(data)) {
          console.warn("Data is not an array or is undefined:", response.data);
          return { data: [], options: { nextOffset: undefined } };
        }

        const filteredResults = data.map((item: any) => {
          const filteredItem: Record<string, any> = {};
          connector.fields.forEach(field => {
            if (item[field] !== undefined && item[field] !== null) {
              filteredItem[field] = item[field];
            }
          });
          console.log("Filtered Result:", JSON.stringify(filteredItem, null, 2));
          return filteredItem;
        });

        totalFetched += filteredResults.length;

        return {
          data: filteredResults,
          options: {
            nextOffset: has_more && totalFetched < totalLimit && data.length > 0 ? data[data.length - 1].id : undefined,
          },
        };
      } catch (error: any) {
        if (error.response && typeof error.response.status === 'number') {
          const status = error.response.status;
          console.log('Error status:', status);
          if (status === 401) {
            throw new Error('Invalid API key provided');
          } else if (status === 429) {
            const retryAfter = error.response.headers['retry-after'] ? parseInt(error.response.headers['retry-after'], 10) * 1000 : 1000;
            console.log(`Rate limit hit, waiting ${retryAfter}ms`);
            await delay(retryAfter);
            console.log('Retrying download after delay');
            return this.download(pageOptions);
          }
          console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
        }
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        console.error("Download error:", errorMessage);
        throw new Error(`Download failed: ${errorMessage}`);
      }
    },

    upload: async function(data: any[]): Promise<void> {
      const config = await buildRequestConfig();
      for (const item of data) {
        try {
          const formData = new URLSearchParams();
          for (const [key, value] of Object.entries(item)) {
            formData.append(key, String(value));
          }
          await axios.post(`${StripeAdapter.base_url}${endpoint.path}`, formData, config);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : 'Unknown error';
          console.error("Upload error:", errorMessage);
          throw error;
        }
      }
    },

    disconnect: async function(): Promise<void> {
      console.log("Disconnecting from Stripe adapter (no-op)");
    },
  };
}

export { stripe, StripeAdapter };