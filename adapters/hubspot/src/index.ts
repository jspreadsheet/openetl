/**
 * HubSpot Adapter for OpenETL
 * https://componade.com/openetl
 *
 * @TODO:
 * Performance Optimization
 * Issue: The upload function processes items sequentially with individual POST requests, which could be slow for large datasets. HubSpot supports batch endpoints (e.g., /crm/v3/objects/contacts/batch/create).
 * Fix: Add batch upload support for efficiency.
 */

import { HttpAdapter, Connector, AuthConfig, OAuth2Auth, AdapterInstance, FilterGroup, Filter } from 'openetl';

import axios, { isAxiosError } from 'axios';

const HubSpotAdapter: HttpAdapter = {
  id: "hubspot-adapter",
  name: "HubSpot CRM Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  credential_type: "oauth2", // Update to reflect actual usage
  base_url: "https://api.hubapi.com",
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
    provider: "hubspot",
    description: "Adapter for HubSpot CRM and Marketing APIs",
    version: "v3", // Most current stable API version as of Feb 2025
  },
  endpoints: [
    // CRM Objects
    {
      id: "contacts",
      path: "/crm/v3/objects/contacts",
      method: "GET",
      description: "Retrieve all contacts from HubSpot",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-contact",
      path: "/crm/v3/objects/contacts/batch/create",
      method: "POST",
      description: "Create a new contact in HubSpot",
      supported_actions: ["upload"],
    },
    {
      id: "companies",
      path: "/crm/v3/objects/companies",
      method: "GET",
      description: "Retrieve all companies from HubSpot",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-company",
      path: "/crm/v3/objects/companies/batch/create",
      method: "POST",
      description: "Create a new company in HubSpot",
      supported_actions: ["upload"],
    },
    {
      id: "deals",
      path: "/crm/v3/objects/deals",
      method: "GET",
      description: "Retrieve all deals from HubSpot",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-deal",
      path: "/crm/v3/objects/deals/batch/create",
      method: "POST",
      description: "Create a new deal in HubSpot",
      supported_actions: ["upload"],
    },
    {
      id: "tickets",
      path: "/crm/v3/objects/tickets",
      method: "GET",
      description: "Retrieve all support tickets from HubSpot",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-ticket",
      path: "/crm/v3/objects/tickets/batch/create",
      method: "POST",
      description: "Create a new support ticket in HubSpot",
      supported_actions: ["upload"],
    },
    {
      id: "products",
      path: "/crm/v3/objects/products",
      method: "GET",
      description: "Retrieve all products from HubSpot",
      supported_actions: ["download", "sync"],
    },
    {
      id: "create-product",
      path: "/crm/v3/objects/products",
      method: "POST",
      description: "Create a new product in HubSpot",
      supported_actions: ["upload"],
    },

    // // Marketing Endpoints
    // {
    //   id: "marketing-emails",
    //   path: "/marketing/v3/emails",
    //   method: "GET",
    //   description: "Retrieve all marketing emails from HubSpot",
    //   supported_actions: ["download", "sync"],
    // },
    // {
    //   id: "create-marketing-email",
    //   path: "/marketing/v3/emails",
    //   method: "POST",
    //   description: "Create a new marketing email in HubSpot",
    //   supported_actions: ["upload"],
    // },
    // {
    //   id: "forms",
    //   path: "/forms/v2/forms",
    //   method: "GET",
    //   description: "Retrieve all forms from HubSpot",
    //   supported_actions: ["download", "sync"],
    // },
    // {
    //   id: "create-form",
    //   path: "/forms/v2/forms",
    //   method: "POST",
    //   description: "Create a new form in HubSpot",
    //   supported_actions: ["upload"],
    // },

    // Analytics Endpoints
    {
      id: "analytics-events",
      path: "/events/v3/events",
      method: "GET",
      description: "Retrieve analytics events from HubSpot",
      supported_actions: ["download", "sync"],
    },

    // // Engagements (Activities)
    // {
    //   id: "engagements",
    //   path: "/engagements/v1/engagements/paged",
    //   method: "GET",
    //   description: "Retrieve all engagements (notes, emails, calls, etc.)",
    //   supported_actions: ["download", "sync"],
    // },
    // {
    //   id: "create-engagement",
    //   path: "/engagements/v1/engagements",
    //   method: "POST",
    //   description: "Create a new engagement (e.g., note, email, call)",
    //   supported_actions: ["upload"],
    // },

    // Pipelines
    {
      id: "pipelines",
      path: "/crm/v3/pipelines/deals",
      method: "GET",
      description: "Retrieve all deal pipelines from HubSpot",
      supported_actions: ["download", "sync"],
    },
    {
      id: "ticket-pipelines",
      path: "/crm/v3/pipelines/tickets",
      method: "GET",
      description: "Retrieve all ticket pipelines from HubSpot",
      supported_actions: ["download", "sync"],
    },

    // Owners
    {
      id: "owners",
      path: "/crm/v3/owners",
      method: "GET",
      description: "Retrieve all owners (users) in HubSpot",
      supported_actions: ["download", "sync"],
    },
  ],
};

async function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function hubspot(connector: Connector, auth: AuthConfig): AdapterInstance {
    const endpoint = HubSpotAdapter.endpoints.find(e => e.id === connector.endpoint_id);
    if (!endpoint) {
        throw new Error(`Endpoint ${connector.endpoint_id} not found in HubSpot adapter`);
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
                auth.credentials.token_url || 'https://api.hubapi.com/oauth/v1/token',
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
            throw new Error("HubSpot adapter requires OAuth2 authentication");
        }
        if (!auth.credentials.access_token || (auth.expires_at && new Date(auth.expires_at) < new Date())) {
            await refreshOAuthToken();
        }
        return {
            headers: {
                'Authorization': `Bearer ${auth.credentials.access_token}`,
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
        if (connector.fields.length > 0) params.properties = connector.fields.join(',');
        if (connector.filters && connector.filters.length > 0) {
            params.filterGroups = connector.filters.map(filter => {
                if ('op' in filter) {
                    return {
                        filters: (filter as FilterGroup).filters.map((f: Filter | FilterGroup) => {
                            if (!('op' in f)) {
                                return {
                                    propertyName: (f as Filter).field,
                                    operator: mapOperator((f as Filter).operator),
                                    value: (f as Filter).value,
                                };
                            }
                            throw new Error('Nested filter groups are not supported');
                        }),
                    };
                }
                return {
                    filters: [{
                        propertyName: (filter as Filter).field,
                        operator: mapOperator((filter as Filter).operator),
                        value: (filter as Filter).value,
                    }],
                };
            });
        }
        if (connector.sort && connector.sort.length > 0) {
            params.sorts = connector.sort.map(sort => ({
                propertyName: sort.field,
                direction: sort.type === 'asc' ? 'ASCENDING' : 'DESCENDING',
            }));
        }
        return params;
    }

    function mapOperator(operator: string): string {
        const operatorMap: Record<string, string> = {
            '=': 'EQ', '!=': 'NEQ', '>': 'GT', '>=': 'GTE', '<': 'LT', '<=': 'LTE',
            'contains': 'CONTAINS_TOKEN', 'not_contains': 'NOT_CONTAINS_TOKEN',
            'in': 'IN', 'not_in': 'NOT_IN', 'between': 'BETWEEN', 'not_between': 'NOT_BETWEEN',
            'is_null': 'IS_NULL', 'is_not_null': 'NOT_NULL',
        };
        return operatorMap[operator] || operator;
    }

    const maxItemsPerPage = 100;

    const download: AdapterInstance['download'] = async function(pageOptions) {
        const config = await buildRequestConfig();

        const { limit, offset } = pageOptions;

        if (typeof limit === 'undefined') {
            throw new Error('Number of items per page is required by the HubSpot adapter');
        }

        if (limit > maxItemsPerPage) {
            throw new Error('Number of items per page is greater than the maximum allowed by the HubSpot adapter');
        }

        config.params.limit = limit;

        const after = typeof offset !== 'undefined' && offset > 0 ? offset.toString() : undefined;
        if (after) {
            config.params.after = after;
        }

        const response = await axios.get(`${HubSpotAdapter.base_url}${endpoint.path}`, config);
        console.log("API Response:", JSON.stringify(response.data, null, 2));

        const { paging, results } = response.data;

        if (!Array.isArray(results)) {
            console.warn("Results is not an array or is undefined:", response.data);
            return { data: [], options: { nextOffset: paging?.next?.after } };
        }

        let filteredResults;
        if (connector.fields.length > 0) {
            filteredResults = results.map((item: any) => {
                const filteredItem: Record<string, any> = {};
                connector.fields.forEach(field => {
                    if (item.properties && item.properties[field] !== undefined && item.properties[field] !== null) {
                        filteredItem[field] = item.properties[field];
                    }
                });
                console.log("Filtered Result:", JSON.stringify(filteredItem, null, 2));
                return filteredItem;
            });
        } else {
            filteredResults = results;
        }

        return {
            data: filteredResults,
            options: {
                nextOffset: paging?.next?.after ? paging.next.after : undefined,
            },
        };
    }

    const handleDownloadError = (error: any) => {
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';

        if (error.response && typeof error.response.status === 'number') {
            const status = error.response.status;
            console.log('Error status:', status);
            console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
        } else {
            console.error("Download error:", errorMessage);
        }

        return new Error(`Download failed: ${errorMessage}`);
    }

    return {
        paginationType: 'cursor',
        maxItemsPerPage,
        connect: async function(): Promise<void> {
            const config = await buildRequestConfig();
            try {
                console.log("Testing connection to HubSpot...");
                await axios.get(`${HubSpotAdapter.base_url}/crm/v3/objects/contacts`, {
                    ...config,
                    params: { limit: 1, ...config.params },
                });
                console.log("Connection successful");
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                console.error("Connection test failed:", errorMessage);
                throw new Error(`Failed to connect to HubSpot: ${errorMessage}`);
            }
        },

        download: async function(pageOptions) {
            try {
                return await download(pageOptions);
            } catch (error: any) {
                // Check for error with response structure
                if (error.response && typeof error.response.status === 'number') {
                    const status = error.response.status;
                    if (status === 401) {
                        console.log('Error status 401 detected, refreshing token');
                        await refreshOAuthToken();
                        console.log('Token refreshed, retrying');

                        try {
                            return await download(pageOptions);
                        } catch (error) {
                            throw handleDownloadError(error);
                        }
                    } else if (status === 429) {
                        const retryAfter = error.response.headers['retry-after'] ? parseInt(error.response.headers['retry-after'], 10) * 1000 : 1000;
                        console.log(`Rate limit hit, waiting ${retryAfter}ms`);
                        await delay(retryAfter);
                        console.log('Retrying download after delay');

                        try {
                            return await download(pageOptions);
                        } catch (error) {
                            throw handleDownloadError(error);
                        }
                    }
                }

                throw handleDownloadError(error);
            }
        },

        upload: async function(data: any[]): Promise<void> {
            const config = await buildRequestConfig();

            try {
                await axios.post(
                    `${HubSpotAdapter.base_url}${endpoint.path}`,
                    {
                        inputs: data,
                    },
                    config
                );
            } catch (error) {
                let errorMessage;

                if (!(error instanceof Error)) {
                    errorMessage = 'Unknown error';
                } else if (isAxiosError(error) && error.response?.data.message) {
                    errorMessage = error.response?.data.message;

                    error = new Error(errorMessage);
                } else {
                    errorMessage = error.message;
                }

                console.error("Upload error:", errorMessage);
                throw error;
            }
        },
        disconnect: async function(): Promise<void> {
            console.log("Disconnecting from HubSpot adapter (no-op)");
        },
    };
}

export { hubspot, HubSpotAdapter };