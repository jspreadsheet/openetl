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

const maxItemsPerPage = 100;

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
    pagination: {
        type: 'cursor',
        maxItemsPerPage,
    },
    endpoints: [
        { id: "leads", path: "/crm/v3/Leads", method: "GET", description: "Retrieve all leads from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-lead", path: "/crm/v3/Leads", method: "POST", description: "Create a new lead in Zoho CRM", supported_actions: ["upload"] },
        { id: "accounts", path: "/crm/v3/Accounts", method: "GET", description: "Retrieve all accounts from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-account", path: "/crm/v3/Accounts", method: "POST", description: "Create a new account in Zoho CRM", supported_actions: ["upload"] },
        { id: "contacts", path: "/crm/v3/Contacts", method: "GET", description: "Retrieve all contacts from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-contact", path: "/crm/v3/Contacts", method: "POST", description: "Create a new contact in Zoho CRM", supported_actions: ["upload"] },
        { id: "deals", path: "/crm/v3/Deals", method: "GET", description: "Retrieve all deals from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-deal", path: "/crm/v3/Deals", method: "POST", description: "Create a new deal in Zoho CRM", supported_actions: ["upload"] },
        { id: "campaigns", path: "/crm/v3/Campaigns", method: "GET", description: "Retrieve all campaigns from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-campaign", path: "/crm/v3/Campaigns", method: "POST", description: "Create a new campaign in Zoho CRM", supported_actions: ["upload"] },
        { id: "tasks", path: "/crm/v3/Tasks", method: "GET", description: "Retrieve all tasks from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-task", path: "/crm/v3/Tasks", method: "POST", description: "Create a new task in Zoho CRM", supported_actions: ["upload"] },
        { id: "cases", path: "/crm/v3/Cases", method: "GET", description: "Retrieve all cases from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-case", path: "/crm/v3/Cases", method: "POST", description: "Create a new case in Zoho CRM", supported_actions: ["upload"] },
        { id: "events", path: "/crm/v3/Events", method: "GET", description: "Retrieve all events from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-event", path: "/crm/v3/Events", method: "POST", description: "Create a new event in Zoho CRM", supported_actions: ["upload"] },
        { id: "calls", path: "/crm/v3/Calls", method: "GET", description: "Retrieve all calls from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-call", path: "/crm/v3/Calls", method: "POST", description: "Create a new call in Zoho CRM", supported_actions: ["upload"] },
        { id: "solutions", path: "/crm/v3/Solutions", method: "GET", description: "Retrieve all solutions from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-solution", path: "/crm/v3/Solutions", method: "POST", description: "Create a new solution in Zoho CRM", supported_actions: ["upload"] },
        { id: "products", path: "/crm/v3/Products", method: "GET", description: "Retrieve all products from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-product", path: "/crm/v3/Products", method: "POST", description: "Create a new product in Zoho CRM", supported_actions: ["upload"] },
        { id: "vendors", path: "/crm/v3/Vendors", method: "GET", description: "Retrieve all vendors from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-vendor", path: "/crm/v3/Vendors", method: "POST", description: "Create a new vendor in Zoho CRM", supported_actions: ["upload"] },
        { id: "pricebooks", path: "/crm/v3/Price_Books", method: "GET", description: "Retrieve all price books from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-pricebook", path: "/crm/v3/Price_Books", method: "POST", description: "Create a new price book in Zoho CRM", supported_actions: ["upload"] },
        { id: "quotes", path: "/crm/v3/Quotes", method: "GET", description: "Retrieve all quotes from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-quote", path: "/crm/v3/Quotes", method: "POST", description: "Create a new quote in Zoho CRM", supported_actions: ["upload"] },
        { id: "salesorders", path: "/crm/v3/Sales_Orders", method: "GET", description: "Retrieve all sales orders from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-salesorder", path: "/crm/v3/Sales_Orders", method: "POST", description: "Create a new sales order in Zoho CRM", supported_actions: ["upload"] },
        { id: "purchaseorders", path: "/crm/v3/Purchase_Orders", method: "GET", description: "Retrieve all purchase orders from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-purchaseorder", path: "/crm/v3/Purchase_Orders", method: "POST", description: "Create a new purchase order in Zoho CRM", supported_actions: ["upload"] },
        { id: "invoices", path: "/crm/v3/Invoices", method: "GET", description: "Retrieve all invoices from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-invoice", path: "/crm/v3/Invoices", method: "POST", description: "Create a new invoice in Zoho CRM", supported_actions: ["upload"] },
        { id: "appointments", path: "/crm/v3/Appointments", method: "GET", description: "Retrieve all appointments from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-appointment", path: "/crm/v3/Appointments", method: "POST", description: "Create a new appointment in Zoho CRM", supported_actions: ["upload"] },
        { id: "appointments-rescheduled-history", path: "/crm/v3/Appointments__Rescheduled_History", method: "GET", description: "Retrieve appointments rescheduled history from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "services", path: "/crm/v3/Services", method: "GET", description: "Retrieve all services from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "create-service", path: "/crm/v3/Services", method: "POST", description: "Create a new service in Zoho CRM", supported_actions: ["upload"] },
        { id: "activities", path: "/crm/v3/Activities", method: "GET", description: "Retrieve all activities from Zoho CRM", supported_actions: ["download", "sync"] },
        { id: "users", path: "/crm/v3/users", method: "GET", description: "Retrieve all users in Zoho CRM", supported_actions: ["download", "sync"] },
    ],
};

async function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function zoho(connector: Connector, auth: AuthConfig): AdapterInstance {
    const log = function (...args: any) {
        if (connector.debug) {
            console.log(...arguments);
        }
    };
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
        log("Refreshing OAuth token...");
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
            log("Token refreshed successfully");
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

    const download: AdapterInstance['download'] = async function (pageOptions) {
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
        getConfig: () => {
            return ZohoAdapter
        },
        connect: async function (): Promise<void> {
            const config = await buildRequestConfig();
            try {
                log("Testing connection to Zoho...");
                const testPath = endpoint.method === "GET"
                    ? endpoint.path
                    : endpoint.path; // For POST endpoints, we still use the same path but with GET

                const testConfig = {
                    ...config,
                    params: { per_page: 1, ...config.params }, // Minimal request
                };

                if (endpoint.method === "POST") {
                    log(`Endpoint ${endpoint.id} is POST-only; attempting read-only test on ${testPath}`);
                }

                await axios.get(`${ZohoAdapter.base_url}${testPath}`, testConfig);
                log("Connection successful");
            } catch (error) {
                const errorMessage = error instanceof Error ? error.message : 'Unknown error';
                throw new Error(`Failed to connect to Zoho: ${errorMessage}`);
            }
        },

        download: async function (pageOptions) {
            log('inside download..')
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

        upload: async function (data: any[]): Promise<void> {
            log('inside upload..')
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

        disconnect: async function (): Promise<void> {
            log("Disconnecting from Zoho adapter (no-op)");
        },
    };
}

export { zoho, ZohoAdapter };