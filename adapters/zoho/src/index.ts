/**
 * Zoho CRM Adapter for OpenETL
 * https://componade.com/openetl
 *
 * @TODO:
 * - Add batch upload support (/crm/v3/{module}/upsert)
 * - Implement rate limit handling based on Zoho's limits (varies by plan)
 */

import { HttpAdapter, Connector, AuthConfig, OAuth2Auth, AdapterInstance } from 'openetl';
import axios, { isAxiosError } from 'axios';

const maxItemsPerPageforGetRoutes = 200;
const maxItemsPerPageforPostRoutes = 100;

const appLocationMap: Record<string, string | undefined> = {
    "eu": "https://accounts.zoho.eu",
    "ae": "https://accounts.zoho.ae",
    "au": "https://accounts.zoho.com.au",
    "in": "https://accounts.zoho.in",
    "jp": "https://accounts.zoho.jp",
    "uk": "https://accounts.zoho.uk",
    "us": "https://accounts.zoho.com",
    "ca": "https://accounts.zohocloud.ca",
    "sa": "https://accounts.zoho.sa",
};

const ZohoAdapter: HttpAdapter = {
    id: "zoho",
    name: "Zoho CRM Adapter",
    type: "http",
    category: 'SaaS & CRM Applications',
    image: 'https://static.cdnlogo.com/logos/z/54/zoho.svg',
    action: ["download", "upload", "sync"],
    credential_type: "oauth2",
    base_url: "https://www.zohoapis.com",
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
        provider: "zoho",
        description: "Adapter for Zoho CRM API",
        version: "v7",
    },
    pagination: {
        type: 'cursor',
        maxItemsPerPage: maxItemsPerPageforGetRoutes,
    },
    endpoints: [
        {
            id: "leads",
            path: "/crm/v7/Leads/search",
            method: "GET",
            description: "Retrieve leads from Zoho CRM",
            supported_actions: ["download", "sync"],
            tool: "zoho_search_leads",
            defaultFields: [
                'First_Name',
                'Last_Name',
                'Company',
                'Description',
                'Email',
                'Phone',
                'Mobile',
                'Created_Time'
            ]
        },
        {
            id: "create-lead",
            path: "/crm/v7/Leads",
            method: "POST",
            description: "Create new leads in Zoho CRM",
            supported_actions: ["upload"],
            tool: "zoho_create_leads",
            settings: {
                pagination: {
                    type: 'offset',
                    maxItemsPerPage: maxItemsPerPageforPostRoutes,
                }
            }
        },
        // { id: "accounts", path: "/crm/v3/Accounts", method: "GET", description: "Retrieve all accounts from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-account", path: "/crm/v3/Accounts", method: "POST", description: "Create a new account in Zoho CRM", supported_actions: ["upload"] },
        {
            id: "contacts",
            path: "/crm/v7/Contacts/search",
            method: "GET",
            description: "Retrieve contacts from Zoho CRM",
            supported_actions: ["download", "sync"],
            tool: "zoho_search_contacts",
            defaultFields: [
                'First_Name',
                'Last_Name',
                'Description',
                'Email',
                'Phone',
                'Mobile',
                'Created_Time',
            ]
        },
        {
            id: "create-contact",
            path: "/crm/v7/Contacts",
            method: "POST",
            description: "Create new contacts in Zoho CRM",
            supported_actions: ["upload"],
            tool: "zoho_create_contacts",
            settings: {
                pagination: {
                    type: 'offset',
                    maxItemsPerPage: maxItemsPerPageforPostRoutes,
                }
            }
        },
        {
            id: "deals",
            path: "/crm/v7/Deals/search",
            method: "GET",
            description: "Retrieve deals from Zoho CRM",
            supported_actions: ["download", "sync"],
            tool: "zoho_search_deals",
            defaultFields: [
                'Deal_Name',
                'Contact_Name',
                'Description',
                'Type',
                'Amount',
                'Stage',
                'Closing_Date',
            ]
        },
        {
            id: "create-deal",
            path: "/crm/v7/Deals",
            method: "POST",
            description: "Create new deals in Zoho CRM",
            supported_actions: ["upload"],
            tool: "zoho_create_deals",
            settings: {
                pagination: {
                    type: 'offset',
                    maxItemsPerPage: maxItemsPerPageforPostRoutes,
                }
            }
        },
        {
            id: "campaigns",
            path: "/crm/v7/Campaigns/search",
            method: "GET",
            description: "Retrieve campaigns from Zoho CRM",
            supported_actions: ["download", "sync"],
            tool: "zoho_search_campaigns",
            defaultFields: [
                'Campaign_Name',
                'Type',
                'Description',
                'Status',
                'Start_Date',
                'End_Date',
                'Expected',
                'Actual_Cost',
                'Budgeted_Cost',
                'Expected_Response',
                'Num_sent'
            ]
        },
        {
            id: "create-campaign",
            path: "/crm/v7/Campaigns",
            method: "POST",
            description: "Create new campaigns in Zoho CRM",
            supported_actions: ["upload"],
            tool: "zoho_create_campaigns",
            settings: {
                pagination: {
                    type: 'offset',
                    maxItemsPerPage: maxItemsPerPageforPostRoutes,
                }
            }
        },
        // { id: "tasks", path: "/crm/v3/Tasks", method: "GET", description: "Retrieve all tasks from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-task", path: "/crm/v3/Tasks", method: "POST", description: "Create a new task in Zoho CRM", supported_actions: ["upload"] },
        // { id: "cases", path: "/crm/v3/Cases", method: "GET", description: "Retrieve all cases from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-case", path: "/crm/v3/Cases", method: "POST", description: "Create a new case in Zoho CRM", supported_actions: ["upload"] },
        // { id: "events", path: "/crm/v3/Events", method: "GET", description: "Retrieve all events from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-event", path: "/crm/v3/Events", method: "POST", description: "Create a new event in Zoho CRM", supported_actions: ["upload"] },
        // { id: "calls", path: "/crm/v3/Calls", method: "GET", description: "Retrieve all calls from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-call", path: "/crm/v3/Calls", method: "POST", description: "Create a new call in Zoho CRM", supported_actions: ["upload"] },
        // { id: "solutions", path: "/crm/v3/Solutions", method: "GET", description: "Retrieve all solutions from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-solution", path: "/crm/v3/Solutions", method: "POST", description: "Create a new solution in Zoho CRM", supported_actions: ["upload"] },
        // { id: "products", path: "/crm/v3/Products", method: "GET", description: "Retrieve all products from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-product", path: "/crm/v3/Products", method: "POST", description: "Create a new product in Zoho CRM", supported_actions: ["upload"] },
        // { id: "vendors", path: "/crm/v3/Vendors", method: "GET", description: "Retrieve all vendors from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-vendor", path: "/crm/v3/Vendors", method: "POST", description: "Create a new vendor in Zoho CRM", supported_actions: ["upload"] },
        // { id: "pricebooks", path: "/crm/v3/Price_Books", method: "GET", description: "Retrieve all price books from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-pricebook", path: "/crm/v3/Price_Books", method: "POST", description: "Create a new price book in Zoho CRM", supported_actions: ["upload"] },
        // { id: "quotes", path: "/crm/v3/Quotes", method: "GET", description: "Retrieve all quotes from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-quote", path: "/crm/v3/Quotes", method: "POST", description: "Create a new quote in Zoho CRM", supported_actions: ["upload"] },
        // { id: "salesorders", path: "/crm/v3/Sales_Orders", method: "GET", description: "Retrieve all sales orders from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-salesorder", path: "/crm/v3/Sales_Orders", method: "POST", description: "Create a new sales order in Zoho CRM", supported_actions: ["upload"] },
        // { id: "purchaseorders", path: "/crm/v3/Purchase_Orders", method: "GET", description: "Retrieve all purchase orders from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-purchaseorder", path: "/crm/v3/Purchase_Orders", method: "POST", description: "Create a new purchase order in Zoho CRM", supported_actions: ["upload"] },
        // { id: "invoices", path: "/crm/v3/Invoices", method: "GET", description: "Retrieve all invoices from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-invoice", path: "/crm/v3/Invoices", method: "POST", description: "Create a new invoice in Zoho CRM", supported_actions: ["upload"] },
        // { id: "appointments", path: "/crm/v3/Appointments", method: "GET", description: "Retrieve all appointments from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-appointment", path: "/crm/v3/Appointments", method: "POST", description: "Create a new appointment in Zoho CRM", supported_actions: ["upload"] },
        // { id: "appointments-rescheduled-history", path: "/crm/v3/Appointments__Rescheduled_History", method: "GET", description: "Retrieve appointments rescheduled history from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "services", path: "/crm/v3/Services", method: "GET", description: "Retrieve all services from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "create-service", path: "/crm/v3/Services", method: "POST", description: "Create a new service in Zoho CRM", supported_actions: ["upload"] },
        // { id: "activities", path: "/crm/v3/Activities", method: "GET", description: "Retrieve all activities from Zoho CRM", supported_actions: ["download", "sync"] },
        // { id: "users", path: "/crm/v3/users", method: "GET", description: "Retrieve all users in Zoho CRM", supported_actions: ["download", "sync"] },
    ],
    helpers: {
        getCode: function(redirectUrl: string, client_id: string) {
            const searchParams = new URLSearchParams({
                client_id: client_id,
                redirect_uri: redirectUrl,
                response_type: 'code',
                scope: 'ZohoSearch.securesearch.READ,ZohoCRM.modules.contacts.ALL,ZohoCRM.modules.leads.ALL,ZohoCRM.modules.deals.ALL,ZohoCRM.modules.campaigns.ALL,ZohoCRM.settings.fields.READ,ZohoCRM.settings.custom_views.READ',
                access_type: 'offline',
            });

            return `https://accounts.zoho.com/oauth/v2/auth?` + searchParams.toString();
        },
        getTokens: async function(redirectUrl: string, client_id: string, secret_id: string, queryParams: string) {
            const params = new URLSearchParams(queryParams);
            const code = params.get('code');
            const location = params.get('location');

            if (!code) {
                throw new Error('Invalid code');
            }

            if (!location) {
                throw new Error('Invalid location');
            }

            let url = appLocationMap[location];

            if (!url) {
                throw new Error('Invalid location');
            }

            url += '/oauth/v2/token';

            try {
                const tokenResponse = await axios.post(url, undefined, {
                    params: {
                        client_id: client_id,
                        grant_type: 'authorization_code',
                        client_secret: secret_id,
                        redirect_uri: redirectUrl,
                        code: code
                    }
                });

                return tokenResponse.data;
            } catch (error) {
                if (isAxiosError(error)) {
                    return error.response?.data.message;
                } else {
                    return error instanceof Error ? error.message : 'Unknown error';
                }
            }
        }
    }
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
                'https://accounts.zoho.com/oauth/v2/token',
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
        if (!auth.credentials.access_token || !auth.expires_at || new Date(auth.expires_at) < new Date()) {
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

    const download: AdapterInstance['download'] = async function (pageOptions) {
        const config = await buildRequestConfig();
        const { limit, offset } = pageOptions;

        if (typeof limit === 'undefined') {
            throw new Error('Number of items per page is required by the Zoho adapter');
        }
        if (limit > maxItemsPerPageforGetRoutes) {
            throw new Error(`Number of items per page exceeds the maximum number allowed for the ${endpoint.id} endpoint of the Zoho adapter (${maxItemsPerPageforGetRoutes})`);
        }

        config.params.per_page = limit;
        const page =  offset ? Math.floor(Number(offset)/limit) + 1 : 1;

        if (page) {
            config.params.page = page;
        }

        let url = ZohoAdapter.base_url + endpoint.path;
        if (!config.params.criteria && url.endsWith('/search')) {
            url = url.replace(/\/search$/, '');
        }

        const response = await axios.get(url, config);
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
        let errorMessage;

        if (isAxiosError(error) && error.response?.data?.message) {
            errorMessage = error.response.data.message;

            if (errorMessage === 'One of the expected parameter is missing') {
                errorMessage += ': ' + error.response.data.details.param_name;
            }
        } else {
            errorMessage = error instanceof Error ? error.message : 'Unknown error';
        }

        return new Error(`Download failed: ${errorMessage}`);
    }

    return {
        getConfig: () => {
            return ZohoAdapter
        },
        download: async function (pageOptions) {
            if (!endpoint.supported_actions.includes('download')) {
                throw new Error(`${endpoint.id} endpoint don't support download`);
            }

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
            if (!endpoint.supported_actions.includes('upload')) {
                throw new Error(`${endpoint.id} endpoint don't support upload`);
            }

            if (data.length > maxItemsPerPageforPostRoutes) {
                throw new Error(`Number of items per page (${data.length}), exceeds the maximum number allowed for the ${endpoint.id} endpoint of the Zoho adapter (${maxItemsPerPageforPostRoutes})`);
            }

            log('inside upload..')
            
            const config = await buildRequestConfig();
            try {
                await axios.post(
                    `${ZohoAdapter.base_url}${endpoint.path}`,
                    { data },
                    config
                );
            } catch (error) {
                let errorMessage;

                if (
                    isAxiosError(error) &&
                    error.response?.data?.data &&
                    error.response?.data?.data[0] &&
                    error.response?.data?.data[0].message
                ) {
                    errorMessage = error.response.data.data[0].message;

                    if (errorMessage === 'required field not found') {
                        errorMessage += ': ' + error.response.data.data[0].details.api_name;
                    }
                } else {
                    errorMessage = error instanceof Error ? error.message : 'Unknown error'
                }

                console.error("Upload error:", errorMessage);
                throw new Error(`Upload failed: ${errorMessage}`);
            }
        },
    };
}

export { zoho, ZohoAdapter };