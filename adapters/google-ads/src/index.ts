/**
 * Google Ads Adapter for OpenETL
 * https://componade.com/openetl
 */

import axios from 'axios';
import jsuites from 'jsuites';

import { DatabaseAdapter, Connector, AdapterInstance, AuthConfig, Filter, FilterGroup, OAuth2Auth } from 'openetl';

const baseUrl = "https://googleads.googleapis.com/v19";
const customerId = '9993315554';
const loginCustomerId = '5401287074';
const developerToken = 'SCnYdMFo64q7kqBuWF4zjA';

const GoogleAdsAdapter: DatabaseAdapter = {
  id: "google-ads",
  name: "Google Ads API Adapter",
  type: "database",
  action: ["download"],
  credential_type: "basic",
  metadata: {
    provider: "google-ads",
    description: "Adapter for Google Ads API operations",
    version: "1.0",
  },
  endpoints: [
    { id: "table_query", query_type: "table", description: "Query a specific table", supported_actions: ["download"] },
    { id: "custom_query", query_type: "custom", description: "Run a custom query", supported_actions: ["download"] },
  ],
};

function googleAds(connector: Connector, auth: AuthConfig): AdapterInstance {
  const endpoint = GoogleAdsAdapter.endpoints.find(e => e.id === connector.endpoint_id)!;
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in Google Ads API adapter`);
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

    try {
        // Obtém um access token usando o refresh token
        const response = await axios.post('https://oauth2.googleapis.com/token', {
            client_id: auth.credentials.client_id,
            client_secret: auth.credentials.client_secret,
            refresh_token: auth.credentials.refresh_token,
            grant_type: 'refresh_token'
        });

        auth.credentials.access_token = response.data.access_token;
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
          throw new Error("Google Ads adapter requires OAuth2 authentication");
      }

      if (!auth.credentials.access_token || (auth.expires_at && new Date(auth.expires_at) < new Date())) {
          await refreshOAuthToken();
      }

      return {
          headers: {
              'Authorization': `Bearer ${auth.credentials.access_token}`,
              'Content-Type': 'application/json',
              'developer-token': developerToken,
              'login-customer-id': loginCustomerId,
              ...connector.config?.headers,
          },
          params: {
              ...connector.config?.query_params,
          },
      };
  }

  function isFilter(filter: Filter | FilterGroup): filter is Filter {
    return 'field' in filter && 'operator' in filter && 'value' in filter;
  }

  function buildSelectQuery(customLimit?: number): string {
    if (endpoint.id === "custom_query" && connector.config?.custom_query) {
      return connector.config.custom_query;
    }

    if (!connector.config?.table) {
      throw new Error("Table required for table-based endpoints");
    }

    const parts: string[] = [];

    // SELECT clause
    parts.push(`SELECT ${connector.fields.length > 0 ? connector.fields.join(', ') : '*'}`);

    // FROM clause
    parts.push(`FROM ${connector.config.table}`);

    // WHERE clause
    if (connector.filters && connector.filters.length > 0) {
      const whereClauses = connector.filters.map(filter => {
        if (!isFilter(filter)) {
          const subClauses = filter.filters.map(f =>
              isFilter(f) ? `${f.field} ${f.operator} '${f.value}'` : ''
          );
          return `(${subClauses.join(` ${filter.op} `)})`;
        }
        return `${filter.field} ${filter.operator} '${filter.value}'`;
      });
      parts.push(`WHERE ${whereClauses.join(' AND ')}`);
    }

    // ORDER BY clause
    if (connector.sort && connector.sort.length > 0) {
      const orderBy = connector.sort
          .map(sort => `${sort.field} ${sort.type.toUpperCase()}`)
          .join(', ');
      parts.push(`ORDER BY ${orderBy}`);
    }

    // LIMIT and OFFSET
    if (customLimit !== undefined) {
      parts.push(`LIMIT ${customLimit}`);
    }

    return parts.join(' ');
  }

  return {
    download: async function(pageOptions) {
      if (endpoint.id === "table_insert") {
        throw new Error("Table_insert endpoint only supported for upload");
      }

      const config = await buildRequestConfig();

      const query = buildSelectQuery(pageOptions.limit);

      try {
        const response = await axios.post(
          `${baseUrl}/customers/${customerId}/googleAds:search`,
          {
            query
          },
          config
        );
        console.log("API Response:", JSON.stringify(response.data, null, 2));

        const { results } = response.data;

        if (!Array.isArray(results)) {
            console.warn("Results is not an array or is undefined:", response.data);
            return { data: [] };
        }

        let filteredResults;
        if (connector.fields.length > 0) {
            filteredResults = results.map((item: any) => {
                const filteredItem: Record<string, any> = {};
                connector.fields.forEach(field => {
                    if (item) {
                        const value = jsuites.path.call(item, field);

                        if (value !== undefined && value !== null) {
                          jsuites.path.call(filteredItem, field, value);
                        }
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
        };
      } catch (error: any) {
        // Check for error with response structure
        if (error.response && typeof error.response.status === 'number') {
            const status = error.response.status;
            console.log('Error status:', status);
            if (status === 401) {
                console.log('401 detected, refreshing token');
                await refreshOAuthToken();
                console.log('Token refreshed, retrying');
                return this.download(pageOptions);
            }
            console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
        }
        const errorMessage = error instanceof Error ? error.message : 'Unknown error';
        console.error("Download error:", errorMessage);
        throw new Error(`Download failed: ${errorMessage}`);
      }
    },
  };
}

export { googleAds, GoogleAdsAdapter };