/**
 * Gmail Adapter for OpenETL
 * https://componade.com/openetl
 *
 * @TODO:
 * - Add support for batch requests to improve performance (e.g., batch message retrieval).
 * - Implement label management endpoints.
 */

import { HttpAdapter, Connector, AuthConfig, OAuth2Auth, AdapterInstance, Filter, FilterGroup } from 'openetl';
import axios, { isAxiosError } from 'axios';

const GmailAdapter: HttpAdapter = {
  id: "gmail-adapter",
  name: "Gmail API Adapter",
  type: "http",
  action: ["download", "upload", "sync"],
  credential_type: "oauth2",
  base_url: "https://gmail.googleapis.com/gmail/v1",
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
    provider: "google",
    description: "Adapter for Gmail API to manage emails and threads",
    version: "v1",
  },
  endpoints: [
    {
      id: "list-threads",
      path: "/users/me/threads",
      method: "GET",
      description: "Retrieve all email threads from Gmail",
      supported_actions: ["download", "sync"],
      settings: {
        pagination: {
          type: 'cursor',
          maxItemsPerPage: 500,
        }
      }
    },
    {
      id: "list-messages",
      path: "/users/me/messages",
      method: "GET",
      description: "Retrieve email messages from Gmail",
      supported_actions: ["download", "sync"],
      settings: {
        pagination: {
          type: 'cursor',
          maxItemsPerPage: 500,
        }
      }
    },
    {
      id: "get-message",
      path: "/users/me/messages",
      method: "GET",
      description: "Retrieve one email message from Gmail",
      supported_actions: ["download"],
      settings: {
        pagination: false,
      }
    },
    {
      id: "send-message",
      path: "/users/me/messages/send",
      method: "POST",
      description: "Send a new email via Gmail",
      supported_actions: ["upload"],
      settings: {
        pagination: {
          type: 'offset',
          maxItemsPerPage: 1,
        }
      }
    },
    {
      id: "list-labels",
      path: "/users/me/labels",
      method: "GET",
      description: "Retrieve all labels from Gmail",
      supported_actions: ["download", "sync"],
    },
  ],
};

const reponseTargetPropertyMap: Record<string, string> = {
  'list-threads': 'threads',
  'list-messages': 'messages',
  'list-labels': 'labels',
};

function gmail(connector: Connector, auth: AuthConfig): AdapterInstance {
  const log = function (...args: any[]) {
    if (connector.debug) {
      console.log(...arguments);
    }
  };

  const endpoint = GmailAdapter.endpoints.find(e => e.id === connector.endpoint_id);
  if (!endpoint) {
    throw new Error(`Endpoint ${connector.endpoint_id} not found in Gmail adapter`);
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
        auth.credentials.token_url || 'https://oauth2.googleapis.com/token',
        new URLSearchParams({
          grant_type: 'refresh_token',
          client_id: auth.credentials.client_id,
          client_secret: auth.credentials.client_secret,
          refresh_token: auth.credentials.refresh_token,
        }).toString(),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } }
      );
      auth.credentials.access_token = response.data.access_token;
      auth.expires_at = new Date(Date.now() + response.data.expires_in * 1000).toISOString();
      log("Token refreshed successfully");
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      console.error("Token refresh failed:", errorMessage);
      throw new Error(`OAuth token refresh failed: ${errorMessage}`);
    }
  }

  async function buildRequestConfig(isPost?: boolean): Promise<any> {
    if (!isOAuth2Auth(auth)) {
      throw new Error("Gmail adapter requires OAuth2 authentication");
    }
    if (!auth.credentials.access_token || (auth.expires_at && new Date(auth.expires_at) < new Date())) {
      await refreshOAuthToken();
    }

    const params = !isPost
    ? {
      ...buildQueryParams(),
    } : {};

    return {
      headers: {
        'Authorization': `Bearer ${auth.credentials.access_token}`,
        'Content-Type': 'application/json',
        ...connector.config?.headers,
      },
      params: {
        ...params,
        ...connector.config?.query_params,
      },
    };
  }

  function buildQueryParams(): Record<string, any> {
    const params: Record<string, any> = {};

    if (connector.endpoint_id === 'get-message') {
      connector.filters?.forEach((filter) => {
        if ('op' in filter) {
          throw new Error('Filter groups are not supported in Gmail API; use flat filters');
        }

        if (filter.operator === '=') {
          params[filter.field] = filter.value;
        }
      });
    } else if (connector.endpoint_id === 'list-messages') {
      if (connector.filters && connector.filters.length > 0) {
        let q = '';

        connector.filters.forEach(filter => {
          if ('op' in filter) {
            throw new Error('Filter groups are not supported in Gmail API; use flat filters');
          }

          const f = filter as Filter;

          if (['labelIds', 'includeSpamTrash'].includes(f.field) && f.operator === '=') {
            params[f.field] = f.value;
          } else {
            if (q) {
              q += ' ';
            }

            q += `${f.operator === '=' ? '' : '-'}${f.field}:${f.value}`;
          }
        });

        if (q) {
          params.q = q;
        }
      }
    }

    return params;
  }

  const download: AdapterInstance['download'] = async function (pageOptions) {
    const config = await buildRequestConfig();
    const { limit, offset } = pageOptions;

    if (endpoint.id === "list-threads" || endpoint.id === "list-messages") {
      if (typeof limit === 'undefined') {
        throw new Error(`Number of items per page is required by the ${endpoint.id} endpoint of the Gmail adapter`);
      }

      config.params.maxResults = limit;

      const pageToken = typeof offset !== 'undefined' && offset !== 0 && offset !== '0' ? offset.toString() : undefined;
      if (pageToken) {
        config.params.pageToken = pageToken;
      }
    }

    let url = GmailAdapter.base_url + endpoint.path;

    if (endpoint.id === 'get-message') {
        if (!connector.config?.id) {
          throw new Error('get-message endpoint requires a filter specifying the message id');
        }

        url += `/${connector.config?.id}`;
    }

    const response = await axios.get(url, config);
    log("API Response:", JSON.stringify(response.data, null, 2));

    let results

    if (reponseTargetPropertyMap[endpoint.id]) {
      results = response.data[reponseTargetPropertyMap[endpoint.id]] || [];
    } else {
      results = [response.data];
    }

    if (!Array.isArray(results)) {
      console.warn("Results is not an array or is undefined:", response.data);
      return { data: [], options: { nextOffset: response.data.nextPageToken } };
    }

    if (connector.fields.length > 0) {
      results = results.map((item: any) => {
        const filteredItem: Record<string, any> = {};
        connector.fields.forEach(field => {
            if (typeof item[field] !== 'undefined' && item[field] !== null) {
                filteredItem[field] = item[field];
            }
        });
        log("Filtered Result:", JSON.stringify(filteredItem, null, 2));
        return filteredItem;
      });
    }

    return {
      data: results,
      options: {
        nextOffset: response.data.nextPageToken ? response.data.nextPageToken : undefined,
      },
    };
  };

  const handleDownloadError = (error: any) => {
    const errorMessage = error instanceof Error ? error.message : 'Unknown error';
    if (error.response && typeof error.response.status === 'number') {
      const status = error.response.status;
      log('Error status:', status);
      console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
    } else {
      console.error("Download error:", errorMessage);
    }
    return new Error(`Download failed: ${errorMessage}`);
  };

  return {
    getConfig: function() {
      return GmailAdapter;
    },
    download: async function (pageOptions) {
      if (!endpoint.supported_actions.includes('download')) {
        throw new Error(`${endpoint.id} endpoint don't support download`);
      }

      try {
        return await download(pageOptions);
      } catch (error: any) {
        if (error.response && typeof error.response.status === 'number') {
          const status = error.response.status;
          if (status === 401) {
            log('Error status 401 detected, refreshing token');
            await refreshOAuthToken();
            log('Token refreshed, retrying');
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

    upload: async function (data: any[]): Promise<void> {
      if (!endpoint.supported_actions.includes('upload')) {
        throw new Error(`${endpoint.id} endpoint don't support upload`);
      }

      const config = await buildRequestConfig(true);

      for (const item of data) {
        const { to, subject, body } = item;
        const rawEmail = Buffer.from(
          `To: ${to}\r\nSubject: ${subject}\r\n\r\n${body}`
        ).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');

        try {
          await axios.post(
            `${GmailAdapter.base_url}${endpoint.path}`,
            { raw: rawEmail },
            config
          );
          log("Email sent successfully:", { to, subject });
        } catch (error) {
          let errorMessage;
          if (!(error instanceof Error)) {
            errorMessage = 'Unknown error';
          } else if (isAxiosError(error) && error.response?.data?.error?.message) {
            errorMessage = error.response.data.error.message;
            error = new Error(errorMessage);
          } else {
            errorMessage = error.message;
          }
          console.error("Upload error:", errorMessage);
          throw error;
        }
      }
    },
  };
}

export { gmail, GmailAdapter };