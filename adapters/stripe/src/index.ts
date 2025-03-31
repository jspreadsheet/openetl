/**
 * Stripe Adapter for OpenETL
 * https://componade.com/openetl
 *
 */

import { HttpAdapter, Connector, AuthConfig, AdapterInstance } from 'openetl';
import axios, { isAxiosError } from 'axios';

const maxItemsPerPage = 100;

const StripeAdapter: HttpAdapter = {
	id: "stripe",
	name: "Stripe Payments Adapter",
	category: 'E-commerce & Payment Platforms',
	image: 'https://static.cdnlogo.com/logos/s/83/stripe.svg',
	type: "http",
	action: ["download", "upload", "sync"],
	credential_type: "api_key",
	base_url: "https://api.stripe.com/v1",
	metadata: {
		provider: "stripe",
		description: "Adapter for Stripe Payments API",
		version: "v1",
	},
	pagination: {
		type: 'cursor',
		maxItemsPerPage,
	},
	endpoints: [
		{
			id: "charges",
			path: "/charges/search",
			method: "GET",
			description: "Retrieve all charges from Stripe",
			supported_actions: ["download", "sync"],
			tool: 'stripe_search_charges',
		},
		{
			id: "customers",
			path: "/customers/search",
			method: "GET",
			description: "Retrieve all customers from Stripe",
			supported_actions: ["download", "sync"],
			tool: 'stripe_search_customers',
		},
		{
			id: "create-customer",
			path: "/customers",
			method: "POST",
			description: "Create a new customer in Stripe",
			supported_actions: ["upload"],
			tool: 'stripe_create_customers',
		},
		{
			id: "invoices",
			path: "/invoices/search",
			method: "GET",
			description: "Retrieve all invoices from Stripe",
			supported_actions: ["download", "sync"],
			tool: 'stripe_search_invoices',
		},
		{
			id: "create-invoice",
			path: "/invoices",
			method: "POST",
			description: "Create a new invoice in Stripe",
			supported_actions: ["upload"],
			tool: 'stripe_create_invoices',
		},
		// {
		// 	id: "refunds",
		// 	path: "/refunds",
		// 	method: "GET",
		// 	description: "Retrieve all refunds from Stripe",
		// 	supported_actions: ["download", "sync"],
		// },
		// {
		// 	id: "create-refund",
		// 	path: "/refunds",
		// 	method: "POST",
		// 	description: "Create a new refund in Stripe",
		// 	supported_actions: ["upload"],
		// },
		// {
		// 	id: "payment_intents",
		// 	path: "/payment_intents/search",
		// 	method: "GET",
		// 	description: "Retrieve all payment intents from Stripe",
		// 	supported_actions: ["download", "sync"],
		// },
		// {
		// 	id: "create-payment-intent",
		// 	path: "/payment_intents",
		// 	method: "POST",
		// 	description: "Create a new payment intent in Stripe",
		// 	supported_actions: ["upload"],
		// },
		{
			id: "products",
			path: "/products/search",
			method: "GET",
			description: "Retrieve all products from Stripe",
			supported_actions: ["download", "sync"],
			tool: 'stripe_search_products',
		},
		{
			id: "create-product",
			path: "/products",
			method: "POST",
			description: "Create a new product in Stripe",
			supported_actions: ["upload"],
			tool: 'stripe_create_products',
		},
		// {
		// 	id: "subscriptions",
		// 	path: "/subscriptions/search",
		// 	method: "GET",
		// 	description: "Retrieve all subscriptions from Stripe",
		// 	supported_actions: ["download", "sync"],
		// },
		// {
		// 	id: "create-subscription",
		// 	path: "/subscriptions",
		// 	method: "POST",
		// 	description: "Create a new subscription in Stripe",
		// 	supported_actions: ["upload"],
		// },
		// {
		// 	id: "prices",
		// 	path: "/prices/search",
		// 	method: "GET",
		// 	description: "Retrieve all prices from Stripe",
		// 	supported_actions: ["download", "sync"],
		// },
		// {
		// 	id: "create-price",
		// 	path: "/prices",
		// 	method: "POST",
		// 	description: "Create a new price in Stripe",
		// 	supported_actions: ["upload"],
		// },
	],
};

async function delay(ms: number): Promise<void> {
	return new Promise(resolve => setTimeout(resolve, ms));
}

export function appendObject(
	formData: URLSearchParams,
	propertyValue: any,
	propertyName: string
) {
	if (Array.isArray(propertyValue)) {
		propertyValue.forEach((item, index) => {
			appendObject(formData, item, propertyName + `[${index}]`);
		});
	} else if (typeof propertyValue === "object") {
		if (propertyValue) {
			Object.entries(propertyValue).forEach(([key, value]) => {
				appendObject(formData, value, propertyName + `[${key}]`);
			});
		}
	} else {
		if (propertyValue !== undefined && propertyValue !== null) {
			formData.append(propertyName, String(propertyValue));
		}
	}
}

function stripe(connector: Connector, auth: AuthConfig): AdapterInstance {
	const endpoint = StripeAdapter.endpoints.find(e => e.id === connector.endpoint_id);
	if (!endpoint) {
		throw new Error(`Endpoint ${connector.endpoint_id} not found in Stripe adapter`);
	}
	if (auth.type !== 'api_key' || !auth.credentials.api_key) {
		throw new Error("Stripe adapter requires an API key for authentication");
	}

	const log = (...args: any[]) => {
		if (connector.debug) console.log(...args);
	};

	const getRequestHeaders = function() {
		return {
			'Authorization': `Bearer ${auth.credentials.api_key}`, // Type-safe after guard
			...connector.config?.headers,
			'Stripe-Version': '2025-02-24.acacia',
		}
	}

	function buildDownloadRequestConfig(pageOptions?: { limit?: number; offset?: string | number }) {
		if (auth.type !== 'api_key' || !auth.credentials.api_key) {
			throw new Error("Stripe adapter requires an API key for authentication");
		}

		let isSearchEndpoint = endpoint!.path.endsWith('/search');

		const params = {
			...(isSearchEndpoint ? getSearchQueryParams() : getListQueryParams()),
			...connector.config?.query_params,
		};

		if (!params.query) {
			isSearchEndpoint = false;
		}

		if (pageOptions?.limit) {
			params.limit = pageOptions.limit;
		}

		if (pageOptions?.offset) {
			params[isSearchEndpoint ? 'page' : 'starting_after'] = pageOptions.offset;
		}

		const config = {
			headers: getRequestHeaders(),
			params,
		};
		log("Request config:", JSON.stringify(config, null, 2));

		return {
			isSearchEndpoint,
			config
		};
	}

	const buildUploadRequestConfig = function(pageOptions?: { limit?: number; offset?: string | number }){
		if (auth.type !== 'api_key' || !auth.credentials.api_key) {
			throw new Error("Stripe adapter requires an API key for authentication");
		}

		const config = {
			headers: getRequestHeaders(),
		};
		log("Request config:", JSON.stringify(config, null, 2));

		return config;
	}

	function getListQueryParams(): Record<string, any> {
		const params: Record<string, any> = {};

		if (connector.filters && connector.filters.length > 0) {
			connector.filters.forEach(filter => {
				if ('field' in filter && 'value' in filter) {
					params[filter.field] = filter.value;
				}
			});
		}

		return params;
	}

	function getSearchQueryParams(): Record<string, any> {
		if (connector.filters && connector.filters.length > 0) {
			let queries: string[] = [];

			connector.filters.forEach(filter => {
				if ('field' in filter && 'value' in filter) {
					const value = typeof filter.value === 'string'
						? `"${filter.value.replace(/"/g, '\"')}"`
						: filter.value;

					if (filter.operator === '!=') {
						queries.push(`-${filter.field}:${value}`);
					} else {
						let operator = filter.operator === '=' ? ':' : filter.operator;

						queries.push(filter.field + operator + value);
					}
				}
			});

			if (queries.length > 0) {
				return {
					query: queries.join(' AND '),
				};
			}
		}

		return {};
	}

	const download: AdapterInstance['download'] = async function (pageOptions) {
		if (typeof pageOptions.limit === 'undefined') {
			throw new Error('Number of items per page is required by the Stripe adapter');
		}
		if (pageOptions.limit > maxItemsPerPage) {
			throw new Error('Number of items per page exceeds Stripe maximum');
		}

		const { config, isSearchEndpoint } = buildDownloadRequestConfig(pageOptions);

		try {
			let url = `${StripeAdapter.base_url}${endpoint.path}`;

			if (url.includes('/search') && !isSearchEndpoint) {
				url = url.replace('/search', '');
			}

			const response = await axios.get(url, config);
			const { data, has_more } = response.data;

			log("API Response:", JSON.stringify(response.data, null, 2));

			if (!Array.isArray(data)) {
				console.warn("Data is not an array or is undefined:", response.data);
				return { data: [], options: { nextOffset: undefined } };
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

			// Set nextOffset for cursor pagination
			let nextOffset;
			if (has_more) {
				if (isSearchEndpoint) {
					nextOffset = response.data.next_page;
				} else if (data.length > 0) {
					nextOffset = data[data.length - 1].id;
				}
			}

			return {
				data: filteredResults,
				options: { nextOffset }
			};
		} catch (error) {
			throw handleDownloadError(error);
		}
	};

	const handleDownloadError = (error: any) => {
		let errorMessage;

		if (isAxiosError(error)) {
			errorMessage = error.response?.data?.error?.message || 'Unknown error';
		} else {
			errorMessage = error instanceof Error ? error.message : 'Unknown error';
		}

		if (error.response && typeof error.response.status === 'number') {
			console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
		}

		throw new Error(`Download failed: ${errorMessage}`);
	};

	return {
		getConfig: () => {
			return StripeAdapter;
		},

		download: async function (pageOptions) {
			if (!endpoint.supported_actions.includes('download')) {
				throw new Error(`${endpoint.id} endpoint doesn't support download`);
			}

			try {
				return await download(pageOptions);
			} catch (error: any) {
				if (error.response?.status === 401) {
					throw new Error("Invalid API key; please check your credentials");
				} else if (error.response?.status === 429) {
					const retryAfter = error.response.headers['retry-after']
						? parseInt(error.response.headers['retry-after'], 10) * 1000
						: 1000;
					log(`Rate limit hit, waiting ${retryAfter}ms`);
					await delay(retryAfter);
					return await download(pageOptions);
				}
				throw handleDownloadError(error);
			}
		},

		upload: async function (data: any[]): Promise<void> {
			if (!endpoint.supported_actions.includes('upload')) {
				throw new Error(`${endpoint.id} endpoint doesn't support upload`);
			}
			if (data.length !== 1) {
				throw new Error('Stripe adapter only supports uploading one product at a time');
			}

			const config = buildUploadRequestConfig();
			try {
				const formData = new URLSearchParams();
				const item = data[0];

				Object.entries(item).forEach(([key, value]) => {
					const valueType = typeof value;

					if (valueType === "object") {
						appendObject(formData, value, key);
					} else if (valueType === "string") {
						formData.append(key, value as string);
					} else if (valueType === "number" || valueType === "boolean") {
						formData.append(key, String(value));
					}
				});

				const response = await axios.post(
					`${StripeAdapter.base_url}${endpoint.path}`,
					formData.toString(),
					{
						...config,
						headers: {
							...config.headers,
							'Content-Type': 'application/x-www-form-urlencoded',
						},
					}
				);
				log("Upload successful:", JSON.stringify(response.data, null, 2));
			} catch (error) {
				const errorMessage = isAxiosError(error) && error.response?.data?.error?.message
					? error.response.data.error.message
					: error instanceof Error ? error.message : 'Unknown error';
				console.error("Upload error:", errorMessage);
				throw new Error(`Upload failed: ${errorMessage}`);
			}
		},
	};
}

export { stripe, StripeAdapter };