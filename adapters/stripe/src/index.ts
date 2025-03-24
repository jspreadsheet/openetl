/**
 * Stripe Adapter for OpenETL
 * https://componade.com/openetl
 *
 */

import { HttpAdapter, Connector, AuthConfig, AdapterInstance } from 'openetl';
import axios, { isAxiosError } from 'axios';

const maxItemsPerPage = 100;

const StripeAdapter: HttpAdapter = {
	id: "stripe-adapter",
	name: "Stripe Payments Adapter",
	type: "http",
	action: ["download", "upload", "sync"],
	credential_type: "api_key",
	base_url: "https://api.stripe.com/v1",
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
		{
			id: "payment_intents",
			path: "/payment_intents",
			method: "GET",
			description: "Retrieve all payment intents from Stripe",
			supported_actions: ["download", "sync"],
		},
		{
			id: "create-payment-intent",
			path: "/payment_intents",
			method: "POST",
			description: "Create a new payment intent in Stripe",
			supported_actions: ["upload"],
		},
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
	if (auth.type !== 'api_key' || !auth.credentials.api_key) {
		throw new Error("Stripe adapter requires an API key for authentication");
	}

	const log = (...args: any[]) => {
		if (connector.debug) console.log(...args);
	};

	async function buildRequestConfig(pageOptions?: { limit?: number; offset?: string | number }): Promise<any> {
		if (auth.type !== 'api_key' || !auth.credentials.api_key) {
			throw new Error("Stripe adapter requires an API key for authentication");
		}
		const params = {
			...buildQueryParams(),
			...connector.config?.query_params,
		};
		if (pageOptions?.limit) {
			params.limit = pageOptions.limit;
		}
		if (pageOptions?.offset) {
			if (typeof pageOptions.offset !== 'string' || !pageOptions.offset.match(/^[a-z]{2,}_[A-Za-z0-9]+$/)) {
				throw new Error(`Invalid offset '${pageOptions.offset}' for Stripe pagination; must be a valid Stripe ID`);
			}
			params.starting_after = pageOptions.offset;
		}
		const config = {
			headers: {
				'Authorization': `Bearer ${auth.credentials.api_key}`, // Type-safe after guard
				...connector.config?.headers,
			},
			params,
		};
		log("Request config:", JSON.stringify(config, null, 2));
		return config;
	}

	function buildQueryParams(): Record<string, any> {
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

	const download: AdapterInstance['download'] = async function (pageOptions) {
		if (typeof pageOptions.limit === 'undefined') {
			throw new Error('Number of items per page is required by the Stripe adapter');
		}
		if (pageOptions.limit > maxItemsPerPage) {
			throw new Error('Number of items per page exceeds Stripe maximum');
		}

		console.log('starting a download...')
		console.log(pageOptions)

		const config = await buildRequestConfig(pageOptions);
		try {
			const response = await axios.get(`${StripeAdapter.base_url}${endpoint.path}`, config);
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
			const nextOffset = has_more && data.length > 0 ? data[data.length - 1].id : undefined;

			return {
				data: filteredResults,
				options: { nextOffset }
			};
		} catch (error) {
			throw handleDownloadError(error);
		}
	};

	const handleDownloadError = (error: any) => {
		const errorMessage = error instanceof Error ? error.message : 'Unknown error';
		if (error.response && typeof error.response.status === 'number') {
			console.error("Download error response:", JSON.stringify(error.response.data, null, 2));
		}
		throw new Error(`Download failed: ${errorMessage}`);
	};

	return {
		getConfig: () => {
			return StripeAdapter;
		},
		connect: async function (): Promise<void> {
			const config = await buildRequestConfig();
			try {
				log("Testing connection to Stripe...");
				await axios.get(`${StripeAdapter.base_url}/charges`, {
					...config,
					params: { limit: 1 },
				});
				log("Connection successful");
			} catch (error) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error';
				throw new Error(`Failed to connect to Stripe: ${errorMessage}`);
			}
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

			const config = await buildRequestConfig();
			delete config.params;
			try {
				const formData = new URLSearchParams();
				const item = data[0];
				Object.entries(item).forEach(([key, value]) => {
					formData.append(key, String(value));
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

		disconnect: async function (): Promise<void> {
			log("Disconnecting from Stripe adapter (no-op)");
		},
	};
}

export { stripe, StripeAdapter };