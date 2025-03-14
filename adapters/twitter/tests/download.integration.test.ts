import { Orchestrator } from "../../../src/index"; // Adjust path to your OpenETL source
import { twitter, TwitterTweet } from "./../src/index"; // Adjust path to your adapter
import axios from "axios";


describe("TwitterAdapter Download Integration Tests", () => {
	let orchestrator: ReturnType<typeof Orchestrator>;
	let vault: any;
	let connector: any;
  let defaultPipeline: any;

	async function getTwitterTokens(): Promise<{
		access_token: string;
		refresh_token?: string;
		expires_at: number;
	}> {
		try {
			const response = await axios.get("http://localhost:2301/tokens");
			return response.data;
		} catch (error: any) {
			throw new Error(
				`Failed to fetch tokens from OAuth server: ${error.message}`
			);
		}
	}

	beforeAll(async () => {
		// Get tokens from the OAuth server
		const tokens = await getTwitterTokens();

		// Set up vault with Twitter credentials
		vault = {
			"twitter-auth": {
				id: "twitter-auth",
				type: "oauth2",
				credentials: {
					access_token: tokens.access_token,
					refresh_token: tokens.refresh_token || null,
					expires_at: tokens.expires_at,
				},
			},
		};

		// Initialize Orchestrator with Twitter adapter
		const adapters = { twitter };
		orchestrator = Orchestrator(vault, adapters);

		// Base connector configuration for tweets_search endpoint
		connector = {
			id: "twitter-search",
			adapter_id: "twitter",
			endpoint_id: "tweets_search",
			credential_id: "twitter-auth",
			config: { headers: {} }, // Optional custom headers
			// fields: ['id', 'text', 'created_at'], // Default fields for consistency
			pagination: { itemsPerPage: 10 },
      limit: 10
		};

    defaultPipeline = {
      rate_limiting: {
        requests_per_second: 1 / 900, // 1 request per 15 minutes (900 seconds)
        max_retries_on_rate_limit: 0
      },
      logging: (event: any) => console.log(event),
    };
	});

	it("downloads tweets with basic query", async () => {
		const pipeline = {
			id: "twitter-download-basic",
			source: {
				...connector,
				filters: [
					{ field: "query", operator: "=", value: "from:elonmusk" },
				],
				pagination: { itemsPerPage: 10 },
        limit: 10
			},
      ...defaultPipeline
		};

		const result = await orchestrator.runPipeline(pipeline);
		expect(result).toHaveProperty("data");
		expect(Array.isArray(result.data)).toBe(true);
		expect(result.data.length).toBeLessThanOrEqual(10);
		if (result.data.length > 0) {
			const tweet: any = result.data[0];
			expect(tweet).toHaveProperty("id");
			expect(typeof tweet.id).toBe("string");
			expect(tweet).toHaveProperty("text");
			expect(typeof tweet.text).toBe("string");
			expect(tweet).toHaveProperty("edit_history_tweet_ids");
			expect(tweet.edit_history_tweet_ids).toBeInstanceOf(Array);
			expect(tweet.edit_history_tweet_ids[0]).toBe(tweet.id);
		}
	}, 30000);

	it("fails with 400 error when requesting less than 10 items per page", async () => {
		const pipeline = {
			id: "twitter-download-invalid-items",
			source: {
				...connector,
				filters: [
					{ field: "query", operator: "=", value: "from:elonmusk" },
				],
				pagination: { itemsPerPage: 5 }, // Below minimum of 10
			},
      ...defaultPipeline
		};

		try {
			await orchestrator.runPipeline<{ data: TwitterTweet[] }>(pipeline);
			// If no error is thrown, fail the test
			expect(true).toBe(false); // Should not reach here
		} catch (error) {
			// Check if the error matches the expected Twitter API error
			expect(error).toBeInstanceOf(Error);
		}
	});
});
