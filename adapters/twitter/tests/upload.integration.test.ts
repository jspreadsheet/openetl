import { AdapterInstance, AuthConfig, Connector, Orchestrator } from '../../../src/index'; // Adjust path to your OpenETL source
import axios from 'axios';
import { twitter } from './../src/index';

describe('TwitterAdapter Upload Integration Tests', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let connector: any;
  let pipeline: any;

  async function getTwitterTokens(): Promise<{ access_token: string; refresh_token?: string; expires_at: number }> {
    try {
      const response = await axios.get('http://localhost:2301/tokens');
      return response.data;
    } catch (error: any) {
      throw new Error(`Failed to fetch tokens from OAuth server: ${error.message}`);
    }
  }

  beforeAll(async () => {
    const tokens = await getTwitterTokens();

    vault = {
      'twitter-auth': {
        id: 'twitter-auth',
        type: 'oauth2',
        credentials: {
          access_token: tokens.access_token,
          refresh_token: tokens.refresh_token || null,
          expires_at: tokens.expires_at,
        },
      },
    };

    connector = {
      id: 'twitter-post',
      adapter_id: 'twitter',
      endpoint_id: 'tweet_post',
      credential_id: 'twitter-auth',
      config: { headers: {} },
    };

    pipeline = {
      id: 'twitter-upload-test',
      data: [{ text: `Test tweet from OpenETL - ${Date.now()}` }], // Unique message with timestamp
      target: connector,
      error_handling: {
        max_retries: 0,
        retry_interval: 300,
        fail_on_error: true,
      },
      rate_limiting: {
        requests_per_second: 1,
        max_retries_on_rate_limit: 1,
      },
    };
    
    orchestrator = Orchestrator(vault, { twitter });
  });

  it('uploads a simple tweet successfully using tweet_post endpoint', async () => {
    const result = await orchestrator.runPipeline(pipeline);
    expect(result).toEqual({
      data: expect.arrayContaining([
        expect.objectContaining({
          text: expect.stringContaining('Test tweet from OpenETL'),
        }),
      ]),
    });
  });
});