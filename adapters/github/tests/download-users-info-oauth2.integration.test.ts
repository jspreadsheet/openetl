import { Orchestrator } from '../../../src/index';
import { github } from './../src/index';
import axios from 'axios';

describe('GitHubAdapter User Info Integration Tests with OAuth2', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let baseConnector: any;
  const oauthServerUrl = 'http://localhost:2215';

  beforeAll(async () => {
    let accessToken;
    try {
      const tokenResponse = await axios.get(`${oauthServerUrl}/tokens`);
      accessToken = tokenResponse.data.access_token;
      if (!accessToken) {
        throw new Error('No access_token found in response');
      }
    } catch (error: any) {
      throw new Error(`Failed to fetch OAuth2 token from ${oauthServerUrl}/tokens: ${error.message}`);
    }

    vault = {
      'github-auth': {
        id: 'github-auth',
        type: 'oauth2',
        credentials: { access_token: accessToken },
      },
    };

    const adapters = { github };
    orchestrator = Orchestrator(vault, adapters);

    baseConnector = {
      id: 'github-user-info',
      adapter_id: 'github',
      endpoint_id: 'user_info',
      credential_id: 'github-auth',
      config: { username: 'octocat' }
    };
  }, 30000);

  it('downloads public user info for a known user', async () => {
    const pipeline = {
      id: 'github-user-info-octocat',
      source: {
        ...baseConnector,
        config: { username: 'octocat' },
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBe(1);
    const user: any = result.data[0];
    expect(user.login).toBe('octocat');
    expect(user).toHaveProperty('id');
    expect(typeof user.id).toBe('number');
    expect(user).toHaveProperty('public_repos');
    expect(user).toHaveProperty('followers');
  }, 30000);

  it('fails with invalid username', async () => {
    const pipeline: any = {
      id: 'github-user-info-invalid',
      source: {
        ...baseConnector,
        config: { username: 'nonexistentuser123456789' },
      },
      error_handling: { max_retries: 0, fail_on_error: true },
    };

    await expect(orchestrator.runPipeline(pipeline)).rejects.toMatchObject({
      message: expect.stringContaining('GitHub API error 404'),
    });
  }, 30000);
});