import { Orchestrator } from '../../../src/index';
import { github } from './../src/index';
import axios from 'axios';

describe('GitHubAdapter Create Repository Integration Tests with OAuth2', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let baseConnector: any;
  let username: string;
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
      id: 'github-create-repo',
      adapter_id: 'github',
      endpoint_id: 'create_repository',
      credential_id: 'github-auth',
      config: {},
    };

    const userResponse = await axios.get('https://api.github.com/user', {
      headers: {
        Authorization: `Bearer ${vault['github-auth'].credentials.access_token}`, // Updated to access_token
        'Accept': 'application/vnd.github+json',
      },
    });
    username = userResponse.data.login;
  }, 30000);

  async function deleteRepo(repoName: string) {
    try {
      await axios.delete(`https://api.github.com/repos/${username}/${repoName}`, {
        headers: {
          Authorization: `Bearer ${vault['github-auth'].credentials.access_token}`, // Updated to access_token
          'Accept': 'application/vnd.github+json',
        },
      });
    } catch (error: any) {
      console.warn(`Failed to delete repo ${repoName}: ${error.message}`);
    }
  }

  it('creates a public repository successfully', async () => {
    const repoName = `test-repo-${Date.now()}`;
    const pipeline = {
      id: 'github-create-repo-public',
      target: {
        ...baseConnector,
      },
      data: [{
        title: repoName,
        name: repoName,
        description: 'A test public repository',
        private: false,
      }],
    };

    await orchestrator.runPipeline(pipeline);
    const response = await axios.get(`https://api.github.com/repos/${username}/${repoName}`, {
      headers: { Authorization: `Bearer ${vault['github-auth'].credentials.access_token}` },
    });
    expect(response.data.name).toBe(repoName);
    expect(response.data.description).toBe('A test public repository');
    expect(response.data.private).toBe(false);

    // await deleteRepo(repoName);
  }, 30000);

  it('fails when creating a repository with duplicate name', async () => {
    const repoName = `test-dupe-repo-${Date.now()}`;
    await orchestrator.runPipeline({
      id: 'github-create-repo-dupe-first',
      target: { ...baseConnector },
      data: [{ title: repoName, name: repoName }],
    });

    const pipeline: any = {
      id: 'github-create-repo-dupe-second',
      target: { ...baseConnector },
      data: [{ title: repoName, name: repoName }],
      error_handling: { max_retries: 0, fail_on_error: true },
    };

    await expect(orchestrator.runPipeline(pipeline)).rejects.toMatchObject({
      message: expect.stringContaining('GitHub API error 422'),
    });

    // await deleteRepo(repoName);
  }, 30000);

  it('fails with invalid data (missing title)', async () => {
    const pipeline: any = {
      id: 'github-create-repo-no-title',
      target: { ...baseConnector },
      data: [{ description: 'No title here' }],
      error_handling: { max_retries: 0, fail_on_error: true },
    };

    await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(
      "Each upload item must have a 'title' string field"
    );
  }, 30000);

  it('fails with invalid authentication', async () => {
    const badVault: any = {
      'bad-auth': {
        id: 'bad-auth',
        type: 'oauth2',
        credentials: { access_token: 'invalid-token' },
      },
    };
    const badOrchestrator = Orchestrator(badVault, { github });

    const pipeline: any = {
      id: 'github-create-repo-bad-auth',
      target: {
        ...baseConnector,
        credential_id: 'bad-auth',
      },
      data: [{ title: `test-repo-bad-auth-${Date.now()}` }],
      error_handling: { max_retries: 0, fail_on_error: true },
    };

    await expect(badOrchestrator.runPipeline(pipeline)).rejects.toMatchObject({
      message: expect.stringContaining('GitHub API error 401'),
    });
  }, 30000);

  it('creates multiple repositories in one pipeline', async () => {
    const repoName1 = `test-multi-1-${Date.now()}`;
    const repoName2 = `test-multi-2-${Date.now()}`;
    const pipeline = {
      id: 'github-create-repo-multi',
      target: { ...baseConnector },
      data: [
        { title: repoName1, name: repoName1, description: 'First test repo' },
        { title: repoName2, name: repoName2, description: 'Second test repo' },
      ],
    };

    await orchestrator.runPipeline(pipeline);
    const response1 = await axios.get(`https://api.github.com/repos/${username}/${repoName1}`, {
      headers: { Authorization: `Bearer ${vault['github-auth'].credentials.access_token}` },
    });
    const response2 = await axios.get(`https://api.github.com/repos/${username}/${repoName2}`, {
      headers: { Authorization: `Bearer ${vault['github-auth'].credentials.access_token}` },
    });
    expect(response1.data.name).toBe(repoName1);
    expect(response2.data.name).toBe(repoName2);

    // await deleteRepo(repoName1);
    // await deleteRepo(repoName2);
  }, 30000);
});