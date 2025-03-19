import { Orchestrator } from '../../../src/index'; 
import { github } from './../src/index';
import token from './token';

describe('GitHubAdapter User Info Integration Tests', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let baseConnector: any;

  beforeAll(async () => {
    const pat = process.env.GITHUB_PAT || token; 
    if (!pat) {
      throw new Error('Please set GITHUB_PAT environment variable with a valid token');
    }

    vault = {
      'github-auth': {
        id: 'github-auth',
        type: 'api_key',
        credentials: { api_key: pat },
      },
    };

    const adapters = { github };
    orchestrator = Orchestrator(vault, adapters);

    baseConnector = {
      id: 'github-user-info',
      adapter_id: 'github',
      endpoint_id: 'user_info',
      credential_id: 'github-auth',
      config: { username: 'octocat' }, 
    };
  });

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

  it('downloads user info with specific fields', async () => {
    const pipeline = {
      id: 'github-user-info-fields',
      source: {
        ...baseConnector,
        config: { username: 'octocat' },
        fields: ['login', 'name', 'bio'],
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBe(1);
    const user: any = result.data[0];
    expect(user).toHaveProperty('login');
    expect(user.login).toBe('octocat');
    expect(user).toHaveProperty('name');
    expect(user).toHaveProperty('bio');
    
    const keys = Object.keys(user);
    expect(keys).toEqual(expect.arrayContaining(['login', 'name', 'bio']));
    
  }, 30000);

  it('downloads info for a different user', async () => {
    const pipeline = {
      id: 'github-user-info-torvalds',
      source: {
        ...baseConnector,
        config: { username: 'torvalds' }, 
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBe(1);
    const user: any = result.data[0];
    expect(user.login).toBe('torvalds');
    expect(user).toHaveProperty('name');
    expect(user.name).toContain('Linus'); 
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