import { Orchestrator } from '../../../src/index';
import { github } from './../src/index';
import token from './token';

describe('GitHubAdapter Authenticated User Profile Integration Tests', () => {
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
      id: 'github-user-profile',
      adapter_id: 'github',
      endpoint_id: 'user_profile',
      credential_id: 'github-auth',
      config: {}, 
    };
  });

  it('downloads authenticated user profile successfully', async () => {
    const pipeline = {
      id: 'github-user-profile-basic',
      source: {
        ...baseConnector,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBe(1); 
    const user: any = result.data[0];
    expect(user).toHaveProperty('login');
    expect(typeof user.login).toBe('string');
    expect(user).toHaveProperty('id');
    expect(typeof user.id).toBe('number');
    expect(user).toHaveProperty('name');
    expect(user).toHaveProperty('email');
    expect(user).toHaveProperty('created_at');
  }, 30000);

  it('downloads user profile with specific fields', async () => {
    const pipeline = {
      id: 'github-user-profile-fields',
      source: {
        ...baseConnector,
        fields: ['login', 'name', 'bio'],
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBe(1);
    const user: any = result.data[0];
    expect(user).toHaveProperty('login');
    expect(user).toHaveProperty('name');
    expect(user).toHaveProperty('bio');
    
    const keys = Object.keys(user);
    expect(keys).toEqual(expect.arrayContaining(['login', 'name', 'bio']));
    
  }, 30000);

  it('handles missing email scope gracefully', async () => {
    
    const pipeline = {
      id: 'github-user-profile-no-email',
      source: {
        ...baseConnector,
        fields: ['login', 'name', 'email'],
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBe(1);
    const user: any = result.data[0];
    expect(user).toHaveProperty('login');
    expect(user).toHaveProperty('name');
    
    expect(user.email === null || user.email === undefined).toBe(true);
  }, 30000);
});