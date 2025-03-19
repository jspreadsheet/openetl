import { Orchestrator } from '../../../src/index';
import { github } from './../src/index';
import token from './token';

describe('GitHubAdapter User Repos Integration Tests', () => {
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
      id: 'github-user-repos',
      adapter_id: 'github',
      endpoint_id: 'user_repos',
      credential_id: 'github-auth',
      config: {},
      pagination: { itemsPerPage: 10 },
    };
  });

  it('downloads repositories from authenticated user', async () => {
    const pipeline = {
      id: 'github-user-repos-all',
      source: {
        ...baseConnector,
        limit: 20,
      },
    };

    const result: any = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeGreaterThan(0);
    expect(result.data.length).toBeLessThanOrEqual(20);
    result.data.forEach((repo: any) => {
      expect(repo).toHaveProperty('id');
      expect(repo).toHaveProperty('name');
      expect(repo).toHaveProperty('full_name');
      expect(repo).toHaveProperty('owner');
      expect(repo.owner).toHaveProperty('login');
    });
  }, 30000);

  it('downloads public repos with type filter', async () => {
    const pipeline = {
      id: 'github-user-repos-public',
      source: {
        ...baseConnector,
        endpoint_id: 'user_public_repos',
        config: {
          owner: 'axios'
        },
        filters: [{ field: 'type', operator: '=', value: 'public' }],
        limit: 10,
      },
    };

    const result: any = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(10);
    const axiosRepositories = [
      'axios',
      'axios-docs',
      'karma-moxios',
      'moxios'
    ];
    const foundRepositories = result.data.map((d: any) => d.name);
    expect(foundRepositories).toEqual(axiosRepositories);
    result.data.forEach((repo: any) => {
      expect(repo.private).toBe(false);
      expect(repo).toHaveProperty('id');
      expect(repo).toHaveProperty('name');
    });
  }, 30000);

  it('downloads repos sorted by created date', async () => {
    const pipeline = {
      id: 'github-user-repos-sorted',
      source: {
        ...baseConnector,
        endpoint_id: 'user_public_repos',
        config: {
          owner: 'axios'
        },
        filters: [
          { field: 'sort', operator: '=', value: 'created' },
          { field: 'direction', operator: '=', value: 'desc' },
        ],
        limit: 5,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(5);
    expect(result.data.length).toBeGreaterThan(0);
    const dates:any = result.data.map((repo: any) => new Date(repo.created_at).getTime());
    for (let i = 1; i < dates.length; i++) {
      expect(dates[i - 1]).toBeGreaterThanOrEqual(dates[i]);
    }
  }, 30000);

  it('downloads repos with specific fields', async () => {
    const pipeline = {
      id: 'github-user-repos-fields',
      source: {
        ...baseConnector,
        endpoint_id: 'user_public_repos',
        config: {
          owner: 'axios'
        },
        fields: ['id', 'name', 'description'],
        limit: 5,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(5);
    result.data.forEach((repo: any) => {
      expect(repo).toHaveProperty('id');
      expect(repo).toHaveProperty('name');
      expect(repo).toHaveProperty('description');
      const keys = Object.keys(repo);
      expect(keys).toEqual(expect.arrayContaining(['id', 'name', 'description']));
    });
  }, 30000);

  it('handles pagination with filters', async () => {
    const pipeline = {
      id: 'github-user-repos-paginated',
      source: {
        ...baseConnector,
        endpoint_id: 'user_public_repos',
        config: {
          owner: 'nodejs'
        },
        filters: [{ field: 'type', operator: '=', value: 'all' }],
        pagination: { itemsPerPage: 5 },
        limit: 15,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeGreaterThan(5);
    expect(result.data.length).toBeLessThanOrEqual(15);
    result.data.forEach((repo: any) => {
      expect(repo).toHaveProperty('id');
      expect(repo).toHaveProperty('name');
    });
  }, 30000);

  it('fails with invalid authentication', async () => {
    const badVault: any = {
      'bad-auth': {
        id: 'bad-auth',
        type: 'api_key',
        credentials: { api_key: 'invalid-token' },
      },
    };
    const badOrchestrator = Orchestrator(badVault, { github });

    const pipeline: any = {
      id: 'github-user-repos-fail',
      source: {
        ...baseConnector,
        endpoint_id: 'user_public_repos',
        config: {
          owner: 'nodejs'
        },
        credential_id: 'bad-auth',
      },
      error_handling: { max_retries: 0, fail_on_error: true },
    };

    await expect(badOrchestrator.runPipeline(pipeline)).rejects.toMatchObject({
      message: expect.stringContaining('GitHub API error 401'),
    });
  }, 30000);
});