import { Orchestrator } from '../../../src/index'; // Adjust path
import { github } from './../src/index';

describe('GitHubAdapter Download Integration Tests', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let baseConnector: any;

  beforeAll(async () => {
    const pat = process.env.GITHUB_PAT || 'ghp_Kc3ylFnQ7UY...';

    vault = {
      'github-auth': {
        id: 'github-auth',
        type: 'api_key',
        credentials: {
          api_key: pat,
        },
      },
    };

    const adapters = { github };
    orchestrator = Orchestrator(vault, adapters);

    baseConnector = {
      id: 'github-commits',
      adapter_id: 'github',
      endpoint_id: 'repo_commits',
      credential_id: 'github-auth',
      config: {
        owner: 'jspreadsheet',
        repo: 'openetl',
      },
      fields: ['sha', 'commit.message', 'author.login'],
      pagination: { itemsPerPage: 10 },
    };
  });

  it('downloads commits from jspreadsheet/openetl', async () => {
    const pipeline = {
      id: 'github-commits-download',
      source: {
        ...baseConnector,
        limit: 20, // Fetch up to 20 commits (2 pages)
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(20);
    if (result.data.length > 0) {
      const commit: any = result.data[0];
      expect(commit).toHaveProperty('sha');
      expect(typeof commit.sha).toBe('string');
      expect(commit).toHaveProperty('commit');
      expect(commit.commit).toHaveProperty('message');
      expect(typeof commit.commit.message).toBe('string');
      expect(commit).toHaveProperty('author');
      expect(commit.author).toHaveProperty('login');
      expect(typeof commit.author.login).toBe('string');
    }
  }, 30000);

  it('downloads commits with specific branch filter', async () => {
    const pipeline = {
      id: 'github-commits-branch',
      source: {
        ...baseConnector,
        filters: [{ field: 'sha', operator: '=', value: 'main' }], // Filter by branch
        limit: 10,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(10);
    if (result.data.length > 0) {
      result.data.forEach((commit: any) => {
        expect(commit).toHaveProperty('sha');
        expect(commit).toHaveProperty('commit');
        expect(commit.commit).toHaveProperty('message');
      });
    }
  }, 30000);

  it('fails with invalid authentication', async () => {
    const badVault: any = {
      'github-auth': {
        id: 'github-auth',
        type: 'api_key',
        credentials: {
          api_key: 'eqjweiuwqej',
        },
      },
    };

    const badOrchestrator = Orchestrator(badVault, { github });

    const pipeline = {
      id: 'github-commits-fail',
      source: {
        ...baseConnector,
        credential_id: 'github-auth',
      },
    };

    await expect(badOrchestrator.runPipeline(pipeline)).rejects.toThrow(/GitHub API error 401/);
  }, 30000);
});