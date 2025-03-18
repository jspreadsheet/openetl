import { Orchestrator } from '../../../src/index'; // Adjust path
import { github } from './../src/index';
import axios from 'axios';

jest.mock('axios', () => {
  const actualAxios = jest.requireActual('axios') as typeof axios;
  return {
    ...actualAxios,
    get: jest.fn(actualAxios.get),
  };
});

describe('GitHubAdapter Download Integration Tests', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let baseConnector: any;

  beforeEach(() => {
    // Reset the axios.get mock before each test
    (axios.get as jest.MockedFunction<typeof axios.get>).mockClear();
  });

  beforeAll(async () => {
    const pat = process.env.GITHUB_PAT || 'ghp_Kc3y...';

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

  it('downloads a single page of commits', async () => {
    const pipeline = {
      id: 'github-commits-single-page',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 5 },
        limit: 5, // Matches itemsPerPage to ensure one page
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(5); // Up to 5 commits
    expect(result.data.length).toBeGreaterThan(0); // Expect some commits
    result.data.forEach((commit: any) => {
      expect(commit).toHaveProperty('sha');
      expect(commit).toHaveProperty('commit');
      expect(commit.commit).toHaveProperty('message');
      expect(commit).toHaveProperty('author');
      expect(commit.author).toHaveProperty('login');
    });
  }, 30000);

  it('downloads multiple pages of commits', async () => {
    const pipeline = {
      id: 'github-commits-multi-page',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 5 },
        limit: 15, // Requires 3 pages (5 items per page)
      },
    };

    const result = await orchestrator.runPipeline(pipeline);

    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeGreaterThan(5); // More than one page
    expect(result.data.length).toBeLessThanOrEqual(15); // Up to 15 commits
    result.data.forEach((commit: any) => {
      expect(commit).toHaveProperty('sha');
      expect(commit).toHaveProperty('commit');
      expect(commit.commit).toHaveProperty('message');
    });

    const axiosGetMock = axios.get as jest.MockedFunction<typeof axios.get>;
    expect(axiosGetMock).toHaveBeenCalledTimes(3);
  }, 30000);

  it('handles pagination until end of commits', async () => {
    const pipeline = {
      id: 'github-commits-end-of-data',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 10 },
        limit: 1000, // Large limit to fetch all commits
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeGreaterThan(0);
    expect(result.data.length).toBeLessThanOrEqual(1000);
    // jspreadsheet/openetl has fewer than 1000 commits, so should stop early
    expect(result.data.length).toBeLessThan(100); // Rough estimate based on repo size
    result.data.forEach((commit: any) => {
      expect(commit).toHaveProperty('sha');
      expect(commit).toHaveProperty('commit');
      expect(commit.commit).toHaveProperty('message');
    });
  }, 30000);

  it('respects initial page offset', async () => {
    // First, fetch the first page to get a baseline
    const firstPagePipeline = {
      id: 'github-commits-first-page',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 5 },
        limit: 5,
      },
    };

    const firstResult: any = await orchestrator.runPipeline(firstPagePipeline);
    expect(firstResult.data.length).toBeGreaterThan(0);
    const firstPageSha = firstResult.data[0].sha;

    // Now fetch starting from page 2
    const pipeline = {
      id: 'github-commits-offset',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 5, pageOffsetKey: 2 }, // Start at page 2
        limit: 5,
      },
    };

    const result: any = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(5);
    if (result.data.length > 0) {
      expect(result.data[0].sha).not.toBe(firstPageSha); // Should be different from page 1
      result.data.forEach((commit: any) => {
        expect(commit).toHaveProperty('sha');
        expect(commit).toHaveProperty('commit');
        expect(commit.commit).toHaveProperty('message');
      });
    }
  }, 30000);


  it('handles small page size with large limit', async () => {
    const pipeline = {
      id: 'github-commits-small-page',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 2 },
        limit: 10, // Requires 5 pages
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeGreaterThan(2); // More than one page
    expect(result.data.length).toBeLessThanOrEqual(10);
    result.data.forEach((commit: any) => {
      expect(commit).toHaveProperty('sha');
      expect(commit).toHaveProperty('commit');
      expect(commit.commit).toHaveProperty('message');
    });
  }, 30000);


});