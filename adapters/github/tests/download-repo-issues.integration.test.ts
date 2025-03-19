import { Orchestrator } from '../../../src/index';
import { github } from './../src/index';
import axios from 'axios';
import token from './token';

jest.mock('axios', () => {
  const actualAxios = jest.requireActual('axios') as typeof axios;
  return {
    ...actualAxios,
    get: jest.fn(actualAxios.get),
  };
});

describe('GitHubAdapter Repo Issues Integration Tests', () => {
  let orchestrator: ReturnType<typeof Orchestrator>;
  let vault: any;
  let baseConnector: any;

  beforeAll(async () => {
    const pat = process.env.GITHUB_PAT || token;

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
      id: 'github-issues',
      adapter_id: 'github',
      endpoint_id: 'repo_issues',
      credential_id: 'github-auth',
      config: {
        owner: 'axios',
        repo: 'axios',
      },
      fields: ['id', 'title', 'state', 'created_at', 'user.login'],
      pagination: { itemsPerPage: 10 },
    };
  });

  beforeEach(() => {
    (axios.get as jest.MockedFunction<typeof axios.get>).mockClear();
  });

  it('downloads a single page of issues', async () => {
    const pipeline = {
      id: 'github-issues-single-page',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 5 },
        limit: 5,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(5);
    expect(result.data.length).toBeGreaterThan(0);
    result.data.forEach((issue: any) => {
      expect(issue).toHaveProperty('id');
      expect(typeof issue.id).toBe('number');
      expect(issue).toHaveProperty('title');
      expect(typeof issue.title).toBe('string');
      expect(issue).toHaveProperty('state');
      expect(['open', 'closed']).toContain(issue.state);
      expect(issue).toHaveProperty('created_at');
      expect(issue).toHaveProperty('user');
      expect(issue.user).toHaveProperty('login');
    });
  }, 30000);

  it('downloads multiple pages of issues with pagination', async () => {
    const pipeline = {
      id: 'github-issues-multi-page',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 5 },
        limit: 15,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeGreaterThan(5);
    expect(result.data.length).toBeLessThanOrEqual(15);
    result.data.forEach(issue => {
      expect(issue).toHaveProperty('id');
      expect(issue).toHaveProperty('title');
      expect(issue).toHaveProperty('state');
    });

    const axiosGetMock = axios.get as jest.MockedFunction<typeof axios.get>;
    expect(axiosGetMock).toHaveBeenCalledTimes(3);
    expect(axiosGetMock).toHaveBeenNthCalledWith(
      1,
      'https://api.github.com/repos/axios/axios/issues',
      expect.objectContaining({
        params: { per_page: '5', page: '1' },
      })
    );
    expect(axiosGetMock).toHaveBeenNthCalledWith(
      2,
      'https://api.github.com/repos/axios/axios/issues',
      expect.objectContaining({
        params: { per_page: '5', page: '2' },
      })
    );
    
    expect(axiosGetMock).toHaveBeenNthCalledWith(
      3,
      'https://api.github.com/repos/axios/axios/issues',
      expect.objectContaining({
        params: { per_page: '5', page: '3' },
      })
    );
  }, 30000);

  it('downloads issues with state filter', async () => {
    const pipeline = {
      id: 'github-issues-filtered',
      source: {
        ...baseConnector,
        filters: [{ field: 'state', operator: '=', value: 'open' }],
        pagination: { itemsPerPage: 10 },
        limit: 10,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeLessThanOrEqual(10);
    expect(result.data.length).toBeGreaterThan(0);
    result.data.forEach((issue: any) => {
      expect(issue).toHaveProperty('id');
      expect(issue).toHaveProperty('title');
      expect(issue.state).toBe('open');
    });
  }, 30000);

  it('handles fetching all issues until end', async () => {
    const pipeline = {
      id: 'github-issues-all',
      source: {
        ...baseConnector,
        pagination: { itemsPerPage: 20 },
        limit: 1000,
      },
    };

    const result = await orchestrator.runPipeline(pipeline);
    expect(result.data).toBeInstanceOf(Array);
    expect(result.data.length).toBeGreaterThan(0);
    expect(result.data.length).toBeLessThanOrEqual(1000);
    expect(result.data.length).toBeGreaterThan(100);
    result.data.forEach(issue => {
      expect(issue).toHaveProperty('id');
      expect(issue).toHaveProperty('title');
      expect(issue).toHaveProperty('state');
    });
  }, 30000);

});