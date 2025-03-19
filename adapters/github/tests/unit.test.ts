import { Orchestrator } from "../../../src/index";
import { github } from "./../src/index";
import axios from "axios";

jest.mock("axios");
const mockedAxios = axios as jest.Mocked<typeof axios>;

describe("GitHubAdapter Unit Tests with Orchestrator", () => {
	let orchestrator: ReturnType<typeof Orchestrator>;
	let vault: any;
	let baseConnector: any;

	const mockIssue = {
		id: 1,
		title: "Test Issue",
		state: "open",
		created_at: "2023-01-01",
		user: { login: "testuser" },
	};
	const mockCommit = {
		sha: "abc123",
		commit: { message: "Test commit" },
		author: { login: "testuser" },
	};
	const mockRepo = {
		id: 1,
		name: "test-repo",
		full_name: "testuser/test-repo",
	};
	const mockUser = { login: "testuser", name: "Test User" };
	const mockEmail = { email: "test@example.com", primary: true };

	beforeEach(() => {
		mockedAxios.get.mockReset();
		mockedAxios.post.mockReset();

		vault = {
			"github-auth": {
				id: "github-auth",
				type: "api_key",
				credentials: { api_key: "valid-token" },
			},
		};

		const adapters = { github };
		orchestrator = Orchestrator(vault, adapters);

		baseConnector = {
			id: "github-base",
			adapter_id: "github",
			credential_id: "github-auth",
			config: { owner: "testuser", repo: "test-repo" },
			fields: ["id", "title"],
			pagination: { itemsPerPage: 10 },
		};
	});

	describe("API Method Calls", () => {
		it("downloads repo issues successfully", async () => {
			mockedAxios.get.mockResolvedValue({
				data: [mockIssue],
				headers: { link: '<...page=2>; rel="next"' },
			});
			const pipeline = {
				id: "github-issues",
				source: {
					...baseConnector,
					endpoint_id: "repo_issues",
					limit: 5,
					pagination: { itemsPerPage: 5 },
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data).toEqual([mockIssue]);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/repos/testuser/test-repo/issues",
				expect.objectContaining({
					params: { per_page: "5", page: "1" },
				})
			);
		});

		it("downloads user repos successfully", async () => {
			mockedAxios.get.mockResolvedValue({
				data: [mockRepo],
				headers: { link: '<...page=2>; rel="next"' },
			});
			const pipeline = {
				id: "github-user-repos",
				source: {
					...baseConnector,
					endpoint_id: "user_repos",
					config: {},
					limit: 5,
					pagination: { itemsPerPage: 5 },
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data).toEqual([mockRepo]);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/user/repos",
				expect.objectContaining({
					params: { per_page: "5", page: "1" },
				})
			);
		});

		it("uploads an issue successfully", async () => {
			mockedAxios.post.mockResolvedValue({ data: { id: 1 } });
			const pipeline = {
				id: "github-create-issue",
				target: { ...baseConnector, endpoint_id: "create_issue" },
				data: [{ title: "New Issue", body: "Test body" }],
			};

			await orchestrator.runPipeline(pipeline);
			expect(mockedAxios.post).toHaveBeenCalledWith(
				"https://api.github.com/repos/testuser/test-repo/issues",
				{ title: "New Issue", body: "Test body" },
				expect.objectContaining({ headers: expect.any(Object) })
			);
		});

		it("downloads user profile", async () => {
			mockedAxios.get.mockResolvedValue({ data: [mockUser] });
			const pipeline = {
				id: "github-user-profile",
				source: {
					...baseConnector,
					endpoint_id: "user_profile",
					config: {},
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data).toEqual([mockUser]);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/user",
				expect.any(Object)
			);
		});

		it("downloads user emails successfully", async () => {
			mockedAxios.get.mockResolvedValue({ data: [mockEmail] });
			const pipeline = {
				id: "github-user-emails",
				source: {
					...baseConnector,
					endpoint_id: "user_emails",
					config: {},
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data).toEqual([mockEmail]);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/user/emails",
				expect.any(Object)
			);
		});
	});

	describe("Pagination", () => {
		it("downloads single page with offset pagination", async () => {
			mockedAxios.get.mockResolvedValue({
				data: Array(5).fill(mockCommit),
				headers: { link: '<...page=2>; rel="next"' },
			});
			const pipeline = {
				id: "github-commits-single",
				source: {
					...baseConnector,
					endpoint_id: "repo_commits",
					pagination: { itemsPerPage: 5 },
					limit: 5,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data.length).toBe(5);
			expect(mockedAxios.get).toHaveBeenCalledTimes(1);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/repos/testuser/test-repo/commits",
				expect.objectContaining({
					params: { per_page: "5", page: "1" },
				})
			);
		});

		it("downloads multiple pages with offset pagination", async () => {
			mockedAxios.get
				.mockResolvedValueOnce({
					data: Array(5).fill(mockCommit),
					headers: { link: '<...page=2>; rel="next"' },
				})
				.mockResolvedValueOnce({
					data: Array(5).fill(mockCommit),
					headers: { link: '<...page=3>; rel="next"' },
				})
				.mockResolvedValueOnce({
					data: Array(3).fill(mockCommit),
					headers: {},
				});

			const pipeline = {
				id: "github-commits-multi",
				source: {
					...baseConnector,
					endpoint_id: "repo_commits",
					pagination: { itemsPerPage: 5 },
					limit: 13,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data.length).toBe(13);
			expect(mockedAxios.get).toHaveBeenCalledTimes(3);
			expect(mockedAxios.get).toHaveBeenNthCalledWith(
				1,
				"https://api.github.com/repos/testuser/test-repo/commits",
				expect.objectContaining({
					params: { per_page: "5", page: "1" },
				})
			);
			expect(mockedAxios.get).toHaveBeenNthCalledWith(
				2,
				expect.any(String),
				expect.objectContaining({
					params: { per_page: "5", page: "2" },
				})
			);
			expect(mockedAxios.get).toHaveBeenNthCalledWith(
				3,
				expect.any(String),
				expect.objectContaining({
					params: { per_page: "5", page: "3" },
				})
			);
		});

		it("respects initial offset", async () => {
			mockedAxios.get.mockResolvedValue({
				data: Array(5).fill(mockIssue),
				headers: {},
			});
			const pipeline = {
				id: "github-issues-offset",
				source: {
					...baseConnector,
					endpoint_id: "repo_issues",
					pagination: { itemsPerPage: 5, pageOffsetKey: 5 },
					limit: 5,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data.length).toBe(5);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/repos/testuser/test-repo/issues",
				expect.objectContaining({
					params: { per_page: "5", page: "2" },
				})
			);
		});
	});

	describe("Edge Cases", () => {
		it("handles empty response", async () => {
			mockedAxios.get.mockResolvedValue({ data: [], headers: {} });
			const pipeline = {
				id: "github-issues-empty",
				source: {
					...baseConnector,
					endpoint_id: "repo_issues",
					limit: 5,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data).toEqual([]);
			expect(mockedAxios.get).toHaveBeenCalledTimes(1);
		});

		it("throws error for missing owner/repo config", async () => {
			const badConnector = {
				...baseConnector,
				config: {},
				endpoint_id: "repo_issues",
			};
			const pipeline = {
				id: "github-issues-no-config",
				source: badConnector,
			};

			await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(
				"Connector config must include owner for repo-specific endpoints"
			);
			expect(mockedAxios.get).not.toHaveBeenCalled();
		});

		it("handles API error with retries", async () => {
			mockedAxios.get
				.mockRejectedValueOnce({
					response: { status: 429, data: { message: "Rate limit" } },
				})
				.mockResolvedValueOnce({ data: [mockIssue], headers: {} });

			const pipeline = {
				id: "github-issues-retry",
				source: {
					...baseConnector,
					endpoint_id: "repo_issues",
					limit: 5,
				},
				error_handling: {
					max_retries: 1,
					retry_interval: 100,
					fail_on_error: false,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data).toEqual([mockIssue]);
			expect(mockedAxios.get).toHaveBeenCalledTimes(2);
		});
	});

	describe("Authentication Scenarios", () => {
		it("throws error for invalid authentication", async () => {
			const badVault: any = {
				"github-auth": {
					id: "github-auth",
					type: "api_key",
					credentials: {},
				},
			};
			const badOrchestrator = Orchestrator(badVault, { github });

			const pipeline = {
				id: "github-issues-bad-auth",
				source: { ...baseConnector, endpoint_id: "repo_issues" },
			};

			await expect(badOrchestrator.runPipeline(pipeline)).rejects.toThrow(
				"GitHub adapter requires api_key or oauth2 authentication with a valid token"
			);
			expect(mockedAxios.get).not.toHaveBeenCalled();
		});
	});

	describe("Pagination Edge Cases", () => {
		it("handles zero offset correctly", async () => {
			mockedAxios.get.mockResolvedValue({
				data: Array(5).fill(mockCommit),
				headers: { link: '<...page=2>; rel="next"' },
			});
			const pipeline = {
				id: "github-commits-zero-offset",
				source: {
					...baseConnector,
					endpoint_id: "repo_commits",
					pagination: { itemsPerPage: 5, pageOffsetKey: 0 },
					limit: 5,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data.length).toBe(5);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/repos/testuser/test-repo/commits",
				expect.objectContaining({
					params: { per_page: "5", page: "1" },
				})
			);
		});

		it("handles large offset beyond data", async () => {
			mockedAxios.get.mockResolvedValue({ data: [], headers: {} });
			const pipeline = {
				id: "github-issues-large-offset",
				source: {
					...baseConnector,
					endpoint_id: "repo_issues",
					pagination: { itemsPerPage: 5, pageOffsetKey: 1000 },
					limit: 5,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data).toEqual([]);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/repos/testuser/test-repo/issues",
				expect.objectContaining({
					params: { per_page: "5", page: "201" },
				})
			);
		});

		it("handles limit less than itemsPerPage", async () => {
			mockedAxios.get.mockResolvedValue({
				data: Array(3).fill(mockIssue),
				headers: {},
			});
			const pipeline = {
				id: "github-issues-small-limit",
				source: {
					...baseConnector,
					endpoint_id: "repo_issues",
					pagination: { itemsPerPage: 5 },
					limit: 3,
				},
			};

			const result = await orchestrator.runPipeline(pipeline);
			expect(result.data.length).toBe(3);
			expect(mockedAxios.get).toHaveBeenCalledWith(
				"https://api.github.com/repos/testuser/test-repo/issues",
				expect.objectContaining({
					params: { per_page: "5", page: "1" },
				})
			);
		});
	});

	describe("Upload Edge Cases", () => {
		it("throws error for invalid upload data", async () => {
			const pipeline = {
				id: "github-create-issue-invalid",
				target: { ...baseConnector, endpoint_id: "create_issue" },
				data: [{ body: "No title" }],
			};

			await expect(orchestrator.runPipeline(pipeline)).rejects.toThrow(
				"Each upload item must have a 'title' string field"
			);
			expect(mockedAxios.post).not.toHaveBeenCalled();
		});

		it("uploads multiple issues successfully", async () => {
			mockedAxios.post.mockResolvedValue({ data: { id: 1 } });
			const pipeline = {
				id: "github-create-multi-issue",
				target: { ...baseConnector, endpoint_id: "create_issue" },
				data: [
					{ title: "Issue 1", body: "Body 1" },
					{ title: "Issue 2", body: "Body 2" },
				],
			};

			await orchestrator.runPipeline(pipeline);
			expect(mockedAxios.post).toHaveBeenCalledTimes(2);
			expect(mockedAxios.post).toHaveBeenNthCalledWith(
				1,
				"https://api.github.com/repos/testuser/test-repo/issues",
				{ title: "Issue 1", body: "Body 1" },
				expect.any(Object)
			);
			expect(mockedAxios.post).toHaveBeenNthCalledWith(
				2,
				"https://api.github.com/repos/testuser/test-repo/issues",
				{ title: "Issue 2", body: "Body 2" },
				expect.any(Object)
			);
		});
	});

	describe("Error Handling", () => {
		it("handles 403 Forbidden error", async () => {
      const errorResponse = {
				response: { status: 403, data: { message: "Forbidden" } },
			};
			mockedAxios.get.mockRejectedValue(errorResponse);
			const pipeline = {
				id: "github-issues-forbidden",
				source: {
					...baseConnector,
					endpoint_id: "repo_issues",
					limit: 5,
				},
				error_handling: {
					max_retries: 0,
					retry_interval: 0,
					fail_on_error: true,
				},
			};

      try {
        await orchestrator.runPipeline(pipeline)
      } catch(err) {
        console.log(err);
        expect(err).toEqual(errorResponse);
      }
			expect(mockedAxios.get).toHaveBeenCalledTimes(1);
		});
	});
});
