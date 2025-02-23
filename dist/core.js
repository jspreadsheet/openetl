export default function Orchestrator(vault, availableAdapters) {
    const adapters = new Map();
    function registerAdapter(id, adapter) {
        adapters.set(id, adapter);
    }
    function getCredentials(credentialId) {
        const auth = vault[credentialId];
        if (!auth) {
            throw new Error(`Credentials not found for id: ${credentialId}`);
        }
        return auth;
    }
    async function executeWithRetries(operation, pipeline, stage, errorHandling) {
        for (let attempt = 0; attempt <= errorHandling.max_retries; attempt++) {
            try {
                return await operation();
            }
            catch (error) {
                const errorMessage = error instanceof Error ? error.message : String(error);
                pipeline.logging?.({
                    type: "error",
                    message: `Attempt ${attempt + 1} failed in ${stage}: ${errorMessage}`,
                    timestamp: new Date().toISOString(),
                    error: error,
                });
                if (attempt === errorHandling.max_retries) {
                    if (errorHandling.fail_on_error)
                        throw error;
                    return null;
                }
                await new Promise(resolve => setTimeout(resolve, errorHandling.retry_interval));
            }
        }
        return null;
    }
    async function runPipeline(pipeline) {
        if (!pipeline.source && !pipeline.data) {
            pipeline.logging?.({
                type: "error",
                message: "Pipeline must have either a source or data",
                timestamp: new Date().toISOString(),
            });
            return;
        }
        const eh = pipeline.error_handling || { max_retries: 0, retry_interval: 1000, fail_on_error: true };
        const rl = pipeline.rate_limiting || { requests_per_second: Infinity, concurrent_requests: Infinity, max_retries_on_rate_limit: 0 };
        let data;
        pipeline.logging?.({ type: "start", message: "Pipeline started", timestamp: new Date().toISOString() });
        try {
            if (pipeline.source) {
                const Adapter = adapters.get(pipeline.source.adapter_id);
                if (!Adapter) {
                    throw new Error(`Adapter ${pipeline.source.adapter_id} not found`);
                }
                const auth = getCredentials(pipeline.source.credential_id);
                const adapter = Adapter(pipeline.source, auth);
                // Connect before download to validate or refresh token
                await adapter.connect();
                pipeline.logging?.({
                    type: "info",
                    message: "Connected to source adapter",
                    timestamp: new Date().toISOString(),
                });
                data = await executeWithRetries(() => adapter.download(rl), pipeline, "extract", eh) || [];
                if (pipeline.source.transform && adapter.transform) {
                    data = await adapter.transform(data);
                }
                pipeline.logging?.({
                    type: "extract",
                    message: "Data extracted",
                    timestamp: new Date().toISOString(),
                    dataCount: data.length,
                });
            }
            else {
                data = pipeline.data;
                pipeline.logging?.({
                    type: "extract",
                    message: "Using provided data",
                    timestamp: new Date().toISOString(),
                    dataCount: data.length,
                });
            }
            pipeline.onload?.(data);
            if (pipeline.target) {
                const beforeSendResult = pipeline.onbeforesend?.(data);
                if (beforeSendResult === false) {
                    pipeline.logging?.({
                        type: "complete",
                        message: "Pipeline halted by onbeforesend",
                        timestamp: new Date().toISOString(),
                    });
                    return;
                }
                const finalData = Array.isArray(beforeSendResult) ? beforeSendResult : data;
                const targetAdapterFactory = adapters.get(pipeline.target.adapter_id);
                if (!targetAdapterFactory) {
                    throw new Error(`Target adapter ${pipeline.target.adapter_id} not found`);
                }
                const targetAuth = getCredentials(pipeline.target.credential_id);
                const targetAdapter = targetAdapterFactory(pipeline.target, targetAuth);
                await targetAdapter.connect();
                pipeline.logging?.({
                    type: "info",
                    message: "Connected to target adapter",
                    timestamp: new Date().toISOString(),
                });
                if (!targetAdapter.upload) {
                    throw new Error(`Upload not supported by adapter ${pipeline.target.adapter_id}`);
                }
                await executeWithRetries(() => targetAdapter.upload(finalData, rl), pipeline, "load", eh);
            }
            pipeline.logging?.({
                type: "complete",
                message: "Pipeline finished",
                timestamp: new Date().toISOString(),
            });
        }
        catch (error) {
            pipeline.logging?.({
                type: "error",
                message: `Pipeline failed: ${error.message}`,
                timestamp: new Date().toISOString(),
                error: error,
            });
            if (eh.fail_on_error)
                throw error;
        }
    }
    if (availableAdapters) {
        Object.entries(availableAdapters).forEach(([id, adapter]) => {
            registerAdapter(id, adapter);
        });
    }
    return { registerAdapter, runPipeline };
}
