# Pipeline Interface and Execution Flow

## Type Definitions

### PipelineEvent
- Description: Defines structured events for logging, including errors.
- Fields:
    - type: Can be "start", "extract", "transform", "load", "error", or "complete".
    - message: A string describing the event.
    - timestamp: An ISO string representing when the event occurred.
    - dataCount: Optional number of records processed.
    - error: Optional error object for error events.

### Pipeline
- Description: Defines the ETL pipeline configuration with source, target, and processing options.
- Fields:
    - id: A unique string identifier for the pipeline.
    - data: Optional array of objects; mandatory if source is omitted, ignored if source is present.
    - source: Optional Connector for extracting and transforming data.
    - target: Optional Connector for loading data.
    - schedule: Optional scheduling configuration.
        - frequency: Can be "hourly", "daily", or "weekly".
        - at: A string specifying the time (e.g., "00:00 UTC").
    - logging: Optional function that takes a PipelineEvent and handles logging.
    - onload: Function called with data after extraction or when provided.
    - onbeforesend: Function called before sending to target; can return:
        - An array of objects to transform the data.
        - false to halt the pipeline.
        - Nothing (void) to continue with original data.
    - error_handling: Optional error handling settings.
        - max_retries: Number of retry attempts on failure.
        - retry_interval: Delay between retries in milliseconds.
        - fail_on_error: Boolean; if true, pipeline stops on error; if false, continues.
    - rate_limiting: Optional rate limiting settings.
        - requests_per_second: Maximum requests per second.
        - concurrent_requests: Maximum simultaneous requests.
        - max_retries_on_rate_limit: Maximum retries when rate-limited.

## How These Fit Into the Execution Flow

Here’s the updated flow with error handling and rate limiting:

- Start: Log an event with type "start".
- Data Source:
    - If source exists: Extract data using the source Connector, respecting rate limiting (e.g., throttle requests to the specified requests per second).
        - On failure: Retry up to the maximum retries specified in error handling, with a delay between retries as defined by the retry interval.
        - If retries fail and fail_on_error is true, stop the pipeline; otherwise, log the error and proceed with an empty data set.
    - If source is missing: Use the provided data (throw an error if neither source nor data exists).
- Onload: Call the onload function with the extracted or provided data.
- Pre-Send (if target exists):
    - Call the onbeforesend function with the data.
    - If it returns:
        - An array of objects: Use the transformed data for the next step.
        - false: Halt the pipeline.
        - Nothing (void): Proceed with the original data.
- Load: Send data to the target Connector, respecting rate limiting.
    - On failure: Retry up to the maximum retries specified in error handling.
    - On rate limit (e.g., receiving a 429 response): Retry up to the maximum retries specified for rate limiting.
    - If retries fail and fail_on_error is true, stop the pipeline; otherwise, log the error and continue.
- Complete: Log an event with type "complete".