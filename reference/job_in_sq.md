# Check if job IDs are currently in the job queue

Queries the job queue using the `sq` command to determine if one or more
job IDs are currently active (running or queued).

## Usage

``` r
job_in_sq(job_id)
```

## Arguments

- job_id:

  Numeric or character vector. Job IDs to check in the queue.

## Value

Logical vector. Returns `TRUE` for job IDs found in the queue, `FALSE`
for job IDs not found or if the `sq` command fails.

## Details

This function executes the `sq` command to retrieve current job queue
information and parses the output to check for the presence of specified
job IDs. The function:

1.  Runs `system2("sq")` to get queue status

2.  Parses the output as a space-separated table with headers

3.  Checks if job IDs exist in the `JOBID` column

If the `sq` command fails or its output has no `JOBID` column, the
function returns `FALSE` with a warning. A header-only listing is an
empty queue.

## See also

[`monitor_jobs`](https://nash-delcamp-slp.github.io/mlxtrx/reference/monitor_jobs.md)
for monitoring job completion,
[`execute_job`](https://nash-delcamp-slp.github.io/mlxtrx/reference/execute_job.md)
for submitting jobs

## Examples

``` r
if (FALSE) { # \dontrun{
# Check single job ID
job_in_sq(12345)

# Check multiple job IDs
job_in_sq(c(12345, 67890))

job_in_sq(c("12345", "67890"))
} # }
```
