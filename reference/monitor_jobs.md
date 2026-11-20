# Monitor multiple jobs and record completed ones

Polls every `poll_interval` seconds until each submitted job has
finished, then records its completion in the run store with
`record_completion()`. A job with a job ID has finished when it is no
longer in the queue; a job run by the local runners `monolix.sh`,
`simulx.sh` or `pkanalix.sh` has finished already; any other job without
a job ID has finished when one of its project type's result files
(`summary.txt` for Monolix) exists in the output directory saved in its
`run.json`. Runs whose submission failed are skipped. A run whose
completion cannot be recorded gives a warning naming it, and the other
runs are still monitored and recorded.

## Usage

``` r
monitor_jobs(
  path,
  run_id,
  run_dir,
  job_id,
  cmd,
  store,
  poll_interval = 5,
  sq_failure_limit = 12
)
```

## Arguments

- path:

  Character vector. Paths to Monolix project files.

- run_id:

  Character vector. Run IDs.

- run_dir:

  Character vector. Run folders in the store.

- job_id:

  Integer vector. External job IDs (from job scheduler).

- cmd:

  Character vector. The commands used for each job.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).

- poll_interval:

  Numeric. Seconds between polls.

- sq_failure_limit:

  Integer. Consecutive `sq` failures after which runs with a job ID are
  no longer monitored.

## Value

NULL (invisible). Called for side effects.

## Details

The queue is read with `sq` once per poll. When `sq` fails, a warning is
given and the jobs with a job ID count as still queued for that poll.
After `sq_failure_limit` consecutive failures those runs are no longer
monitored and are left unrecorded, for
[`finalize_runs()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/finalize_runs.md)
to record once they finish.
