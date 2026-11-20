# Record completion of runs whose launching session ended

Records completion, marked as recorded late, for runs that were launched
but never recorded, for example because the R session that launched them
died. A run with a job ID is recorded once its job has left the queue
(`sq`). A run launched with the local runners `monolix.sh`,
`pkanalix.sh` or `simulx.sh` had finished before its launch was
recorded, so it is recorded straight away. Any other run without a job
ID is recorded once its output folder holds one of its project type's
result files (`summary.txt` for Monolix). If `sq` is missing or fails
the function stops, because a running job would otherwise be recorded as
finished.

## Usage

``` r
finalize_runs(store = default_store(), run_id = NULL, retry_crashed = FALSE)
```

## Arguments

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).

- run_id:

  Character vector. Limit finalization to these run IDs. `NULL`
  (default) considers every run in the store.

- retry_crashed:

  Logical. Record runs whose earlier completion attempt crashed into a
  fresh attempt folder. Otherwise they are only reported.

## Value

A data frame with columns `run_id` and `action`, one row per run that
was considered. `action` is `"recorded_late"`, `"still_queued"`,
`"no_summary_yet"`, `"claimed_elsewhere"`, `"completion_crashed"`,
`"other_user"` or `"skipped_submit_failed"` (only for runs named in
`run_id`). Recorded runs are not listed.

## Details

Only runs whose `run.json` user is the current user are recorded,
because `sq` may list only the current user's jobs and another user's
running job would then look finished. Other users' runs are reported as
`"other_user"` and must be finalized by their owner.

## Examples

``` r
if (FALSE) { # \dontrun{
finalize_runs()
finalize_runs(retry_crashed = TRUE)
} # }
```
