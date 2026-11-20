# Check completion status of project files in a directory

Validates that all project files in a directory have successful run
records. A successful run is one whose latest run (by `submitted_at`)
has a completion timestamp and, for Monolix, PKanalix and Simulx
projects, parsed results.

## Usage

``` r
check_completion_status(
  directory = ".",
  ext = c("mlxtran", "pkx", "smlx"),
  store = default_store()
)
```

## Arguments

- directory:

  Character scalar. Directory path to check for project files.

- ext:

  Character vector. File extensions to consider as project files.
  Defaults to c("mlxtran", "pkx", "smlx").

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it.

## Value

A data frame with columns:

- `project_file` (character): Full path to the project file

- `has_run` (logical): Whether any run record exists for this file

- `has_successful_run` (logical): Whether the latest run completed with
  results

- `latest_run_id` (character): Most recent run_id (NA if no runs)

- `latest_completed_at` (POSIXct): Latest completion timestamp (NA if
  not completed)

- `recorded_late` (logical): Whether the latest run's completion was
  recorded late (NA if it was not recorded)

- `status` (character): Summary status ("completed", "completed_late",
  "incomplete", "no_results", "no_runs")
