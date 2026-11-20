# Check project file modification status

Detects project files that have been modified since their latest
completed run (the completed run with the latest `submitted_at`).

## Usage

``` r
check_project_file_status(
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

- `run_id` (character): Latest submitted completed run ID (NA if no
  completed runs)

- `run_completed_at` (POSIXct): When the run was completed (NA if not
  completed)

- `recorded_timestamp` (POSIXct): Project file timestamp at run time

- `recorded_sha256` (character): Project file SHA-256 checksum at run
  time

- `current_timestamp` (POSIXct): Current project file modification time

- `current_sha256` (character): Current project file SHA-256 checksum

- `timestamp_changed` (logical): Whether modification time changed since
  run

- `checksum_changed` (logical): Whether file content changed since run

- `file_exists` (logical): Whether project file still exists

- `status` (character): Summary status
