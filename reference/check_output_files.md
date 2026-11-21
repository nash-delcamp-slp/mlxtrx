# Check specified output files alignment with latest runs

Validates that the specified output files were generated from the most
recent run that should have produced them. Ensures output files are
current and haven't been modified since generation.

## Usage

``` r
check_output_files(output_files, store = default_store())
```

## Arguments

- output_files:

  Character vector. Paths to output files to validate.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it.

## Value

A data frame with columns:

- `file_path` (character): Full path to the output file

- `file_name` (character): File name (basename)

- `run_id` (character): Most recently submitted run that recorded this
  file (NA if not found)

- `project_path` (character): Project file of that run (NA if not found)

- `run_submitted_at` (POSIXct): When the generating run was submitted

- `run_completed_at` (POSIXct): When the generating run completed

- `recorded_timestamp` (POSIXct): File timestamp when recorded in the
  store

- `recorded_sha256` (character): SHA-256 checksum when recorded

- `current_timestamp` (POSIXct): Current file modification time

- `current_sha256` (character): Current SHA-256 checksum

- `timestamp_changed` (logical): Whether modification time changed since
  recording

- `checksum_changed` (logical): Whether file content changed since
  recording

- `file_exists` (logical): Whether file still exists

- `predates_submission` (logical): Whether file predates the run
  submission

- `recorded_late` (logical): Whether the run's completion was recorded
  late

- `status` (character): Summary status: `"missing"`, `"no_run_record"`,
  `"incomplete_run"`, `"predates_run"`, `"modified"`,
  `"timestamp_changed"`, `"suspect"`, `"unchanged_late"` or
  `"unchanged"` (see
  [`check_output_file_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_output_file_status.md))
