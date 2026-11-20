# Check input file modification status

Detects input files that have been modified since the run began.
Compares current file timestamps and checksums against recorded values.

## Usage

``` r
check_input_file_status(run_ids = NULL, store = default_store())
```

## Arguments

- run_ids:

  Character vector. Specific run IDs to check. If NULL (default), checks
  all runs.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it.

## Value

A data frame with columns:

- `run_id` (character): The run identifier

- `file_path` (character): Full path to the input file

- `file_name` (character): File name (basename)

- `recorded_timestamp` (POSIXct): File timestamp when the run was
  submitted

- `recorded_sha256` (character): SHA-256 checksum when the run was
  submitted

- `current_timestamp` (POSIXct): Current file modification time

- `current_sha256` (character): Current SHA-256 checksum

- `timestamp_changed` (logical): Whether modification time changed

- `checksum_changed` (logical): Whether file content changed

- `file_exists` (logical): Whether file still exists

- `recorded_late` (logical): Whether the run's completion was recorded
  late (NA if it was not recorded)

- `status` (character): Summary status
