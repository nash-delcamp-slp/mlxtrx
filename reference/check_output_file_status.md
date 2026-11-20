# Check output file integrity

Identifies output files that have been manually changed, deleted, or
updated with another tool since they were recorded. Outputs of runs
whose completion was recorded late are never reported as plain
`"unchanged"`.

## Usage

``` r
check_output_file_status(run_ids = NULL, store = default_store())
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

- `file_path` (character): Full path to the output file

- `file_name` (character): File name (basename)

- `recorded_timestamp` (POSIXct): Timestamp when output was recorded

- `recorded_sha256` (character): SHA-256 checksum when recorded

- `current_timestamp` (POSIXct): Current file modification time

- `current_sha256` (character): Current SHA-256 checksum

- `timestamp_changed` (logical): Whether modification time changed

- `checksum_changed` (logical): Whether file content changed

- `file_exists` (logical): Whether file still exists

- `recorded_late` (logical): Whether the run's completion was recorded
  late

- `status` (character): Summary status: `"missing"`, `"modified"`,
  `"timestamp_changed"`, `"suspect"` (unchanged, but flagged when the
  late recording found it written long after the other outputs),
  `"unchanged_late"` (unchanged, recorded late) or `"unchanged"`
