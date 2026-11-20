# Generate a table with details about executed Monolix runs

Creates a formatted table displaying information about Monolix jobs that
have been submitted and recorded in the run store. The table can include
basic job information or detailed file tracking data.

## Usage

``` r
runs_table(
  store = default_store(),
  format = "gt",
  output_file = NULL,
  include_files = FALSE,
  ignore_failed_runs = FALSE,
  latest_only = FALSE,
  include_summary = FALSE
)
```

## Arguments

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it.

- format:

  Character scalar. Output format for the table. Either `"gt"` for a
  formatted gt table (default) or `"csv"` for a data frame.

- output_file:

  Character scalar or `NULL`. File path to save output.

- include_files:

  Logical scalar. Whether to include details about input and output
  files in the table. Default is `FALSE`.

- ignore_failed_runs:

  Logical scalar. Whether to exclude runs where `completed_at` is
  missing or the run finished without results (i.e., failed, incomplete
  or `"no_results"` runs). Default is `FALSE`.

- latest_only:

  Logical scalar. Whether to return only the latest run (by
  `submitted_at`) for each project file. Default is `FALSE`.

- include_summary:

  Logical scalar. Whether to include the statistics parsed from the
  run's results (see
  [`runs_data()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/runs_data.md)).
  Default is `FALSE`.

## Value

When `format = "gt"`, returns a `gt_tbl` object. When `format = "csv"`
and `output_file` is specified, returns the data frame invisibly after
writing to file. When `format = "csv"` and `output_file` is `NULL`,
returns the data frame. Returns `NULL` invisibly if no runs are found in
the store.

## See also

[`runs_data`](https://nash-delcamp-slp.github.io/mlxtrx/reference/runs_data.md)
for extracting the underlying data,
[`mono`](https://nash-delcamp-slp.github.io/mlxtrx/reference/mono.md)
for submitting Monolix jobs and recording runs,
[`default_store`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)
for the default run store

## Examples

``` r
if (FALSE) { # \dontrun{
# Table of all runs
runs_table()

# Table with input/output file information
runs_table(include_files = TRUE)

# Table excluding failed runs
runs_table(ignore_failed_runs = TRUE)

# Table showing only latest run per project file
runs_table(latest_only = TRUE)

# Table with the parsed run statistics
runs_table(include_summary = TRUE)

# Export to CSV
runs_table(format = "csv", output_file = "runs_summary.csv")

# Read the store of another folder
runs_table(store = "/path/to/monolix/.mlxtrx")
} # }
```
