# Get file information for specific runs

Reads the project, input and output file records of one or more runs
from the run store.

## Usage

``` r
get_run_files(run_id, store = default_store())
```

## Arguments

- run_id:

  Character vector. The run ID(s) to return file records for.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it.

## Value

A data frame with columns: `run_id`, `io_type` (`"project"`, `"input"`
or `"output"`), `path`, `name`, `timestamp`, `sha256`, `size`, `builtin`
and `role` (an input's role: `"data"`, `"model"`, `"other"` or
`"config"`; NA for project and output rows), ordered by `run_id`,
`io_type` and `path`. Timestamps are in the session time zone. Has no
rows when none of the runs have file records.
