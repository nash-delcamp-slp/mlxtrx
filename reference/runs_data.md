# Extract data about executed Monolix runs

Reads the run store for Monolix jobs that have been submitted and
recorded. Returns a data frame with basic job information or detailed
file tracking data.

## Usage

``` r
runs_data(
  store = default_store(),
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

- include_files:

  Logical scalar. Whether to include details about input and output
  files in the data. Default is `FALSE`.

- ignore_failed_runs:

  Logical scalar. Whether to exclude runs where `completed_at` is
  missing or the run finished without results (i.e., failed, incomplete
  or `"no_results"` runs). Default is `FALSE`.

- latest_only:

  Logical scalar. Whether to return only the latest run (by
  `submitted_at`) for each project file. Default is `FALSE`.

- include_summary:

  Logical scalar. Whether to include the statistics parsed from the
  run's results (`summary.txt` for Monolix, the result tables for
  PKanalix, the `Simulation/` and `Endpoints/` files for Simulx).
  Default is `FALSE`.

## Value

A data frame with run information, newest submission first. Contains
columns: `run_id` (character), `job_id`, `project_name`, `project_path`,
`data_file`, `model_file`, `thread`, `tool`, `mode`, `config`,
`project_file_timestamp`, `project_file_sha256`, `cmd`,
`software_version`, `user`, `submitted_at`, `completed_at`, `state`,
`recorded_late`. `user` is the account that launched the run.
`software_version` is the MonolixSuite version stated in the run's
`summary.txt` (Monolix, PKanalix) when recorded, else the version
implied by the command (e.g. `"2024R1"` for `mono24`), else `NA`.
`state` is one of `"submit_failed"`, `"unrecorded"`,
`"completion_crashed"`, `"recorded"`, `"recorded_late"` or
`"no_results"` (recorded, but a Monolix, PKanalix or Simulx run without
results). When `include_files = TRUE`, additionally contains one row per
input and output file with `io_type`, `file_name`, `file_path`,
`file_timestamp` and `sha256`. When `include_summary = TRUE`,
additionally contains the run statistics: for Monolix, those of
summary.txt and one `n_obs_<type>` column per observation type; for
PKanalix, `tasks` (the tasks with results, e.g. `"nca,ca,be"`),
`n_individuals`, the compartmental analysis criteria `ofv` (-2LL),
`aic`, `bic` and `ca_cost`, and per bioequivalence parameter the columns
`be_<parameter>_ratio`, `be_<parameter>_ci_lower`,
`be_<parameter>_ci_upper` and `be_<parameter>_bioequivalent` (logical).
A run with more than one formulation gets
`be_<formulation>_<parameter>_...` columns. For Simulx, `tasks`
(`"simulation"`, `"simulation,endpoint"`). Columns of the other tools
are `NA`. Characters in `<type>`, `<parameter>` and `<formulation>`
other than letters, digits and `_` are replaced by `_`, so `"obsid 1"`
gives `n_obs_obsid_1`. Timestamps are in the session time zone. Returns
`NULL` invisibly, with a message, when there are no runs.
