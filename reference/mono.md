# Submit Monolix, PKanalix and Simulx jobs to grid and record runs in the run store

Executes one or more commands that submit Monolix, PKanalix or Simulx
jobs to the grid and records each run in the run store. `pk()` and
`sim()` call `mono()` with the PKanalix and Simulx commands.

## Usage

``` r
mono(
  path,
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL,
  cmd = getOption("mlxtrx.monolix_cmd", "mono24"),
  store = default_store(),
  require_charts_data = TRUE
)

pk(
  path,
  thread = NULL,
  mode = NULL,
  cmd = getOption("mlxtrx.pkanalix_cmd", "pk24"),
  store = default_store(),
  require_charts_data = TRUE
)

sim(
  path,
  thread = NULL,
  mode = NULL,
  cmd = getOption("mlxtrx.simulx_cmd", "sim24"),
  store = default_store(),
  require_charts_data = FALSE
)
```

## Arguments

- path:

  Character vector. Paths to existing Monolix (`.mlxtran`), PKanalix
  (`.pkx`) or Simulx (`.smlx`) project files.

- output_dir:

  Character vector or `NULL`. Directory for output files relative to the
  project path. Passed as `--output-dir` argument to the Monolix
  command. If `NULL` (default), Monolix uses the export path from the
  project file. Should be same length as `path`, length 1, or `NULL`.

- thread:

  Integer vector or `NULL`. Number of threads. Passed as `--thread`
  argument to the command. Should be same length as `path`, length 1, or
  `NULL`.

- tool:

  Character vector or `NULL`. Tool to launch assessment (`"monolix"`,
  `"modelBuilding"`, `"bootstrap"`). Passed as `--tool` argument to the
  Monolix command. If `NULL` (default), Monolix uses `"monolix"`. Should
  be same length as `path`, length 1, or `NULL`.

- mode:

  Character vector or `NULL`. Console mode (`"none"`, `"basic"`, or
  `"complete"`). Passed as `--mode` argument to the command. If `NULL`
  (default), the command uses `"basic"`. Should be same length as
  `path`, length 1, or `NULL`.

- config:

  Character vector or `NULL`. Configuration file path. Passed as
  `--config` argument to the Monolix command, and recorded as an input
  of the run (role `"config"`) so a later edit shows as a changed input.
  Should be same length as `path`, length 1, or `NULL`.

- cmd:

  Character vector. The command to execute. Must be identifiable by
  [`Sys.which()`](https://rdrr.io/r/base/Sys.which.html) and contain
  "mono", "pk" or "sim" in the name: a grid command such as `mono24`,
  `pk24` or `sim24`, or a local runner `monolix.sh`, `pkanalix.sh` or
  `simulx.sh`. Defaults to the `mlxtrx.monolix_cmd` option or "mono24"
  for `mono()`, the `mlxtrx.pkanalix_cmd` option or "pk24" for `pk()`
  and the `mlxtrx.simulx_cmd` option or "sim24" for `sim()`. Should be
  same length as `path` or length 1.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Created when the first run is recorded.

- require_charts_data:

  A logical indicating whether charts data must be exported. This
  requires the relevant config.ini file includes options
  `exportChartsData=true`, `export-charts-datasets=true`, and, except
  for PKanalix, `exportVPCSimulations=true` (see
  [`check_config_ini()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_config_ini.md)).

## Value

Character vector. The run IDs of all jobs, in the order of `path`.

## Details

Jobs are submitted one at a time. For each job, this function:

1.  Records the project and input files (path, SHA-256, size,
    modification time) and the settings passed to the command, and
    creates a run folder `<store>/runs/<run_id>/`

2.  Submits the job using
    [`system2()`](https://rdrr.io/r/base/system2.html) and extracts the
    job ID from the submission output

3.  Writes `run.json` straight away: the records above, the command, job
    ID, software version, submission time and output, user and host

If a step fails, no further jobs are submitted and the error names the
runs already submitted, which
[`finalize_runs()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/finalize_runs.md)
can record once they finish.

A submission that yields no job ID and no local result is recorded with
`submission.ok = false` and is not monitored.

Once all jobs are submitted, the function polls the job queue until
every submitted job has finished, then records each run's completion in
a `completion/` folder of the run: the output files written since
submission (`outputs.json`), the statistics parsed from the run's
results (`summary.json`: `summary.txt` for Monolix, the result tables
under `PKanalix/IndividualParameters` for PKanalix, the files under
`Simulation/` and `Endpoints/` for Simulx) and, last, `done.json`. A
Monolix, PKanalix or Simulx run recorded without `summary.json` is in
state `"no_results"` (see
[`runs_data()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/runs_data.md)).

The local runners `monolix.sh`, `pkanalix.sh` and `simulx.sh` run the
project in the foreground with `--no-gui`; the submission succeeded when
the command exits with status 0, and the run is recorded straight away.

Paths inside the store are relative to the folder that holds the store
when the file is inside it, and absolute otherwise.

## Examples

``` r
if (FALSE) { # \dontrun{
# Basic usage with default settings
mono("path/to/project.mlxtran")

# Multiple jobs
mono(c("project1.mlxtran", "project2.mlxtran"))

# Specify custom output directory and thread count
mono(
  path = "project.mlxtran",
  output_dir = "custom-project-output",
  thread = 4
)

# Record runs in a specific store
mono("project.mlxtran", store = "path/to/monolix/.mlxtrx")

# PKanalix project on the grid, or with the local runner
pk("project.pkx")
pk("project.pkx", cmd = "/path/to/MonolixSuite2024R1/bin/pkanalix.sh")
} # }
```
