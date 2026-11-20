# Execute a single Monolix job

Submits a single Monolix job to the grid and extracts the job ID.

## Usage

``` r
execute_job(
  path,
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL,
  cmd = "mono24"
)
```

## Arguments

- path:

  Character scalar. Path to a single existing Monolix project file.

- output_dir:

  Character scalar or `NULL`. Directory for output files.

- thread:

  Integer scalar or `NULL`. Number of threads used by Monolix.

- tool:

  Character scalar or `NULL`. Tool to launch assessment.

- mode:

  Character scalar or `NULL`. Console mode.

- config:

  Character scalar or `NULL`. Configuration file path.

- cmd:

  Character scalar. The Monolix command to execute.

## Value

A list with `job_id` (integer, `NA` when no job ID was found), `ok`
(logical) and `output` (character, the command's output). For the local
runners `monolix.sh`, `simulx.sh` and `pkanalix.sh`, `ok` is `TRUE` when
the command exits with status 0. For other commands, `ok` is `TRUE` when
a job ID was found or the command reports that results have been loaded.
