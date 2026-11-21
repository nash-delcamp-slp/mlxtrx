# Package index

## Launch runs

Submit Monolix, PKanalix and Simulx projects and record each run.

- [`mono()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/mono.md)
  [`pk()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/mono.md)
  [`sim()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/mono.md)
  : Submit Monolix, PKanalix and Simulx jobs to grid and record runs in
  the run store

## Status

One condition per model, as a data frame or a printed report.

- [`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md)
  : Report one condition per model
- [`print_report_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/print_report_summary.md)
  : Print a run status report

## File checks

Per-file and per-project checks against the recorded runs.

- [`check_completion_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_completion_status.md)
  : Check completion status of project files in a directory
- [`check_project_file_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_project_file_status.md)
  : Check project file modification status
- [`check_input_file_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_input_file_status.md)
  : Check input file modification status
- [`check_output_file_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_output_file_status.md)
  : Check output file integrity
- [`check_output_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_output_files.md)
  : Check specified output files alignment with latest runs

## Results

Recorded runs, their files and statistics as data frames.

- [`runs_data()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/runs_data.md)
  : Extract data about executed Monolix runs
- [`get_run_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/get_run_files.md)
  : Get file information for specific runs
- [`source_note()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/source_note.md)
  : One-line source footnote for a table, figure or listing

## Store and recovery

- [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)
  : Get the run store path
- [`finalize_runs()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/finalize_runs.md)
  : Record completion of runs whose launching session ended

## Setup

- [`check_config_ini()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_config_ini.md)
  : Check Monolix/PKanalix/Simulx config file
