# Changelog

## mlxtrx 0.1.0

First release.

### Launching runs

- [`mono()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/mono.md),
  [`pk()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/mono.md)
  and
  [`sim()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/mono.md)
  launch Monolix, PKanalix and Simulx projects, many per call, on the
  grid (`mono24`, `pk24`, `sim24`) or locally (`monolix.sh`,
  `pkanalix.sh`, `simulx.sh`).
- Before launching, the command is checked and
  [`check_config_ini()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_config_ini.md)
  checks the charts-data export settings in the tool’s `config.ini`.

### Run store

- Runs are recorded in a store of plain JSON files, one folder per run,
  set with `options(mlxtrx.store = )` or `store =`
  ([`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Files are written atomically and never overwritten, so concurrent
  sessions can share a store.
- Each run records the command, settings, software version, user, host,
  submission and completion times, and every input and output file with
  its SHA-256 hash. Inputs are every file the project references, plus a
  config file passed with `config =`.
- The software version comes from the run’s `summary.txt` when Monolix
  or PKanalix reports it, else from the command.
- [`finalize_runs()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/finalize_runs.md)
  records runs whose launching session ended before they finished,
  flagging outputs written well after the rest.

### Results

- Run states: `submit_failed`, `unrecorded`, `completion_crashed`,
  `recorded`, `recorded_late` and `no_results`.
- Statistics: Monolix values from `summary.txt` and observation counts;
  PKanalix tasks, individuals, compartmental analysis criteria and
  bioequivalence ratios; Simulx tasks.
- [`runs_data()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/runs_data.md)
  returns every run as one data frame;
  [`get_run_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/get_run_files.md)
  lists a run’s files.

### Status

- [`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md)
  gives one condition per model, file or folder: `never_run`, `failed`,
  `unfinished`, `inputs_changed`, `output_edited`, `output_suspect`,
  `clean_late`, `clean` or `untracked`.
  [`print_report_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/print_report_summary.md)
  prints it as a report.
- [`check_completion_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_completion_status.md),
  [`check_project_file_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_project_file_status.md),
  [`check_input_file_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_input_file_status.md),
  [`check_output_file_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_output_file_status.md)
  and
  [`check_output_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_output_files.md)
  answer narrower questions about project, input and output files.
