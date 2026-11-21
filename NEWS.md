# mlxtrx 0.2.0

## Archiving key runs

* `archive_run()` keeps a complete copy of a clean run: the project file,
  every input (data, model, regressor, dose and Simulx external files, a
  `config =` file), every recorded output and the run's `config.ini` copy.
  Files are stored once each under their SHA-256, inside the store by default
  or in another folder (`options(mlxtrx.archive = )` or `archive =`). The live
  project folder stays fully usable.
* `verify_archive()` checks every archived copy against its hash.
* `restore_run()` rebuilds chosen parts of an archived run in a new folder,
  never overwriting a file. By default it restores the project and its
  outputs; model, data and config files are opt-in, and for each model or
  data file left out it says whether the current file still matches the
  archived copy. Called interactively without choosing parts, it shows a
  menu.
* `retrieve_file()` fetches one archived file, e.g. the exact data a run used.
* `run_status()` gains an `archived` column.

## Status and sources

* `run_status()` reports `untracked_results` for a model whose output folder
  holds results that no recorded run produced, e.g. from the GUI or
  lixoftConnectors.
* `source_note()` returns a TFL footnote naming the runs behind its source
  files, and warns when a source file changed. `check_output_files()` gains
  `project_path`.

## Reproducibility

* Each run keeps a copy of the tool's `config.ini` in its folder in the store,
  recorded in `run.json`. `run_status()` does not compare it, so editing
  preferences doesn't flag models.

## Fixes

* Project files with upper-case extensions (`.MLXTRAN`, `.PKX`, `.SMLX`) are
  found by `run_status()` and the `check_*()` functions.
* A runner given by name (`cmd = "monolix.sh"`) is located on PATH, so its
  software version and `config.ini` copy are recorded.

# mlxtrx 0.1.0

First release.

## Launching runs

* `mono()`, `pk()` and `sim()` launch Monolix, PKanalix and Simulx projects,
  many per call, on the grid (`mono24`, `pk24`, `sim24`) or locally
  (`monolix.sh`, `pkanalix.sh`, `simulx.sh`).
* Before launching, the command is checked and `check_config_ini()` checks
  the charts-data export settings in the tool's `config.ini`.

## Run store

* Runs are recorded in a store of plain JSON files, one folder per run, set
  with `options(mlxtrx.store = )` or `store =` (`default_store()`). Files are
  written atomically and never overwritten, so concurrent sessions can share
  a store.
* Each run records the command, settings, software version, user, host,
  submission and completion times, and every input and output file with its
  SHA-256 hash. Inputs are every file the project references, plus a config
  file passed with `config =`.
* The software version comes from the run's `summary.txt` when Monolix or
  PKanalix reports it, else from the command.
* `finalize_runs()` records runs whose launching session ended before they
  finished, flagging outputs written well after the rest.

## Results

* Run states: `submit_failed`, `unrecorded`, `completion_crashed`,
  `recorded`, `recorded_late` and `no_results`.
* Statistics: Monolix values from `summary.txt` and observation counts;
  PKanalix tasks, individuals, compartmental analysis criteria and
  bioequivalence ratios; Simulx tasks.
* `runs_data()` returns every run as one data frame; `get_run_files()` lists
  a run's files.

## Status

* `run_status()` gives one condition per model, file or folder:
  `never_run`, `failed`, `unfinished`, `inputs_changed`, `output_edited`,
  `output_suspect`, `clean_late`, `clean` or `untracked`.
  `print_report_summary()` prints it as a report.
* `check_completion_status()`, `check_project_file_status()`,
  `check_input_file_status()`, `check_output_file_status()` and
  `check_output_files()` answer narrower questions about project, input and
  output files.
