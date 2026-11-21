# mlxtrx

mlxtrx launches Monolix, PKanalix and Simulx runs and keeps a record of each
one: what ran, with which settings and files, when, and by whom. Every file is
fingerprinted by its SHA-256 hash, so at any time you can ask whether the
results on disk still match the files that produced them.

- Launch many models in one call, on the grid (`mono24`, `pk24`, `sim24`) or
  locally (`monolix.sh`, `pkanalix.sh`, `simulx.sh`).
- Detect whether each run succeeded and extract its fit statistics.
- Get one condition per model, file or folder: never run, failed, unfinished,
  inputs changed, output edited, or clean.
- Check the export settings MonolixSuite needs before launching.
- Collect every run in one data frame to sort, filter or export.

## Installation

```r
# install.packages("remotes")
remotes::install_github("nash-delcamp-slp/mlxtrx")
```

## Set the run store

Runs are recorded in a store: a folder of plain JSON files, one folder per
run. Point mlxtrx at it once per session, usually from the project's
`.Rprofile`:

```r
options(mlxtrx.store = "/path/to/<project>/monolix/.mlxtrx")
```

Every function also takes `store =` directly. Without either, mlxtrx stops and
tells you how to set it. The store is created when the first run is recorded.

## Launch runs

```r
library(mlxtrx)

run_ids <- mono(c("base/run1.mlxtran", "base/run2.mlxtran"), thread = 4)
pk("nca/<study>_nca.pkx")
sim("sim/<project>_sim.smlx")
```

`mono()` and `pk()` first check the charts-data export settings in the
tool's `config.ini` (`check_config_ini()`). All three record each run as it is
submitted, wait for the grid jobs to leave the queue, then record the outputs
and statistics. The default commands are `mono24`, `pk24` and `sim24`; set
`options(mlxtrx.monolix_cmd = )`, `mlxtrx.pkanalix_cmd` or `mlxtrx.simulx_cmd`,
or pass `cmd =`, to use another command such as a local `monolix.sh`.

Projects built or edited with lixoftConnectors: save them with
`saveProject()`, then run them with `mono()` or `pk()`.

## Check status

```r
run_status("base")
print_report_summary("base")
#> === RUN STATUS REPORT ===
#> Path: /path/to/<project>/monolix/base
#> ...
#> Models: 3
#>   inputs_changed  1
#>   clean           2
#>
#> Needs attention:
#>   run2.mlxtran  inputs_changed
#>     data/<study>_pk.csv modified
```

`run_status()` returns one row per model with a single condition, most serious
first: `never_run`, `untracked_results` (results in the model's output folder
that no recorded run produced, e.g. from the GUI or lixoftConnectors),
`failed`, `unfinished`, `inputs_changed`, `output_edited`, `output_suspect`,
`clean_late`, `clean`. A file that no run recorded is
`untracked`. Pass a folder, a project file, or any input or output file.
`print_report_summary()` prints the same result as a report.

The `check_*()` functions answer narrower questions about project, input and
output files.

## Archive key runs

```r
archive_run("base/run3.mlxtran")
run_status("base")$archived
```

`archive_run()` keeps an exact copy of a model's latest run: the project file,
its inputs, its outputs and the run's `config.ini` copy. Each file is stored
once under its SHA-256 in `<store>/objects` (set `options(mlxtrx.archive = )`
for another folder), so a data set shared by several runs is stored once.
The run must be `clean` or `clean_late`. The project folder is not touched
and stays usable.

`verify_archive()` re-hashes every archived file and reports each one as `ok`,
`missing` or `mismatch`.

```r
run_id <- run_status("base/run3.mlxtran")$run_id
restore_run(run_id, "restored/run3")
# add the data and config files to that restore
restore_run(run_id, "restored/run3", project = FALSE, outputs = FALSE,
            data = TRUE, config = TRUE)
retrieve_file(run_id, "base/data/pk.csv", "old")
```

`restore_run()` writes an archived run's files under `to` in the same folder
layout, so the restored project finds the restored copies of files under the
store's folder. By default it restores the project and outputs and leaves out
the model and data (`model = FALSE`, `data = FALSE`), since those are often
shared files maintained elsewhere; for each one left out it says whether the
file now at its original path matches the archived copy. `config = TRUE` adds
the `config` file and the run's `config.ini`. Files with an absolute path
outside the store's folder go under `to/_absolute/`; the project file is not
rewritten, so it still points at the original location, and a warning names
each such file and its restored copy. Nothing is written if any target file
already exists or any archived copy fails its hash check. Called
interactively without any of these arguments, it shows a menu of the groups
with their file counts and sizes, the defaults marked `(default)`.
`retrieve_file()` writes one archived file. If the archive folder has moved,
pass its new location as `archive =` to `verify_archive()`, `restore_run()`
or `retrieve_file()`.

## Results table

`runs_data()` returns every recorded run as one data frame, newest first:
project, data and model files, settings, command, software version, user,
times and state. Sort, filter or export it like any data frame.

```r
runs <- runs_data(include_summary = TRUE, latest_only = TRUE)
write.csv(runs, "runs.csv", row.names = FALSE)
get_run_files(runs$run_id[1])
```

Statistics include, for Monolix, the values from `summary.txt` and observation
counts; for PKanalix, the tasks run, the number of individuals, the
compartmental analysis criteria and the bioequivalence ratios; for Simulx, the
tasks run. `get_run_files()` lists every input and output file of a run with
its SHA-256 hash. `source_note()` turns the plot or table data files behind a
figure or table into a one-line footnote naming their runs, for TFL footnotes.

## When a session dies

If the R session that launched runs ends before they finish, their completion
is not recorded. Record it later with:

```r
finalize_runs()
```

These runs are marked as recorded late, and any output written well after the
rest of the run's outputs is flagged as suspect.
