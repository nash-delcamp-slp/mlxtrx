# Report one condition per model

Tells, for each model a path covers, whether the results on disk still
match the files that produced them. Each model is judged by its latest
run (the run with the latest `submitted_at` for that project file,
whatever its state), the same rule as
[`check_completion_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_completion_status.md).
Only file content (SHA-256) counts: a changed timestamp with the same
content is not a change.

## Usage

``` r
run_status(path = ".", store = default_store())
```

## Arguments

- path:

  Character vector. Files or directories to report on. Relative paths
  resolve against the working directory. Defaults to `"."`.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it, nor anywhere else.

## Value

A data frame with one row per path and model (duplicates removed), with
columns:

- `path` (character): The path given, absolute and normalised

- `project_file` (character): Full path to the model's project file (NA
  when untracked)

- `run_id` (character): The model's latest run (NA if none)

- `condition` (character): The most serious condition that applies

- `other_conditions` (character): The other conditions that apply,
  comma-separated, most serious first (NA if none)

- `details` (character): Short notes behind the conditions, separated by
  `"; "`. A failed or unfinished run's state comes first
  (`"submit failed"`, `"no results"`, `"completion not recorded"` or
  `"completion crashed"`), then the files, named relative to the folder
  that holds the store where possible, e.g.
  `"data/pk.csv modified; run1/summary.txt missing"`; `"recorded late"`
  for `"clean_late"`; for an untracked path that only older runs
  recorded, `"recorded only by an older run (<run_id>)"` naming the most
  recent of them (NA if none)

- `archived` (logical): Whether the latest run has been archived with
  [`archive_run()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/archive_run.md)
  (FALSE when there is no run; NA when untracked)

- `submitted_at` (POSIXct): When the latest run was submitted

- `completed_at` (POSIXct): When the latest run completed (NA if not
  completed)

## Details

What `path` covers:

- a directory: every `.mlxtran`, `.pkx` and `.smlx` file under it,
  recursively, one row each (as in
  [`check_completion_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_completion_status.md));

- a project file: one row;

- a file recorded as an input or output of the latest run of some
  models: one row per such model;

- any other existing file: one row with condition `"untracked"`.

A path that does not exist and is not recorded in the store is an error.

Conditions, most serious first:

1.  `"never_run"`: no run of the model is recorded.

2.  `"untracked_results"`: results exist in the model's output folder
    but no recorded run produced them, e.g. a run from the GUI or
    lixoftConnectors.

3.  `"failed"`: the latest run failed to submit or finished without
    results.

4.  `"unfinished"`: the latest run's completion was not recorded, or
    recording it crashed.

5.  `"inputs_changed"`: the project file or a recorded input of the
    latest run has different content or is missing. Monolix library
    models (`lib:`) never count.

6.  `"output_edited"`: a recorded output of the latest run has different
    content or is missing.

7.  `"output_suspect"`: an unchanged recorded output was flagged when
    the latest run's completion was recorded late.

8.  `"clean_late"`: the latest run was recorded late and nothing above
    applies.

9.  `"clean"`: nothing above applies.

10. `"untracked"`: the path is not a model, nor a file recorded by the
    latest run of any model.

## See also

[`print_report_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/print_report_summary.md)
