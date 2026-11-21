# Restore an archived run

Writes the archived copies of a run's files under `to`, recreating the
run's folder layout, so the run can be inspected or rerun. No existing
file is overwritten.

## Usage

``` r
restore_run(
  run_id,
  to,
  project = TRUE,
  model = FALSE,
  data = FALSE,
  outputs = TRUE,
  config = FALSE,
  store = default_store(),
  archive = NULL
)
```

## Arguments

- run_id:

  Character scalar. The run to restore; it must have been archived with
  [`archive_run()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/archive_run.md).

- to:

  Character scalar. The folder to restore into; created if needed.

- project, model, data, outputs, config:

  Logical scalars. Which groups of files to restore.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).

- archive:

  Character scalar or `NULL`. The folder that holds the `objects`
  folder, when it has moved since the run was archived. `NULL` (the
  default) uses the location the manifest records.

## Value

Invisibly, a data frame with one row per restored file and per model or
data file left out, with columns `group`, `path` (the file's original
path), `restored_to` (`NA` when not restored) and `status`:
`"restored"`, or for a model or data file left out
`"not_restored_current_matches"`, `"not_restored_current_differs"` or
`"not_restored_current_missing"` (no file at the original path now).

## Details

Files are grouped as: `project` (the project file), `model` (inputs of
role `"model"`; Monolix library models `lib:` are not files and are not
archived), `data` (every other input except the `config` file: data,
regressors, doses, external files), `outputs` (recorded outputs) and
`config` (the `config` file input and the copy of the tool's
`config.ini`). By default the project and outputs are restored and the
model and data are not, since those are often shared files maintained
elsewhere; for each model or data file left out, a message says whether
the file now at its original path matches the archived copy.

Where: a file recorded relative to the store's folder goes to the same
relative path under `to`, so the restored project's references to files
under the store's folder resolve to the restored copies. A file recorded
with an absolute path (outside the store's folder) goes under
`to/_absolute/`, a Windows drive letter becoming a folder (e.g.
`_absolute/C/data/pk.csv`); the project file is not rewritten, so it
still references the original path, and a warning names each such file
and where its archived copy was written. The `config.ini` copy goes to
`<to>/config.ini`.

Before anything is written, every chosen object is checked against its
recorded SHA-256 and every target path must be free: `to` may exist, but
no file to restore may already be there. On any problem nothing is
written and the error names the file.

Called interactively with none of `project`, `model`, `data`, `outputs`
and `config` given, a menu lists each group with its number of files and
size (and, for model and data, whether the current file matches), the
default groups marked `(default)`. Nothing is preselected; selecting
nothing restores nothing.

The archive is found where the run's manifest records it; pass `archive`
when it has moved. When the archive folder does not exist the error
names it.

## Examples

``` r
if (FALSE) { # \dontrun{
run_id <- run_status("base/run3.mlxtran")$run_id
restore_run(run_id, "restored/run3")
restore_run(run_id, "restored/run3-full", model = TRUE, data = TRUE, config = TRUE)
} # }
```
