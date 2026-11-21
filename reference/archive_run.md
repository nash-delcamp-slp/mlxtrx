# Archive the latest run of a model

Keeps an exact copy of each model's latest run so it can be checked and
brought back later, while the live project folder stays usable: nothing
in it is changed, moved or locked.

## Usage

``` r
archive_run(
  path,
  store = default_store(),
  archive = getOption("mlxtrx.archive", store)
)
```

## Arguments

- path:

  Character vector. Project files, folders, or recorded input or output
  files, as in
  [`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md).

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).

- archive:

  Character scalar. The folder that holds the `objects` folder. Defaults
  to the `mlxtrx.archive` option, or else the store folder itself
  (objects in `<store>/objects`).

## Value

Invisibly, a data frame with one row per model and columns
`project_file`, `run_id`, `n_files` (files in the manifest), `n_new`
(objects written by this call; files already in the archive are not
counted) and `bytes_new` (their total size in bytes).

## Details

What is archived, for the latest run of each model `path` covers (the
same models as
[`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md)):
the project file, every recorded input (model, data, other referenced
files and the `config` file; Monolix library models `lib:` are not files
and are skipped), every recorded output, and the copy of the tool's
`config.ini` kept with the run.

Where: each file is stored once under its SHA-256 as
`<archive>/objects/<first 2 hex>/<remaining 62 hex>`, so a file shared
by several runs (e.g. a data set) is stored only once. Each copy is
checked against the recorded hash before it is put in place, and an
object that already exists is reused, never rewritten. The run's folder
in the store gets a manifest `archive.json` listing each archived file
(its path as recorded, io type, input role, SHA-256 and size), when and
by whom it was archived, and the archive location.

Every model must be `"clean"` or `"clean_late"` in
[`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md),
so the files on disk are those the run recorded; otherwise nothing is
written and the error lists each model with its condition. A model whose
latest run is already archived gets a message and nothing is written for
it.

## Examples

``` r
if (FALSE) { # \dontrun{
archive_run("base/run3.mlxtran")
run_status("base")$archived
} # }
```
