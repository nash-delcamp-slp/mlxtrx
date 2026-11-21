# Retrieve one archived file

Writes the archived copy of one file of a run to `to`, under the file's
name. The object is checked against its recorded SHA-256 first, and an
existing file is never overwritten. The archive is found where the run's
manifest records it, or at `archive` when given.

## Usage

``` r
retrieve_file(run_id, path, to, store = default_store(), archive = NULL)
```

## Arguments

- run_id:

  Character scalar. The run; it must have been archived with
  [`archive_run()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/archive_run.md).

- path:

  Character scalar. The file as recorded in the run: its recorded path,
  its path from the working directory, or its path relative to the
  store's folder.

- to:

  Character scalar. The folder to write into; created if needed.

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

Invisibly, the path of the written file.

## Examples

``` r
if (FALSE) { # \dontrun{
run_id <- run_status("base/run3.mlxtran")$run_id
retrieve_file(run_id, "base/data/pk.csv", "old")
} # }
```
