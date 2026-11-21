# Check archived copies against their hashes

Reads the `archive.json` manifest of every run in the store, finds each
listed file's object in the archive the manifest names, and hashes it
against the recorded SHA-256. An object shared by several manifests is
hashed once per call. Temporary files left in `objects` by an
interrupted write are not referenced by any manifest and are ignored.
Nothing is changed.

## Usage

``` r
verify_archive(store = default_store(), archive = NULL)
```

## Arguments

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).

- archive:

  Character scalar or `NULL`. The folder that holds the `objects`
  folder, for an archive that has moved since it was written. `NULL`
  (the default) uses the location each manifest records.

## Value

A data frame with one row per manifest entry and columns `run_id` (the
run's folder name), `path` (the archived file's original path), `sha256`
(the recorded hash) and `status`: `"ok"` (the object matches),
`"missing"` (no object) or `"mismatch"` (the object's content has a
different hash). A one-line summary is printed as a message.

## Details

A store folder that does not exist, or a manifest that cannot be read
(e.g. partly written), is an error naming it.

## Examples

``` r
if (FALSE) { # \dontrun{
verify_archive()
} # }
```
