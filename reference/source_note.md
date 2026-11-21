# One-line source footnote for a table, figure or listing

Names the runs, and their project files, that produced the plot or table
data files behind a table, figure or listing (TFL), so the TFL can be
traced back to its run IDs. Intended as a footnote. Each run appears
once, in the order its first file appears in `files`. Project paths and
the paths of files no run recorded are relative to the folder that holds
the store where possible. A file no run recorded is listed as
`<file> (no run record)`. An empty `files` is an error.

## Usage

``` r
source_note(files, store = default_store())
```

## Arguments

- files:

  Character vector. Paths to the plot or table data files the TFL was
  made from.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it.

## Value

A character scalar such as
`"Source: theoph/theoph.mlxtran (run 20260101T120000Z-ab12)"`.

## Details

A warning lists every file whose current state is not `"unchanged"` or
`"unchanged_late"` (see
[`check_output_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/check_output_files.md)),
with its status, so a footnote is not trusted for files that were
edited, deleted or never recorded.

## Examples

``` r
if (FALSE) { # \dontrun{
note <- source_note(c("theoph/theoph/summary.txt"))
ggplot2::ggplot(df, ggplot2::aes(time, conc)) +
  ggplot2::geom_line() +
  ggplot2::labs(caption = note)
} # }
```
