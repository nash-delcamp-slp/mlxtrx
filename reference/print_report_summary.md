# Print a run status report

Prints a plain-text report of
[`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md)
for `path`: the path, the time of the report, how many models are in
each condition, and, for every model that is not `"clean"`, its
condition (with any other conditions in parentheses) and its details. It
is a printed view of
[`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md),
which it calls once, so the two always agree.

## Usage

``` r
print_report_summary(path = ".", store = default_store())
```

## Arguments

- path:

  Character vector. Files or directories to report on, as in
  [`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md).
  Defaults to `"."`.

- store:

  Character scalar. The run store directory (see
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md)).
  Defaults to
  [`default_store()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/default_store.md).
  Nothing is written to it.

## Value

The data frame from
[`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md),
invisibly.

## Details

Conditions are listed most serious first, as in
[`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md).
Project files are shown relative to `path` when `path` is a directory
and they are under it; untracked paths are shown as given.

## See also

[`run_status()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/run_status.md)
