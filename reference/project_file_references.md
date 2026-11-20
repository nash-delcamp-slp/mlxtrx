# Files referenced by a MonolixSuite project file

Scans a `.mlxtran`, `.pkx` or `.smlx` project for quoted values of
`file`, `origin` and `path` keys, e.g. `file = 'data/pk.csv'`,
`additionalColumns = {{file='data/regressors.txt'}}` or
`administrations={{file={path='data/doses.xlsx', sheet='doses'}}}`.
Comment lines (starting with `;`) are skipped. A line `file = ...` under
`[FILEINFO]` gives role `"data"`, under `[LONGITUDINAL]` role `"model"`;
every other reference has role `"other"`. A file referenced more than
once keeps its first role. Paths are resolved against the project's
folder; built-in library models (`lib:...`) are kept as written.

## Usage

``` r
project_file_references(path)
```

## Arguments

- path:

  Path to the project file.

## Value

A list of `list(role, path)` in the order of the project file.
