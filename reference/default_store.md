# Get the run store path

Returns the run store directory from the `mlxtrx.store` option. The
value is the store directory itself, conventionally the `.mlxtrx` folder
inside the folder that holds the Monolix projects. A relative value
resolves against the working directory at call time. The directory is
created when the first run is recorded, not when it is read.

## Usage

``` r
default_store()
```

## Value

Character scalar. Absolute path to the run store.

## Details

Set the option once per session with
`options(mlxtrx.store = "/path/to/monolix/.mlxtrx")`. Put that line in
the project's `.Rprofile` to set it for every session.

## Examples

``` r
if (FALSE) { # \dontrun{
options(mlxtrx.store = "/path/to/monolix/.mlxtrx")
default_store()
} # }
```
