# Check Monolix/PKanalix/Simulx config file

Checks if the relevant config.ini file contains required export settings
for charts data: `exportChartsData=true` and
`export-charts-datasets=true` in `[misc]`, plus
`exportVPCSimulations=true` for Monolix and Simulx. In interactive
sessions, offers to fix missing settings.

## Usage

``` r
check_config_ini(
  cmd = getOption("mlxtrx.monolix_cmd", "mono24"),
  offer_fix = interactive()
)
```

## Arguments

- cmd:

  Monolix, PKanalix or Simulx command being executed. Defaults to
  `"mono24"`

- offer_fix:

  Whether to offer interactive fixes. Defaults to
  [`interactive()`](https://rdrr.io/r/base/interactive.html)

## Value

Returns TRUE if all settings are present

## Examples

``` r
if (FALSE) { # \dontrun{
check_config_ini()
} # }
```
