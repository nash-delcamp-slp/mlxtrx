# PKanalix file handler

PKanalix file handler

PKanalix file handler

## Details

Reads `.pkx` projects, which share the `.mlxtran` section syntax. The
input files are every file the project references (see
[`project_file_references()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/project_file_references.md)):
the data file, the model used by compartmental analysis when the project
has one, and files such as separate dose tables. The output directory is
the `exportpath` of `<PKANALIX>` `[SETTINGS]`, or the project name next
to the project file without one.

Results are read from `PKanalix/IndividualParameters/` in the output
directory, one folder per task: `nca/ncaIndividualParameters.txt`,
`ca/caIndividualParameters.txt`, `ca/cost.txt` and
`be/confidenceInterval_<formulation>.txt`. They give the tasks with
results (`tasks`, in the order `nca,ca,be`), the number of individuals
(`n_individuals`, unique `id` in the NCA parameters, else in the CA
parameters), the CA criteria (`-2LL` as `ofv`, `AIC` as `aic`, `BIC` as
`bic`, `Cost` as `ca_cost`) and, per formulation and parameter, the
bioequivalence ratio, its confidence interval and conclusion.

## Super classes

[`mlxtrx::FileHandler`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.md)
-\>
[`mlxtrx::MonolixHandler`](https://nash-delcamp-slp.github.io/mlxtrx/reference/MonolixHandler.md)
-\> `PkanalixHandler`

## Methods

### Public methods

- [`PkanalixHandler$get_output_dir()`](#method-PkanalixHandler-get_output_dir)

- [`PkanalixHandler$result_files()`](#method-PkanalixHandler-result_files)

- [`PkanalixHandler$parse_results()`](#method-PkanalixHandler-parse_results)

- [`PkanalixHandler$clone()`](#method-PkanalixHandler-clone)

Inherited methods

- [`mlxtrx::FileHandler$can_monitor()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-can_monitor)
- [`mlxtrx::MonolixHandler$expects_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/MonolixHandler.html#method-expects_summary)
- [`mlxtrx::MonolixHandler$get_input_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/MonolixHandler.html#method-get_input_files)
- [`mlxtrx::MonolixHandler$parse_file()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/MonolixHandler.html#method-parse_file)
- [`mlxtrx::MonolixHandler$parse_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/MonolixHandler.html#method-parse_summary)

------------------------------------------------------------------------

### Method `get_output_dir()`

Get expected output directory

#### Usage

    PkanalixHandler$get_output_dir(path, output_dir = NULL)

#### Arguments

- `path`:

  Path to project file

- `output_dir`:

  User-specified output directory (may be NULL)

------------------------------------------------------------------------

### Method `result_files()`

Result files of a run into `output_dir`: the individual parameter tables
of NCA and CA, the CA cost table and the bioequivalence confidence
interval tables

#### Usage

    PkanalixHandler$result_files(output_dir, files = NULL)

#### Arguments

- `output_dir`:

  Output directory of the run

- `files`:

  Character vector of candidate file paths, such as the recorded outputs
  of a run. `NULL` (default) looks in `output_dir`.

#### Returns

The result files among `files`, or those that exist in `output_dir` when
`files` is `NULL`

------------------------------------------------------------------------

### Method `parse_results()`

Parse PKanalix result files into the content of `summary.json`

#### Usage

    PkanalixHandler$parse_results(files)

#### Arguments

- `files`:

  Result files to parse, as returned by `result_files()`

#### Returns

A list with `summary_stats`, `observations` (empty) and `bioequivalence`
(one entry per formulation and parameter), or `NULL` when the files
cannot be parsed

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    PkanalixHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
