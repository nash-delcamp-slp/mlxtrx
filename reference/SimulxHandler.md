# Simulx file handler

Simulx file handler

Simulx file handler

## Details

Reads `.smlx` projects, which share the `.mlxtran` section syntax. The
input files are every file the project references. The output directory
is the `exportpath` of `<SIMULX>` `[SETTINGS]`, or the project name next
to the project file without one. A run has results when it wrote files
under `Simulation/` or `Endpoints/` in the output directory; `tasks`
names the tasks with results (`simulation`, `endpoint`).

## Super classes

[`mlxtrx::FileHandler`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.md)
-\>
[`mlxtrx::MonolixHandler`](https://nash-delcamp-slp.github.io/mlxtrx/reference/MonolixHandler.md)
-\> `SimulxHandler`

## Methods

### Public methods

- [`SimulxHandler$get_output_dir()`](#method-SimulxHandler-get_output_dir)

- [`SimulxHandler$result_files()`](#method-SimulxHandler-result_files)

- [`SimulxHandler$parse_results()`](#method-SimulxHandler-parse_results)

- [`SimulxHandler$clone()`](#method-SimulxHandler-clone)

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

    SimulxHandler$get_output_dir(path, output_dir = NULL)

#### Arguments

- `path`:

  Path to project file

- `output_dir`:

  User-specified output directory (may be NULL)

------------------------------------------------------------------------

### Method `result_files()`

Result files of a run into `output_dir`: the files under `Simulation/`
and `Endpoints/`

#### Usage

    SimulxHandler$result_files(output_dir, files = NULL)

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

Parse Simulx result files into the content of `summary.json`: the tasks
with results

#### Usage

    SimulxHandler$parse_results(files)

#### Arguments

- `files`:

  Result files, as returned by `result_files()`

#### Returns

A list with `summary_stats` (`tasks`) and `observations` (empty), or
`NULL` when `files` is empty

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    SimulxHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
