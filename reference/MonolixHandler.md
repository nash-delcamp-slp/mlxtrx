# Monolix file handler

Monolix file handler

Monolix file handler

## Super class

[`mlxtrx::FileHandler`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.md)
-\> `MonolixHandler`

## Methods

### Public methods

- [`MonolixHandler$parse_file()`](#method-MonolixHandler-parse_file)

- [`MonolixHandler$get_input_files()`](#method-MonolixHandler-get_input_files)

- [`MonolixHandler$get_output_dir()`](#method-MonolixHandler-get_output_dir)

- [`MonolixHandler$expects_summary()`](#method-MonolixHandler-expects_summary)

- [`MonolixHandler$parse_summary()`](#method-MonolixHandler-parse_summary)

- [`MonolixHandler$clone()`](#method-MonolixHandler-clone)

Inherited methods

- [`mlxtrx::FileHandler$can_monitor()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-can_monitor)
- [`mlxtrx::FileHandler$parse_results()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-parse_results)
- [`mlxtrx::FileHandler$result_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-result_files)

------------------------------------------------------------------------

### Method `parse_file()`

Parse project file and extract metadata

#### Usage

    MonolixHandler$parse_file(path)

#### Arguments

- `path`:

  Path to project file

------------------------------------------------------------------------

### Method `get_input_files()`

Input files of a project: every file the project references (see
[`project_file_references()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/project_file_references.md))

#### Usage

    MonolixHandler$get_input_files(path)

#### Arguments

- `path`:

  Path to project file

#### Returns

A list of `list(role, path)`

------------------------------------------------------------------------

### Method `get_output_dir()`

Get expected output directory

#### Usage

    MonolixHandler$get_output_dir(path, output_dir = NULL)

#### Arguments

- `path`:

  Path to project file

- `output_dir`:

  User-specified output directory (may be NULL)

------------------------------------------------------------------------

### Method `expects_summary()`

Monolix runs produce a summary

#### Usage

    MonolixHandler$expects_summary()

------------------------------------------------------------------------

### Method `parse_summary()`

Parse Monolix summary file

#### Usage

    MonolixHandler$parse_summary(summary_file_path)

#### Arguments

- `summary_file_path`:

  Path to summary.txt file

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    MonolixHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
