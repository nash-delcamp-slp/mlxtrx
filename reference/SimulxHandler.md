# Simulx file handler

Simulx file handler

Simulx file handler

## Super class

[`mlxtrx::FileHandler`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.md)
-\> `SimulxHandler`

## Methods

### Public methods

- [`SimulxHandler$parse_file()`](#method-SimulxHandler-parse_file)

- [`SimulxHandler$get_input_files()`](#method-SimulxHandler-get_input_files)

- [`SimulxHandler$get_output_dir()`](#method-SimulxHandler-get_output_dir)

- [`SimulxHandler$clone()`](#method-SimulxHandler-clone)

Inherited methods

- [`mlxtrx::FileHandler$can_monitor()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-can_monitor)
- [`mlxtrx::FileHandler$expects_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-expects_summary)
- [`mlxtrx::FileHandler$parse_results()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-parse_results)
- [`mlxtrx::FileHandler$parse_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-parse_summary)
- [`mlxtrx::FileHandler$result_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-result_files)

------------------------------------------------------------------------

### Method `parse_file()`

Parse project file and extract metadata

#### Usage

    SimulxHandler$parse_file(path)

#### Arguments

- `path`:

  Path to project file

------------------------------------------------------------------------

### Method `get_input_files()`

Get input files from parsed content

#### Usage

    SimulxHandler$get_input_files(parsed_content, project_dir)

#### Arguments

- `parsed_content`:

  Result from parse_file()

- `project_dir`:

  Directory containing the project file

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

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    SimulxHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
