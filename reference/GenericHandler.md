# Generic file handler for unknown file types

Generic file handler for unknown file types

Generic file handler for unknown file types

## Super class

[`mlxtrx::FileHandler`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.md)
-\> `GenericHandler`

## Methods

### Public methods

- [`GenericHandler$parse_file()`](#method-GenericHandler-parse_file)

- [`GenericHandler$get_input_files()`](#method-GenericHandler-get_input_files)

- [`GenericHandler$get_output_dir()`](#method-GenericHandler-get_output_dir)

- [`GenericHandler$can_monitor()`](#method-GenericHandler-can_monitor)

- [`GenericHandler$clone()`](#method-GenericHandler-clone)

Inherited methods

- [`mlxtrx::FileHandler$expects_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-expects_summary)
- [`mlxtrx::FileHandler$parse_results()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-parse_results)
- [`mlxtrx::FileHandler$parse_summary()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-parse_summary)
- [`mlxtrx::FileHandler$result_files()`](https://nash-delcamp-slp.github.io/mlxtrx/reference/FileHandler.html#method-result_files)

------------------------------------------------------------------------

### Method `parse_file()`

Parse project file and extract metadata

#### Usage

    GenericHandler$parse_file(path)

#### Arguments

- `path`:

  Path to project file

------------------------------------------------------------------------

### Method `get_input_files()`

Get input files from parsed content

#### Usage

    GenericHandler$get_input_files(parsed_content, project_dir)

#### Arguments

- `parsed_content`:

  Result from parse_file()

- `project_dir`:

  Directory containing the project file

------------------------------------------------------------------------

### Method `get_output_dir()`

Get expected output directory

#### Usage

    GenericHandler$get_output_dir(path, output_dir = NULL)

#### Arguments

- `path`:

  Path to project file

- `output_dir`:

  User-specified output directory (may be NULL)

------------------------------------------------------------------------

### Method `can_monitor()`

Check if job monitoring is supported

#### Usage

    GenericHandler$can_monitor()

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    GenericHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
