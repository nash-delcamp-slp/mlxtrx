# Base file handler class

Base file handler class

Base file handler class

## Methods

### Public methods

- [`FileHandler$parse_file()`](#method-FileHandler-parse_file)

- [`FileHandler$get_input_files()`](#method-FileHandler-get_input_files)

- [`FileHandler$get_output_dir()`](#method-FileHandler-get_output_dir)

- [`FileHandler$can_monitor()`](#method-FileHandler-can_monitor)

- [`FileHandler$parse_summary()`](#method-FileHandler-parse_summary)

- [`FileHandler$expects_summary()`](#method-FileHandler-expects_summary)

- [`FileHandler$result_files()`](#method-FileHandler-result_files)

- [`FileHandler$parse_results()`](#method-FileHandler-parse_results)

- [`FileHandler$clone()`](#method-FileHandler-clone)

------------------------------------------------------------------------

### Method `parse_file()`

Parse project file and extract metadata

#### Usage

    FileHandler$parse_file(path)

#### Arguments

- `path`:

  Path to project file

------------------------------------------------------------------------

### Method `get_input_files()`

Input files of a project

#### Usage

    FileHandler$get_input_files(path)

#### Arguments

- `path`:

  Path to project file

#### Returns

A list of `list(role, path)`, `role` one of `"data"`, `"model"` or
`"other"`

------------------------------------------------------------------------

### Method `get_output_dir()`

Get expected output directory

#### Usage

    FileHandler$get_output_dir(path, output_dir = NULL)

#### Arguments

- `path`:

  Path to project file

- `output_dir`:

  User-specified output directory (may be NULL)

------------------------------------------------------------------------

### Method `can_monitor()`

Check if job monitoring is supported

#### Usage

    FileHandler$can_monitor()

------------------------------------------------------------------------

### Method `parse_summary()`

Parse summary file

#### Usage

    FileHandler$parse_summary(summary_file_path)

#### Arguments

- `summary_file_path`:

  Path to summary.txt file

------------------------------------------------------------------------

### Method `expects_summary()`

Whether a finished run of this project type produces results (a
`summary.json` in its completion record)

#### Usage

    FileHandler$expects_summary()

------------------------------------------------------------------------

### Method `result_files()`

Result files of a run into `output_dir`: the files this handler parses
into `summary.json`. The default is `summary.txt` in the output
directory.

#### Usage

    FileHandler$result_files(output_dir, files = NULL)

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

Parse result files into the content of `summary.json`

#### Usage

    FileHandler$parse_results(files)

#### Arguments

- `files`:

  Result files to parse, as returned by `result_files()`

#### Returns

A list, or `NULL` when nothing could be parsed

------------------------------------------------------------------------

### Method `clone()`

The objects of this class are cloneable with this method.

#### Usage

    FileHandler$clone(deep = FALSE)

#### Arguments

- `deep`:

  Whether to make a deep clone.
