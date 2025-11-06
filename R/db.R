#' Get default database file path
#'
#' Determines the preferred database file path based on the project structure.
#' Uses the substage directory identified by `utilscognigen::path_substage()` and
#' creates a path to `runs.duckdb` within that directory.
#'
#' @param path Character vector. Starting path for substage identification.
#'   Defaults to the current working directory.
#'
#' @return Character scalar. Path to the default database file (`runs.duckdb`)
#'   in the identified directory.
#'
#' @details
#' This function uses `utilscognigen::path_substage()` to identify the substage
#' directory from the given path. If multiple unique stages are identified,
#' the function throws an error. The database file is always named `runs.duckdb`.
#'
#' @examples
#' \dontrun{
#' # Get default database path from current directory
#' default_db()
#'
#' # Get database path for specific project
#' default_db("/path/to/project")
#' }
#'
#' @seealso
#' \code{\link{default_db_conn}} for creating connections to the default database
#'
#' @export
default_db <- function(path = getwd()) {
  db_dir <- unique(utilscognigen::path_substage(path = path))
  if (length(db_dir) > 1) {
    stop(
      "More than 1 unique stage identified: ",
      paste0(db_dir, collapse = ", ")
    )
  }
  file.path(
    db_dir,
    "runs.duckdb"
  )
}

#' Create default database connection
#'
#' Creates a DuckDB database connection to the default database file.
#' The connection should be closed by the caller when no longer needed.
#'
#' @param db Character scalar. Path to the database file.
#'   Defaults to the path returned by `default_db()`.
#'
#' @return A `duckdb_connection` object that inherits from `DBIConnection`.
#'
#' @details
#' This function creates a connection to a DuckDB database. If the database
#' file doesn't exist, DuckDB will create it automatically. The caller is
#' responsible for closing the connection using `DBI::dbDisconnect()`.
#'
#' @examples
#' \dontrun{
#' # Create connection to default database
#' conn <- default_db_conn()
#' # ... use connection ...
#' DBI::dbDisconnect(conn)
#'
#' # Create connection to specific database
#' conn <- default_db_conn("/path/to/custom.duckdb")
#' DBI::dbDisconnect(conn)
#' }
#'
#' @seealso
#' \code{\link{default_db}} for determining the default database path,
#' \code{\link{db_create_tables}} for initializing database schema
#'
#' @export
default_db_conn <- function(db = default_db()) {
  db_conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db)
  if (length(DBI::dbListTables(db_conn)) == 0) {
    db_create_tables(db_conn)
  }
  db_conn
}

#' Create database tables for Monolix/Simulx run tracking
#'
#' Creates the required database tables for storing run information,
#' files, and their relationships. Tables are created only if they don't
#' already exist.
#'
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return NULL (invisible). Called for side effects.
#'
#' @details
#' This function creates three tables with the following schema:
#'
#' \strong{run}:
#' \itemize{
#'   \item `run_id` (INTEGER PRIMARY KEY): Auto-generated unique run identifier
#'   \item `job_id` (INTEGER): External job ID (may be NULL if extraction fails)
#'   \item `project_file_id` (INTEGER): Foreign key to file table for project file
#'   \item `data_file_id` (INTEGER): Foreign key to file table for data file
#'   \item `model_file_id` (INTEGER): Foreign key to file table for model file
#'   \item `thread` (INTEGER): Number of threads used by grid jobs
#'   \item `tool` (TEXT): Tool to launch assessment
#'   \item `mode` (TEXT): Console mode
#'   \item `config` (TEXT): Configuration file path
#'   \item `cmd` (TEXT): Command used to submit the job
#'   \item `submitted_at` (TIMESTAMP): Job submission timestamp
#'   \item `completed_at` (TIMESTAMP): Job completion timestamp (NULL until completed)
#' }
#'
#' \strong{file}:
#' \itemize{
#'   \item `id` (INTEGER PRIMARY KEY): Auto-generated unique file identifier
#'   \item `path` (TEXT): Full path to the file
#'   \item `name` (TEXT): File name (basename)
#' }
#'
#' \strong{run_file}:
#' \itemize{
#'   \item `run_id` (INTEGER): Foreign key to run table
#'   \item `file_id` (INTEGER): Foreign key to file table
#'   \item `io_type` (TEXT): Type of file relation (project, input, output)
#'   \item `timestamp` (TIMESTAMP): File modification timestamp
#'   \item `md5_checksum` (TEXT): MD5 hash of the file
#'   \item `recorded_at` (TIMESTAMP): When the record was created
#' }
#'
#' @keywords internal
db_create_tables <- function(db_conn = default_db_conn()) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Create sequences for auto-incrementing IDs
  DBI::dbExecute(
    db_conn,
    "CREATE SEQUENCE IF NOT EXISTS run_id_seq START 1"
  )

  DBI::dbExecute(
    db_conn,
    "CREATE SEQUENCE IF NOT EXISTS file_id_seq START 1"
  )

  # Create file table
  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS file (
      id INTEGER PRIMARY KEY DEFAULT nextval('file_id_seq'),
      path TEXT UNIQUE,
      name TEXT NOT NULL
    )"
  )

  # Create run table
  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS run (
      run_id INTEGER PRIMARY KEY DEFAULT nextval('run_id_seq'),
      job_id INTEGER,
      project_file_id INTEGER,
      data_file_id INTEGER,
      model_file_id INTEGER,
      thread INTEGER,
      tool TEXT,
      mode TEXT,
      config TEXT,
      cmd TEXT,
      submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      completed_at TIMESTAMP DEFAULT NULL,
      FOREIGN KEY (project_file_id) REFERENCES file (id),
      FOREIGN KEY (data_file_id) REFERENCES file (id),
      FOREIGN KEY (model_file_id) REFERENCES file (id)
    )"
  )

  # Create run_file junction table
  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS run_file (
      run_id INTEGER NOT NULL,
      file_id INTEGER NOT NULL,
      io_type TEXT NOT NULL CHECK (io_type IN ('project', 'input', 'output')),
      timestamp TIMESTAMP,
      md5_checksum TEXT,
      recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      PRIMARY KEY (run_id, file_id, io_type),
      FOREIGN KEY (run_id) REFERENCES run (run_id),
      FOREIGN KEY (file_id) REFERENCES file (id)
    )"
  )
}

#' Get or create file record
#'
#' Retrieves an existing file record or creates a new one if it doesn't exist.
#' Handles both regular files and built-in model files.
#'
#' @param file_path Character scalar. Full path to the file or built-in model.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'
#' @return Integer. The file ID.
#'
#' @keywords internal
get_or_create_file <- function(file_path, db_conn) {
  # Check if this is a built-in library file
  is_builtin <- startsWith(file_path, "lib:") &&
    endsWith(file_path, ".txt") &&
    !file.exists(file_path)

  if (is_builtin) {
    normalized_path <- file_path
    file_name <- file_path
  } else {
    normalized_path <- normalizePath(file_path, mustWork = FALSE)
    file_name <- basename(file_path)
  }

  # Check if file already exists
  existing <- DBI::dbGetQuery(
    db_conn,
    "SELECT id FROM file WHERE path = ?",
    params = list(normalized_path)
  )

  if (nrow(existing) > 0) {
    return(existing$id[1])
  }

  # Create new file record
  DBI::dbExecute(
    db_conn,
    "INSERT INTO file (path, name) VALUES (?, ?)",
    params = list(normalized_path, file_name)
  )

  # Return the new file ID
  result <- DBI::dbGetQuery(
    db_conn,
    "SELECT currval('file_id_seq') as id"
  )

  return(result$id[1])
}

#' Get file information for specific runs
#'
#' Retrieves information about files associated with one or more runs
#' from the database using the new schema.
#'
#' @param run_id Integer vector. The run ID(s) to query file information for.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns: `run_id`, `file_id`, `io_type`, `path`,
#'   `name`, `timestamp`, `md5_checksum`, and `recorded_at`. Returns an empty
#'   data frame if no files are found for the specified run ID(s).
#'
#' @export
get_run_files <- function(run_id, db_conn = default_db_conn()) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Handle empty run_id vector
  if (length(run_id) == 0) {
    return(data.frame(
      run_id = integer(0),
      file_id = integer(0),
      io_type = character(0),
      path = character(0),
      name = character(0),
      timestamp = as.POSIXct(character(0)),
      md5_checksum = character(0),
      recorded_at = as.POSIXct(character(0))
    ))
  }

  # Create placeholder string for IN clause
  placeholders <- paste(rep("?", length(run_id)), collapse = ",")

  files_data <- DBI::dbGetQuery(
    db_conn,
    paste0(
      "
      SELECT 
        rf.run_id,
        rf.file_id,
        rf.io_type,
        f.path,
        f.name,
        rf.timestamp,
        rf.md5_checksum,
        rf.recorded_at
      FROM run_file rf
      JOIN file f ON rf.file_id = f.id
      WHERE rf.run_id IN (",
      placeholders,
      ")
      ORDER BY rf.run_id, rf.io_type, f.path
      "
    ),
    params = as.list(run_id)
  )

  # Convert UTC timestamps to system timezone
  files_data |>
    dplyr::mutate(
      timestamp = as.POSIXct(.data[["timestamp"]], tz = "UTC") |>
        lubridate::with_tz(Sys.timezone()),
      recorded_at = as.POSIXct(.data[["recorded_at"]], tz = "UTC") |>
        lubridate::with_tz(Sys.timezone())
    )
}
