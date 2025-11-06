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
  DBI::dbConnect(duckdb::duckdb(), dbdir = db)
}

#' Create database tables for Monolix/Simulx run tracking
#'
#' Creates the required database tables for storing run information,
#' input files, and output files. Tables are created only if they don't
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
#' \strong{runs}:
#' \itemize{
#'   \item `run_id` (INTEGER PRIMARY KEY): Auto-generated unique run identifier
#'   \item `job_id` (INTEGER): External job ID (may be NULL if extraction fails)
#'   \item `path` (TEXT): Path to the Monolix/Simulx file
#'   \item `data_file` (TEXT): Path to the data file
#'   \item `model_file` (TEXT): Path to the model file
#'   \item `thread` (INTEGER): Number of threads used by grid jobs
#'   \item `tool` (TEXT): Tool to launch assessment
#'   \item `mode` (TEXT): Console mode
#'   \item `config` (TEXT): Configuration file path
#'   \item `cmd` (TEXT): Command used to submit the job
#'   \item `submitted_at` (TIMESTAMP): Job submission timestamp
#'   \item `completed_at` (TIMESTAMP): Job completion timestamp (NULL until completed)
#' }
#'
#' \strong{input_files}:
#' \itemize{
#'   \item `run_id` (INTEGER): Foreign key to runs
#'   \item `file_path` (TEXT): Path to the input file
#'   \item `file_timestamp` (TIMESTAMP): File modification timestamp
#'   \item `md5_checksum` (TEXT): MD5 hash of the file
#'   \item `recorded_at` (TIMESTAMP): When the record was created
#' }
#'
#' \strong{output_files}:
#' \itemize{
#'   \item `run_id` (INTEGER): Foreign key to runs
#'   \item `file_path` (TEXT): Path to the output file
#'   \item `file_timestamp` (TIMESTAMP): File modification timestamp
#'   \item `md5_checksum` (TEXT): MD5 hash of the file
#'   \item `recorded_at` (TIMESTAMP): When the record was created
#' }
#'
#' @keywords internal
db_create_tables <- function(db_conn = default_db_conn()) {
  DBI::dbExecute(
    db_conn,
    "CREATE SEQUENCE IF NOT EXISTS run_id_seq START 1"
  )
  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS runs (
      run_id INTEGER PRIMARY KEY DEFAULT nextval('run_id_seq'),
      job_id INTEGER,
      path TEXT,
      data_file TEXT,
      model_file TEXT,
      thread INTEGER,
      tool TEXT,
      mode TEXT,
      config TEXT,
      cmd TEXT,
      submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      completed_at TIMESTAMP DEFAULT NULL
    )"
  )

  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS input_files (
      run_id INTEGER,
      file_path TEXT,
      file_timestamp TIMESTAMP,
      md5_checksum TEXT,
      recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (run_id) REFERENCES runs (run_id)
    )"
  )

  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS output_files (
      run_id INTEGER,
      file_path TEXT,
      file_timestamp TIMESTAMP,
      md5_checksum TEXT,
      recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (run_id) REFERENCES runs (run_id)
    )"
  )
}

#' Get file information for specific runs
#'
#' Retrieves information about input and output files associated
#' with one or more runs from the database.
#'
#' @param run_id Integer vector. The run ID(s) to query file information for.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns: `run_id`, `file_type`, `file_path`,
#'   `file_timestamp`, `md5_checksum`, and `recorded_at`. Returns an empty
#'   data frame if no files are found for the specified run ID(s).
#'
#' @export
get_run_files <- function(run_id, db_conn = default_db_conn()) {
  on.exit(DBI::dbDisconnect(db_conn), add = TRUE)

  # Handle empty run_id vector
  if (length(run_id) == 0) {
    return(data.frame(
      run_id = integer(0),
      file_type = character(0),
      file_path = character(0),
      file_timestamp = as.POSIXct(character(0)),
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
        run_id,
        'input' as file_type,
        file_path,
        file_timestamp,
        md5_checksum,
        recorded_at
      FROM input_files 
      WHERE run_id IN (",
      placeholders,
      ")
      
      UNION ALL
      
      SELECT 
        run_id,
        'output' as file_type,
        file_path,
        file_timestamp,
        md5_checksum,
        recorded_at
      FROM output_files 
      WHERE run_id IN (",
      placeholders,
      ")
      
      ORDER BY run_id, file_type, file_path
      "
    ),
    params = c(as.list(run_id), as.list(run_id))
  )

  # Convert UTC timestamps to system timezone
  files_data |>
    dplyr::mutate(
      file_timestamp = as.POSIXct(.data[["file_timestamp"]], tz = "UTC") |>
        lubridate::with_tz(Sys.timezone()),
      recorded_at = as.POSIXct(.data[["recorded_at"]], tz = "UTC") |>
        lubridate::with_tz(Sys.timezone())
    )
}
