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

#' Create database tables for Monolix run tracking
#'
#' Creates the required database tables for storing Monolix job information,
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
#' \strong{mono_jobs}:
#' \itemize{
#'   \item `job_id` (INTEGER PRIMARY KEY): Unique job identifier
#'   \item `path` (TEXT): Path to the Monolix project file
#'   \item `cmd` (TEXT): Command used to submit the job
#'   \item `submitted_at` (TIMESTAMP): Job submission timestamp
#'   \item `completed_at` (TIMESTAMP): Job completion timestamp (NULL until completed)
#' }
#'
#' \strong{input_files}:
#' \itemize{
#'   \item `job_id` (INTEGER): Foreign key to mono_jobs
#'   \item `file_path` (TEXT): Path to the input file
#'   \item `file_timestamp` (TIMESTAMP): File modification timestamp
#'   \item `md5_checksum` (TEXT): MD5 hash of the file
#'   \item `recorded_at` (TIMESTAMP): When the record was created
#' }
#'
#' \strong{output_files}:
#' \itemize{
#'   \item `job_id` (INTEGER): Foreign key to mono_jobs
#'   \item `file_path` (TEXT): Path to the output file
#'   \item `file_timestamp` (TIMESTAMP): File modification timestamp
#'   \item `md5_checksum` (TEXT): MD5 hash of the file
#'   \item `recorded_at` (TIMESTAMP): When the record was created
#' }
#'
#' @examples
#' \dontrun{
#' # Create tables with default connection
#' db_create_tables()
#'
#' # Create tables with custom connection
#' conn <- DBI::dbConnect(duckdb::duckdb(), "custom.db")
#' db_create_tables(conn)
#' DBI::dbDisconnect(conn)
#' }
#'
#' @seealso
#' \code{\link{default_db_conn}} for creating database connections,
#' \code{\link{mono}} for submitting jobs that use these tables
#'
#' @keywords internal
db_create_tables <- function(db_conn = default_db_conn()) {
  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS mono_jobs (
      job_id INTEGER PRIMARY KEY,
      path TEXT,
      cmd TEXT,
      submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      completed_at TIMESTAMP DEFAULT NULL
    )"
  )

  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS input_files (
      job_id INTEGER,
      file_path TEXT,
      file_timestamp TIMESTAMP,
      md5_checksum TEXT,
      recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (job_id) REFERENCES mono_jobs (job_id)
    )"
  )

  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS output_files (
      job_id INTEGER,
      file_path TEXT,
      file_timestamp TIMESTAMP,
      md5_checksum TEXT,
      recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (job_id) REFERENCES mono_jobs (job_id)
    )"
  )
}

#' Get file information for specific jobs
#'
#' Retrieves information about input and output files associated
#' with one or more Monolix jobs from the database.
#'
#' @param job_id Integer vector. The job ID(s) to query file information for.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns: `job_id`, `file_type`, `file_path`,
#'   `file_timestamp`, `md5_checksum`, and `recorded_at`. Returns an empty
#'   data frame if no files are found for the specified job ID(s).
#'   All timestamps are converted to the system timezone.
#'
#' @details
#' This function queries both the `input_files` and `output_files` tables
#' to retrieve comprehensive file information for the specified jobs. The
#' `file_type` column indicates whether each file is an "input" or "output" file.
#' Timestamps are stored in UTC but converted to the system timezone for display.
#'
#' When multiple job IDs are provided, results are ordered by job ID, then
#' file type, then file path.
#'
#' @examples
#' \dontrun{
#' # Get files for a specific job
#' get_job_files(12345)
#'
#' # Get files for multiple jobs
#' get_job_files(c(12345, 12346, 12347))
#'
#' # Use custom database connection
#' conn <- DBI::dbConnect(duckdb::duckdb(), "custom.db")
#' get_job_files(12345, db_conn = conn)
#' }
#'
#' @seealso
#' \code{\link{mono}} for submitting jobs that create file records,
#' \code{\link{runs_data}} for getting information about all runs,
#' \code{\link{default_db_conn}} for database connections
#'
#' @export
get_job_files <- function(job_id, db_conn = default_db_conn()) {
  on.exit(DBI::dbDisconnect(db_conn), add = TRUE)

  # Handle empty job_id vector
  if (length(job_id) == 0) {
    return(data.frame(
      job_id = integer(0),
      file_type = character(0),
      file_path = character(0),
      file_timestamp = as.POSIXct(character(0)),
      md5_checksum = character(0),
      recorded_at = as.POSIXct(character(0))
    ))
  }

  # Create placeholder string for IN clause
  placeholders <- paste(rep("?", length(job_id)), collapse = ",")

  files_data <- DBI::dbGetQuery(
    db_conn,
    paste0(
      "
      SELECT 
        job_id,
        'input' as file_type,
        file_path,
        file_timestamp,
        md5_checksum,
        recorded_at
      FROM input_files 
      WHERE job_id IN (",
      placeholders,
      ")
      
      UNION ALL
      
      SELECT 
        job_id,
        'output' as file_type,
        file_path,
        file_timestamp,
        md5_checksum,
        recorded_at
      FROM output_files 
      WHERE job_id IN (",
      placeholders,
      ")
      
      ORDER BY job_id, file_type, file_path
      "
    ),
    params = c(as.list(job_id), as.list(job_id))
  )

  # Convert UTC timestamps to system timezone
  files_data |>
    dplyr::mutate(
      file_timestamp = as.POSIXct(file_timestamp, tz = "UTC") |>
        lubridate::with_tz(Sys.timezone()),
      recorded_at = as.POSIXct(recorded_at, tz = "UTC") |>
        lubridate::with_tz(Sys.timezone())
    )
}
