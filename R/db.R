# helper function to get the path to the preferred database
default_db <- function(path = getwd()) {
  db_dir <- unique(utilscognigen::path_stage(path = path))
  if (length(db_dir) > 1) {
    stop(
      "More than 1 unique stage identified: ",
      paste0(db_dir, collapse = ", ")
    )
  }
  file.path(
    db_dir,
    "monolix",
    "runs.duckdb"
  )
}

default_db_conn <- function(db = default_db()) {
  DBI::dbConnect(duckdb::duckdb(), dbdir = db)
}

db_create_tables <- function(db_conn = default_db_conn()) {
  DBI::dbExecute(
    db_conn,
    "
    CREATE TABLE IF NOT EXISTS mono_jobs (
      job_id INTEGER PRIMARY KEY,
      path TEXT,
      cmd TEXT,
      submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

# Additional helper function to query file information for a specific job
get_job_files <- function(job_id, db_conn = default_db_conn()) {
  on.exit(DBI::dbDisconnect(db_conn), add = TRUE)

  files_data <- DBI::dbGetQuery(
    db_conn,
    "
    SELECT 
      'input' as file_type,
      file_path,
      file_timestamp,
      md5_checksum,
      recorded_at
    FROM input_files 
    WHERE job_id = ?
    
    UNION ALL
    
    SELECT 
      'output' as file_type,
      file_path,
      file_timestamp,
      md5_checksum,
      recorded_at
    FROM output_files 
    WHERE job_id = ?
    
    ORDER BY file_type, file_path
    ",
    params = list(job_id, job_id)
  )

  files_data |>
    dplyr::mutate(
      file_timestamp = as.POSIXct(file_timestamp),
      recorded_at = as.POSIXct(recorded_at)
    )
}
