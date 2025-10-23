# function to execute the command to submit a monolix job to the grid and store record of run in a database
mono <- function(
  path,
  db_conn = default_db_conn(path),
  cmd = "mono24",
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL
) {
  assertthat::assert_that(
    length(path) == 1,
    file.exists(path),
    msg = "`path` should be the path to a single existing Monolix project file."
  )

  assertthat::assert_that(
    is.character(cmd),
    length(cmd) == 1,
    Sys.which(cmd) != "",
    grepl("mono", cmd),
    msg = "`cmd` must be a 'mono' command identified by `Sys.which()`"
  )

  assertthat::assert_that(
    inherits(db_conn, "DBIObject"),
    msg = "`db_conn` must be a valid database connection"
  )

  on.exit(DBI::dbDisconnect(db_conn), add = TRUE)

  result <- system2(
    cmd,
    args = c(
      paste("-p", shQuote(path)),
      if (!is.null(output_dir)) {
        paste("--output-dir", shQuote(output_dir))
      } else {
        NULL
      },
      if (!is.null(thread)) paste("--thread", thread) else NULL,
      if (!is.null(tool)) paste("--tool", tool) else NULL,
      if (!is.null(mode)) paste("--mode", mode) else NULL,
      if (!is.null(config)) paste("--config", config) else NULL
    ),
    stdout = TRUE,
    stderr = TRUE
  )

  job_id <- NA
  if (length(result) > 0) {
    job_match <- stringr::str_extract(
      result[1],
      "(?<=Submitted batch job )\\d+"
    )
    if (!is.na(job_match)) {
      job_id <- as.integer(job_match)
    }
  }

  if (is.na(job_id)) {
    warning(
      "Could not extract job ID from ",
      cmd,
      " output: ",
      paste(result, collapse = "\n")
    )
    return(invisible(NULL))
  }

  message("Submitted ", cmd, " job ", job_id, " for file: ", path)

  # Extract information from project file
  path_content <- parse_mlxtran(path)
  input_file <- path_content$DATAFILE$FILEINFO$file

  if (is.null(output_dir)) {
    output_dir <- file.path(
      dirname(path),
      path_content$MONOLIX$SETTINGS$GLOBAL$exportpath
    )
  }

  job_complete <- FALSE
  while (!job_complete) {
    job_complete <- !job_in_sq(job_id)
    Sys.sleep(5)
  }

  # Create tables if they don't exist
  db_create_tables(db_conn)

  # Insert the job record
  DBI::dbExecute(
    db_conn,
    "INSERT INTO mono_jobs (job_id, path, cmd) VALUES (?, ?, ?)",
    params = list(job_id, path, cmd)
  )

  # Record input file information
  if (!is.null(input_file) && input_file != "") {
    # Make input file path absolute if it's relative
    if (!file.path(input_file) |> fs::is_absolute_path()) {
      input_file <- file.path(dirname(path), input_file)
    }

    input_timestamp <- get_file_timestamp(input_file)
    input_md5 <- calculate_md5(input_file)

    DBI::dbExecute(
      db_conn,
      "INSERT INTO input_files (job_id, file_path, file_timestamp, md5_checksum) VALUES (?, ?, ?, ?)",
      params = list(job_id, input_file, input_timestamp, input_md5)
    )
  }

  # Record output files information
  if (dir.exists(output_dir)) {
    output_files <- list.files(output_dir, full.names = TRUE, recursive = TRUE)

    for (output_file in output_files) {
      if (file.exists(output_file) && !dir.exists(output_file)) {
        # Only process actual files, not directories
        output_timestamp <- get_file_timestamp(output_file)
        output_md5 <- calculate_md5(output_file)

        DBI::dbExecute(
          db_conn,
          "INSERT INTO output_files (job_id, file_path, file_timestamp, md5_checksum) VALUES (?, ?, ?, ?)",
          params = list(job_id, output_file, output_timestamp, output_md5)
        )
      }
    }
  }

  return(TRUE)
}
