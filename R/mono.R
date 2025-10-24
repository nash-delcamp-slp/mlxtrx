#' Submit Monolix jobs to grid and record runs in database
#'
#' Executes one or more commands that submit Monolix jobs to the grid and
#' stores a record of the runs in a database.
#'
#' @param path Character vector. Paths to existing Monolix project files.
#' @param output_dir Character vector or `NULL`. Directory for
#'   output files relative to the project path.
#'   Passed as `--output-dir` argument to the Monolix command.
#'   If `NULL` (default), Monolix uses the export path from the project file.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param thread Integer vector or `NULL`. Number of threads used by Monolix.
#'   Passed as `--thread` argument to the Monolix command.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param tool Character vector or `NULL`. Tool to launch assessment
#'   (`"monolix"`, `"modelBuilding"`, `"bootstrap"`).
#'   Passed as `--tool` argument to the Monolix command.
#'   If `NULL` (default), Monolix uses `"monolix"`.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param mode Character vector or `NULL`. Console mode
#'   (`"none"`, `"basic"`, or `"complete"`).
#'   Passed as `--mode` argument to the Monolix command.
#'   If `NULL` (default), Monolix uses `"basic"`.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param config Character vector or `NULL`. Configuration file path.
#'   Passed as `--config` argument to the Monolix command.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param cmd Character vector. The Monolix command to execute. Must be
#'   identifiable by `Sys.which()` and contain "mono" in the name.
#'   Default is "mono24". Should be same length as `path` or length 1.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return Integer vector. Returns the run IDs for all submitted jobs.
#'
#' @details
#' This function performs the following operations for each job:
#' \enumerate{
#'   \item Validates inputs and submits the Monolix job using `system2()`
#'   \item Extracts the job ID from the submission output
#'   \item Monitors job completion by checking the job queue
#'   \item Records job information in the `mono_jobs` table
#'   \item Tracks input file metadata in the `input_files` table
#'   \item Records all output files with timestamps and checksums in the `output_files` table
#' }
#'
#' The database schema includes three tables:
#' \itemize{
#'   \item `mono_jobs`: Job metadata (ID, path, command, submission time)
#'   \item `input_files`: Input file tracking with timestamps and MD5 checksums
#'   \item `output_files`: Output file tracking with timestamps and MD5 checksums
#' }
#'
#' The function automatically creates database tables if they don't exist and
#' closes the database connection on exit.
#'
#' @examples
#' \dontrun{
#' # Basic usage with default settings
#' mono("path/to/project.mlxtran")
#'
#' # Multiple jobs
#' mono(c("project1.mlxtran", "project2.mlxtran"))
#'
#' # Specify custom output directory and thread count
#' mono(
#'   path = "project.mlxtran",
#'   output_dir = "custom-project-output",
#'   thread = 4
#' )
#'
#' # Use custom database connection
#' conn <- DBI::dbConnect(duckdb::duckdb(), "custom.db")
#' mono("project.mlxtran", db_conn = conn)
#' }
#'
#' @seealso
#' \code{\link{default_db_conn}} for default database connections,
#' \code{\link{get_job_files}} for querying recorded file information,
#' \code{\link{execute_job}} for executing a single job
#'
#' @export
mono <- function(
  path,
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL,
  cmd = "mono24",
  db_conn = default_db_conn(db = default_db(path))
) {
  assertthat::assert_that(
    all(file.exists(path)),
    msg = "All `path`s should be paths to existing Monolix project files."
  )

  assertthat::assert_that(
    is.character(cmd),
    all(Sys.which(cmd) != ""),
    all(grepl("mono", cmd)),
    msg = "All `cmd` commands must be 'mono' commands that can be identified by `Sys.which()`."
  )

  assertthat::assert_that(
    inherits(db_conn, "DBIObject"),
    msg = "`db_conn` must be a valid database connection"
  )

  on.exit(DBI::dbDisconnect(db_conn), add = TRUE)

  # Create tables if they don't exist
  db_create_tables(db_conn)

  # Build argument list dynamically, excluding NULL values
  args_list <- list(
    path = path,
    cmd = cmd
  )

  if (!is.null(output_dir)) {
    args_list$output_dir <- output_dir
  }
  if (!is.null(thread)) {
    args_list$thread <- thread
  }
  if (!is.null(tool)) {
    args_list$tool <- tool
  }
  if (!is.null(mode)) {
    args_list$mode <- mode
  }
  if (!is.null(config)) {
    args_list$config <- config
  }

  # Submit all jobs using do.call + mapply
  job_ids <- do.call(
    mapply,
    c(
      list(
        FUN = execute_job,
        SIMPLIFY = TRUE,
        USE.NAMES = FALSE
      ),
      args_list
    )
  )

  # Record all runs in database and get run_ids
  cmd_recycled <- rep_len(cmd, length(path))
  run_ids <- integer(length(path))

  for (i in seq_along(path)) {
    # Parse project file for metadata
    path_content <- parse_mlxtran(path[i])
    data_file <- path_content$DATAFILE$FILEINFO$file

    # Resolve relative paths
    if (!fs::is_absolute_path(data_file) && !file.exists(data_file)) {
      data_file_built <- file.path(dirname(path[i]), data_file)
      if (file.exists(data_file_built)) {
        data_file <- data_file_built
      }
    }
    data_file <- normalizePath(data_file)

    model_file <- path_content$MODEL$LONGITUDINAL$file
    if (!fs::is_absolute_path(model_file) && !file.exists(model_file)) {
      model_file_built <- file.path(dirname(path[i]), model_file)
      if (file.exists(model_file_built)) {
        model_file <- model_file_built
      }
    }
    model_file <- normalizePath(model_file, mustWork = FALSE)

    # Insert job record - let database auto-generate run_id
    DBI::dbExecute(
      db_conn,
      "INSERT INTO mono_jobs (job_id, path, data_file, model_file, cmd) VALUES (?, ?, ?, ?, ?)",
      params = list(
        if (is.na(job_ids[i])) NULL else job_ids[i], # NULL for failed extractions
        normalizePath(path[i]),
        data_file,
        model_file,
        cmd_recycled[i]
      )
    )

    # Get the auto-generated run_id
    run_ids[i] <- DBI::dbGetQuery(
      db_conn,
      "SELECT last_insert_rowid() as run_id"
    )$run_id

    # Record input files
    if (!is.null(data_file) && file.exists(data_file)) {
      DBI::dbExecute(
        db_conn,
        "INSERT INTO input_files (run_id, file_path, file_timestamp, md5_checksum) VALUES (?, ?, ?, ?)",
        params = list(
          run_ids[i],
          data_file,
          get_file_timestamp(data_file),
          calculate_md5(data_file)
        )
      )
    }

    if (!is.null(model_file) && file.exists(model_file)) {
      DBI::dbExecute(
        db_conn,
        "INSERT INTO input_files (run_id, file_path, file_timestamp, md5_checksum) VALUES (?, ?, ?, ?)",
        params = list(
          run_ids[i],
          model_file,
          get_file_timestamp(model_file),
          calculate_md5(model_file)
        )
      )
    }
  }

  # Monitor jobs that have valid job_ids
  valid_job_indices <- which(!is.na(job_ids))

  if (length(valid_job_indices) > 0) {
    monitor_jobs(
      path = path[valid_job_indices],
      run_id = run_ids[valid_job_indices],
      job_id = job_ids[valid_job_indices],
      output_dir = if (is.null(output_dir)) {
        NULL
      } else {
        output_dir[valid_job_indices]
      },
      cmd = cmd_recycled[valid_job_indices],
      db_conn = db_conn
    )
  }

  return(run_ids)
}


#' Execute a single Monolix job
#'
#' Submits a single Monolix job to the grid and extracts the job ID.
#'
#' @param path Character scalar. Path to a single existing Monolix project file.
#' @param output_dir Character scalar or `NULL`. Directory for output files.
#' @param thread Integer scalar or `NULL`. Number of threads used by Monolix.
#' @param tool Character scalar or `NULL`. Tool to launch assessment.
#' @param mode Character scalar or `NULL`. Console mode.
#' @param config Character scalar or `NULL`. Configuration file path.
#' @param cmd Character scalar. The Monolix command to execute.
#'
#' @return Integer scalar with job ID, or `NA` if job submission failed.
#'
#' @keywords internal
execute_job <- function(
  path,
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL,
  cmd = "mono24"
) {
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
    return(NA)
  }

  message("Submitted ", cmd, " job ", job_id, " for file: ", path)
  return(job_id)
}

#' Monitor multiple jobs and record completed ones
#'
#' @param path Character vector. Paths to Monolix project files.
#' @param run_id Integer vector. Database run IDs.
#' @param job_id Integer vector. External job IDs (from job scheduler).
#' @param output_dir Character vector or `NULL`. Output directories.
#' @param cmd Character vector. The commands used for each job.
#' @param db_conn Database connection object.
#'
#' @return NULL (invisible). Called for side effects.
#'
#' @importFrom rlang %||%
#'
#' @keywords internal
monitor_jobs <- function(
  path,
  run_id,
  job_id,
  output_dir = NULL,
  cmd,
  db_conn
) {
  assertthat::assert_that(
    length(path) == length(run_id) && length(run_id) == length(job_id),
    msg = "`path`, `run_id`, and `job_id` must have the same length"
  )

  if (!is.null(output_dir)) {
    assertthat::assert_that(
      length(output_dir) == length(path),
      msg = "`output_dir` must be NULL or same length as `path`"
    )
  }

  # Track which jobs are still running
  running_jobs <- seq_along(job_id)

  while (length(running_jobs) > 0) {
    # Check which running jobs have completed
    completed_now <- c()

    for (i in running_jobs) {
      if (!job_in_sq(job_id[i])) {
        completed_now <- c(completed_now, i)
      }
    }

    # Process completed jobs
    if (length(completed_now) > 0) {
      for (i in completed_now) {
        # Extract information from project file
        path_content <- parse_mlxtran(path[i])

        current_output_dir <- output_dir[i] %||%
          file.path(
            dirname(path[i]),
            path_content$MONOLIX$SETTINGS$GLOBAL$exportpath
          )

        # Resolve relative output directory path
        if (
          !fs::is_absolute_path(current_output_dir) &&
            !file.exists(current_output_dir)
        ) {
          current_output_dir_built <- file.path(
            dirname(path[i]),
            current_output_dir
          )
          if (file.exists(current_output_dir_built)) {
            current_output_dir <- current_output_dir_built
          }
        }
        current_output_dir <- normalizePath(current_output_dir)

        # Track latest modification time for completion timestamp
        latest_mod_time <- NULL

        # Record output files information
        if (dir.exists(current_output_dir)) {
          output_files <- list.files(
            current_output_dir,
            full.names = TRUE,
            recursive = TRUE
          )

          for (output_file in output_files) {
            if (file.exists(output_file) && !dir.exists(output_file)) {
              output_timestamp <- get_file_timestamp(output_file)
              output_md5 <- calculate_md5(output_file)

              # Track the latest modification time
              if (
                is.null(latest_mod_time) || output_timestamp > latest_mod_time
              ) {
                latest_mod_time <- output_timestamp
              }

              DBI::dbExecute(
                db_conn,
                "INSERT INTO output_files (run_id, file_path, file_timestamp, md5_checksum) VALUES (?, ?, ?, ?)",
                params = list(
                  run_id[i],
                  output_file,
                  output_timestamp,
                  output_md5
                )
              )
            }
          }
        }

        # Update job completion time
        completion_time <- latest_mod_time %||% Sys.time()

        DBI::dbExecute(
          db_conn,
          "UPDATE mono_jobs SET completed_at = ? WHERE run_id = ?",
          params = list(completion_time, run_id[i])
        )
      }

      # Remove completed jobs from running list
      running_jobs <- setdiff(running_jobs, completed_now)
    }

    # Sleep before next check if there are still running jobs
    if (length(running_jobs) > 0) {
      Sys.sleep(5)
    }
  }

  invisible(NULL)
}
