#' Submit Monolix and Simulx jobs to grid and record runs in database
#'
#' Executes one or more commands that submit Monolix/Simulx jobs to the grid and
#' stores a record of the runs in a database.
#'
#' @param path Character vector. Paths to existing Monolix or Simulx project files.
#' @param output_dir Character vector or `NULL`. Directory for
#'   output files relative to the project path.
#'   Passed as `--output-dir` argument to the Monolix command.
#'   If `NULL` (default), Monolix uses the export path from the project file.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param thread Integer vector or `NULL`. Number of threads used by Monolix/Simulx.
#'   Passed as `--thread` argument to the Monolix and Simulx commands.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param tool Character vector or `NULL`. Tool to launch assessment
#'   (`"monolix"`, `"modelBuilding"`, `"bootstrap"`).
#'   Passed as `--tool` argument to the Monolix command.
#'   If `NULL` (default), Monolix uses `"monolix"`.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param mode Character vector or `NULL`. Console mode
#'   (`"none"`, `"basic"`, or `"complete"`).
#'   Passed as `--mode` argument to the Monolix and Simulx commands.
#'   If `NULL` (default), Monolix and Simulx use `"basic"`.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param config Character vector or `NULL`. Configuration file path.
#'   Passed as `--config` argument to the Monolix command.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param cmd Character vector. The Monolix or Simulx command to execute. Must be
#'   identifiable by `Sys.which()` and contain "mono" or "sim" in the name.
#'   Default is "mono24". Should be same length as `path` or length 1.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return Integer vector. Returns the run IDs for all submitted jobs.
#'
#' @details
#' This function performs the following operations for each job:
#' \enumerate{
#'   \item Validates inputs and submits the job using `system2()`
#'   \item Extracts the job ID from the submission output
#'   \item Monitors job completion by checking the job queue
#'   \item Records job information in the `runs` table
#'   \item Tracks input file metadata in the `input_files` table
#'   \item Records all output files with timestamps and checksums in the `output_files` table
#' }
#'
#' The database schema includes three tables:
#' \itemize{
#'   \item `runs`: Job metadata (ID, path, command, submission time)
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
#' @export
mono <- function(
  path,
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL,
  cmd = getOption("mlxtrx.monolix_cmd", "mono24"),
  db_conn = default_db_conn(db = default_db(path))
) {
  assertthat::assert_that(
    all(file.exists(path)),
    msg = "All `path`s should be paths to existing Monolix or Simulx project files."
  )

  assertthat::assert_that(
    is.character(cmd),
    all(Sys.which(cmd) != ""),
    all(grepl("mono|sim", cmd, ignore.case = TRUE)),
    msg = "All `cmd` commands must be 'mono' or 'sim' commands that can be identified by `Sys.which()`."
  )

  assertthat::assert_that(
    inherits(db_conn, "DBIObject"),
    msg = "`db_conn` must be a valid database connection"
  )

  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Create tables if they don't exist
  db_create_tables(db_conn)

  # Recycle all arguments to match length of path
  cmd_recycled <- rep_len(cmd, length(path))
  output_dir_recycled <- if (!is.null(output_dir)) {
    rep_len(output_dir, length(path))
  } else {
    NULL
  }
  thread_recycled <- if (!is.null(thread)) {
    rep_len(thread, length(path))
  } else {
    NULL
  }
  tool_recycled <- if (!is.null(tool)) rep_len(tool, length(path)) else NULL
  mode_recycled <- if (!is.null(mode)) rep_len(mode, length(path)) else NULL
  config_recycled <- if (!is.null(config)) {
    rep_len(config, length(path))
  } else {
    NULL
  }

  # Build argument list dynamically, excluding NULL values
  args_list <- list(
    path = path,
    cmd = cmd_recycled
  )

  if (!is.null(output_dir_recycled)) {
    args_list$output_dir <- output_dir_recycled
  }
  if (!is.null(thread_recycled)) {
    args_list$thread <- thread_recycled
  }
  if (!is.null(tool_recycled)) {
    args_list$tool <- tool_recycled
  }
  if (!is.null(mode_recycled)) {
    args_list$mode <- mode_recycled
  }
  if (!is.null(config_recycled)) {
    args_list$config <- config_recycled
  }

  # Submit all jobs using do.call + mapply
  submission_times <- rep(Sys.time(), length(path))

  job_ids <- do.call(
    mapply,
    c(
      list(
        FUN = function(..., submission_idx) {
          submission_times[submission_idx] <<- Sys.time()
          execute_job(...)
        },
        submission_idx = seq_along(path),
        SIMPLIFY = TRUE,
        USE.NAMES = FALSE
      ),
      args_list
    )
  )

  # Record all runs in database and get run_ids
  run_ids <- integer(length(path))

  for (i in seq_along(path)) {
    ext <- fs::path_ext(path[i])
    handler <- get_file_handler(ext)

    # Parse file and extract metadata
    parsed_content <- handler$parse_file(path[i])
    input_files <- handler$get_input_files(parsed_content, dirname(path[i]))

    # Extract individual files with fallback to NA
    data_file <- input_files$data_file %||% NA_character_
    model_file <- input_files$model_file %||% NA_character_

    # Store handler capability for monitoring
    can_monitor <- handler$can_monitor()

    # Resolve config file path if provided
    config_file <- NULL
    if (!is.null(config_recycled)) {
      config_file <- config_recycled[i]
      if (!fs::is_absolute_path(config_file) && !file.exists(config_file)) {
        config_file_built <- file.path(dirname(path[i]), config_file)
        if (file.exists(config_file_built)) {
          config_file <- config_file_built
        }
      }
      config_file <- normalizePath(config_file, mustWork = FALSE)
    }
    # Insert job record with all parameters
    DBI::dbExecute(
      db_conn,
      "INSERT INTO runs (job_id, path, data_file, model_file, thread, tool, mode, config, cmd, submitted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      params = list(
        if (is.na(job_ids[i])) NA_real_ else job_ids[i],
        normalizePath(path[i]),
        data_file,
        model_file,
        if (!is.null(thread_recycled)) thread_recycled[i] else NA_integer_,
        if (!is.null(tool_recycled)) tool_recycled[i] else NA_character_,
        if (!is.null(mode_recycled)) mode_recycled[i] else NA_character_,
        if (!is.null(config_file)) config_file else NA_character_,
        cmd_recycled[i],
        submission_times[i]
      )
    )

    # Get the auto-generated run_id
    run_ids[i] <- DBI::dbGetQuery(
      db_conn,
      "SELECT currval('run_id_seq') as run_id"
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

  monitor_jobs(
    path = path,
    run_id = run_ids,
    job_id = job_ids,
    output_dir = output_dir_recycled,
    cmd = cmd_recycled,
    db_conn = db_conn
  )

  return(run_ids)
}

#' @rdname mono
#' @export
sim <- function(
  path,
  thread = NULL,
  mode = NULL,
  cmd = getOption("mlxtrx.simulx_cmd", "sim24"),
  db_conn = default_db_conn(db = default_db(path))
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }
  mono(path = path, thread = thread, mode = mode, cmd = cmd, db_conn = db_conn)
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
      if (basename(cmd) == "monolix.sh") "--no-gui" else NULL,
      paste("-p", shQuote(normalizePath(path))),
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
    if (
      any(stringr::str_detect(
        result[1:10],
        "Results have been successfully loaded"
      ))
    ) {
      message("Submitted ", cmd, " for file: ", path)
      return(NA)
    }
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
      job_completed <- FALSE

      if (!is.na(job_id[i])) {
        job_completed <- !job_in_sq(job_id[i])
      } else {
        ext <- fs::path_ext(path[i])
        handler <- get_file_handler(ext)

        if (!handler$can_monitor()) {
          warning(
            "Can't currently monitor .",
            ext,
            " jobs that are not submitted to the grid."
          )
          job_completed <- TRUE
        } else {
          # Use handler-specific monitoring logic
          current_output_dir <- handler$get_output_dir(path[i], output_dir[i])
          summary_file <- file.path(current_output_dir, "summary.txt")
          job_completed <- file.exists(summary_file)
        }
      }

      if (job_completed) {
        completed_now <- c(completed_now, i)
      }
    }

    # Process completed jobs
    if (length(completed_now) > 0) {
      for (i in completed_now) {
        ext <- fs::path_ext(path[i])
        handler <- get_file_handler(ext)

        current_output_dir <- normalizePath(handler$get_output_dir(
          path[i],
          output_dir[i]
        ))

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
          "UPDATE runs SET completed_at = ? WHERE run_id = ?",
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
