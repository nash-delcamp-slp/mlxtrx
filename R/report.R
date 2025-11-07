#' Check completion status of project files in a directory
#'
#' Validates that all project files in a directory have successful run records.
#' A successful run is defined as having a non-NULL completed_at timestamp.
#'
#' @param directory Character scalar. Directory path to check for project files.
#' @param ext Character vector. File extensions to consider as
#'   project files. Defaults to c("mlxtran", "smlx").
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `project_file` (character): Full path to the project file
#'     \item `has_run` (logical): Whether any run record exists for this file
#'     \item `has_successful_run` (logical): Whether a completed run exists
#'     \item `latest_run_id` (integer): Most recent run_id (NA if no runs)
#'     \item `latest_completed_at` (POSIXct): Latest completion timestamp (NA if not completed)
#'     \item `status` (character): Summary status ("completed", "incomplete", "no_runs")
#'   }
#'
#' @export
check_completion_status <- function(
  directory = ".",
  ext = c("mlxtran", "smlx"),
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Find all project files in directory
  project_files <- list.files(
    directory,
    pattern = paste0("\\.", ext, "$", collapse = "|"),
    full.names = TRUE,
    recursive = TRUE
  )

  if (length(project_files) == 0) {
    return(data.frame(
      project_file = character(0),
      has_run = logical(0),
      has_successful_run = logical(0),
      latest_run_id = integer(0),
      latest_completed_at = as.POSIXct(character(0)),
      status = character(0)
    ))
  }

  # Normalize paths for database comparison
  normalized_paths <- normalizePath(project_files, mustWork = FALSE)

  # Query database for run information
  placeholders <- paste(rep("?", length(normalized_paths)), collapse = ",")

  run_data <- DBI::dbGetQuery(
    db_conn,
    paste0(
      "
      SELECT 
        f.path as project_file,
        r.run_id,
        r.completed_at,
        ROW_NUMBER() OVER (PARTITION BY f.path ORDER BY r.submitted_at DESC) as rn
      FROM file f
      LEFT JOIN run r ON f.id = r.project_file_id
      WHERE f.path IN (",
      placeholders,
      ")
      "
    ),
    params = as.list(normalized_paths)
  )

  # Get the most recent run for each project file
  latest_runs <- run_data |>
    dplyr::filter(.data[["rn"]] == 1 | is.na(.data[["run_id"]])) |>
    dplyr::select(-.data[["rn"]]) |>
    dplyr::mutate(
      completed_at = as.POSIXct(.data[["completed_at"]], tz = "UTC") |>
        lubridate::with_tz(Sys.timezone())
    )

  # Create results for all project files
  results <- data.frame(
    project_file = normalized_paths,
    stringsAsFactors = FALSE
  ) |>
    dplyr::left_join(latest_runs, by = "project_file") |>
    dplyr::mutate(
      has_run = !is.na(.data[["run_id"]]),
      has_successful_run = !is.na(.data[["completed_at"]]),
      latest_run_id = .data[["run_id"]],
      latest_completed_at = .data[["completed_at"]],
      status = dplyr::case_when(
        !.data[["has_run"]] ~ "no_runs",
        .data[["has_successful_run"]] ~ "completed",
        TRUE ~ "incomplete"
      )
    ) |>
    dplyr::select(
      .data[["project_file"]],
      .data[["has_run"]],
      .data[["has_successful_run"]],
      .data[["latest_run_id"]],
      .data[["latest_completed_at"]],
      .data[["status"]]
    )

  results
}

#' Check input file modification status
#'
#' Detects input files that have been modified since the latest run began.
#' Compares current file timestamps and checksums against recorded values.
#'
#' @param run_ids Integer vector. Specific run IDs to check. If NULL (default),
#'   checks all runs.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `run_id` (integer): The run identifier
#'     \item `file_path` (character): Full path to the input file
#'     \item `file_name` (character): File name (basename)
#'     \item `recorded_timestamp` (POSIXct): Timestamp when run was recorded
#'     \item `recorded_checksum` (character): MD5 checksum when run was recorded
#'     \item `current_timestamp` (POSIXct): Current file modification time
#'     \item `current_checksum` (character): Current MD5 checksum
#'     \item `timestamp_changed` (logical): Whether modification time changed
#'     \item `checksum_changed` (logical): Whether file content changed
#'     \item `file_exists` (logical): Whether file still exists
#'     \item `status` (character): Summary status
#'   }
#'
#' @export
check_input_file_status <- function(
  run_ids = NULL,
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Build query based on whether run_ids are specified
  if (is.null(run_ids)) {
    input_files_query <- "
      SELECT 
        rf.run_id,
        f.path,
        f.name,
        rf.timestamp as recorded_timestamp,
        rf.md5_checksum as recorded_checksum,
        r.submitted_at
      FROM run_file rf
      JOIN file f ON rf.file_id = f.id
      JOIN run r ON rf.run_id = r.run_id
      WHERE rf.io_type = 'input'
      ORDER BY rf.run_id, f.path
    "
    input_files <- DBI::dbGetQuery(db_conn, input_files_query)
  } else {
    if (length(run_ids) == 0) {
      return(data.frame(
        run_id = integer(0),
        file_path = character(0),
        file_name = character(0),
        recorded_timestamp = as.POSIXct(character(0)),
        recorded_checksum = character(0),
        current_timestamp = as.POSIXct(character(0)),
        current_checksum = character(0),
        timestamp_changed = logical(0),
        checksum_changed = logical(0),
        file_exists = logical(0),
        status = character(0)
      ))
    }

    placeholders <- paste(rep("?", length(run_ids)), collapse = ",")
    input_files_query <- paste0(
      "
      SELECT 
        rf.run_id,
        f.path,
        f.name,
        rf.timestamp as recorded_timestamp,
        rf.md5_checksum as recorded_checksum,
        r.submitted_at
      FROM run_file rf
      JOIN file f ON rf.file_id = f.id
      JOIN run r ON rf.run_id = r.run_id
      WHERE rf.io_type = 'input'
        AND rf.run_id IN (",
      placeholders,
      ")
      ORDER BY rf.run_id, f.path
      "
    )
    input_files <- DBI::dbGetQuery(
      db_conn,
      input_files_query,
      params = as.list(run_ids)
    )
  }

  if (nrow(input_files) == 0) {
    return(data.frame(
      run_id = integer(0),
      file_path = character(0),
      file_name = character(0),
      recorded_timestamp = as.POSIXct(character(0)),
      recorded_checksum = character(0),
      current_timestamp = as.POSIXct(character(0)),
      current_checksum = character(0),
      timestamp_changed = logical(0),
      checksum_changed = logical(0),
      file_exists = logical(0),
      status = character(0)
    ))
  }

  # Convert timestamps
  input_files <- input_files |>
    dplyr::mutate(
      recorded_timestamp = .data[["recorded_timestamp"]] |>
        lubridate::with_tz(Sys.timezone()),
      submitted_at = .data[["submitted_at"]] |>
        lubridate::with_tz(Sys.timezone())
    )

  # Check current file status
  results <- input_files |>
    dplyr::rowwise() |>
    dplyr::mutate(
      file_exists = file.exists(.data[["path"]]),
      current_timestamp = if (.data[["file_exists"]]) {
        file.mtime(.data[["path"]]) |>
          lubridate::with_tz(Sys.timezone())
      } else {
        as.POSIXct(NA)
      },
      current_checksum = calculate_md5(.data[["path"]])
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      timestamp_changed = !is.na(.data[["current_timestamp"]]) &
        !is.na(.data[["recorded_timestamp"]]) &
        as.integer(.data[["current_timestamp"]]) !=
          as.integer(.data[["recorded_timestamp"]]),
      checksum_changed = !is.na(.data[["current_checksum"]]) &
        !is.na(.data[["recorded_checksum"]]) &
        .data[["current_checksum"]] != .data[["recorded_checksum"]],
      status = dplyr::case_when(
        # built-in model files
        !.data[["file_exists"]] &
          stringr::str_detect(.data[["name"]], "lib.*txt") &
          !.data[["checksum_changed"]] &
          !.data[["timestamp_changed"]] ~ "unchanged",
        # all other cases
        !.data[["file_exists"]] ~ "missing",
        .data[["checksum_changed"]] ~ "modified",
        .data[["timestamp_changed"]] ~ "timestamp_changed",
        TRUE ~ "unchanged"
      )
    ) |>
    dplyr::select(
      run_id = .data[["run_id"]],
      file_path = .data[["path"]],
      file_name = .data[["name"]],
      .data[["recorded_timestamp"]],
      recorded_checksum = .data[["recorded_checksum"]],
      .data[["current_timestamp"]],
      .data[["current_checksum"]],
      .data[["timestamp_changed"]],
      .data[["checksum_changed"]],
      .data[["file_exists"]],
      .data[["status"]]
    )

  results
}

#' Check project file modification status
#'
#' Detects project files that have been modified since their latest successful run.
#'
#' @param directory Character scalar. Directory path to check for project files.
#' @param ext Character vector. File extensions to consider as
#'   project files. Defaults to c("mlxtran", "smlx").
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `project_file` (character): Full path to the project file
#'     \item `run_id` (integer): Most recent completed run ID (NA if no completed runs)
#'     \item `run_completed_at` (POSIXct): When the run was completed (NA if not completed)
#'     \item `recorded_timestamp` (POSIXct): Project file timestamp at run time
#'     \item `recorded_checksum` (character): Project file checksum at run time
#'     \item `current_timestamp` (POSIXct): Current project file modification time
#'     \item `current_checksum` (character): Current project file checksum
#'     \item `timestamp_changed` (logical): Whether modification time changed since run
#'     \item `checksum_changed` (logical): Whether file content changed since run
#'     \item `file_exists` (logical): Whether project file still exists
#'     \item `status` (character): Summary status
#'   }
#'
#' @export
check_project_file_status <- function(
  directory = ".",
  ext = c("mlxtran", "smlx"),
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Find all project files in directory
  project_files <- list.files(
    directory,
    pattern = paste0("\\.", ext, "$", collapse = "|"),
    full.names = TRUE,
    recursive = TRUE
  )

  if (length(project_files) == 0) {
    return(data.frame(
      project_file = character(0),
      run_id = integer(0),
      run_completed_at = as.POSIXct(character(0)),
      recorded_timestamp = as.POSIXct(character(0)),
      recorded_checksum = character(0),
      current_timestamp = as.POSIXct(character(0)),
      current_checksum = character(0),
      timestamp_changed = logical(0),
      checksum_changed = logical(0),
      file_exists = logical(0),
      status = character(0)
    ))
  }

  # Normalize paths for database comparison
  normalized_paths <- normalizePath(project_files, mustWork = FALSE)

  # Query for latest successful runs and their project file timestamps
  placeholders <- paste(rep("?", length(normalized_paths)), collapse = ",")

  project_run_data <- DBI::dbGetQuery(
    db_conn,
    paste0(
      "
      SELECT 
        f.path as project_file,
        r.run_id,
        r.completed_at,
        rf.timestamp as recorded_timestamp,
        rf.md5_checksum as recorded_checksum,
        ROW_NUMBER() OVER (PARTITION BY f.path ORDER BY r.completed_at DESC) as rn
      FROM file f
      JOIN run r ON f.id = r.project_file_id
      JOIN run_file rf ON r.run_id = rf.run_id AND rf.file_id = f.id
      WHERE f.path IN (",
      placeholders,
      ")
        AND r.completed_at IS NOT NULL
        AND rf.io_type = 'input'
      "
    ),
    params = as.list(normalized_paths)
  )

  # Get most recent completed run for each project file
  if (nrow(project_run_data) > 0) {
    latest_project_runs <- project_run_data |>
      dplyr::filter(.data[["rn"]] == 1) |>
      dplyr::select(-.data[["rn"]]) |>
      dplyr::mutate(
        run_completed_at = .data[["completed_at"]] |>
          lubridate::with_tz(Sys.timezone()),
        recorded_timestamp = .data[["recorded_timestamp"]] |>
          lubridate::with_tz(Sys.timezone())
      ) |>
      dplyr::select(-.data[["completed_at"]])
  } else {
    latest_project_runs <- data.frame(
      project_file = character(0),
      run_id = integer(0),
      run_completed_at = as.POSIXct(character(0)),
      recorded_timestamp = as.POSIXct(character(0)),
      recorded_checksum = character(0)
    )
  }

  # Create results for all project files
  results <- data.frame(
    project_file = normalized_paths,
    stringsAsFactors = FALSE
  ) |>
    dplyr::left_join(latest_project_runs, by = "project_file") |>
    dplyr::rowwise() |>
    dplyr::mutate(
      file_exists = file.exists(.data[["project_file"]]),
      current_timestamp = if (.data[["file_exists"]]) {
        file.mtime(.data[["project_file"]]) |>
          lubridate::with_tz(Sys.timezone())
      } else {
        as.POSIXct(NA)
      },
      current_checksum = calculate_md5(.data[["project_file"]])
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      # Only check for changes if we have a recorded run
      timestamp_changed = !is.na(.data[["current_timestamp"]]) &
        !is.na(.data[["recorded_timestamp"]]) &
        as.integer(.data[["current_timestamp"]]) !=
          as.integer(.data[["recorded_timestamp"]]),
      checksum_changed = !is.na(.data[["current_checksum"]]) &
        !is.na(.data[["recorded_checksum"]]) &
        .data[["current_checksum"]] != .data[["recorded_checksum"]],
      status = dplyr::case_when(
        !.data[["file_exists"]] ~ "missing",
        is.na(.data[["run_id"]]) ~ "no_completed_runs",
        .data[["checksum_changed"]] ~ "modified",
        .data[["timestamp_changed"]] ~ "timestamp_changed",
        TRUE ~ "unchanged"
      )
    ) |>
    dplyr::select(
      .data[["project_file"]],
      .data[["run_id"]],
      .data[["run_completed_at"]],
      .data[["recorded_timestamp"]],
      .data[["recorded_checksum"]],
      .data[["current_timestamp"]],
      .data[["current_checksum"]],
      .data[["timestamp_changed"]],
      .data[["checksum_changed"]],
      .data[["file_exists"]],
      .data[["status"]]
    )

  results
}

#' Check output file integrity
#'
#' Identifies output files that have been manually changed, deleted, or updated
#' with another tool since they were recorded.
#'
#' @param run_ids Integer vector. Specific run IDs to check. If NULL (default),
#'   checks all runs.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `run_id` (integer): The run identifier
#'     \item `file_path` (character): Full path to the output file
#'     \item `file_name` (character): File name (basename)
#'     \item `recorded_timestamp` (POSIXct): Timestamp when output was recorded
#'     \item `recorded_checksum` (character): MD5 checksum when recorded
#'     \item `current_timestamp` (POSIXct): Current file modification time
#'     \item `current_checksum` (character): Current MD5 checksum
#'     \item `timestamp_changed` (logical): Whether modification time changed
#'     \item `checksum_changed` (logical): Whether file content changed
#'     \item `file_exists` (logical): Whether file still exists
#'     \item `status` (character): Summary status
#'   }
#'
#' @export
check_output_file_status <- function(
  run_ids = NULL,
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Build query based on whether run_ids are specified
  if (is.null(run_ids)) {
    output_files_query <- "
      SELECT 
        rf.run_id,
        f.path,
        f.name,
        rf.timestamp as recorded_timestamp,
        rf.md5_checksum as recorded_checksum,
        r.completed_at
      FROM run_file rf
      JOIN file f ON rf.file_id = f.id
      JOIN run r ON rf.run_id = r.run_id
      WHERE rf.io_type = 'output'
        AND r.completed_at IS NOT NULL
      ORDER BY rf.run_id, f.path
    "
    output_files <- DBI::dbGetQuery(db_conn, output_files_query)
  } else {
    if (length(run_ids) == 0) {
      return(data.frame(
        run_id = integer(0),
        file_path = character(0),
        file_name = character(0),
        recorded_timestamp = as.POSIXct(character(0)),
        recorded_checksum = character(0),
        current_timestamp = as.POSIXct(character(0)),
        current_checksum = character(0),
        timestamp_changed = logical(0),
        checksum_changed = logical(0),
        file_exists = logical(0),
        status = character(0)
      ))
    }

    placeholders <- paste(rep("?", length(run_ids)), collapse = ",")
    output_files_query <- paste0(
      "
      SELECT 
        rf.run_id,
        f.path,
        f.name,
        rf.timestamp as recorded_timestamp,
        rf.md5_checksum as recorded_checksum,
        r.completed_at
      FROM run_file rf
      JOIN file f ON rf.file_id = f.id
      JOIN run r ON rf.run_id = r.run_id
      WHERE rf.io_type = 'output'
        AND r.completed_at IS NOT NULL
        AND rf.run_id IN (",
      placeholders,
      ")
      ORDER BY rf.run_id, f.path
      "
    )
    output_files <- DBI::dbGetQuery(
      db_conn,
      output_files_query,
      params = as.list(run_ids)
    )
  }

  if (nrow(output_files) == 0) {
    return(data.frame(
      run_id = integer(0),
      file_path = character(0),
      file_name = character(0),
      recorded_timestamp = as.POSIXct(character(0)),
      recorded_checksum = character(0),
      current_timestamp = as.POSIXct(character(0)),
      current_checksum = character(0),
      timestamp_changed = logical(0),
      checksum_changed = logical(0),
      file_exists = logical(0),
      status = character(0)
    ))
  }

  # Convert timestamps
  output_files <- output_files |>
    dplyr::mutate(
      recorded_timestamp = .data[["recorded_timestamp"]] |>
        lubridate::with_tz(Sys.timezone()),
      completed_at = .data[["completed_at"]] |>
        lubridate::with_tz(Sys.timezone())
    )

  # Check current file status
  results <- output_files |>
    dplyr::rowwise() |>
    dplyr::mutate(
      file_exists = file.exists(.data[["path"]]),
      current_timestamp = if (.data[["file_exists"]]) {
        file.mtime(.data[["path"]]) |>
          lubridate::with_tz(Sys.timezone())
      } else {
        as.POSIXct(NA)
      },
      current_checksum = calculate_md5(.data[["path"]])
    ) |>
    dplyr::ungroup() |>
    dplyr::mutate(
      timestamp_changed = !is.na(.data[["current_timestamp"]]) &
        !is.na(.data[["recorded_timestamp"]]) &
        as.integer(.data[["current_timestamp"]]) !=
          as.integer(.data[["recorded_timestamp"]]),
      checksum_changed = !is.na(.data[["current_checksum"]]) &
        !is.na(.data[["recorded_checksum"]]) &
        .data[["current_checksum"]] != .data[["recorded_checksum"]],
      status = dplyr::case_when(
        !.data[["file_exists"]] ~ "missing",
        .data[["checksum_changed"]] ~ "modified",
        .data[["timestamp_changed"]] ~ "timestamp_changed",
        TRUE ~ "unchanged"
      )
    ) |>
    dplyr::select(
      run_id = .data[["run_id"]],
      file_path = .data[["path"]],
      file_name = .data[["name"]],
      .data[["recorded_timestamp"]],
      recorded_checksum = .data[["recorded_checksum"]],
      .data[["current_timestamp"]],
      .data[["current_checksum"]],
      .data[["timestamp_changed"]],
      .data[["checksum_changed"]],
      .data[["file_exists"]],
      .data[["status"]]
    )

  results
}

#' Generate comprehensive project report
#'
#' Combines all validation checks into a single comprehensive report.
#'
#' @param directory Character scalar. Directory path to check for project files.
#' @param run_ids Integer vector. Specific run IDs to check. If NULL (default),
#'   checks all runs.
#' @param ext Character vector. File extensions to consider as
#'   project files. Defaults to c("mlxtran", "smlx").
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A list with components:
#'   \itemize{
#'     \item `summary`: Summary statistics across all checks
#'     \item `completion_status`: Results from check_completion_status()
#'     \item `project_file_status`: Results from check_project_file_status()
#'     \item `input_file_status`: Results from check_input_file_status()
#'     \item `output_file_status`: Results from check_output_file_status()
#'     \item `timestamp`: When the report was generated
#'   }
#'
#' @keywords internal
generate_project_report <- function(
  directory = ".",
  run_ids = NULL,
  ext = c("mlxtran", "smlx"),
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  directory <- normalizePath(directory)

  timestamp <- Sys.time()

  # Run all checks
  completion_status <- check_completion_status(
    directory = directory,
    ext = ext,
    db_conn = db_conn
  )

  project_file_status <- check_project_file_status(
    directory = directory,
    ext = ext,
    db_conn = db_conn
  )

  input_file_status <- check_input_file_status(
    run_ids = run_ids,
    db_conn = db_conn
  )

  output_file_status <- check_output_file_status(
    run_ids = run_ids,
    db_conn = db_conn
  )

  # Generate summary statistics
  summary <- list(
    directory = directory,
    total_project_files = nrow(completion_status),
    project_files_completed = sum(
      completion_status$has_successful_run,
      na.rm = TRUE
    ),
    project_files_incomplete = sum(
      !completion_status$has_successful_run & completion_status$has_run,
      na.rm = TRUE
    ),
    project_files_no_runs = sum(!completion_status$has_run, na.rm = TRUE),
    project_files_modified = sum(
      project_file_status$checksum_changed,
      na.rm = TRUE
    ),
    project_files_missing = sum(
      !project_file_status$file_exists,
      na.rm = TRUE
    ),
    input_files_checked = nrow(input_file_status),
    input_files_modified = sum(
      input_file_status$checksum_changed,
      na.rm = TRUE
    ),
    # status is "unchanged" for missing input files only for built-in model files.
    # that is why input_file_status$file_exists is not used.
    input_files_missing = sum(
      input_file_status$status == "missing",
      na.rm = TRUE
    ),
    output_files_checked = nrow(output_file_status),
    output_files_modified = sum(
      output_file_status$checksum_changed,
      na.rm = TRUE
    ),
    output_files_missing = sum(!output_file_status$file_exists, na.rm = TRUE),
    report_timestamp = timestamp
  )

  # Return comprehensive report
  list(
    summary = summary,
    completion_status = completion_status,
    project_file_status = project_file_status,
    input_file_status = input_file_status,
    output_file_status = output_file_status,
    timestamp = timestamp
  )
}

#' Print project report summary
#'
#' Prints a formatted summary of the project report to the console.
#'
#' @param directory Character scalar. Directory path to check for project files.
#' @param run_ids Integer vector. Specific run IDs to check. If NULL (default),
#'   checks all runs.
#' @param ext Character vector. File extensions to consider as
#'   project files. Defaults to c("mlxtran", "smlx").
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return NULL (invisible). Called for side effects (printing).
#'
#' @export
print_report_summary <- function(
  directory = ".",
  run_ids = NULL,
  ext = c("mlxtran", "smlx"),
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  report <- generate_project_report(
    directory = directory,
    run_ids = run_ids,
    ext = ext,
    db_conn = db_conn
  )

  cat("=== PROJECT VALIDATION REPORT ===\n")
  cat("Directory:", report$summary$directory, "\n")
  cat(
    "Generated:",
    format(report$summary$report_timestamp, "%Y-%m-%d %H:%M:%S %Z"),
    "\n\n"
  )

  cat("PROJECT COMPLETION:\n")
  cat("  Total project files:", report$summary$total_project_files, "\n")
  cat("  Successfully completed:", report$summary$project_files_completed, "\n")
  cat("  Incomplete runs:", report$summary$project_files_incomplete, "\n")
  cat("  No runs recorded:", report$summary$project_files_no_runs, "\n\n")

  cat("PROJECT FILE MODIFICATIONS:\n")
  cat(
    "  Project files modified since run:",
    report$summary$project_files_modified,
    "\n"
  )
  cat("  Missing project files:", report$summary$project_files_missing, "\n\n")

  cat("INPUT FILE MODIFICATIONS:\n")
  cat("  Total input files checked:", report$summary$input_files_checked, "\n")
  cat("  Files modified since run:", report$summary$input_files_modified, "\n")
  cat("  Missing input files:", report$summary$input_files_missing, "\n\n")

  cat("OUTPUT FILE INTEGRITY:\n")
  cat(
    "  Total output files checked:",
    report$summary$output_files_checked,
    "\n"
  )
  cat("  Files modified after run:", report$summary$output_files_modified, "\n")
  cat("  Missing output files:", report$summary$output_files_missing, "\n\n")

  # Highlight issues
  issues <- c()
  if (report$summary$project_files_no_runs > 0) {
    issues <- c(
      issues,
      paste(
        report$summary$project_files_no_runs,
        "project files have no run records"
      )
    )
  }
  if (report$summary$project_files_incomplete > 0) {
    issues <- c(
      issues,
      paste(
        report$summary$project_files_incomplete,
        "project files have incomplete runs"
      )
    )
  }
  if (report$summary$project_files_modified > 0) {
    issues <- c(
      issues,
      paste(
        report$summary$project_files_modified,
        "project files have been modified since their last run"
      )
    )
  }
  if (report$summary$project_files_missing > 0) {
    issues <- c(
      issues,
      paste(report$summary$project_files_missing, "project files are missing")
    )
  }
  if (report$summary$input_files_modified > 0) {
    issues <- c(
      issues,
      paste(
        report$summary$input_files_modified,
        "input files have been modified"
      )
    )
  }
  if (report$summary$output_files_modified > 0) {
    issues <- c(
      issues,
      paste(
        report$summary$output_files_modified,
        "output files have been modified"
      )
    )
  }
  if (report$summary$input_files_missing > 0) {
    issues <- c(
      issues,
      paste(report$summary$input_files_missing, "input files are missing")
    )
  }
  if (report$summary$output_files_missing > 0) {
    issues <- c(
      issues,
      paste(report$summary$output_files_missing, "output files are missing")
    )
  }

  if (length(issues) > 0) {
    cat("ISSUES IDENTIFIED:\n")
    for (issue in issues) {
      cat("  ⚠️ ", issue, "\n")
    }
  } else {
    cat("✅ No issues identified\n")
  }

  invisible(NULL)
}
