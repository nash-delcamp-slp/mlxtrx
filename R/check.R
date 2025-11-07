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

#' Check specified output files alignment with latest runs
#'
#' Validates that the specified output files were generated from the most recent
#' run that should have produced them. Ensures output files are current and
#' haven't been modified since generation.
#'
#' @param output_files Character vector. Paths to output files to validate.
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `file_path` (character): Full path to the output file
#'     \item `file_name` (character): File name (basename)
#'     \item `run_id` (integer): Most recent run ID that generated this file (NA if not found)
#'     \item `run_submitted_at` (POSIXct): When the generating run was submitted
#'     \item `run_completed_at` (POSIXct): When the generating run completed
#'     \item `recorded_timestamp` (POSIXct): File timestamp when recorded in database
#'     \item `recorded_checksum` (character): MD5 checksum when recorded
#'     \item `current_timestamp` (POSIXct): Current file modification time
#'     \item `current_checksum` (character): Current MD5 checksum
#'     \item `timestamp_changed` (logical): Whether modification time changed since recording
#'     \item `checksum_changed` (logical): Whether file content changed since recording
#'     \item `file_exists` (logical): Whether file still exists
#'     \item `predates_submission` (logical): Whether file predates the run submission
#'     \item `status` (character): Summary status
#'   }
#'
#' @export
check_output_files <- function(
  output_files,
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  if (length(output_files) == 0) {
    return(data.frame(
      file_path = character(0),
      file_name = character(0),
      run_id = integer(0),
      run_submitted_at = as.POSIXct(character(0)),
      run_completed_at = as.POSIXct(character(0)),
      recorded_timestamp = as.POSIXct(character(0)),
      recorded_checksum = character(0),
      current_timestamp = as.POSIXct(character(0)),
      current_checksum = character(0),
      timestamp_changed = logical(0),
      checksum_changed = logical(0),
      file_exists = logical(0),
      predates_submission = logical(0),
      status = character(0)
    ))
  }

  # Normalize paths for database comparison
  normalized_paths <- normalizePath(output_files, mustWork = FALSE)

  # Query for the latest run that generated each output file
  placeholders <- paste(rep("?", length(normalized_paths)), collapse = ",")

  output_run_data <- DBI::dbGetQuery(
    db_conn,
    paste0(
      "
      SELECT 
        f.path as file_path,
        f.name as file_name,
        rf.run_id,
        r.submitted_at,
        r.completed_at,
        rf.timestamp as recorded_timestamp,
        rf.md5_checksum as recorded_checksum,
        ROW_NUMBER() OVER (PARTITION BY f.path ORDER BY r.submitted_at DESC) as rn
      FROM file f
      JOIN run_file rf ON f.id = rf.file_id
      JOIN run r ON rf.run_id = r.run_id
      WHERE f.path IN (",
      placeholders,
      ")
        AND rf.io_type = 'output'
      "
    ),
    params = as.list(normalized_paths)
  )

  # Get the most recent run for each output file
  if (nrow(output_run_data) > 0) {
    latest_output_runs <- output_run_data |>
      dplyr::filter(.data[["rn"]] == 1) |>
      dplyr::select(-.data[["rn"]]) |>
      dplyr::mutate(
        run_submitted_at = .data[["submitted_at"]] |>
          lubridate::with_tz(Sys.timezone()),
        run_completed_at = .data[["completed_at"]] |>
          lubridate::with_tz(Sys.timezone()),
        recorded_timestamp = .data[["recorded_timestamp"]] |>
          lubridate::with_tz(Sys.timezone())
      ) |>
      dplyr::select(
        -.data[["submitted_at"]],
        -.data[["completed_at"]]
      )
  } else {
    latest_output_runs <- data.frame(
      file_path = character(0),
      file_name = character(0),
      run_id = integer(0),
      run_submitted_at = as.POSIXct(character(0)),
      run_completed_at = as.POSIXct(character(0)),
      recorded_timestamp = as.POSIXct(character(0)),
      recorded_checksum = character(0)
    )
  }

  # Create results for all requested files
  results <- data.frame(
    file_path = normalized_paths,
    file_name = basename(normalized_paths),
    stringsAsFactors = FALSE
  ) |>
    dplyr::left_join(latest_output_runs, by = "file_path") |>
    dplyr::mutate(
      file_name = dplyr::coalesce(
        .data[["file_name.y"]],
        .data[["file_name.x"]]
      )
    ) |>
    dplyr::select(-.data[["file_name.x"]], -.data[["file_name.y"]]) |>
    dplyr::rowwise() |>
    dplyr::mutate(
      file_exists = file.exists(.data[["file_path"]]),
      current_timestamp = if (.data[["file_exists"]]) {
        file.mtime(.data[["file_path"]]) |>
          lubridate::with_tz(Sys.timezone())
      } else {
        as.POSIXct(NA)
      },
      current_checksum = calculate_md5(.data[["file_path"]])
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
      predates_submission = !is.na(.data[["current_timestamp"]]) &
        !is.na(.data[["run_submitted_at"]]) &
        .data[["current_timestamp"]] < .data[["run_submitted_at"]],
      status = dplyr::case_when(
        !.data[["file_exists"]] ~ "missing",
        is.na(.data[["run_id"]]) ~ "no_run_record",
        is.na(.data[["run_completed_at"]]) ~ "incomplete_run",
        .data[["predates_submission"]] ~ "predates_run",
        .data[["checksum_changed"]] ~ "modified",
        .data[["timestamp_changed"]] ~ "timestamp_changed",
        TRUE ~ "unchanged"
      )
    ) |>
    dplyr::select(
      .data[["file_path"]],
      .data[["file_name"]],
      .data[["run_id"]],
      .data[["run_submitted_at"]],
      .data[["run_completed_at"]],
      .data[["recorded_timestamp"]],
      .data[["recorded_checksum"]],
      .data[["current_timestamp"]],
      .data[["current_checksum"]],
      .data[["timestamp_changed"]],
      .data[["checksum_changed"]],
      .data[["file_exists"]],
      .data[["predates_submission"]],
      .data[["status"]]
    )

  results
}
