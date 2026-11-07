#' Check completion status of project files in a directory
#'
#' Validates that all project files in a directory have successful run records.
#' A successful run is one whose latest run (by `submitted_at`) has a
#' completion timestamp.
#'
#' @param directory Character scalar. Directory path to check for project files.
#' @param ext Character vector. File extensions to consider as
#'   project files. Defaults to c("mlxtran", "smlx").
#' @param store Character scalar. Path to the run store (a `.mlxtrx` folder).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `project_file` (character): Full path to the project file
#'     \item `has_run` (logical): Whether any run record exists for this file
#'     \item `has_successful_run` (logical): Whether the latest run completed
#'     \item `latest_run_id` (character): Most recent run_id (NA if no runs)
#'     \item `latest_completed_at` (POSIXct): Latest completion timestamp (NA if not completed)
#'     \item `recorded_late` (logical): Whether the latest run's completion was
#'       recorded late (NA if it was not recorded)
#'     \item `status` (character): Summary status ("completed",
#'       "completed_late", "incomplete", "no_runs")
#'   }
#'
#' @export
check_completion_status <- function(
  directory = ".",
  ext = c("mlxtran", "smlx"),
  store = default_store()
) {
  project_files <- find_project_files(directory, ext)
  runs <- store_read(store)$runs
  runs$project_file <- path_key(runs$project_path)
  latest <- latest_by(runs, runs$project_file, runs$submitted_at)
  idx <- match(project_files, latest$project_file)

  has_run <- !is.na(idx)
  has_successful_run <- !is.na(latest$completed_at[idx])
  recorded_late <- latest$recorded_late[idx]

  data.frame(
    project_file = project_files,
    has_run = has_run,
    has_successful_run = has_successful_run,
    latest_run_id = latest$run_id[idx],
    latest_completed_at = latest$completed_at[idx],
    recorded_late = recorded_late,
    status = dplyr::case_when(
      !has_run ~ "no_runs",
      has_successful_run & recorded_late %in% TRUE ~ "completed_late",
      has_successful_run ~ "completed",
      TRUE ~ "incomplete"
    ),
    stringsAsFactors = FALSE
  )
}

#' Check input file modification status
#'
#' Detects input files that have been modified since the run began.
#' Compares current file timestamps and checksums against recorded values.
#'
#' @param run_ids Character vector. Specific run IDs to check. If NULL (default),
#'   checks all runs.
#' @param store Character scalar. Path to the run store (a `.mlxtrx` folder).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `run_id` (character): The run identifier
#'     \item `file_path` (character): Full path to the input file
#'     \item `file_name` (character): File name (basename)
#'     \item `recorded_timestamp` (POSIXct): File timestamp when the run was submitted
#'     \item `recorded_sha256` (character): SHA-256 checksum when the run was submitted
#'     \item `current_timestamp` (POSIXct): Current file modification time
#'     \item `current_sha256` (character): Current SHA-256 checksum
#'     \item `timestamp_changed` (logical): Whether modification time changed
#'     \item `checksum_changed` (logical): Whether file content changed
#'     \item `file_exists` (logical): Whether file still exists
#'     \item `recorded_late` (logical): Whether the run's completion was
#'       recorded late (NA if it was not recorded)
#'     \item `status` (character): Summary status
#'   }
#'
#' @export
check_input_file_status <- function(
  run_ids = NULL,
  store = default_store()
) {
  check_run_ids(run_ids)
  data <- store_read(store)
  files <- run_file_records(data, "input", run_ids)

  results <- compare_with_disk(files$path, files$timestamp, files$sha256)
  status <- dplyr::case_when(
    # Monolix library models are not files on disk
    files$builtin &
      !results$file_exists &
      !results$checksum_changed &
      !results$timestamp_changed ~ "unchanged",
    !results$file_exists ~ "missing",
    results$checksum_changed ~ "modified",
    results$timestamp_changed ~ "timestamp_changed",
    TRUE ~ "unchanged"
  )

  data.frame(
    run_id = files$run_id,
    file_path = files$path,
    file_name = files$name,
    recorded_timestamp = files$timestamp,
    recorded_sha256 = files$sha256,
    results,
    recorded_late = files$recorded_late,
    status = status,
    stringsAsFactors = FALSE
  )
}

#' Check project file modification status
#'
#' Detects project files that have been modified since their latest completed
#' run.
#'
#' @param directory Character scalar. Directory path to check for project files.
#' @param ext Character vector. File extensions to consider as
#'   project files. Defaults to c("mlxtran", "smlx").
#' @param store Character scalar. Path to the run store (a `.mlxtrx` folder).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `project_file` (character): Full path to the project file
#'     \item `run_id` (character): Most recent completed run ID (NA if no completed runs)
#'     \item `run_completed_at` (POSIXct): When the run was completed (NA if not completed)
#'     \item `recorded_timestamp` (POSIXct): Project file timestamp at run time
#'     \item `recorded_sha256` (character): Project file SHA-256 checksum at run time
#'     \item `current_timestamp` (POSIXct): Current project file modification time
#'     \item `current_sha256` (character): Current project file SHA-256 checksum
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
  store = default_store()
) {
  project_files <- find_project_files(directory, ext)
  data <- store_read(store)
  runs <- data$runs[!is.na(data$runs$completed_at), , drop = FALSE]
  runs$project_file <- path_key(runs$project_path)
  latest <- latest_by(runs, runs$project_file, runs$completed_at)
  idx <- match(project_files, latest$project_file)
  run_id <- latest$run_id[idx]

  project_records <- data$files[data$files$io_type == "project", , drop = FALSE]
  rec <- match(run_id, project_records$run_id)
  recorded_timestamp <- project_records$timestamp[rec]
  recorded_sha256 <- project_records$sha256[rec]

  results <- compare_with_disk(project_files, recorded_timestamp, recorded_sha256)

  data.frame(
    project_file = project_files,
    run_id = run_id,
    run_completed_at = latest$completed_at[idx],
    recorded_timestamp = recorded_timestamp,
    recorded_sha256 = recorded_sha256,
    results,
    status = dplyr::case_when(
      !results$file_exists ~ "missing",
      is.na(run_id) ~ "no_completed_runs",
      results$checksum_changed ~ "modified",
      results$timestamp_changed ~ "timestamp_changed",
      TRUE ~ "unchanged"
    ),
    stringsAsFactors = FALSE
  )
}

#' Check output file integrity
#'
#' Identifies output files that have been manually changed, deleted, or updated
#' with another tool since they were recorded. Outputs of runs whose completion
#' was recorded late are never reported as plain `"unchanged"`.
#'
#' @param run_ids Character vector. Specific run IDs to check. If NULL (default),
#'   checks all runs.
#' @param store Character scalar. Path to the run store (a `.mlxtrx` folder).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `run_id` (character): The run identifier
#'     \item `file_path` (character): Full path to the output file
#'     \item `file_name` (character): File name (basename)
#'     \item `recorded_timestamp` (POSIXct): Timestamp when output was recorded
#'     \item `recorded_sha256` (character): SHA-256 checksum when recorded
#'     \item `current_timestamp` (POSIXct): Current file modification time
#'     \item `current_sha256` (character): Current SHA-256 checksum
#'     \item `timestamp_changed` (logical): Whether modification time changed
#'     \item `checksum_changed` (logical): Whether file content changed
#'     \item `file_exists` (logical): Whether file still exists
#'     \item `recorded_late` (logical): Whether the run's completion was
#'       recorded late
#'     \item `status` (character): Summary status: `"missing"`, `"modified"`,
#'       `"timestamp_changed"`, `"suspect"` (unchanged, but flagged when the
#'       late recording found it written long after the other outputs),
#'       `"unchanged_late"` (unchanged, recorded late) or `"unchanged"`
#'   }
#'
#' @export
check_output_file_status <- function(
  run_ids = NULL,
  store = default_store()
) {
  check_run_ids(run_ids)
  data <- store_read(store)
  files <- run_file_records(data, "output", run_ids)
  files <- files[!is.na(files$completed_at), , drop = FALSE]

  results <- compare_with_disk(files$path, files$timestamp, files$sha256)

  data.frame(
    run_id = files$run_id,
    file_path = files$path,
    file_name = files$name,
    recorded_timestamp = files$timestamp,
    recorded_sha256 = files$sha256,
    results,
    recorded_late = files$recorded_late,
    status = output_status(
      results,
      files$recorded_late,
      is_late_flagged(path_key(files$path), files$late_flags)
    ),
    stringsAsFactors = FALSE
  )
}

#' Check specified output files alignment with latest runs
#'
#' Validates that the specified output files were generated from the most recent
#' run that should have produced them. Ensures output files are current and
#' haven't been modified since generation.
#'
#' @param output_files Character vector. Paths to output files to validate.
#' @param store Character scalar. Path to the run store (a `.mlxtrx` folder).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return A data frame with columns:
#'   \itemize{
#'     \item `file_path` (character): Full path to the output file
#'     \item `file_name` (character): File name (basename)
#'     \item `run_id` (character): Most recently submitted run that recorded
#'       this file (NA if not found)
#'     \item `run_submitted_at` (POSIXct): When the generating run was submitted
#'     \item `run_completed_at` (POSIXct): When the generating run completed
#'     \item `recorded_timestamp` (POSIXct): File timestamp when recorded in the store
#'     \item `recorded_sha256` (character): SHA-256 checksum when recorded
#'     \item `current_timestamp` (POSIXct): Current file modification time
#'     \item `current_sha256` (character): Current SHA-256 checksum
#'     \item `timestamp_changed` (logical): Whether modification time changed since recording
#'     \item `checksum_changed` (logical): Whether file content changed since recording
#'     \item `file_exists` (logical): Whether file still exists
#'     \item `predates_submission` (logical): Whether file predates the run submission
#'     \item `recorded_late` (logical): Whether the run's completion was
#'       recorded late
#'     \item `status` (character): Summary status: `"missing"`,
#'       `"no_run_record"`, `"incomplete_run"`, `"predates_run"`, `"modified"`,
#'       `"timestamp_changed"`, `"suspect"`, `"unchanged_late"` or
#'       `"unchanged"` (see [check_output_file_status()])
#'   }
#'
#' @export
check_output_files <- function(
  output_files,
  store = default_store()
) {
  file_paths <- path_key(as.character(output_files))
  data <- store_read(store)
  outputs <- run_file_records(data, "output", NULL)
  outputs$key <- path_key(outputs$path)
  latest <- latest_by(outputs, outputs$key, outputs$submitted_at)
  idx <- match(file_paths, latest$key)

  run_id <- latest$run_id[idx]
  run_submitted_at <- latest$submitted_at[idx]
  run_completed_at <- latest$completed_at[idx]
  recorded_late <- latest$recorded_late[idx]
  results <- compare_with_disk(
    file_paths,
    latest$timestamp[idx],
    latest$sha256[idx]
  )
  predates_submission <- !is.na(results$current_timestamp) &
    !is.na(run_submitted_at) &
    results$current_timestamp < run_submitted_at

  later_status <- output_status(
    results,
    recorded_late,
    is_late_flagged(file_paths, latest$late_flags[idx])
  )

  data.frame(
    file_path = file_paths,
    file_name = basename(file_paths),
    run_id = run_id,
    run_submitted_at = run_submitted_at,
    run_completed_at = run_completed_at,
    recorded_timestamp = latest$timestamp[idx],
    recorded_sha256 = latest$sha256[idx],
    results,
    predates_submission = predates_submission,
    recorded_late = recorded_late,
    status = dplyr::case_when(
      !results$file_exists ~ "missing",
      is.na(run_id) ~ "no_run_record",
      is.na(run_completed_at) ~ "incomplete_run",
      predates_submission ~ "predates_run",
      TRUE ~ later_status
    ),
    stringsAsFactors = FALSE
  )
}

# Project files under `directory`, as path_key() paths.
find_project_files <- function(directory, ext) {
  path_key(list.files(
    directory,
    pattern = paste0("\\.", ext, "$", collapse = "|"),
    full.names = TRUE,
    recursive = TRUE
  ))
}

# Comparable form of a path: absolute, symlinks resolved where the file
# exists, `/` separators.
path_key <- function(path) {
  normalizePath(path, winslash = "/", mustWork = FALSE)
}

check_run_ids <- function(run_ids) {
  assertthat::assert_that(
    is.null(run_ids) || is.character(run_ids),
    msg = "`run_ids` must be NULL or a character vector"
  )
}

# File records of one io_type with their run's submitted_at, completed_at,
# recorded_late and late_flags, ordered by run_id and path. `run_ids` NULL
# keeps all runs.
run_file_records <- function(data, io_type, run_ids) {
  files <- data$files[data$files$io_type == io_type, , drop = FALSE]
  if (!is.null(run_ids)) {
    files <- files[files$run_id %in% run_ids, , drop = FALSE]
  }
  files <- files[order(files$run_id, files$path), , drop = FALSE]
  run <- match(files$run_id, data$runs$run_id)
  files$submitted_at <- data$runs$submitted_at[run]
  files$completed_at <- data$runs$completed_at[run]
  files$recorded_late <- data$runs$recorded_late[run]
  files$late_flags <- data$runs$late_flags[run]
  rownames(files) <- NULL
  files
}

# The current state on disk of `paths` against their recorded timestamps and
# SHA-256 checksums. Changes are FALSE when either side is missing.
compare_with_disk <- function(paths, recorded_timestamp, recorded_sha256) {
  file_exists <- file.exists(paths)
  current_timestamp <- lubridate::with_tz(file.mtime(paths), store_tz())
  current_sha256 <- vapply(
    paths,
    calculate_sha256,
    character(1),
    USE.NAMES = FALSE
  )
  data.frame(
    current_timestamp = current_timestamp,
    current_sha256 = current_sha256,
    timestamp_changed = !is.na(current_timestamp) &
      !is.na(recorded_timestamp) &
      as.integer(current_timestamp) != as.integer(recorded_timestamp),
    checksum_changed = !is.na(current_sha256) &
      !is.na(recorded_sha256) &
      current_sha256 != recorded_sha256,
    file_exists = file_exists,
    stringsAsFactors = FALSE
  )
}

# Whether each path is among its run's late_flags (a list of path vectors).
is_late_flagged <- function(paths, late_flags) {
  vapply(
    seq_along(paths),
    function(i) paths[[i]] %in% path_key(late_flags[[i]] %||% character()),
    logical(1)
  )
}

# Status of recorded outputs: a would-be "unchanged" becomes "suspect" when
# the late recording flagged the file, or "unchanged_late" when the run was
# recorded late.
output_status <- function(results, recorded_late, flagged) {
  dplyr::case_when(
    !results$file_exists ~ "missing",
    results$checksum_changed ~ "modified",
    results$timestamp_changed ~ "timestamp_changed",
    flagged ~ "suspect",
    recorded_late %in% TRUE ~ "unchanged_late",
    TRUE ~ "unchanged"
  )
}
