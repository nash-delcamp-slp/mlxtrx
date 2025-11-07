#' Generate comprehensive project report
#'
#' Combines all validation checks into a single comprehensive report.
#'
#' @param directory Character scalar. Directory path to check for project files.
#' @param run_ids Integer vector. Specific run IDs to check. If NULL (default),
#'   checks all runs.
#' @param ext Character vector. File extensions to consider as
#'   project files. Defaults to c("mlxtran", "smlx").
#' @param output_files Character vector. Optional output file paths to validate
#'   alignment with latest runs. If NULL (default), no output file alignment
#'   check is performed.
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
#'     \item `output_files_alignment`: Results from check_output_files() (if output_files provided)
#'     \item `timestamp`: When the report was generated
#'   }
#'
#' @keywords internal
generate_project_report <- function(
  directory = ".",
  run_ids = NULL,
  ext = c("mlxtran", "smlx"),
  output_files = NULL,
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

  # Optional output files alignment check
  output_files_alignment <- NULL
  if (!is.null(output_files) && length(output_files) > 0) {
    output_files_alignment <- check_output_files(
      output_files = output_files,
      db_conn = db_conn
    )
  }

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

  # Add output files alignment summary if checked
  if (!is.null(output_files_alignment)) {
    summary$output_files_alignment_checked <- nrow(output_files_alignment)
    summary$output_files_alignment_issues <- sum(
      output_files_alignment$status != "unchanged",
      na.rm = TRUE
    )
    summary$output_files_predates_submission <- sum(
      output_files_alignment$predates_submission,
      na.rm = TRUE
    )
    summary$output_files_no_run_record <- sum(
      output_files_alignment$status == "no_run_record",
      na.rm = TRUE
    )
  }

  # Return comprehensive report
  result <- list(
    summary = summary,
    completion_status = completion_status,
    project_file_status = project_file_status,
    input_file_status = input_file_status,
    output_file_status = output_file_status,
    timestamp = timestamp
  )

  if (!is.null(output_files_alignment)) {
    result$output_files_alignment <- output_files_alignment
  }

  result
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
#' @param output_files Character vector. Optional output file paths to validate
#'   alignment with latest runs. If NULL (default), no output file alignment
#'   section is included in the report.
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
  output_files = NULL,
  db_conn = default_db_conn()
) {
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  report <- generate_project_report(
    directory = directory,
    run_ids = run_ids,
    ext = ext,
    output_files = output_files,
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

  # Optional output files alignment section
  if (!is.null(output_files) && !is.null(report$output_files_alignment)) {
    cat("OUTPUT FILES ALIGNMENT:\n")
    cat(
      "  Files checked for alignment:",
      report$summary$output_files_alignment_checked,
      "\n"
    )
    cat(
      "  Files with alignment issues:",
      report$summary$output_files_alignment_issues,
      "\n"
    )
    cat(
      "  Files predating submission:",
      report$summary$output_files_predates_submission,
      "\n"
    )
    cat(
      "  Files with no run record:",
      report$summary$output_files_no_run_record,
      "\n\n"
    )
  }

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

  # Add output files alignment issues if checked
  if (
    !is.null(output_files) &&
      !is.null(report$summary$output_files_alignment_issues)
  ) {
    if (report$summary$output_files_alignment_issues > 0) {
      issues <- c(
        issues,
        paste(
          report$summary$output_files_alignment_issues,
          "output files have alignment issues with latest runs"
        )
      )
    }
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
