#' Extract data about executed Monolix runs
#'
#' Reads the run store for Monolix jobs that have been submitted and recorded.
#' Returns a data frame with basic job information or detailed file tracking
#' data.
#'
#' @param store Character scalar. The run store directory (see [default_store()]).
#'   Defaults to `default_store()`. Nothing is written to it.
#' @param include_files Logical scalar. Whether to include details about input and
#'   output files in the data. Default is `FALSE`.
#' @param ignore_failed_runs Logical scalar. Whether to exclude runs where
#'   `completed_at` is missing or the run finished without results (i.e., failed,
#'   incomplete or `"no_results"` runs). Default is `FALSE`.
#' @param latest_only Logical scalar. Whether to return only the latest run
#'   (by `submitted_at`) for each project file. Default is `FALSE`.
#' @param include_summary Logical scalar. Whether to include information from the
#'   summary.txt file. Default is `FALSE`.
#'
#' @return A data frame with run information, newest submission first. Contains
#'   columns: `run_id` (character), `job_id`, `project_name`, `project_path`,
#'   `data_file`, `model_file`, `thread`, `tool`, `mode`, `config`,
#'   `project_file_timestamp`, `project_file_sha256`, `cmd`, `submitted_at`,
#'   `completed_at`, `state`, `recorded_late`. `state` is one of `"submit_failed"`,
#'   `"unrecorded"`, `"completion_crashed"`, `"recorded"`, `"recorded_late"` or
#'   `"no_results"` (recorded, but a Monolix run without a summary).
#'   When `include_files = TRUE`, additionally contains one row per input and
#'   output file with `io_type`, `file_name`, `file_path`, `file_timestamp` and
#'   `sha256`.
#'   When `include_summary = TRUE`, additionally contains run summary from
#'   summary.txt and one `n_obs_<type>` column per observation type. Characters in
#'   `<type>` other than letters, digits and `_` are replaced by `_`, so
#'   `"obsid 1"` gives `n_obs_obsid_1`.
#'   Timestamps are in the session time zone. Returns `NULL` invisibly, with a
#'   message, when there are no runs.
#'
#' @export
runs_data <- function(
  store = default_store(),
  include_files = FALSE,
  ignore_failed_runs = FALSE,
  latest_only = FALSE,
  include_summary = FALSE
) {
  assertthat::assert_that(
    assertthat::is.string(store),
    msg = "`store` must be a character scalar"
  )
  assertthat::assert_that(
    is.logical(include_files) && length(include_files) == 1,
    msg = "`include_files` must be a logical scalar"
  )
  assertthat::assert_that(
    is.logical(ignore_failed_runs) && length(ignore_failed_runs) == 1,
    msg = "`ignore_failed_runs` must be a logical scalar"
  )
  assertthat::assert_that(
    is.logical(latest_only) && length(latest_only) == 1,
    msg = "`latest_only` must be a logical scalar"
  )
  assertthat::assert_that(
    is.logical(include_summary) && length(include_summary) == 1,
    msg = "`include_summary` must be a logical scalar"
  )

  data <- store_read(store)
  runs <- data$runs
  if (ignore_failed_runs) {
    runs <- runs[
      !is.na(runs$completed_at) & runs$state != "no_results",
      ,
      drop = FALSE
    ]
  }
  if (latest_only) {
    runs <- latest_by(runs, runs$project_path, runs$submitted_at)
  }

  if (nrow(runs) == 0) {
    message("No runs found")
    return(invisible(NULL))
  }

  runs <- runs[order(runs$submitted_at, decreasing = TRUE), , drop = FALSE]

  if (include_summary) {
    runs <- dplyr::left_join(runs, data$summary, by = "run_id")
    observations_wide <- pivot_observations(data$observations)
    if (!is.null(observations_wide)) {
      runs <- dplyr::left_join(runs, observations_wide, by = "run_id")
    }
  }

  summary_columns <- c(
    "exploratory_iterations",
    "smoothing_iterations",
    "fisher_matrix_estimation",
    "eigenvalue_min",
    "eigenvalue_max",
    "ofv",
    "aic",
    "bicc",
    "bic",
    "n_individuals",
    "n_doses",
    "summary_extracted_at"
  )

  if (include_files) {
    files <- data$files[
      data$files$io_type %in% c("input", "output"),
      c("run_id", "io_type", "path", "name", "timestamp", "sha256"),
      drop = FALSE
    ]
    names(files) <- c(
      "run_id",
      "io_type",
      "file_path",
      "file_name",
      "file_timestamp",
      "sha256"
    )
    runs$run_order <- seq_len(nrow(runs))
    runs <- dplyr::left_join(runs, files, by = "run_id")
    runs <- runs[
      order(runs$run_order, runs$io_type, runs$file_path),
      ,
      drop = FALSE
    ]
    columns <- c(
      "run_id",
      "job_id",
      "project_path",
      "project_name",
      "data_file",
      "model_file",
      "thread",
      "tool",
      "mode",
      "config",
      "project_file_timestamp",
      "project_file_sha256",
      "io_type",
      "file_name",
      "file_path",
      "file_timestamp",
      "sha256",
      "cmd",
      "submitted_at",
      "completed_at",
      "state",
      "recorded_late",
      summary_columns
    )
  } else {
    columns <- c(
      "run_id",
      "job_id",
      "project_name",
      "project_path",
      "data_file",
      "model_file",
      "thread",
      "tool",
      "mode",
      "config",
      "project_file_timestamp",
      "project_file_sha256",
      "cmd",
      "submitted_at",
      "completed_at",
      "state",
      "recorded_late",
      summary_columns
    )
  }

  out <- dplyr::select(
    runs,
    dplyr::any_of(columns),
    dplyr::starts_with("n_obs_")
  )
  rownames(out) <- NULL
  out
}

# Observation counts as one row per run with an `n_obs_<type>` column per
# observation type; NULL when there are none. Each run of characters outside
# [A-Za-z0-9_] in a type becomes "_" (leading and trailing "_" dropped), and
# types that end up with the same name are made unique with a "_<n>" suffix.
pivot_observations <- function(observations) {
  if (nrow(observations) == 0) {
    return(NULL)
  }
  wide <- data.frame(
    run_id = unique(observations$run_id),
    stringsAsFactors = FALSE
  )
  types <- unique(observations$obs_type)
  col_names <- make.unique(
    paste0("n_obs_", gsub("^_+|_+$", "", gsub("[^A-Za-z0-9_]+", "_", types))),
    sep = "_"
  )
  for (i in seq_along(types)) {
    type <- types[[i]]
    of_type <- observations[observations$obs_type == type, , drop = FALSE]
    wide[[col_names[[i]]]] <-
      of_type$n_observations[match(wide$run_id, of_type$run_id)]
  }
  wide
}

# Rows of `df` holding the largest `time` within each value of `key`; the
# first such row wins ties.
latest_by <- function(df, key, time) {
  ord <- order(time, decreasing = TRUE)
  df <- df[ord, , drop = FALSE]
  df[!duplicated(key[ord]), , drop = FALSE]
}

#' Generate a table with details about executed Monolix runs
#'
#' Creates a formatted table displaying information about Monolix jobs that have
#' been submitted and recorded in the run store. The table can include basic job
#' information or detailed file tracking data.
#'
#' @param store Character scalar. The run store directory (see [default_store()]).
#'   Defaults to `default_store()`. Nothing is written to it.
#' @param format Character scalar. Output format for the table.
#'   Either `"gt"` for a formatted gt table (default) or `"csv"` for a data frame.
#' @param output_file Character scalar or `NULL`. File path to save output.
#' @param include_files Logical scalar. Whether to include details about input and
#'   output files in the table. Default is `FALSE`.
#' @param ignore_failed_runs Logical scalar. Whether to exclude runs where
#'   `completed_at` is missing or the run finished without results (i.e., failed,
#'   incomplete or `"no_results"` runs). Default is `FALSE`.
#' @param latest_only Logical scalar. Whether to return only the latest run
#'   (by `submitted_at`) for each project file. Default is `FALSE`.
#' @param include_summary Logical scalar. Whether to include information from the
#'   summary.txt file. Default is `FALSE`.
#'
#' @return When `format = "gt"`, returns a `gt_tbl` object. When `format = "csv"`
#'   and `output_file` is specified, returns the data frame invisibly after writing
#'   to file. When `format = "csv"` and `output_file` is `NULL`, returns the data frame.
#'   Returns `NULL` invisibly if no runs are found in the store.
#'
#' @examples
#' \dontrun{
#' # Table of all runs
#' runs_table()
#'
#' # Table with input/output file information
#' runs_table(include_files = TRUE)
#'
#' # Table excluding failed runs
#' runs_table(ignore_failed_runs = TRUE)
#'
#' # Table showing only latest run per project file
#' runs_table(latest_only = TRUE)
#'
#' # Table with summary.txt fields
#' runs_table(include_summary = TRUE)
#'
#' # Export to CSV
#' runs_table(format = "csv", output_file = "runs_summary.csv")
#'
#' # Read the store of another folder
#' runs_table(store = "/path/to/monolix/.mlxtrx")
#' }
#'
#' @seealso
#' \code{\link{runs_data}} for extracting the underlying data,
#' \code{\link{mono}} for submitting Monolix jobs and recording runs,
#' \code{\link{default_store}} for the default run store
#'
#' @export
runs_table <- function(
  store = default_store(),
  format = "gt",
  output_file = NULL,
  include_files = FALSE,
  ignore_failed_runs = FALSE,
  latest_only = FALSE,
  include_summary = FALSE
) {
  format <- match.arg(format, c("gt", "csv"))

  # Get the data using runs_data()
  runs_formatted <- runs_data(
    store = store,
    include_files = include_files,
    ignore_failed_runs = ignore_failed_runs,
    latest_only = latest_only,
    include_summary = include_summary
  )

  # Return early if no data
  if (is.null(runs_formatted)) {
    return(invisible(NULL))
  }

  # Handle output format
  if (format == "csv") {
    if (!is.null(output_file)) {
      readr::write_csv(runs_formatted, output_file)
      message("CSV written to: ", output_file)
      return(invisible(runs_formatted))
    } else {
      return(runs_formatted)
    }
  } else {
    # Create gt table
    if (include_files) {
      gt_table <- runs_formatted |>
        dplyr::mutate(
          run_id = sprintf(
            "%s '%s' (%s)",
            .data[["cmd"]],
            .data[["project_path"]],
            .data[["run_id"]]
          ),
          job_id = NULL,
          project_path = NULL,
          project_name = NULL,
          cmd = NULL
        ) |>
        gt::gt(groupname_col = "run_id") |>
        gt::tab_header(
          title = "Executed Runs with File Details",
          subtitle = paste("Total runs:", length(unique(runs_formatted$run_id)))
        ) |>
        gt::cols_label(
          io_type = "Type",
          file_name = "File",
          file_path = "Path",
          file_timestamp = "File Modified",
          sha256 = "SHA-256",
          data_file = "Data File",
          model_file = "Model File",
          thread = "Threads",
          tool = "Tool",
          mode = "Mode",
          config = "Config File",
          project_file_timestamp = "Project Modified",
          project_file_sha256 = "Project SHA-256",
          submitted_at = "Submitted",
          completed_at = "Completed",
          state = "State",
          recorded_late = "Late"
        ) |>
        gt::fmt_datetime(
          columns = dplyr::any_of(c(
            "submitted_at",
            "completed_at",
            "project_file_timestamp",
            "file_timestamp"
          )),
          date_style = "yMMMd",
          time_style = "Hm"
        )
    } else {
      gt_table <- runs_formatted |>
        gt::gt() |>
        gt::tab_header(
          title = "Executed Runs",
          subtitle = paste("Total runs:", nrow(runs_formatted))
        ) |>
        gt::cols_label(
          run_id = "Run ID",
          project_name = "Project",
          project_path = "Path",
          data_file = "Data File",
          model_file = "Model File",
          thread = "Threads",
          tool = "Tool",
          mode = "Mode",
          config = "Config File",
          project_file_timestamp = "Project Modified",
          project_file_sha256 = "Project SHA-256",
          submitted_at = "Submitted",
          completed_at = "Completed",
          state = "State",
          recorded_late = "Late"
        ) |>
        gt::fmt_datetime(
          columns = dplyr::any_of(c(
            "submitted_at",
            "completed_at",
            "project_file_timestamp"
          )),
          date_style = "yMMMd",
          time_style = "Hm"
        )
    }

    gt_table <- gt_table |>
      gt::tab_style(
        style = gt::cell_text(font = gt::google_font("JetBrains Mono")),
        locations = gt::cells_body(
          columns = dplyr::contains(c("job_id", "path", "sha256"))
        )
      ) |>
      gt::tab_options(
        table.font.size = 12,
        heading.title.font.size = 16
      )

    if (!is.null(output_file)) {
      gt::gtsave(gt_table, output_file)
      message("HTML written to: ", output_file)
      return(invisible(gt_table))
    } else {
      return(gt_table)
    }
  }
}

#' Get file information for specific runs
#'
#' Reads the project, input and output file records of one or more runs from
#' the run store.
#'
#' @param run_id Character vector. The run ID(s) to return file records for.
#' @param store Character scalar. The run store directory (see [default_store()]).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return A data frame with columns: `run_id`, `io_type` (`"project"`,
#'   `"input"` or `"output"`), `path`, `name`, `timestamp`, `sha256`, `size`
#'   and `builtin`, ordered by `run_id`, `io_type` and `path`. Timestamps are in
#'   the session time zone. Has no rows when none of the runs have file records.
#'
#' @export
get_run_files <- function(run_id, store = default_store()) {
  assertthat::assert_that(
    is.character(run_id),
    msg = "`run_id` must be a character vector"
  )
  files <- store_read(store)$files
  files <- files[files$run_id %in% run_id, , drop = FALSE]
  files <- files[order(files$run_id, files$io_type, files$path), , drop = FALSE]
  rownames(files) <- NULL
  files
}
