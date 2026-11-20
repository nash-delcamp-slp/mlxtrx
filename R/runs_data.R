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
#' @param include_summary Logical scalar. Whether to include the statistics
#'   parsed from the run's results (`summary.txt` for Monolix, the result
#'   tables for PKanalix, the `Simulation/` and `Endpoints/` files for
#'   Simulx). Default is `FALSE`.
#'
#' @return A data frame with run information, newest submission first. Contains
#'   columns: `run_id` (character), `job_id`, `project_name`, `project_path`,
#'   `data_file`, `model_file`, `thread`, `tool`, `mode`, `config`,
#'   `project_file_timestamp`, `project_file_sha256`, `cmd`, `software_version`,
#'   `user`, `submitted_at`, `completed_at`, `state`, `recorded_late`.
#'   `user` is the account that launched the run.
#'   `software_version` is the MonolixSuite version stated in the run's
#'   `summary.txt` (Monolix, PKanalix) when recorded, else the version implied
#'   by the command (e.g. `"2024R1"` for `mono24`), else `NA`.
#'   `state` is one of `"submit_failed"`, `"unrecorded"`,
#'   `"completion_crashed"`, `"recorded"`, `"recorded_late"` or `"no_results"`
#'   (recorded, but a Monolix, PKanalix or Simulx run without results).
#'   When `include_files = TRUE`, additionally contains one row per input and
#'   output file with `io_type`, `file_name`, `file_path`, `file_timestamp` and
#'   `sha256`.
#'   When `include_summary = TRUE`, additionally contains the run statistics:
#'   for Monolix, those of summary.txt and one `n_obs_<type>` column per
#'   observation type; for PKanalix, `tasks` (the tasks with results, e.g.
#'   `"nca,ca,be"`), `n_individuals`, the compartmental analysis criteria
#'   `ofv` (-2LL), `aic`, `bic` and `ca_cost`, and per bioequivalence
#'   parameter the columns `be_<parameter>_ratio`, `be_<parameter>_ci_lower`,
#'   `be_<parameter>_ci_upper` and `be_<parameter>_bioequivalent` (logical).
#'   A run with more than one formulation gets
#'   `be_<formulation>_<parameter>_...` columns. For Simulx, `tasks`
#'   (`"simulation"`, `"simulation,endpoint"`). Columns of the other tools
#'   are `NA`. Characters in `<type>`, `<parameter>` and `<formulation>`
#'   other than letters, digits and `_` are replaced by `_`, so `"obsid 1"`
#'   gives `n_obs_obsid_1`.
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
    bioequivalence_wide <- pivot_bioequivalence(data$bioequivalence)
    if (!is.null(bioequivalence_wide)) {
      runs <- dplyr::left_join(runs, bioequivalence_wide, by = "run_id")
    }
  }

  summary_columns <- c(
    "tasks",
    "exploratory_iterations",
    "smoothing_iterations",
    "fisher_matrix_estimation",
    "eigenvalue_min",
    "eigenvalue_max",
    "ofv",
    "aic",
    "bicc",
    "bic",
    "ca_cost",
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
      "software_version",
      "user",
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
      "software_version",
      "user",
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
    dplyr::starts_with("n_obs_"),
    dplyr::starts_with("be_")
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
  col_names <- make.unique(paste0("n_obs_", syntactic_part(types)), sep = "_")
  for (i in seq_along(types)) {
    type <- types[[i]]
    of_type <- observations[observations$obs_type == type, , drop = FALSE]
    wide[[col_names[[i]]]] <-
      of_type$n_observations[match(wide$run_id, of_type$run_id)]
  }
  wide
}

# Bioequivalence rows as one row per run with `be_<parameter>_ratio`,
# `_ci_lower`, `_ci_upper` and `_bioequivalent` columns per parameter; NULL
# when there are none. A run with more than one formulation gets
# `be_<formulation>_<parameter>_...` columns. Names are cleaned as in
# pivot_observations(), and parameters that end up with the same name are
# made unique with a "_<n>" suffix.
pivot_bioequivalence <- function(bioequivalence) {
  if (nrow(bioequivalence) == 0) {
    return(NULL)
  }
  n_formulations <- tapply(
    bioequivalence$formulation,
    bioequivalence$run_id,
    function(x) length(unique(x))
  )
  several <- n_formulations[bioequivalence$run_id] > 1L
  label <- ifelse(
    several,
    paste(bioequivalence$formulation, bioequivalence$parameter, sep = "_"),
    bioequivalence$parameter
  )
  wide <- data.frame(
    run_id = unique(bioequivalence$run_id),
    stringsAsFactors = FALSE
  )
  labels <- unique(label)
  prefixes <- make.unique(paste0("be_", syntactic_part(labels)), sep = "_")
  for (i in seq_along(labels)) {
    of_label <- bioequivalence[label == labels[[i]], , drop = FALSE]
    rows <- match(wide$run_id, of_label$run_id)
    for (stat in c("ratio", "ci_lower", "ci_upper", "bioequivalent")) {
      wide[[paste0(prefixes[[i]], "_", stat)]] <- of_label[[stat]][rows]
    }
  }
  wide
}

# `x` with each run of characters outside [A-Za-z0-9_] replaced by "_" and
# leading and trailing "_" dropped.
syntactic_part <- function(x) {
  gsub("^_+|_+$", "", gsub("[^A-Za-z0-9_]+", "_", x))
}

# Rows of `df` holding the largest `time` within each value of `key`; the
# first such row wins ties.
latest_by <- function(df, key, time) {
  ord <- order(time, decreasing = TRUE)
  df <- df[ord, , drop = FALSE]
  df[!duplicated(key[ord]), , drop = FALSE]
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
