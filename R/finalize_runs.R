#' Record completion of runs whose launching session ended
#'
#' Records completion, marked as recorded late, for runs that were launched but
#' never recorded, for example because the R session that launched them died.
#' A run with a job ID is recorded once its job has left the queue (`sq`). A
#' run without a job ID is recorded once its output folder holds `summary.txt`.
#' If `sq` is missing or fails the function stops, because a running job would
#' otherwise be recorded as finished.
#'
#' @param store Character scalar. The run store directory (see [default_store()]). Defaults to
#'   [default_store()].
#' @param run_id Character vector. Limit finalization to these run IDs.
#'   `NULL` (default) considers every run in the store.
#' @param retry_crashed Logical. Record runs whose earlier completion attempt
#'   crashed into a fresh attempt folder. Otherwise they are only reported.
#'
#' @return A data frame with columns `run_id` and `action`, one row per run
#'   that was considered. `action` is `"recorded_late"`, `"still_queued"`,
#'   `"no_summary_yet"`, `"claimed_elsewhere"`, `"completion_crashed"` or
#'   `"skipped_submit_failed"` (only for runs named in `run_id`). Recorded runs
#'   are not listed.
#'
#' @examples
#' \dontrun{
#' finalize_runs()
#' finalize_runs(retry_crashed = TRUE)
#' }
#'
#' @export
finalize_runs <- function(
  store = default_store(),
  run_id = NULL,
  retry_crashed = FALSE
) {
  runs <- store_read(store)$runs
  if (!is.null(run_id)) {
    runs <- runs[runs$run_id %in% run_id, , drop = FALSE]
  }

  candidate_states <- c("unrecorded", if (retry_crashed) "completion_crashed")
  shown_states <- c(
    if (!retry_crashed) "completion_crashed",
    if (!is.null(run_id)) "submit_failed"
  )
  shown <- runs[runs$state %in% shown_states, , drop = FALSE]
  candidates <- runs[runs$state %in% candidate_states, , drop = FALSE]

  queued <- character()
  if (any(!is.na(candidates$job_id))) {
    queued <- sq_job_ids()
  }

  actions <- c(
    stats::setNames(
      ifelse(
        shown$state == "submit_failed",
        "skipped_submit_failed",
        "completion_crashed"
      ),
      shown$run_id
    ),
    stats::setNames(
      vapply(
        seq_len(nrow(candidates)),
        function(i) finalize_run(candidates[i, ], store, queued),
        character(1)
      ),
      candidates$run_id
    )
  )
  result <- data.frame(
    run_id = names(actions) %||% character(),
    action = unname(actions) %||% character(),
    stringsAsFactors = FALSE
  )
  result <- result[order(result$run_id), , drop = FALSE]
  rownames(result) <- NULL

  report_finalize_counts(result)
  result
}

# The action taken for one candidate run (a one-row data frame).
finalize_run <- function(run, store, queued) {
  if (!is.na(run$job_id)) {
    if (as.character(run$job_id) %in% queued) {
      return("still_queued")
    }
  } else if (!file.exists(run_summary_path(run))) {
    return("no_summary_yet")
  }
  crashed <- run$state == "completion_crashed"
  outcome <- tryCatch(
    record_completion(run$run_dir, store, late = TRUE, retry = crashed),
    error = function(e) {
      warning(
        "Recording ",
        run$run_id,
        " failed: ",
        conditionMessage(e),
        call. = FALSE
      )
      "completion_crashed"
    }
  )
  if (identical(outcome, "recorded")) "recorded_late" else outcome
}

run_summary_path <- function(run) {
  output_dir <- if (is.na(run$output_dir)) NULL else run$output_dir
  get_file_handler(tools::file_ext(run$project_path))$get_summary_file_path(
    run$project_path,
    output_dir
  )
}

report_finalize_counts <- function(result) {
  if (nrow(result) == 0L) {
    message("No runs to finalize.")
    return(invisible())
  }
  counts <- table(result$action)
  message(
    "finalize_runs: ",
    paste0(names(counts), " = ", as.integer(counts), collapse = ", ")
  )
}
