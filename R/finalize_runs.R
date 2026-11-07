#' Record completion of runs whose launching session ended
#'
#' Records completion, marked as recorded late, for runs that were launched but
#' never recorded, for example because the R session that launched them died.
#' A run with a job ID is recorded once its job has left the queue (`sq`). A
#' run launched with the local runners `monolix.sh` or `simulx.sh` had finished
#' before its launch was recorded, so it is recorded straight away. Any other
#' run without a job ID is recorded once its output folder holds `summary.txt`.
#' If `sq` is missing or fails the function stops, because a running job would
#' otherwise be recorded as finished.
#'
#' Only runs whose `run.json` user is the current user are recorded, because
#' `sq` may list only the current user's jobs and another user's running job
#' would then look finished. Other users' runs are reported as `"other_user"`
#' and must be finalized by their owner.
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
#'   `"no_summary_yet"`, `"claimed_elsewhere"`, `"completion_crashed"`,
#'   `"other_user"` or `"skipped_submit_failed"` (only for runs named in
#'   `run_id`). Recorded runs
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
  all_runs <- store_read(store)$runs
  runs <- all_runs
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
  mine <- candidates$user %in% Sys.info()[["user"]]
  others <- candidates[!mine, , drop = FALSE]
  candidates <- candidates[mine, , drop = FALSE]

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
    stats::setNames(rep("other_user", nrow(others)), others$run_id),
    stats::setNames(
      vapply(
        seq_len(nrow(candidates)),
        function(i) {
          finalize_run(candidates[i, ], store, queued, all_runs)
        },
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

# The action taken for one candidate run (a one-row data frame). Outputs
# written once a later run into the same output directory was submitted are
# not credited to this run. A local runner's run has finished by the time run.json exists,
# so it does not wait for summary.txt.
finalize_run <- function(run, store, queued, all_runs) {
  if (!is.na(run$job_id)) {
    if (as.character(run$job_id) %in% queued) {
      return("still_queued")
    }
  } else if (
    !basename(run$cmd) %in% c("monolix.sh", "simulx.sh") &&
      !file.exists(run_summary_path(run))
  ) {
    return("no_summary_yet")
  }
  crashed <- run$state == "completion_crashed"
  outcome <- tryCatch(
    record_completion(
      run$run_dir,
      store,
      late = TRUE,
      retry = crashed,
      until = next_submission(run, all_runs)
    ),
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

# Earliest submission time of a later run (any state) whose saved output
# directory is this run's, or NULL when there is none.
next_submission <- function(run, all_runs) {
  later <- all_runs$submitted_at[
    all_runs$output_dir == run$output_dir &
      all_runs$submitted_at > run$submitted_at
  ]
  later <- later[!is.na(later)]
  if (length(later) == 0L) NULL else min(later)
}

# summary.txt in the output directory saved at launch, or NA without one.
run_summary_path <- function(run) {
  if (is.na(run$output_dir)) {
    return(NA_character_)
  }
  file.path(run$output_dir, "summary.txt")
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
  others <- result$run_id[result$action == "other_user"]
  if (length(others) > 0L) {
    message(
      "Runs launched by other users must be finalized by their owner: ",
      paste(others, collapse = ", ")
    )
  }
}
