#' Check if job IDs are currently in the job queue
#'
#' Queries the job queue using the `sq` command to determine if one or more
#' job IDs are currently active (running or queued).
#'
#' @param job_id Numeric or character vector. Job IDs to check in the queue.
#'
#' @return Logical vector. Returns `TRUE` for job IDs found in the queue,
#'   `FALSE` for job IDs not found or if the `sq` command fails.
#'
#' @details
#' This function executes the `sq` command to retrieve current job queue
#' information and parses the output to check for the presence of specified
#' job IDs. The function:
#' \enumerate{
#'   \item Runs `system2("sq")` to get queue status
#'   \item Parses the output as a space-separated table with headers
#'   \item Checks if job IDs exist in the `JOBID` column
#' }
#'
#' If the `sq` command fails or its output has no `JOBID` column, the function
#' returns `FALSE` with a warning. A header-only listing is an empty queue.
#'
#' @examples
#' \dontrun{
#' # Check single job ID
#' job_in_sq(12345)
#'
#' # Check multiple job IDs
#' job_in_sq(c(12345, 67890))
#'
#' job_in_sq(c("12345", "67890"))
#' }
#'
#' @seealso
#' \code{\link{monitor_jobs}} for monitoring job completion,
#' \code{\link{execute_job}} for submitting jobs
#'
#' @keywords internal
job_in_sq <- function(job_id) {
  assertthat::assert_that(
    is.numeric(job_id) || is.character(job_id),
    msg = "`job_id` should be a vector of job IDs (numeric or character)."
  )

  queued <- tryCatch(
    sq_job_ids(),
    error = function(e) {
      warning(conditionMessage(e), call. = FALSE)
      NULL
    }
  )
  if (is.null(queued)) {
    return(FALSE)
  }
  as.character(job_id) %in% queued
}

# Job IDs currently in the queue, from the `JOBID` column of `sq`. Errors with
# "sq failed" when `sq` is missing, exits non-zero or prints no `JOBID`
# column; a header-only listing is an empty queue.
sq_job_ids <- function() {
  result <- tryCatch(
    suppressWarnings(system2("sq", stdout = TRUE, stderr = TRUE)),
    error = function(e) {
      stop("sq failed: ", conditionMessage(e), call. = FALSE)
    }
  )
  if (any(attr(result, "status") != 0, na.rm = TRUE)) {
    stop("sq failed: ", paste(result, collapse = " "), call. = FALSE)
  }
  result <- result[nzchar(trimws(result))]
  if (length(result) == 0L || !"JOBID" %in% strsplit(trimws(result[1]), "\\s+")[[1]]) {
    stop("sq failed: no JOBID column in output", call. = FALSE)
  }
  if (length(result) == 1L) {
    return(character())
  }
  sq_data <- readr::read_table(
    I(paste(result, collapse = "\n")),
    col_types = readr::cols(.default = "c")
  )
  sq_data$JOBID
}
