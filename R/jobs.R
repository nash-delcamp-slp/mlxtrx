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
#' If the `sq` command fails, returns no output, or the output cannot be
#' parsed, the function returns `FALSE` and issues appropriate warnings.
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

  # Convert to character for consistent handling
  job_id <- as.character(job_id)

  # Run sq command and capture output
  result <- system2("sq", stdout = TRUE, stderr = TRUE)

  # Handle case where sq command fails or returns no output
  if (any(attr(result, "status") != 0, na.rm = TRUE)) {
    warning("sq command failed")
    return(FALSE)
  }

  if (length(result) == 0) {
    return(FALSE)
  }

  # Parse as data frame - assuming space-separated output with headers
  # Create temporary connection to read the output
  sq_data <- tryCatch(
    {
      readr::read_table(
        I(paste(result, collapse = "\n")),
        col_types = readr::cols(.default = "c")
      )
    },
    error = function(e) {
      warning("Failed to parse sq output as data frame: ", e$message)
      return(NULL)
    }
  )

  # Return FALSE if parsing failed
  if (is.null(sq_data)) {
    return(FALSE)
  }

  # Check if JOBID column exists and contains the job_id
  if (!"JOBID" %in% names(sq_data)) {
    warning("No JOBID column found in sq output")
    return(FALSE)
  }

  # Check if job_id is in the JOBID column
  job_id %in% sq_data$JOBID
}
