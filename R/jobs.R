# function to check if a job is in the sq
job_in_sq <- function(job_id) {
  assertthat::assert_that(
    length(job_id) == 1,
    is.numeric(job_id) || is.character(job_id),
    msg = "`job_id` should be a single job ID (numeric or character)."
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
