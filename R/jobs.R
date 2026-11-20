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
