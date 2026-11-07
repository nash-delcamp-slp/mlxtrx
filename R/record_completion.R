# Records what a finished run produced into a completion folder of the run:
# outputs.json, summary.json (when summary.txt parses) and, last, done.json.
# Outputs are read from the output directory saved in run.json at launch.
# Returns "recorded", or "claimed_elsewhere" when another process holds the
# completion claim (nothing is written then). `late` marks a recording made
# by someone other than the launching session; a retry is always late.
# `until` is the submission time of a later run of the same project: outputs
# written at or after it may be that run's, so they are flagged, and
# summary.txt among them is not parsed.
record_completion <- function(
  run_dir,
  store,
  late = FALSE,
  retry = FALSE,
  late_tolerance = 300,
  until = NULL
) {
  late <- isTRUE(late) || isTRUE(retry)
  root <- store_root_dir(store)
  run <- jsonlite::read_json(
    file.path(run_dir, "run.json"),
    simplifyVector = FALSE
  )
  project_path <- path_from_record(run$project, root)
  submitted_at <- as.POSIXct(
    run$submitted_at,
    format = "%Y-%m-%dT%H:%M:%OSZ",
    tz = "UTC"
  )
  if (length(submitted_at) != 1L || is.na(submitted_at)) {
    stop("run.json has no valid submitted_at", call. = FALSE)
  }
  if (is.null(run$output_dir$path)) {
    stop("run.json has no output_dir", call. = FALSE)
  }
  handler <- get_file_handler(tools::file_ext(project_path))
  output_dir <- path_from_record(run$output_dir, root)
  summary_path <- file.path(output_dir, "summary.txt")

  # Everything above can fail on a malformed run.json; the claim comes after
  # it so such a failure leaves no completion folder.
  completion_dir <- claim_completion(run_dir, retry = retry)
  if (is.null(completion_dir)) {
    message("Completion of ", basename(run_dir), " is already being recorded.")
    return("claimed_elsewhere")
  }

  files <- list.files(
    output_dir,
    recursive = TRUE,
    full.names = TRUE,
    all.files = TRUE,
    no.. = TRUE
  )
  listed_mtimes <- file.info(files)$mtime
  files <- files[!is.na(listed_mtimes) & listed_mtimes >= submitted_at]
  outputs <- lapply(files, file_record, root = root)
  store_write_json(outputs, file.path(completion_dir, "outputs.json"))
  mtimes <- as.POSIXct(
    vapply(outputs, function(rec) rec$mtime, character(1)),
    format = "%Y-%m-%dT%H:%M:%OSZ",
    tz = "UTC"
  )

  flagged <- if (late) late_output_files(files, mtimes, late_tolerance) else character()
  if (!is.null(until)) {
    flagged <- union(flagged, files[mtimes >= until])
  }

  if (summary_is_output(setdiff(files, flagged), summary_path)) {
    parsed <- handler$parse_summary(summary_path)
    if (!is.null(parsed)) {
      parsed$summary_extracted_at <- format_utc(Sys.time())
      store_write_json(parsed, file.path(completion_dir, "summary.json"))
    }
  }

  recorded_at <- Sys.time()
  info <- Sys.info()
  store_write_json(
    list(
      completed_at = format_utc(if (length(mtimes) > 0) max(mtimes) else recorded_at),
      recorded_at = format_utc(recorded_at),
      recorded_by = paste0(info[["user"]], "@", info[["nodename"]]),
      recorded_late = late,
      late_flags = lapply(flagged, path_to_record, root = root)
    ),
    file.path(completion_dir, "done.json")
  )
  "recorded"
}

# Whether summary.txt is among the recorded outputs.
summary_is_output <- function(files, summary_path) {
  any(fs::path_norm(files) == fs::path_norm(summary_path))
}

# Outputs written more than `tolerance` seconds after the median output mtime.
late_output_files <- function(files, mtimes, tolerance) {
  if (length(files) == 0L) {
    return(character())
  }
  reference <- stats::median(mtimes)
  files[as.numeric(difftime(mtimes, reference, units = "secs")) > tolerance]
}
