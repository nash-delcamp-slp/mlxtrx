# Records what a finished run produced into a completion folder of the run:
# outputs.json, summary.json (when the handler's result files parse) and,
# last, done.json. Outputs are read from the output directory saved in
# run.json at launch. Only result files that are recorded outputs and are not
# flagged are parsed. Returns "recorded", or "claimed_elsewhere" when another
# process holds the completion claim (nothing is written then). `late` marks
# a recording made by someone other than the launching session; a retry is
# always late. `until` is the submission time of a later run of the same
# project or into the same output directory: outputs written at or after it
# may be that run's, so they are flagged.
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

  result_files <- handler$result_files(output_dir, setdiff(files, flagged))
  if (length(result_files) > 0L) {
    parsed <- handler$parse_results(result_files)
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

# Outputs written more than `tolerance` seconds after the median output mtime.
late_output_files <- function(files, mtimes, tolerance) {
  if (length(files) == 0L) {
    return(character())
  }
  reference <- stats::median(mtimes)
  files[as.numeric(difftime(mtimes, reference, units = "secs")) > tolerance]
}
