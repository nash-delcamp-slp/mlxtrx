# Records what a finished run produced into a completion folder of the run:
# outputs.json, summary.json (when summary.txt parses) and, last, done.json.
# Returns "recorded", or "claimed_elsewhere" when another process holds the
# completion claim (nothing is written then). `late` marks a recording made
# by someone other than the launching session; a retry is always late.
record_completion <- function(
  run_dir,
  store,
  late = FALSE,
  retry = FALSE,
  late_tolerance = 300
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

  completion_dir <- claim_completion(run_dir, retry = retry)
  if (is.null(completion_dir)) {
    message("Completion of ", basename(run_dir), " is already being recorded.")
    return("claimed_elsewhere")
  }

  handler <- get_file_handler(tools::file_ext(project_path))
  output_dir <- handler$get_output_dir(project_path, run$settings$output_dir)
  summary_path <- handler$get_summary_file_path(
    project_path,
    run$settings$output_dir
  )

  files <- list.files(
    output_dir,
    recursive = TRUE,
    full.names = TRUE,
    all.files = TRUE,
    no.. = TRUE
  )
  mtimes <- file.info(files)$mtime
  keep <- !is.na(mtimes) & mtimes >= submitted_at
  files <- files[keep]
  mtimes <- mtimes[keep]
  outputs <- lapply(files, file_record, root = root)
  store_write_json(outputs, file.path(completion_dir, "outputs.json"))

  if (file.exists(summary_path)) {
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
      late_flags = if (late) {
        lapply(
          late_output_files(files, mtimes, summary_path, late_tolerance),
          path_to_record,
          root = root
        )
      } else {
        list()
      }
    ),
    file.path(completion_dir, "done.json")
  )
  "recorded"
}

# Outputs written more than `tolerance` seconds after the reference time: the
# summary.txt mtime when it is an output, else the median output mtime.
late_output_files <- function(files, mtimes, summary_path, tolerance) {
  if (length(files) == 0L) {
    return(character())
  }
  is_summary <- fs::path_norm(files) == fs::path_norm(summary_path)
  reference <- if (any(is_summary)) {
    mtimes[is_summary][[1]]
  } else {
    stats::median(mtimes)
  }
  files[as.numeric(difftime(mtimes, reference, units = "secs")) > tolerance]
}
