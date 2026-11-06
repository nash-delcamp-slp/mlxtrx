# Reads the run store into data frames. Never creates or writes anything.
#
# Returns list(runs, files, summary, observations). Run folders that cannot be
# read are skipped with one warning each. Timestamps are POSIXct in the
# session time zone; paths are absolute, resolved against the store's substage.
store_read <- function(store) {
  root <- store_root_dir(store)
  runs_dir <- file.path(store, "runs")
  run_dirs <- if (dir.exists(runs_dir)) {
    sort(list.dirs(runs_dir, recursive = FALSE, full.names = TRUE))
  } else {
    character()
  }
  parsed <- list()
  for (run_dir in run_dirs) {
    res <- tryCatch(
      read_run_folder(run_dir, root),
      error = function(e) {
        warning(
          "Skipping unreadable run folder ",
          basename(run_dir),
          ": ",
          conditionMessage(e),
          call. = FALSE
        )
        NULL
      }
    )
    if (!is.null(res)) {
      parsed[[length(parsed) + 1L]] <- res
    }
  }
  assemble_store_frames(parsed)
}

# One run folder as list(run, files, summary, observations), each a list of
# plain values or a list of such rows.
read_run_folder <- function(run_dir, root) {
  run_path <- file.path(run_dir, "run.json")
  if (!file.exists(run_path)) {
    stop("run.json not found", call. = FALSE)
  }
  run <- jsonlite::read_json(run_path, simplifyVector = FALSE)
  run_id <- basename(run_dir)
  submission_ok <- !isFALSE(run$submission$ok)
  attempt <- if (submission_ok) find_completion_attempt(run_dir) else NULL

  state <- if (!submission_ok) {
    "submit_failed"
  } else if (is.null(attempt)) {
    "unrecorded"
  } else if (!attempt$has_done) {
    "completion_crashed"
  } else if (attempt$n > 1L || isTRUE(attempt$done$recorded_late)) {
    "recorded_late"
  } else {
    "recorded"
  }
  done <- if (state %in% c("recorded", "recorded_late")) attempt$done else NULL

  project <- run$project
  inputs <- run$inputs %||% list()
  input_path <- function(role) {
    for (inp in inputs) {
      if (identical(inp$role, role)) {
        return(path_from_record(inp, root))
      }
    }
    NA_character_
  }
  project_path <- path_from_record(project, root)

  late_flags <- vapply(
    done$late_flags %||% list(),
    function(p) path_from_record(list(path = p), root),
    character(1)
  )

  row <- list(
    run_id = run_id,
    run_dir = run_dir,
    state = state,
    launcher = scalar_chr(run$launcher),
    job_id = scalar_int(run$job_id),
    project_path = project_path,
    project_name = basename(project_path),
    data_file = input_path("data"),
    model_file = input_path("model"),
    thread = scalar_int(run$settings$thread),
    tool = scalar_chr(run$settings$tool),
    mode = scalar_chr(run$settings$mode),
    config = scalar_chr(run$settings$config),
    output_dir = scalar_chr(run$settings$output_dir),
    cmd = scalar_chr(run$cmd),
    software_version = scalar_chr(run$software_version),
    user = scalar_chr(run$user),
    host = scalar_chr(run$host),
    submitted_at = parse_utc(run$submitted_at),
    submission_ok = submission_ok,
    completed_at = parse_utc(done$completed_at),
    recorded_at = parse_utc(done$recorded_at),
    recorded_late = if (is.null(done)) NA else state == "recorded_late",
    late_flags = list(late_flags),
    project_file_timestamp = parse_utc(project$mtime),
    project_file_sha256 = scalar_chr(project$sha256)
  )

  file_row <- function(io_type, rec) {
    builtin <- isTRUE(rec$builtin)
    path <- path_from_record(rec, root)
    list(
      run_id = run_id,
      io_type = io_type,
      path = path,
      name = basename(path),
      timestamp = parse_utc(rec$mtime),
      sha256 = scalar_chr(rec$sha256),
      size = scalar_dbl(rec$size),
      builtin = builtin
    )
  }
  files <- c(
    list(file_row("project", project)),
    lapply(inputs, function(rec) file_row("input", rec))
  )

  summary <- NULL
  observations <- list()
  if (!is.null(done)) {
    outputs_path <- file.path(attempt$dir, "outputs.json")
    if (file.exists(outputs_path)) {
      outputs <- jsonlite::read_json(outputs_path, simplifyVector = FALSE)
      files <- c(files, lapply(outputs, function(rec) file_row("output", rec)))
    }
    summary_path <- file.path(attempt$dir, "summary.json")
    if (file.exists(summary_path)) {
      sj <- jsonlite::read_json(summary_path, simplifyVector = FALSE)
      stats <- sj$summary_stats %||% list()
      summary <- list(
        run_id = run_id,
        exploratory_iterations = scalar_int(stats$exploratory_iterations),
        smoothing_iterations = scalar_int(stats$smoothing_iterations),
        fisher_matrix_estimation = scalar_lgl(stats$fisher_matrix_estimation),
        eigenvalue_min = scalar_dbl(stats$eigenvalue_min),
        eigenvalue_max = scalar_dbl(stats$eigenvalue_max),
        ofv = scalar_dbl(stats$ofv),
        aic = scalar_dbl(stats$aic),
        bicc = scalar_dbl(stats$bicc),
        bic = scalar_dbl(stats$bic),
        n_individuals = scalar_int(stats$n_individuals),
        n_doses = scalar_int(stats$n_doses),
        summary_extracted_at = parse_utc(sj$summary_extracted_at)
      )
      observations <- lapply(names(sj$observations), function(nm) {
        list(
          run_id = run_id,
          obs_type = nm,
          n_observations = scalar_int(sj$observations[[nm]])
        )
      })
    }
  }
  list(run = row, files = files, summary = summary, observations = observations)
}

# The attempt used for a run: the highest-numbered completion folder holding
# done.json (`completion` is attempt 1). When none has done.json, the highest
# folder is returned with has_done = FALSE. NULL when there is no folder.
find_completion_attempt <- function(run_dir) {
  dirs <- list.dirs(run_dir, recursive = FALSE, full.names = FALSE)
  dirs <- dirs[grepl("^completion(-[0-9]+)?$", dirs)]
  if (length(dirs) == 0L) {
    return(NULL)
  }
  n <- ifelse(dirs == "completion", 1L, as.integer(sub("^completion-?", "", dirs)))
  dirs <- dirs[order(n, decreasing = TRUE)]
  n <- sort(n, decreasing = TRUE)
  for (i in seq_along(dirs)) {
    done_path <- file.path(run_dir, dirs[[i]], "done.json")
    if (file.exists(done_path)) {
      return(list(
        dir = file.path(run_dir, dirs[[i]]),
        n = n[[i]],
        has_done = TRUE,
        done = jsonlite::read_json(done_path, simplifyVector = FALSE)
      ))
    }
  }
  list(dir = file.path(run_dir, dirs[[1]]), n = n[[1]], has_done = FALSE, done = NULL)
}

scalar_chr <- function(x) if (is.null(x)) NA_character_ else as.character(x)
scalar_int <- function(x) if (is.null(x)) NA_integer_ else as.integer(x)
scalar_dbl <- function(x) if (is.null(x)) NA_real_ else as.numeric(x)
scalar_lgl <- function(x) if (is.null(x)) NA else as.logical(x)

# Seconds since the epoch for a JSON UTC timestamp, NA when absent.
parse_utc <- function(x) {
  if (is.null(x)) {
    return(NA_real_)
  }
  as.numeric(as.POSIXct(strptime(x, "%Y-%m-%dT%H:%M:%OSZ", tz = "UTC")))
}

store_tz <- function() {
  tz <- Sys.timezone()
  if (is.na(tz)) "UTC" else tz
}

# Column of `rows[[i]][[name]]` values as a vector of the given type.
col_of <- function(rows, name, type) {
  vapply(rows, function(r) r[[name]], FUN.VALUE = type, USE.NAMES = FALSE)
}

assemble_store_frames <- function(parsed) {
  tz <- store_tz()
  as_time <- function(x) as.POSIXct(x, origin = "1970-01-01", tz = tz)
  runs <- lapply(parsed, `[[`, "run")
  files <- unlist(lapply(parsed, `[[`, "files"), recursive = FALSE)
  summaries <- Filter(Negate(is.null), lapply(parsed, `[[`, "summary"))
  obs <- unlist(lapply(parsed, `[[`, "observations"), recursive = FALSE)

  runs_df <- data.frame(
    run_id = col_of(runs, "run_id", ""),
    run_dir = col_of(runs, "run_dir", ""),
    state = col_of(runs, "state", ""),
    launcher = col_of(runs, "launcher", ""),
    job_id = col_of(runs, "job_id", 0L),
    project_path = col_of(runs, "project_path", ""),
    project_name = col_of(runs, "project_name", ""),
    data_file = col_of(runs, "data_file", ""),
    model_file = col_of(runs, "model_file", ""),
    thread = col_of(runs, "thread", 0L),
    tool = col_of(runs, "tool", ""),
    mode = col_of(runs, "mode", ""),
    config = col_of(runs, "config", ""),
    output_dir = col_of(runs, "output_dir", ""),
    cmd = col_of(runs, "cmd", ""),
    software_version = col_of(runs, "software_version", ""),
    user = col_of(runs, "user", ""),
    host = col_of(runs, "host", ""),
    submitted_at = as_time(col_of(runs, "submitted_at", 0)),
    submission_ok = col_of(runs, "submission_ok", TRUE),
    completed_at = as_time(col_of(runs, "completed_at", 0)),
    recorded_at = as_time(col_of(runs, "recorded_at", 0)),
    recorded_late = col_of(runs, "recorded_late", TRUE),
    project_file_timestamp = as_time(col_of(runs, "project_file_timestamp", 0)),
    project_file_sha256 = col_of(runs, "project_file_sha256", ""),
    stringsAsFactors = FALSE
  )
  runs_df$late_flags <- lapply(runs, function(r) r$late_flags[[1]])
  runs_df <- runs_df[, c(
    setdiff(names(runs_df), c("project_file_timestamp", "project_file_sha256", "late_flags")),
    "late_flags", "project_file_timestamp", "project_file_sha256"
  )]

  files_df <- data.frame(
    run_id = col_of(files, "run_id", ""),
    io_type = col_of(files, "io_type", ""),
    path = col_of(files, "path", ""),
    name = col_of(files, "name", ""),
    timestamp = as_time(col_of(files, "timestamp", 0)),
    sha256 = col_of(files, "sha256", ""),
    size = col_of(files, "size", 0),
    builtin = col_of(files, "builtin", TRUE),
    stringsAsFactors = FALSE
  )

  summary_df <- data.frame(
    run_id = col_of(summaries, "run_id", ""),
    exploratory_iterations = col_of(summaries, "exploratory_iterations", 0L),
    smoothing_iterations = col_of(summaries, "smoothing_iterations", 0L),
    fisher_matrix_estimation = col_of(summaries, "fisher_matrix_estimation", TRUE),
    eigenvalue_min = col_of(summaries, "eigenvalue_min", 0),
    eigenvalue_max = col_of(summaries, "eigenvalue_max", 0),
    ofv = col_of(summaries, "ofv", 0),
    aic = col_of(summaries, "aic", 0),
    bicc = col_of(summaries, "bicc", 0),
    bic = col_of(summaries, "bic", 0),
    n_individuals = col_of(summaries, "n_individuals", 0L),
    n_doses = col_of(summaries, "n_doses", 0L),
    summary_extracted_at = as_time(col_of(summaries, "summary_extracted_at", 0)),
    stringsAsFactors = FALSE
  )

  obs_df <- data.frame(
    run_id = col_of(obs, "run_id", ""),
    obs_type = col_of(obs, "obs_type", ""),
    n_observations = col_of(obs, "n_observations", 0L),
    stringsAsFactors = FALSE
  )

  list(runs = runs_df, files = files_df, summary = summary_df, observations = obs_df)
}
