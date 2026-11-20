#' Report one condition per model
#'
#' Tells, for each model a path covers, whether the results on disk still
#' match the files that produced them. Each model is judged by its latest run
#' (the run with the latest `submitted_at` for that project file, whatever its
#' state), the same rule as [check_completion_status()]. Only file content
#' (SHA-256) counts: a changed timestamp with the same content is not a change.
#'
#' What `path` covers:
#' \itemize{
#'   \item a directory: every `.mlxtran`, `.smlx` and `.pkx` file under it,
#'     recursively, one row each (as in [check_completion_status()]);
#'   \item a project file: one row;
#'   \item a file recorded as an input or output of the latest run of some
#'     models: one row per such model;
#'   \item any other existing file: one row with condition `"untracked"`.
#' }
#' A path that does not exist and is not recorded in the store is an error.
#'
#' Conditions, most serious first:
#' \enumerate{
#'   \item `"never_run"`: no run of the model is recorded.
#'   \item `"failed"`: the latest run failed to submit or finished without
#'     results.
#'   \item `"unfinished"`: the latest run's completion was not recorded, or
#'     recording it crashed.
#'   \item `"inputs_changed"`: the project file or a recorded input of the
#'     latest run has different content or is missing. Monolix library
#'     models (`lib:`) never count.
#'   \item `"output_edited"`: a recorded output of the latest run has
#'     different content or is missing.
#'   \item `"output_suspect"`: an unchanged recorded output was flagged when
#'     the latest run's completion was recorded late.
#'   \item `"clean_late"`: the latest run was recorded late and nothing above
#'     applies.
#'   \item `"clean"`: nothing above applies.
#'   \item `"untracked"`: the path is not a model, nor a file recorded by the
#'     latest run of any model.
#' }
#'
#' @param path Character vector. Files or directories to report on. Relative
#'   paths resolve against the working directory. Defaults to `"."`.
#' @param store Character scalar. The run store directory (see [default_store()]).
#'   Defaults to `default_store()`. Nothing is written to it, nor anywhere else.
#'
#' @return A data frame with one row per path and model (duplicates removed),
#'   with columns:
#'   \itemize{
#'     \item `path` (character): The path given, absolute and normalised
#'     \item `project_file` (character): Full path to the model's project file
#'       (NA when untracked)
#'     \item `run_id` (character): The model's latest run (NA if none)
#'     \item `condition` (character): The most serious condition that applies
#'     \item `other_conditions` (character): The other conditions that apply,
#'       comma-separated, most serious first (NA if none)
#'     \item `details` (character): Short notes behind the conditions,
#'       separated by `"; "`. A failed or unfinished run's state comes first
#'       (`"submit failed"`, `"no results"`, `"completion not recorded"` or
#'       `"completion crashed"`), then the files, named relative to the folder
#'       that holds the store where possible, e.g.
#'       `"data/pk.csv modified; run1/summary.txt missing"`; `"recorded late"`
#'       for `"clean_late"`; for an untracked path that only older runs
#'       recorded, `"recorded only by an older run (<run_id>)"` naming the
#'       most recent of them (NA if none)
#'     \item `submitted_at` (POSIXct): When the latest run was submitted
#'     \item `completed_at` (POSIXct): When the latest run completed (NA if
#'       not completed)
#'   }
#'
#' @export
run_status <- function(path = ".", store = default_store()) {
  assertthat::assert_that(
    is.character(path),
    msg = "`path` must be a character vector"
  )
  data <- store_read(store)
  runs <- data$runs
  runs$project_file <- path_key(runs$project_path)
  latest <- latest_by(runs, runs$project_file, runs$submitted_at)

  files <- data$files[data$files$run_id %in% latest$run_id, , drop = FALSE]
  files$key <- path_key(files$path)
  files$project_file <- latest$project_file[match(files$run_id, latest$run_id)]
  known <- data$files
  known$key <- path_key(known$path)
  known$submitted_at <- runs$submitted_at[match(known$run_id, runs$run_id)]

  targets <- lapply(status_path_key(path), function(p) {
    status_targets(p, files, known)
  })
  targets <- do.call(rbind, c(
    list(data.frame(
      path = character(),
      project_file = character(),
      note = character()
    )),
    targets
  ))
  targets <- targets[!duplicated(targets[c("path", "project_file")]), , drop = FALSE]

  idx <- match(targets$project_file, latest$project_file, incomparables = NA)
  root <- path_key(fs::path_abs(store_root_dir(store)))
  sha_cache <- new.env(parent = emptyenv())
  judged <- lapply(seq_len(nrow(targets)), function(i) {
    if (is.na(targets$project_file[[i]])) {
      note <- targets$note[[i]]
      return(list(conditions = "untracked", details = note[!is.na(note)]))
    }
    if (is.na(idx[[i]])) {
      return(list(conditions = "never_run", details = character()))
    }
    run <- latest[idx[[i]], , drop = FALSE]
    judge_run(run, files[files$run_id == run$run_id, , drop = FALSE], root, sha_cache)
  })
  conditions <- lapply(judged, `[[`, "conditions")
  details <- vapply(judged, function(j) {
    if (length(j$details) == 0L) NA_character_ else paste(j$details, collapse = "; ")
  }, character(1))

  data.frame(
    path = targets$path,
    project_file = targets$project_file,
    run_id = latest$run_id[idx],
    condition = vapply(conditions, `[[`, character(1), 1L),
    other_conditions = vapply(conditions, function(x) {
      if (length(x) < 2L) NA_character_ else paste(x[-1], collapse = ", ")
    }, character(1)),
    details = details,
    submitted_at = latest$submitted_at[idx],
    completed_at = latest$completed_at[idx],
    stringsAsFactors = FALSE,
    row.names = NULL
  )
}

# path_key() of a user-given path: absolute against the working directory,
# with `.` and `..` removed even when the file does not exist.
status_path_key <- function(path) {
  path_key(as.character(fs::path_norm(fs::path_abs(path))))
}

# Rows (path, project_file, note) one path covers. `files` holds the file
# records of each model's latest run with `key` and `project_file`; `known`
# holds every file record in the store with `key` and its run's
# `submitted_at`. An untracked path that older runs recorded gets a note
# naming the most recent of them.
status_targets <- function(p, files, known) {
  ext <- c("mlxtran", "smlx", "pkx")
  rows <- function(project_file, note = NA_character_) {
    data.frame(
      path = rep(p, length(project_file)),
      project_file = project_file,
      note = rep(note, length(project_file)),
      stringsAsFactors = FALSE
    )
  }
  if (dir.exists(p)) {
    return(rows(find_project_files(p, ext)))
  }
  if (!file.exists(p) && !p %in% known$key) {
    stop("Path not found: ", p, call. = FALSE)
  }
  if (grepl(paste0("\\.", ext, "$", collapse = "|"), p)) {
    return(rows(p))
  }
  hits <- files$io_type %in% c("input", "output") & files$key == p
  models <- sort(unique(files$project_file[hits]))
  if (length(models) == 0L) {
    older <- known[known$key == p, , drop = FALSE]
    note <- if (nrow(older) > 0L) {
      newest <- older$run_id[order(older$submitted_at, decreasing = TRUE)][[1]]
      paste0("recorded only by an older run (", newest, ")")
    } else {
      NA_character_
    }
    return(rows(NA_character_, note))
  }
  rows(models)
}

# Conditions (most serious first) and detail notes for one latest run, from
# its file records compared with disk.
judge_run <- function(run, files, root, sha_cache) {
  inputs <- files[files$io_type %in% c("project", "input") & !files$builtin, , drop = FALSE]
  outputs <- files[files$io_type == "output", , drop = FALSE]
  input_change <- disk_change(inputs$key, inputs$sha256, sha_cache)
  output_change <- disk_change(outputs$key, outputs$sha256, sha_cache)
  flagged <- is.na(output_change) &
    is_late_flagged(outputs$key, rep(run$late_flags, nrow(outputs)))

  state <- run$state
  conditions <- c(
    failed = state %in% c("submit_failed", "no_results"),
    unfinished = state %in% c("unrecorded", "completion_crashed"),
    inputs_changed = any(!is.na(input_change)),
    output_edited = any(!is.na(output_change)),
    output_suspect = any(flagged)
  )
  conditions <- names(conditions)[conditions]
  state_note <- c(
    submit_failed = "submit failed",
    no_results = "no results",
    unrecorded = "completion not recorded",
    completion_crashed = "completion crashed"
  )[state]
  details <- c(
    unname(state_note[!is.na(state_note)]),
    paste(rel_to(inputs$key, root), input_change)[!is.na(input_change)],
    paste(rel_to(outputs$key, root), output_change)[!is.na(output_change)],
    paste(rel_to(outputs$key, root), "flagged late")[flagged]
  )
  if (length(conditions) == 0L && state == "recorded_late") {
    conditions <- "clean_late"
    details <- "recorded late"
  }
  if (length(conditions) == 0L) {
    conditions <- "clean"
  }
  list(conditions = conditions, details = details)
}

# "missing", "modified" or NA (unchanged) for each recorded file. Content is
# compared only when both hashes are known. Each path is hashed at most once
# per `sha_cache`.
disk_change <- function(keys, recorded_sha256, sha_cache) {
  current <- vapply(keys, function(k) {
    if (is.null(sha_cache[[k]])) {
      sha_cache[[k]] <- calculate_sha256(k)
    }
    sha_cache[[k]]
  }, character(1), USE.NAMES = FALSE)
  dplyr::case_when(
    is.na(current) ~ "missing",
    !is.na(recorded_sha256) & current != recorded_sha256 ~ "modified",
    TRUE ~ NA_character_
  )
}

# Paths relative to `root` where they are inside it, otherwise unchanged.
rel_to <- function(paths, root) {
  inside <- fs::path_has_parent(paths, root)
  paths[inside] <- as.character(fs::path_rel(paths[inside], root))
  paths
}
