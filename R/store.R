#' Get the run store path
#'
#' Returns the run store directory from the `mlxtrx.store` option. The value is
#' the store directory itself, conventionally the `.mlxtrx` folder inside the
#' folder that holds the Monolix projects. A relative value resolves against
#' the working directory at call time. The directory is created when the first
#' run is recorded, not when it is read.
#'
#' Set the option once per session with
#' `options(mlxtrx.store = "/path/to/monolix/.mlxtrx")`. Put that line in the
#' project's `.Rprofile` to set it for every session.
#'
#' @return Character scalar. Absolute path to the run store.
#'
#' @examples
#' \dontrun{
#' options(mlxtrx.store = "/path/to/monolix/.mlxtrx")
#' default_store()
#' }
#'
#' @export
default_store <- function() {
  store <- getOption("mlxtrx.store")
  if (is.null(store)) {
    stop(
      "No run store set. Pass `store = ` or set the option, e.g.\n",
      "  options(mlxtrx.store = \"/path/to/monolix/.mlxtrx\")\n",
      "Put that line in the project's .Rprofile to set it for every session.",
      call. = FALSE
    )
  }
  if (!is.character(store) || length(store) != 1L || is.na(store) ||
    !nzchar(store)) {
    stop(
      "Option mlxtrx.store must be a single path, e.g. ",
      "\"/path/to/monolix/.mlxtrx\".",
      call. = FALSE
    )
  }
  as.character(fs::path_abs(fs::path_expand(store)))
}

# The folder that holds the store; stored paths are relative to it.
store_root_dir <- function(store) {
  dirname(store)
}

# Run ID: UTC timestamp plus 4 hex chars derived by hashing, so the session's
# random number stream is not consumed.
new_run_id <- function(time = Sys.time(), attempt = 0L) {
  stamp <- format(time, "%Y%m%dT%H%M%SZ", tz = "UTC")
  suffix <- substr(
    digest::digest(
      paste(
        format(time, "%OS6"),
        Sys.getpid(),
        Sys.info()[["nodename"]],
        attempt
      ),
      algo = "sha256",
      serialize = FALSE
    ),
    1,
    4
  )
  paste0(stamp, "-", suffix)
}

# Creates the run folder. dir.create() either succeeds or fails, so a colliding
# ID is retried with a new suffix.
store_create_run <- function(store, time = Sys.time()) {
  runs_dir <- file.path(store, "runs")
  dir.create(runs_dir, recursive = TRUE, showWarnings = FALSE)
  for (attempt in 0:9) {
    run_id <- new_run_id(time, attempt)
    run_dir <- file.path(runs_dir, run_id)
    if (dir.create(run_dir, showWarnings = FALSE)) {
      return(list(run_id = run_id, run_dir = run_dir))
    }
  }
  stop("Could not create a unique run folder in ", runs_dir, call. = FALSE)
}

# Writes JSON atomically: a temporary file in the same folder, then a rename.
# Each folder has a single writer, so the target is checked, not locked.
store_write_json <- function(x, path) {
  if (file.exists(path)) {
    stop("File already exists: ", path, call. = FALSE)
  }
  tmp <- paste0(
    path,
    ".tmp-",
    substr(digest::digest(runif_token(), algo = "sha256", serialize = FALSE), 1, 8)
  )
  on.exit(unlink(tmp), add = TRUE)
  json <- jsonlite::toJSON(
    x,
    auto_unbox = TRUE,
    null = "null",
    na = "null",
    digits = NA,
    pretty = TRUE
  )
  writeLines(json, tmp, useBytes = TRUE)
  if (file.exists(path)) {
    stop("File already exists: ", path, call. = FALSE)
  }
  if (!file.rename(tmp, path)) {
    stop("Could not move ", tmp, " to ", path, call. = FALSE)
  }
  invisible(path)
}

# Token for temporary file names, independent of the session's random stream.
runif_token <- function() {
  paste(
    format(Sys.time(), "%OS6"),
    Sys.getpid(),
    proc.time()[["elapsed"]],
    tempfile()
  )
}

is_builtin_path <- function(path) {
  grepl("^lib:.*\\.txt$", path) && !file.exists(path)
}

# Path as stored in JSON: relative to `root` with `/` separators when inside
# it, absolute otherwise; library models are kept verbatim.
path_to_record <- function(path, root) {
  if (is_builtin_path(path)) {
    return(list(path = path, absolute = FALSE, builtin = TRUE))
  }
  p <- fs::path_norm(fs::path_abs(path))
  r <- fs::path_norm(fs::path_abs(root))
  if (fs::path_has_parent(p, r)) {
    list(path = as.character(fs::path_rel(p, r)), absolute = FALSE)
  } else {
    list(path = as.character(p), absolute = TRUE)
  }
}

path_from_record <- function(rec, root) {
  if (isTRUE(rec$builtin)) {
    return(rec$path)
  }
  if (isTRUE(rec$absolute)) {
    return(as.character(fs::path_norm(rec$path)))
  }
  as.character(fs::path_norm(fs::path_abs(rec$path, start = root)))
}

format_utc <- function(time) {
  format(time, "%Y-%m-%dT%H:%M:%OS3Z", tz = "UTC")
}

# File entry for run.json / outputs.json.
file_record <- function(path, root) {
  rec <- path_to_record(path, root)
  if (isTRUE(rec$builtin)) {
    return(rec)
  }
  info <- file.info(path)
  c(
    rec,
    list(
      sha256 = calculate_sha256(path),
      size = as.numeric(info$size),
      mtime = format_utc(info$mtime)
    )
  )
}

# Claims the right to record completion by creating the folder. Returns the
# folder path, or NULL when another process already holds the claim. With
# `retry = TRUE` the next free `completion-<n>` (n >= 2) is claimed instead.
# A failed dir.create() counts as "taken" only when the folder exists;
# any other failure is an error.
claim_completion <- function(run_dir, retry = FALSE) {
  try_claim <- function(path) {
    if (dir.create(path, showWarnings = FALSE)) {
      return(TRUE)
    }
    if (!dir.exists(path)) {
      stop("Could not create completion folder: ", path, call. = FALSE)
    }
    FALSE
  }
  if (!retry) {
    path <- file.path(run_dir, "completion")
    return(if (try_claim(path)) path else NULL)
  }
  n <- 2L
  repeat {
    path <- file.path(run_dir, paste0("completion-", n))
    if (try_claim(path)) {
      return(path)
    }
    n <- n + 1L
  }
}

# Software version such as "2024R1" for a command, or NA when not recognised.
cmd_software_version <- function(cmd) {
  stringr::str_extract(cmd_version_dir(cmd), "20\\d\\dR\\d")
}
