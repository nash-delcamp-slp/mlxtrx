#' Archive the latest run of a model
#'
#' Keeps an exact copy of each model's latest run so it can be checked and
#' brought back later, while the live project folder stays usable: nothing in
#' it is changed, moved or locked.
#'
#' What is archived, for the latest run of each model `path` covers (the same
#' models as [run_status()]): the project file, every recorded input (model,
#' data, other referenced files and the `config` file; Monolix library models
#' `lib:` are not files and are skipped), every recorded output, and the copy
#' of the tool's `config.ini` kept with the run.
#'
#' Where: each file is stored once under its SHA-256 as
#' `<archive>/objects/<first 2 hex>/<remaining 62 hex>`, so a file shared by
#' several runs (e.g. a data set) is stored only once. Each copy is checked
#' against the recorded hash before it is put in place, and an object that
#' already exists is reused, never rewritten. The run's folder in the store
#' gets a manifest `archive.json` listing each archived file (its path as
#' recorded, io type, input role, SHA-256 and size), when and by whom it was
#' archived, and the archive location.
#'
#' Every model must be `"clean"` or `"clean_late"` in [run_status()], so the
#' files on disk are those the run recorded; otherwise nothing is written and
#' the error lists each model with its condition. A model whose latest run is
#' already archived gets a message and nothing is written for it.
#'
#' @param path Character vector. Project files, folders, or recorded input or
#'   output files, as in [run_status()].
#' @param store Character scalar. The run store directory (see
#'   [default_store()]). Defaults to `default_store()`.
#' @param archive Character scalar. The folder that holds the `objects`
#'   folder. Defaults to the `mlxtrx.archive` option, or else the store
#'   folder itself (objects in `<store>/objects`).
#'
#' @return Invisibly, a data frame with one row per model and columns
#'   `project_file`, `run_id`, `n_files` (files in the manifest), `n_new`
#'   (objects written by this call; files already in the archive are not
#'   counted) and `bytes_new` (their total size in bytes).
#'
#' @examples
#' \dontrun{
#' archive_run("base/run3.mlxtran")
#' run_status("base")$archived
#' }
#'
#' @export
archive_run <- function(
  path,
  store = default_store(),
  archive = getOption("mlxtrx.archive", store)
) {
  assertthat::assert_that(
    is.character(path),
    msg = "`path` must be a character vector"
  )
  assertthat::assert_that(
    assertthat::is.string(archive) && !is.na(archive) && nzchar(archive),
    msg = "`archive` must be a single path"
  )
  archive <- as.character(fs::path_abs(fs::path_expand(archive)))
  root <- store_root_dir(store)

  status <- run_status(path, store)
  status$model <- ifelse(is.na(status$project_file), status$path, status$project_file)
  status <- status[!duplicated(status$model), , drop = FALSE]
  data <- store_read(store)
  run_dirs <- data$runs$run_dir[match(status$run_id, data$runs$run_id)]
  done <- !is.na(run_dirs) & file.exists(file.path(run_dirs, "archive.json"))

  bad <- !done & !status$condition %in% c("clean", "clean_late")
  if (any(bad)) {
    stop(
      "Only clean runs can be archived; nothing was archived:\n",
      paste0("  ", status$model[bad], " (", status$condition[bad], ")", collapse = "\n"),
      call. = FALSE
    )
  }

  entries <- lapply(seq_len(nrow(status)), function(i) {
    if (done[[i]]) {
      return(NULL)
    }
    run_files <- data$files[data$files$run_id == status$run_id[[i]], , drop = FALSE]
    archive_entries(run_files, run_dirs[[i]], root)
  })

  n_files <- integer(nrow(status))
  n_new <- integer(nrow(status))
  bytes_new <- numeric(nrow(status))
  for (i in seq_len(nrow(status))) {
    manifest_path <- file.path(run_dirs[[i]], "archive.json")
    if (done[[i]]) {
      message(status$model[[i]], " already archived (run ", status$run_id[[i]], ")")
      manifest <- jsonlite::read_json(manifest_path, simplifyVector = FALSE)
      n_files[[i]] <- length(manifest$files)
      next
    }
    for (e in entries[[i]]) {
      if (archive_put(e$source, e$sha256, archive)) {
        n_new[[i]] <- n_new[[i]] + 1L
        bytes_new[[i]] <- bytes_new[[i]] + e$size
      }
    }
    info <- Sys.info()
    store_write_json(
      list(
        archived_at = format_utc(Sys.time()),
        archived_by = paste0(info[["user"]], "@", info[["nodename"]]),
        archive = path_to_record(archive, root),
        files = lapply(entries[[i]], function(e) e[c("io_type", "role", "path", "sha256", "size")])
      ),
      manifest_path
    )
    n_files[[i]] <- length(entries[[i]])
  }

  invisible(data.frame(
    project_file = status$project_file,
    run_id = status$run_id,
    n_files = n_files,
    n_new = n_new,
    bytes_new = bytes_new,
    stringsAsFactors = FALSE,
    row.names = NULL
  ))
}

#' Check archived copies against their hashes
#'
#' Reads the `archive.json` manifest of every run in the store, finds each
#' listed file's object in the archive the manifest names, and hashes it
#' against the recorded SHA-256. An object shared by several manifests is
#' hashed once per call. Temporary files left in `objects` by an interrupted
#' write are not referenced by any manifest and are ignored. Nothing is
#' changed.
#'
#' @param store Character scalar. The run store directory (see
#'   [default_store()]). Defaults to `default_store()`.
#'
#' @return A data frame with one row per manifest entry and columns `run_id`
#'   (the run's folder name), `path` (the archived file's original path),
#'   `sha256` (the recorded hash) and `status`: `"ok"` (the object matches),
#'   `"missing"` (no object) or `"mismatch"` (the object's content has a
#'   different hash). A one-line summary is printed as a message.
#'
#' @examples
#' \dontrun{
#' verify_archive()
#' }
#'
#' @export
verify_archive <- function(store = default_store()) {
  root <- store_root_dir(store)
  manifests <- Sys.glob(file.path(store, "runs", "*", "archive.json"))
  manifests <- manifests[order(manifests)]

  run_id <- character()
  path <- character()
  sha256 <- character()
  object <- character()
  for (m in manifests) {
    manifest <- jsonlite::read_json(m, simplifyVector = FALSE)
    archive <- path_from_record(manifest$archive, root)
    for (f in manifest$files) {
      run_id <- c(run_id, basename(dirname(m)))
      path <- c(path, path_from_record(f$path, root))
      sha256 <- c(sha256, f$sha256)
      object <- c(object, archive_object_path(archive, f$sha256))
    }
  }

  actual <- character(length(unique(object)))
  names(actual) <- unique(object)
  for (o in names(actual)) {
    actual[[o]] <- if (file.exists(o)) calculate_sha256(o) else NA_character_
  }
  found <- unname(actual[object])
  status <- ifelse(is.na(found), "missing", ifelse(found == sha256, "ok", "mismatch"))

  message(
    length(status), " archived files: ",
    sum(status == "ok"), " ok, ",
    sum(status == "missing"), " missing, ",
    sum(status == "mismatch"), " mismatch"
  )
  data.frame(
    run_id = run_id,
    path = path,
    sha256 = sha256,
    status = status,
    stringsAsFactors = FALSE,
    row.names = NULL
  )
}

# Files to archive for one run: its project, inputs (except library models)
# and outputs from `run_files` (rows of store_read()$files), plus the
# config.ini copy recorded in run.json. Each entry is list(io_type, role,
# path = path record, sha256, size, source = file to copy). Errors when a
# file has no recorded hash or the config.ini copy is missing, before any
# object is written.
archive_entries <- function(run_files, run_dir, root) {
  run_files <- run_files[!run_files$builtin, , drop = FALSE]
  entries <- lapply(seq_len(nrow(run_files)), function(j) {
    list(
      io_type = run_files$io_type[[j]],
      role = run_files$role[[j]],
      path = path_to_record(run_files$path[[j]], root),
      sha256 = run_files$sha256[[j]],
      size = run_files$size[[j]],
      source = run_files$path[[j]]
    )
  })
  run <- jsonlite::read_json(file.path(run_dir, "run.json"), simplifyVector = FALSE)
  ini <- run$config_ini
  if (!is.null(ini)) {
    copy <- file.path(run_dir, ini$copy)
    if (!file.exists(copy)) {
      stop("config.ini copy not found: ", copy, call. = FALSE)
    }
    entries[[length(entries) + 1L]] <- list(
      io_type = "config_ini",
      role = NA_character_,
      path = path_to_record(path_from_record(ini, root), root),
      sha256 = scalar_chr(ini$sha256),
      size = scalar_dbl(ini$size),
      source = copy
    )
  }
  for (e in entries) {
    if (is.na(e$sha256)) {
      stop("No recorded SHA-256 for ", e$source, "; cannot archive it.", call. = FALSE)
    }
  }
  entries
}

# Where the object with this SHA-256 lives in `archive`.
archive_object_path <- function(archive, sha256) {
  file.path(archive, "objects", substr(sha256, 1L, 2L), substr(sha256, 3L, 64L))
}

# Copies `path` into `archive` as the object for `sha256`. The copy goes to a
# temporary file in the object's folder, its hash is checked against
# `sha256`, then it is renamed into place. Returns TRUE when a new object was
# written, FALSE when the object already existed (it is not rewritten).
archive_put <- function(path, sha256, archive) {
  target <- archive_object_path(archive, sha256)
  if (file.exists(target)) {
    return(FALSE)
  }
  dir.create(dirname(target), recursive = TRUE, showWarnings = FALSE)
  tmp <- paste0(
    target,
    ".tmp-",
    substr(digest::digest(runif_token(), algo = "sha256", serialize = FALSE), 1, 8)
  )
  on.exit(unlink(tmp), add = TRUE)
  if (!isTRUE(file.copy(path, tmp))) {
    stop("Could not copy ", path, " to ", tmp, call. = FALSE)
  }
  actual <- calculate_sha256(tmp)
  if (!identical(actual, sha256)) {
    stop(
      "Copy of ", path, " has hash ", actual, ", expected ", sha256,
      "; it was not archived.",
      call. = FALSE
    )
  }
  if (file.exists(target)) {
    return(FALSE)
  }
  if (!file.rename(tmp, target)) {
    stop("Could not move ", tmp, " to ", target, call. = FALSE)
  }
  TRUE
}
