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

#' Restore an archived run
#'
#' Writes the archived copies of a run's files under `to`, recreating the
#' run's folder layout, so the run can be inspected or rerun. The live
#' project folder is never written to.
#'
#' Files are grouped as: `project` (the project file), `model` (inputs of
#' role `"model"`; Monolix library models `lib:` are not files and are not
#' archived), `data` (every other input except the `config` file: data,
#' regressors, doses, external files), `outputs` (recorded outputs) and
#' `config` (the `config` file input and the copy of the tool's
#' `config.ini`). By default the project and outputs are restored and the
#' model and data are not, since those are often shared files maintained
#' elsewhere; for each model or data file left out, a message says whether
#' the file now at its original path matches the archived copy.
#'
#' Where: a file recorded relative to the store's folder goes to the same
#' relative path under `to`, so the restored project's references resolve.
#' A file recorded with an absolute path goes under `to/_absolute/`, a
#' Windows drive letter becoming a folder (e.g. `_absolute/C/data/pk.csv`).
#' The `config.ini` copy goes to `<to>/config.ini`.
#'
#' Before anything is written, every chosen object is checked against its
#' recorded SHA-256 and every target path must be free: `to` may exist, but
#' no file to restore may already be there. On any problem nothing is
#' written and the error names the file.
#'
#' Called interactively with none of `project`, `model`, `data`, `outputs`
#' and `config` given, a menu lists each group with its number of files and
#' size (and, for model and data, whether the current file matches), with
#' the defaults preselected.
#'
#' @param run_id Character scalar. The run to restore; it must have been
#'   archived with [archive_run()].
#' @param to Character scalar. The folder to restore into; created if
#'   needed.
#' @param project,model,data,outputs,config Logical scalars. Which groups of
#'   files to restore.
#' @param store Character scalar. The run store directory (see
#'   [default_store()]). Defaults to `default_store()`.
#'
#' @return Invisibly, a data frame with one row per restored file and per
#'   model or data file left out, with columns `group`, `path` (the file's
#'   original path), `restored_to` (`NA` when not restored) and `status`:
#'   `"restored"`, or for a model or data file left out
#'   `"not_restored_current_matches"`, `"not_restored_current_differs"` or
#'   `"not_restored_current_missing"` (no file at the original path now).
#'
#' @examples
#' \dontrun{
#' run_id <- run_status("base/run3.mlxtran")$run_id
#' restore_run(run_id, "restored/run3")
#' restore_run(run_id, "restored/run3-full", model = TRUE, data = TRUE, config = TRUE)
#' }
#'
#' @export
restore_run <- function(
  run_id,
  to,
  project = TRUE,
  model = FALSE,
  data = FALSE,
  outputs = TRUE,
  config = FALSE,
  store = default_store()
) {
  assertthat::assert_that(
    assertthat::is.string(to) && !is.na(to) && nzchar(to),
    msg = "`to` must be a single path"
  )
  choice <- c(project = project, model = model, data = data, outputs = outputs, config = config)
  assertthat::assert_that(
    is.logical(choice) && length(choice) == 5L && !anyNA(choice),
    msg = "`project`, `model`, `data`, `outputs` and `config` must each be TRUE or FALSE"
  )
  to <- restore_folder(to)
  files <- restore_manifest_files(run_id, store)
  files$restored_to <- file.path(to, files$target)
  current <- files$group %in% c("model", "data")
  files$current <- NA_character_
  add_current <- function(files, rows) {
    files$current[rows] <- vapply(which(rows), function(i) {
      restore_current_status(files$path[[i]], files$sha256[[i]])
    }, "")
    files
  }

  given <- !c(missing(project), missing(model), missing(data), missing(outputs), missing(config))
  if (is_interactive() && !any(given)) {
    files <- add_current(files, current)
    choice <- restore_menu(restore_groups(files), run_id)[names(choice)]
    choice[is.na(choice)] <- FALSE
    if (!any(choice)) {
      message("Nothing restored.")
      return(invisible(restore_result(files[0, , drop = FALSE], character())))
    }
  }

  chosen <- files[files$group %in% names(choice)[choice], , drop = FALSE]
  chosen <- chosen[!duplicated(chosen[c("restored_to", "sha256")]), , drop = FALSE]
  clash <- duplicated(chosen$restored_to)
  if (any(clash)) {
    stop(
      "Several archived files would be restored to the same path:\n",
      paste0("  ", unique(chosen$restored_to[clash]), collapse = "\n"),
      call. = FALSE
    )
  }
  for (i in seq_len(nrow(chosen))) {
    archive_check_object(chosen$object[[i]], chosen$sha256[[i]], chosen$path[[i]])
  }
  exists <- file.exists(chosen$restored_to)
  if (any(exists)) {
    stop(
      "Files already exist in `to`; nothing was restored:\n",
      paste0("  ", chosen$restored_to[exists], collapse = "\n"),
      call. = FALSE
    )
  }

  for (i in seq_len(nrow(chosen))) {
    archive_copy_object(chosen$object[[i]], chosen$restored_to[[i]])
  }

  left_out <- current & !files$group %in% names(choice)[choice]
  files <- add_current(files, left_out & is.na(files$current))
  left <- files[left_out, , drop = FALSE]
  restore_messages(chosen, left, to)
  status <- c(rep("restored", nrow(chosen)), sprintf("not_restored_current_%s", left$current))
  invisible(restore_result(rbind(chosen, left), status))
}

#' Retrieve one archived file
#'
#' Writes the archived copy of one file of a run to `to`, under the file's
#' name. The object is checked against its recorded SHA-256 first, and an
#' existing file is never overwritten.
#'
#' @param run_id Character scalar. The run; it must have been archived with
#'   [archive_run()].
#' @param path Character scalar. The file as recorded in the run: its
#'   recorded path, its path from the working directory, or its path
#'   relative to the store's folder.
#' @param to Character scalar. The folder to write into; created if needed.
#' @param store Character scalar. The run store directory (see
#'   [default_store()]). Defaults to `default_store()`.
#'
#' @return Invisibly, the path of the written file.
#'
#' @examples
#' \dontrun{
#' run_id <- run_status("base/run3.mlxtran")$run_id
#' retrieve_file(run_id, "base/data/pk.csv", "old")
#' }
#'
#' @export
retrieve_file <- function(run_id, path, to, store = default_store()) {
  assertthat::assert_that(
    assertthat::is.string(path) && !is.na(path) && nzchar(path),
    msg = "`path` must be a single path"
  )
  assertthat::assert_that(
    assertthat::is.string(to) && !is.na(to) && nzchar(to),
    msg = "`to` must be a single path"
  )
  root <- store_root_dir(store)
  to <- restore_folder(to)
  files <- restore_manifest_files(run_id, store)
  wanted <- path_key(c(path, file.path(root, path)))
  hit <- files$record_path == path | path_key(files$path) %in% wanted
  hit <- files[hit, , drop = FALSE]
  hit <- hit[!duplicated(hit[c("path", "sha256")]), , drop = FALSE]
  if (nrow(hit) == 0L) {
    stop(
      "No file ", path, " in the archive of run ", run_id,
      "; nothing was written. Archived files:\n",
      paste0("  ", files$path, collapse = "\n"),
      call. = FALSE
    )
  }
  if (nrow(hit) > 1L) {
    stop(
      path, " matches several archived files of run ", run_id,
      "; nothing was written:\n",
      paste0("  ", hit$path, " (", hit$io_type, ")", collapse = "\n"),
      call. = FALSE
    )
  }
  target <- file.path(to, basename(path))
  archive_check_object(hit$object, hit$sha256, hit$path)
  if (file.exists(target)) {
    stop(target, " already exists; nothing was written.", call. = FALSE)
  }
  archive_copy_object(hit$object, target)
  invisible(target)
}

# For mocking in tests.
is_interactive <- function() interactive()

select_list <- function(choices, preselect, multiple, title) {
  utils::select.list(choices, preselect = preselect, multiple = multiple, title = title)
}

# `to` as an absolute path; errors when it exists and is not a folder.
restore_folder <- function(to) {
  to <- as.character(fs::path_abs(fs::path_expand(to)))
  if (file.exists(to) && !dir.exists(to)) {
    stop(to, " is not a folder; nothing was written.", call. = FALSE)
  }
  to
}

# The files listed in a run's archive.json as a data frame: io_type, group,
# path (original absolute path), record_path (as recorded), target (path
# under the restore folder), sha256, size and object (its path in the
# archive). Errors when the run has no manifest.
restore_manifest_files <- function(run_id, store) {
  assertthat::assert_that(
    assertthat::is.string(run_id) && !is.na(run_id) && nzchar(run_id),
    msg = "`run_id` must be a single run ID"
  )
  root <- store_root_dir(store)
  manifest_path <- file.path(store, "runs", run_id, "archive.json")
  if (!file.exists(manifest_path)) {
    stop(
      "Run ", run_id, " is not archived; run archive_run() on its project first.",
      call. = FALSE
    )
  }
  manifest <- jsonlite::read_json(manifest_path, simplifyVector = FALSE)
  archive <- path_from_record(manifest$archive, root)
  rows <- lapply(manifest$files, function(f) {
    role <- f$role %||% NA_character_
    group <- switch(
      f$io_type,
      project = "project",
      output = "outputs",
      config_ini = "config",
      input = if (identical(role, "model")) {
        "model"
      } else if (identical(role, "config")) {
        "config"
      } else {
        "data"
      },
      stop("Unknown io_type in ", manifest_path, ": ", f$io_type, call. = FALSE)
    )
    target <- if (identical(f$io_type, "config_ini")) {
      "config.ini"
    } else {
      restore_target(f$path)
    }
    data.frame(
      io_type = f$io_type,
      group = group,
      path = path_from_record(f$path, root),
      record_path = f$path$path,
      target = target,
      sha256 = f$sha256,
      size = as.numeric(f$size),
      object = archive_object_path(archive, f$sha256),
      stringsAsFactors = FALSE
    )
  })
  if (length(rows) == 0L) {
    return(data.frame(
      io_type = character(), group = character(), path = character(),
      record_path = character(), target = character(), sha256 = character(),
      size = numeric(), object = character(), stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

# Where a file goes under the restore folder, as a relative path: a path
# recorded relative to the store's folder as it is; an absolute path under
# "_absolute", a Windows drive letter "C:" becoming the folder "C". Errors
# for a path that would leave the restore folder.
restore_target <- function(rec) {
  p <- gsub("\\\\", "/", rec$path)
  if (isTRUE(rec$absolute)) {
    p <- sub("^([A-Za-z]):", "\\1", p)
    p <- file.path("_absolute", sub("^/+", "", p))
  }
  parts <- strsplit(p, "/", fixed = TRUE)[[1]]
  if (fs::is_absolute_path(p) || ".." %in% parts) {
    stop("Archived path ", rec$path, " cannot be restored under `to`.", call. = FALSE)
  }
  p
}

# "matches", "differs" or "missing": the file now at `path` against `sha256`.
restore_current_status <- function(path, sha256) {
  if (!file.exists(path) || dir.exists(path)) {
    return("missing")
  }
  if (identical(calculate_sha256(path), sha256)) "matches" else "differs"
}

# One row per group: group, n (files), bytes, default (preselected in the
# menu) and note (for model and data, whether each current file matches).
restore_groups <- function(files) {
  groups <- c("project", "model", "data", "outputs", "config")
  note <- vapply(groups, function(g) {
    f <- files[files$group == g & !is.na(files$current), , drop = FALSE]
    if (nrow(f) == 0L) {
      return("")
    }
    paste0(" (", paste("current", basename(f$path), f$current, collapse = ", "), ")")
  }, "")
  data.frame(
    group = groups,
    n = vapply(groups, function(g) sum(files$group == g), 0L),
    bytes = vapply(groups, function(g) sum(files$size[files$group == g]), 0),
    default = c(TRUE, FALSE, FALSE, TRUE, FALSE),
    note = unname(note),
    stringsAsFactors = FALSE,
    row.names = NULL
  )
}

# Multi-select menu over the groups that have files, defaults preselected.
# Returns a logical named by every group in `groups`; a cancelled menu
# selects nothing.
restore_menu <- function(groups, run_id) {
  shown <- groups[groups$n > 0L, , drop = FALSE]
  size <- vapply(shown$bytes, function(b) {
    format(structure(b, class = "object_size"), units = "auto")
  }, "")
  labels <- paste0(
    shown$group, "  ", shown$n, ifelse(shown$n == 1L, " file", " files"), "  ",
    size, shown$note
  )
  picked <- select_list(
    labels,
    preselect = labels[shown$default],
    multiple = TRUE,
    title = paste0("Restore which parts of run ", run_id, "?")
  )
  stats::setNames(groups$group %in% shown$group[labels %in% picked], groups$group)
}

# Errors naming `path` unless `object` exists and has hash `sha256`.
archive_check_object <- function(object, sha256, path) {
  if (!file.exists(object)) {
    stop(
      "The archived copy of ", path, " is missing (", object, "); nothing was written.",
      call. = FALSE
    )
  }
  if (!identical(calculate_sha256(object), sha256)) {
    stop(
      "The archived copy of ", path, " does not match its recorded hash (", object,
      "); nothing was written.",
      call. = FALSE
    )
  }
}

archive_copy_object <- function(object, target) {
  dir.create(dirname(target), recursive = TRUE, showWarnings = FALSE)
  if (!isTRUE(file.copy(object, target, overwrite = FALSE))) {
    stop("Could not copy ", object, " to ", target, call. = FALSE)
  }
}

# One line naming the restored groups, then one line per model or data file
# left out with how the current file compares, then a hint when any were.
restore_messages <- function(chosen, left, to) {
  if (nrow(chosen) > 0L) {
    parts <- character()
    for (g in c("project", "model", "data", "outputs", "config")) {
      n <- sum(chosen$group == g)
      if (n == 0L) {
        next
      }
      parts <- c(parts, switch(
        g,
        project = if (n == 1L) "project" else paste(n, "project files"),
        model = paste(n, if (n == 1L) "model file" else "model files"),
        data = paste(n, if (n == 1L) "data file" else "data files"),
        outputs = paste(n, if (n == 1L) "output" else "outputs"),
        config = paste(n, if (n == 1L) "config file" else "config files")
      ))
    }
    message("Restored ", paste(parts, collapse = ", "), " under ", to)
  }
  why <- c(
    matches = "current file matches the archived copy",
    differs = "current file differs from the archived copy",
    missing = "no file at that path now"
  )
  for (i in seq_len(nrow(left))) {
    message(
      if (left$group[[i]] == "model") "Model" else "Data",
      " not restored: ", left$path[[i]], " (", why[[left$current[[i]]]], ")"
    )
  }
  if (nrow(left) > 0L) {
    message("Use data = TRUE / model = TRUE to restore them.")
  }
}

restore_result <- function(files, status) {
  data.frame(
    group = files$group,
    path = files$path,
    restored_to = ifelse(status == "restored", files$restored_to, NA_character_),
    status = status,
    stringsAsFactors = FALSE,
    row.names = NULL
  )
}
