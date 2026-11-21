# A theoph store (see local_archive_store()) whose run "r1" is archived.
local_archived_run <- function(.env = parent.frame()) {
  s <- local_archive_store(.env = .env)
  suppressMessages(archive_run(s$project, store = s$store))
  s
}

same_content <- function(a, b) {
  identical(readBin(a, "raw", file.size(a)), readBin(b, "raw", file.size(b)))
}

files_under <- function(dir) {
  list.files(dir, recursive = TRUE, all.files = TRUE)
}

status_of <- function(res, path) {
  res$status[path_key(res$path) == path_key(path)]
}

test_that("defaults: project and outputs restored, data left with its status", {
  s <- local_archived_run()
  to <- file.path(withr::local_tempdir(), "restored")
  msgs <- testthat::capture_messages(res <- restore_run("r1", to, store = s$store))
  expect_match(msgs[[1]], "^Restored project, 1 output under ")
  expect_match(
    msgs[[2]],
    "Data not restored: .*theoph\\.csv \\(current file matches the archived copy\\)"
  )
  expect_true(same_content(file.path(to, "theoph", "theoph.mlxtran"), s$project))
  expect_true(same_content(file.path(to, "theoph", "theoph", "summary.txt"), s$summary))
  expect_false(file.exists(file.path(to, "theoph", "data", "theoph.csv")))
  expect_false(file.exists(file.path(to, "config.ini")))
  expect_setequal(
    files_under(to),
    c("theoph/theoph.mlxtran", "theoph/theoph/summary.txt")
  )

  expect_s3_class(res, "data.frame")
  expect_named(res, c("group", "path", "restored_to", "status"))
  expect_identical(status_of(res, s$project), "restored")
  expect_identical(status_of(res, s$summary), "restored")
  expect_identical(status_of(res, s$data), "not_restored_current_matches")
  expect_true(is.na(res$restored_to[path_key(res$path) == path_key(s$data)]))
  expect_identical(
    path_key(res$restored_to[path_key(res$path) == path_key(s$project)]),
    path_key(file.path(to, "theoph", "theoph.mlxtran"))
  )
  expect_identical(res$group[path_key(res$path) == path_key(s$data)], "data")

  cat("extra\n", file = s$data, append = TRUE)
  to2 <- withr::local_tempdir()
  msgs <- testthat::capture_messages(res2 <- restore_run("r1", to2, store = s$store))
  expect_identical(status_of(res2, s$data), "not_restored_current_differs")
  expect_true(any(grepl(
    "Data not restored: .*theoph\\.csv \\(current file differs from the archived copy\\)",
    msgs
  )))
  expect_true(any(grepl("Use data = TRUE / model = TRUE to restore them.", msgs, fixed = TRUE)))

  unlink(s$data)
  res3 <- suppressMessages(restore_run("r1", withr::local_tempdir(), store = s$store))
  expect_identical(status_of(res3, s$data), "not_restored_current_missing")
})

test_that("groups: data, outputs and config select what is restored", {
  s <- local_archived_run()
  to <- withr::local_tempdir()
  res <- suppressMessages(
    restore_run("r1", to, data = TRUE, outputs = FALSE, config = TRUE, store = s$store)
  )
  project <- file.path(to, "theoph", "theoph.mlxtran")
  expect_true(same_content(file.path(dirname(project), "data", "theoph.csv"), s$data))
  expect_false(file.exists(file.path(to, "theoph", "theoph", "summary.txt")))
  expect_true(same_content(file.path(to, "config.ini"), s$ini))
  expect_setequal(
    files_under(to),
    c("theoph/theoph.mlxtran", "theoph/data/theoph.csv", "config.ini")
  )
  expect_identical(status_of(res, s$data), "restored")
  expect_identical(status_of(res, s$ini), "restored")
  expect_false(path_key(s$summary) %in% path_key(res$path))
  expect_identical(res$group[path_key(res$path) == path_key(s$ini)], "config")
})

test_that("no overwrite: an existing target file is named and nothing is written", {
  s <- local_archived_run()
  to <- withr::local_tempdir()
  dir.create(file.path(to, "theoph", "theoph"), recursive = TRUE)
  writeLines("mine", file.path(to, "theoph", "theoph", "summary.txt"))
  expect_error(
    restore_run("r1", to, store = s$store),
    "summary\\.txt"
  )
  expect_identical(files_under(to), "theoph/theoph/summary.txt")
  expect_identical(readLines(file.path(to, "theoph", "theoph", "summary.txt")), "mine")
})

test_that("absolute paths: files outside the store folder go under _absolute", {
  root <- local_theoph_root()
  p <- theoph_paths(root)
  outside_dir <- withr::local_tempdir()
  outside <- file.path(outside_dir, "theoph.csv")
  file.copy(p$data, outside)
  spec <- theoph_run_spec(root, "r1")
  spec$run$inputs[[1]] <- c(list(role = "data"), file_record(outside, root))
  s <- local_store_with_runs(list(spec), root = root)
  suppressMessages(archive_run(p$project, store = s$store))

  parent <- withr::local_tempdir()
  to <- file.path(parent, "restored")
  before_outside <- files_under(outside_dir)
  res <- suppressMessages(restore_run("r1", to, data = TRUE, store = s$store))

  abs_part <- sub("^([A-Za-z]):", "\\1", path_key(outside))
  abs_part <- sub("^/+", "", abs_part)
  target <- file.path(to, "_absolute", abs_part)
  expect_true(same_content(target, outside))
  expect_identical(path_key(res$restored_to[res$group == "data"]), path_key(target))
  expect_identical(files_under(parent), file.path("restored", files_under(to)))
  expect_identical(files_under(outside_dir), before_outside)
})

test_that("corrupt object: restore names the file and writes nothing", {
  s <- local_archived_run()
  writeLines("tampered", archive_object_path(s$store, calculate_sha256(s$summary)))
  to <- file.path(withr::local_tempdir(), "restored")
  expect_error(restore_run("r1", to, store = s$store), "summary\\.txt")
  expect_length(files_under(to), 0L)

  unlink(archive_object_path(s$store, calculate_sha256(s$project)))
  expect_error(restore_run("r1", to, store = s$store), "theoph\\.mlxtran")
  expect_length(files_under(to), 0L)
})

test_that("not archived: error says to run archive_run() first", {
  s <- local_archive_store()
  to <- file.path(withr::local_tempdir(), "restored")
  expect_error(restore_run("r1", to, store = s$store), "archive_run\\(\\)")
  expect_error(retrieve_file("r1", s$data, to, store = s$store), "archive_run\\(\\)")
  expect_false(dir.exists(to))
})

test_that("menu: decides the groups only when interactive and no group is given", {
  s <- local_archived_run()
  seen <- NULL
  local_mocked_bindings(
    is_interactive = function() TRUE,
    restore_menu = function(groups, ...) {
      seen <<- groups
      c(project = FALSE, model = FALSE, data = TRUE, outputs = FALSE, config = FALSE)
    }
  )
  to <- withr::local_tempdir()
  res <- suppressMessages(restore_run("r1", to, store = s$store))
  expect_identical(files_under(to), "theoph/data/theoph.csv")
  expect_identical(status_of(res, s$data), "restored")
  expect_setequal(seen$group, c("project", "model", "data", "outputs", "config"))
  expect_equal(seen$n[seen$group == "data"], 1L)
  expect_equal(seen$n[seen$group == "model"], 0L)
  expect_equal(seen$bytes[seen$group == "data"], file.size(s$data))

  seen <- NULL
  to2 <- withr::local_tempdir()
  suppressMessages(restore_run("r1", to2, outputs = TRUE, store = s$store))
  expect_null(seen)
  expect_setequal(
    files_under(to2),
    c("theoph/theoph.mlxtran", "theoph/theoph/summary.txt")
  )
})

test_that("menu: a cancelled menu restores nothing", {
  s <- local_archived_run()
  local_mocked_bindings(
    is_interactive = function() TRUE,
    restore_menu = function(groups, ...) {
      c(project = FALSE, model = FALSE, data = FALSE, outputs = FALSE, config = FALSE)
    }
  )
  to <- file.path(withr::local_tempdir(), "restored")
  expect_message(res <- restore_run("r1", to, store = s$store), "Nothing restored")
  expect_equal(nrow(res), 0L)
  expect_false(dir.exists(to))
})

test_that("restore_menu: labels with count, size and the current-file note", {
  groups <- data.frame(
    group = c("project", "model", "data", "outputs", "config"),
    n = c(1L, 0L, 1L, 92L, 1L),
    bytes = c(500, 0, 2048, 3 * 1024^2, 40),
    default = c(TRUE, FALSE, FALSE, TRUE, FALSE),
    note = c("", "", " (current theoph.csv matches)", "", ""),
    stringsAsFactors = FALSE
  )
  got <- NULL
  local_mocked_bindings(select_list = function(choices, preselect, multiple, title) {
    got <<- list(choices = choices, preselect = preselect, multiple = multiple, title = title)
    choices[c(1, 2)]
  })
  res <- restore_menu(groups, "r1")
  expect_identical(got$choices, c(
    "project  1 file  500 bytes",
    "data  1 file  2 Kb (current theoph.csv matches)",
    "outputs  92 files  3 Mb",
    "config  1 file  40 bytes"
  ))
  expect_identical(got$preselect, got$choices[c(1, 3)])
  expect_true(got$multiple)
  expect_identical(got$title, "Restore which parts of run r1?")
  expect_identical(
    res,
    c(project = TRUE, model = FALSE, data = TRUE, outputs = FALSE, config = FALSE)
  )
})

test_that("retrieve_file: writes one archived file and returns its path", {
  s <- local_archived_run()
  to <- file.path(withr::local_tempdir(), "out")
  out <- retrieve_file("r1", s$data, to, store = s$store)
  expect_identical(path_key(out), path_key(file.path(to, "theoph.csv")))
  expect_true(same_content(out, archive_object_path(s$store, calculate_sha256(s$data))))

  rel <- "theoph/theoph/summary.txt"
  out2 <- retrieve_file("r1", rel, to, store = s$store)
  expect_true(same_content(out2, s$summary))

  expect_error(retrieve_file("r1", s$data, to, store = s$store), "theoph\\.csv")

  expect_error(retrieve_file("r1", "nope.txt", to, store = s$store), "nope\\.txt")
  expect_setequal(files_under(to), c("theoph.csv", "summary.txt"))
})

test_that("to: a file in place of the folder is refused before writing", {
  s <- local_archived_run()
  to <- withr::local_tempfile(lines = "x")
  expect_error(restore_run("r1", to, store = s$store), "not a folder")
  expect_error(retrieve_file("r1", s$data, to, store = s$store), "not a folder")
})
