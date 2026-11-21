test_that("never run: an unrun project file in a directory", {
  root <- local_theoph_root()
  unlink(dirname(theoph_paths(root)$summary), recursive = TRUE)
  s <- local_store_with_runs(list(), root = root)
  res <- run_status(root, store = s$store)
  expect_equal(nrow(res), 1L)
  expect_equal(res$project_file, path_key(theoph_paths(root)$project))
  expect_equal(res$condition, "never_run")
  expect_true(is.na(res$run_id))
  expect_true(is.na(res$details))
})

test_that("untracked results: output files without a recorded run", {
  root <- local_theoph_root()
  s <- local_store_with_runs(list(), root = root)
  res <- run_status(root, store = s$store)
  expect_equal(res$condition, "untracked_results")
  expect_true(is.na(res$run_id))
  expect_match(res$details, "theoph/theoph")
  expect_match(res$details, "not produced by a recorded run")
})

test_that("never run: a copied project whose output folder holds another run's results", {
  s <- local_theoph_store(list(list(id = "r1")))
  copy <- file.path(dirname(s$project), "run2.mlxtran")
  file.copy(s$project, copy)
  res <- run_status(copy, store = s$store)
  expect_equal(res$condition, "never_run")
  expect_true(is.na(res$run_id))
  expect_equal(
    res$details,
    "output folder theoph/theoph holds results of theoph/theoph.mlxtran (run r1)"
  )
})

test_that("never run: an unparseable project file is not an error", {
  root <- withr::local_tempdir()
  writeLines("garbage {{ not a project", file.path(root, "bad.mlxtran"))
  s <- local_store_with_runs(list(), root = root)
  expect_no_error(res <- run_status(root, store = s$store))
  expect_no_warning(run_status(root, store = s$store))
  expect_equal(res$condition, "never_run")
})

test_that("clean: a recorded run with nothing changed", {
  s <- local_theoph_store(list(list(id = "r1")))
  res <- run_status(s$project, store = s$store)
  expect_equal(res$condition, "clean")
  expect_identical(res$run_id, "r1")
  expect_true(is.na(res$other_conditions))
  expect_true(is.na(res$details))
  expect_s3_class(res$submitted_at, "POSIXct")
  expect_false(is.na(res$completed_at))
})

test_that("failed: latest run submit_failed or no_results", {
  root <- local_theoph_root()
  spec <- theoph_run_spec(root, "r1")
  spec$run$submission <- list(ok = FALSE, output = list("error"))
  spec$completions <- NULL
  s <- local_store_with_runs(list(spec), root = root)
  res <- run_status(s$root, store = s$store)
  expect_equal(res$condition, "failed")
  expect_equal(res$details, "submit failed")

  root <- local_theoph_root()
  spec <- theoph_run_spec(root, "r1")
  spec$completions$completion$summary <- NULL
  s <- local_store_with_runs(list(spec), root = root)
  res <- run_status(s$root, store = s$store)
  expect_equal(res$condition, "failed")
  expect_equal(res$details, "no results")
})

test_that("unfinished: the latest run decides, even over an older clean run", {
  root <- local_theoph_root()
  spec <- theoph_run_spec(root, "r1")
  spec$completions <- NULL
  s <- local_store_with_runs(list(spec), root = root)
  res <- run_status(s$root, store = s$store)
  expect_equal(res$condition, "unfinished")
  expect_equal(res$details, "completion not recorded")

  root <- local_theoph_root()
  spec <- theoph_run_spec(root, "r1")
  spec$completions$completion$done <- NULL
  s <- local_store_with_runs(list(spec), root = root)
  res <- run_status(s$root, store = s$store)
  expect_equal(res$condition, "unfinished")
  expect_equal(res$details, "completion crashed")

  root <- local_theoph_root()
  older <- theoph_run_spec(root, "r1", submitted_at = Sys.time() - 7200)
  latest <- theoph_run_spec(root, "r2", submitted_at = Sys.time() - 3600)
  latest$completions <- NULL
  s <- local_store_with_runs(list(older, latest), root = root)
  res <- run_status(s$root, store = s$store)
  expect_equal(res$condition, "unfinished")
  expect_identical(res$run_id, "r2")
  expect_true(is.na(res$completed_at))

  cat("x", file = theoph_paths(root)$data, append = TRUE)
  res <- run_status(s$root, store = s$store)
  expect_equal(res$condition, "unfinished")
  expect_equal(res$other_conditions, "inputs_changed")
  expect_equal(
    res$details,
    "completion not recorded; theoph/data/theoph.csv modified"
  )
})

test_that("inputs changed: data file modified or missing", {
  s <- local_theoph_store(list(list(id = "r1")))
  cat("x", file = s$data, append = TRUE)
  res <- run_status(s$project, store = s$store)
  expect_equal(res$condition, "inputs_changed")
  expect_true(is.na(res$other_conditions))
  expect_match(res$details, "theoph/data/theoph.csv modified", fixed = TRUE)

  unlink(s$data)
  res <- run_status(s$project, store = s$store)
  expect_equal(res$condition, "inputs_changed")
  expect_match(res$details, "theoph/data/theoph.csv missing", fixed = TRUE)
})

test_that("output edited, alone and together with changed inputs", {
  s <- local_theoph_store(list(list(id = "r1")))
  cat("x", file = s$summary, append = TRUE)
  res <- run_status(s$project, store = s$store)
  expect_equal(res$condition, "output_edited")
  expect_match(res$details, "theoph/theoph/summary.txt modified", fixed = TRUE)

  cat("x", file = s$data, append = TRUE)
  res <- run_status(s$project, store = s$store)
  expect_equal(res$condition, "inputs_changed")
  expect_equal(res$other_conditions, "output_edited")
  expect_match(res$details, "theoph/data/theoph.csv modified", fixed = TRUE)
  expect_match(res$details, "theoph/theoph/summary.txt modified", fixed = TRUE)
})

test_that("late: clean_late when unflagged, output_suspect when flagged", {
  s <- local_theoph_store(list(list(id = "r1", recorded_late = TRUE)))
  res <- run_status(s$project, store = s$store)
  expect_equal(res$condition, "clean_late")
  expect_equal(res$details, "recorded late")

  s <- local_theoph_store(list(
    list(id = "r1", recorded_late = TRUE, flag_summary = TRUE)
  ))
  res <- run_status(s$project, store = s$store)
  expect_equal(res$condition, "output_suspect")
  expect_match(res$details, "theoph/theoph/summary.txt flagged late", fixed = TRUE)
})

test_that("timestamp only: a touched file with the same content is clean", {
  s <- local_theoph_store(list(list(id = "r1")))
  Sys.setFileTime(s$data, Sys.time() + 120)
  expect_equal(run_status(s$project, store = s$store)$condition, "clean")
})

test_that("builtin model: a lib: model input never counts as changed", {
  s <- local_theoph_store(list(list(id = "r1")))
  files <- store_read(s$store)$files
  expect_true(any(files$builtin & grepl("^lib:", files$path)))
  res <- run_status(s$project, store = s$store)
  expect_false(res$condition == "inputs_changed")
  expect_true(is.na(res$details))
})

test_that("shared data file: each model is judged against its own latest run", {
  root <- local_theoph_root()
  p <- theoph_paths(root)
  project_b <- file.path(root, "theoph", "theoph_b.mlxtran")
  file.copy(p$project, project_b)
  spec_a <- theoph_run_spec(root, "a")
  cat("x", file = p$data, append = TRUE)
  spec_b <- theoph_run_spec(root, "b")
  spec_b$run$project <- file_record(project_b, root)
  s <- local_store_with_runs(list(spec_a, spec_b), root = root)

  res <- run_status(p$data, store = s$store)
  expect_equal(nrow(res), 2L)
  expect_equal(
    res$condition[res$project_file == path_key(p$project)],
    "inputs_changed"
  )
  expect_equal(res$condition[res$project_file == path_key(project_b)], "clean")
})

test_that("output file path: one row for the model that produced it", {
  s <- local_theoph_store(list(list(id = "r1")))
  res <- run_status(s$summary, store = s$store)
  expect_equal(nrow(res), 1L)
  expect_equal(res$project_file, path_key(s$project))
  expect_equal(res$path, path_key(s$summary))
  expect_equal(res$condition, "clean")
})

test_that("untracked: an existing file the store never recorded", {
  s <- local_theoph_store(list(list(id = "r1")))
  other <- file.path(s$root, "theoph", "notes.txt")
  writeLines("notes", other)
  res <- run_status(other, store = s$store)
  expect_equal(nrow(res), 1L)
  expect_true(is.na(res$project_file))
  expect_true(is.na(res$run_id))
  expect_equal(res$condition, "untracked")
})

test_that("relative path: same rows as the absolute path", {
  s <- local_theoph_store(list(list(id = "r1")))
  expected <- run_status(s$data, store = s$store)
  withr::local_dir(file.path(s$root, "theoph"))
  expect_equal(run_status("data/theoph.csv", store = s$store), expected)
  withr::local_dir("data")
  expect_equal(run_status("../data/theoph.csv", store = s$store), expected)
})

test_that("missing path: error naming it", {
  s <- local_theoph_store(list(list(id = "r1")))
  missing <- file.path(s$root, "nope.csv")
  expect_error(run_status(missing, store = s$store), "nope.csv", fixed = TRUE)
})

test_that("read-only: no files or folders are created", {
  s <- local_theoph_store(list(list(id = "r1")))
  snap <- function() list.files(s$root, recursive = TRUE, all.files = TRUE, include.dirs = TRUE)
  before <- snap()
  run_status(s$root, store = s$store)
  run_status(c(s$data, s$summary), store = s$store)
  expect_identical(snap(), before)
})

test_that("columns: names in order, also for zero rows", {
  cols <- c(
    "path", "project_file", "run_id", "condition", "other_conditions",
    "details", "archived", "submitted_at", "completed_at"
  )
  s <- local_theoph_store(list(list(id = "r1")))
  expect_named(run_status(s$root, store = s$store), cols)

  empty <- withr::local_tempdir()
  res <- run_status(empty, store = s$store)
  expect_equal(nrow(res), 0L)
  expect_named(res, cols)
  expect_type(res$condition, "character")
  expect_s3_class(res$submitted_at, "POSIXct")
})

test_that("deleted input given as path: inputs_changed, noted missing", {
  s <- local_theoph_store(list(list(id = "r1")))
  unlink(s$data)
  res <- run_status(s$data, store = s$store)
  expect_equal(nrow(res), 1L)
  expect_equal(res$condition, "inputs_changed")
  expect_match(res$details, "theoph/data/theoph.csv missing", fixed = TRUE)
})

test_that("untracked path recorded only by older runs names the newest of them", {
  root <- local_theoph_root()
  p <- theoph_paths(root)
  old_data <- file.path(root, "theoph", "data", "old.csv")
  file.copy(p$data, old_data)
  older <- theoph_run_spec(root, "r1", submitted_at = Sys.time() - 10800)
  older$run$inputs[[1]] <- c(list(role = "data"), file_record(old_data, root))
  old2 <- theoph_run_spec(root, "r2", submitted_at = Sys.time() - 7200)
  old2$run$inputs[[1]] <- c(list(role = "data"), file_record(old_data, root))
  latest <- theoph_run_spec(root, "r3", submitted_at = Sys.time() - 3600)
  s <- local_store_with_runs(list(older, old2, latest), root = root)

  res <- run_status(old_data, store = s$store)
  expect_equal(nrow(res), 1L)
  expect_equal(res$condition, "untracked")
  expect_true(is.na(res$project_file))
  expect_equal(res$details, "recorded only by an older run (r2)")

  unlink(old_data)
  res <- run_status(old_data, store = s$store)
  expect_equal(res$condition, "untracked")
  expect_equal(res$details, "recorded only by an older run (r2)")
})

test_that("project files are found whatever the case of their extension", {
  root <- local_theoph_root()
  s <- local_store_with_runs(list(), root = root)
  upper <- file.path(root, "theoph", "OTHER.MLXTRAN")
  file.copy(file.path(root, "theoph", "theoph.mlxtran"), upper)
  status <- run_status(file.path(root, "theoph"), store = s$store)
  expect_true(path_key(upper) %in% status$project_file)
  one <- run_status(upper, store = s$store)
  expect_equal(one$project_file, path_key(upper))
  expect_false(identical(one$condition, "untracked"))
})
