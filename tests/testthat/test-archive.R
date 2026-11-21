# A theoph store with one run "r1" whose run.json records a config.ini
# (kept outside the root) and whose run folder holds its copy.
local_archive_store <- function(.env = parent.frame()) {
  root <- local_theoph_root(.env = .env)
  ini_dir <- withr::local_tempdir(.local_envir = .env)
  ini <- file.path(ini_dir, "config.ini")
  writeLines(c("[misc]", "exportChartsData=true"), ini)
  spec <- theoph_run_spec(root, "r1")
  spec$run$config_ini <- c(file_record(ini, root), list(copy = "config.ini"))
  s <- local_store_with_runs(list(spec), root = root, .env = .env)
  file.copy(ini, file.path(s$run_dirs[["r1"]], "config.ini"))
  c(s, theoph_paths(root), list(ini = ini))
}

read_manifest <- function(s, id = "r1") {
  jsonlite::read_json(
    file.path(s$run_dirs[[id]], "archive.json"),
    simplifyVector = FALSE
  )
}

object_files <- function(archive) {
  list.files(file.path(archive, "objects"), recursive = TRUE)
}

test_that("archive: every recorded file of the latest run gets an object", {
  s <- local_archive_store()
  res <- suppressMessages(archive_run(s$project, store = s$store))

  ini_copy <- file.path(s$run_dirs[["r1"]], "config.ini")
  sources <- c(s$project, s$data, s$summary, ini_copy)
  for (f in sources) {
    obj <- archive_object_path(s$store, calculate_sha256(f))
    expect_true(file.exists(obj), label = f)
    expect_identical(
      readBin(obj, "raw", file.size(obj)),
      readBin(f, "raw", file.size(f))
    )
  }

  m <- read_manifest(s)
  expect_length(m$files, 4L)
  expect_setequal(
    vapply(m$files, `[[`, "", "io_type"),
    c("project", "input", "output", "config_ini")
  )
  roles <- vapply(m$files, function(f) f$role %||% NA_character_, "")
  expect_identical(roles[vapply(m$files, `[[`, "", "io_type") == "input"], "data")
  expect_setequal(
    vapply(m$files, `[[`, "", "sha256"),
    vapply(sources, calculate_sha256, "", USE.NAMES = FALSE)
  )
  root <- store_root_dir(s$store)
  manifest_paths <- vapply(m$files, function(f) path_from_record(f$path, root), "")
  expect_setequal(
    path_key(manifest_paths),
    path_key(c(s$project, s$data, s$summary, s$ini))
  )
  expect_false(is.null(m$archived_at))
  expect_false(is.null(m$archived_by))

  expect_s3_class(res, "data.frame")
  expect_named(res, c("project_file", "run_id", "n_files", "n_new", "bytes_new"))
  expect_identical(res$run_id, "r1")
  expect_equal(res$n_files, 4L)
  expect_equal(res$n_new, 4L)
  expect_equal(res$bytes_new, sum(file.size(sources)))
  expect_true(run_status(s$project, store = s$store)$archived)
})

test_that("archive: the live project folder is left unchanged", {
  s <- local_archive_store()
  snap <- function() {
    f <- list.files(file.path(s$root, "theoph"), recursive = TRUE, full.names = TRUE)
    stats::setNames(vapply(f, calculate_sha256, ""), f)
  }
  before <- snap()
  suppressMessages(archive_run(s$project, store = s$store))
  expect_identical(snap(), before)
})

test_that("default location: objects under the store, or the mlxtrx.archive option", {
  s <- local_archive_store()
  suppressMessages(archive_run(s$project, store = s$store))
  expect_length(object_files(s$store), 4L)
  expect_true(all(grepl("^[0-9a-f]{2}/[0-9a-f]{62}$", object_files(s$store))))
  m <- read_manifest(s)
  expect_identical(
    path_key(path_from_record(m$archive, store_root_dir(s$store))),
    path_key(s$store)
  )

  s2 <- local_archive_store()
  elsewhere <- withr::local_tempdir()
  withr::local_options(mlxtrx.archive = elsewhere)
  suppressMessages(archive_run(s2$project, store = s2$store))
  expect_length(object_files(elsewhere), 4L)
  expect_false(dir.exists(file.path(s2$store, "objects")))
  m2 <- read_manifest(s2)
  expect_identical(
    path_key(path_from_record(m2$archive, store_root_dir(s2$store))),
    path_key(elsewhere)
  )
})

test_that("dedupe: a data file shared by two models is stored once", {
  root <- local_theoph_root()
  p <- theoph_paths(root)
  project2 <- file.path(root, "theoph", "theoph2.mlxtran")
  summary2 <- file.path(root, "theoph", "theoph2", "summary.txt")
  writeLines(c(readLines(p$project), "; second model"), project2)
  dir.create(dirname(summary2))
  writeLines(c(readLines(p$summary), "second"), summary2)

  spec1 <- theoph_run_spec(root, "r1")
  spec2 <- theoph_run_spec(root, "r2")
  spec2$run$project <- file_record(project2, root)
  spec2$run$output_dir <- path_to_record(dirname(summary2), root)
  spec2$completions$completion$outputs <- list(file_record(summary2, root))
  s <- local_store_with_runs(list(spec1, spec2), root = root)

  first <- suppressMessages(archive_run(p$project, store = s$store))
  second <- suppressMessages(archive_run(project2, store = s$store))
  expect_equal(first$n_new, 3L)
  expect_equal(second$n_files, 3L)
  expect_equal(second$n_new, 2L)
  expect_equal(second$bytes_new, sum(file.size(c(project2, summary2))))
  expect_length(object_files(s$store), 5L)
  expect_true(file.exists(archive_object_path(s$store, calculate_sha256(p$data))))
})

test_that("refuses unclean: names the model and its condition, writes nothing", {
  s <- local_archive_store()
  cat("extra\n", file = s$data, append = TRUE)
  expect_error(
    archive_run(s$project, store = s$store),
    "theoph\\.mlxtran.*inputs_changed"
  )
  expect_false(dir.exists(file.path(s$store, "objects")))
  expect_false(file.exists(file.path(s$run_dirs[["r1"]], "archive.json")))
})

test_that("refuses unclean: checks every model before writing anything", {
  s <- local_archive_store()
  never <- file.path(s$root, "theoph", "never.mlxtran")
  file.copy(s$project, never)
  expect_error(
    archive_run(c(s$project, never), store = s$store),
    "never\\.mlxtran.*never_run"
  )
  expect_false(dir.exists(file.path(s$store, "objects")))
  expect_false(file.exists(file.path(s$run_dirs[["r1"]], "archive.json")))
})

test_that("idempotent: archiving again writes nothing", {
  s <- local_archive_store()
  suppressMessages(archive_run(s$project, store = s$store))
  manifest <- file.path(s$run_dirs[["r1"]], "archive.json")
  before <- readLines(manifest)
  expect_message(
    res <- archive_run(s$project, store = s$store),
    "already archived"
  )
  expect_equal(res$n_new, 0L)
  expect_equal(res$bytes_new, 0)
  expect_equal(res$n_files, 4L)
  expect_identical(readLines(manifest), before)
  expect_identical(
    list.files(s$run_dirs[["r1"]], pattern = "archive"),
    "archive.json"
  )
})

test_that("archive_put: reuses an existing object, rejects a wrong hash", {
  archive <- withr::local_tempdir()
  f <- withr::local_tempfile(lines = "content")
  sha <- calculate_sha256(f)
  expect_true(archive_put(f, sha, archive))
  expect_false(archive_put(f, sha, archive))
  expect_identical(object_files(archive), sub("^(..)", "\\1/", sha))

  wrong <- strrep("0", 64)
  expect_error(archive_put(f, wrong, archive), "hash")
  expect_false(file.exists(archive_object_path(archive, wrong)))
  expect_length(list.files(dirname(archive_object_path(archive, wrong))), 0L)
})

test_that("archived: FALSE before archiving, NA for untracked rows", {
  s <- local_archive_store()
  other <- file.path(s$root, "notes.txt")
  writeLines("x", other)
  res <- run_status(c(s$project, other), store = s$store)
  expect_identical(res$archived, c(FALSE, NA))
})
