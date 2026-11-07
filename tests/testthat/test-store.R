test_that("new_run_id has the expected format and varies with attempt", {
  t <- as.POSIXct("2026-11-06 14:23:01", tz = "UTC")
  expect_match(new_run_id(t), "^\\d{8}T\\d{6}Z-[0-9a-f]{4}$")
  expect_match(new_run_id(t), "^20261106T142301Z-")
  expect_false(new_run_id(t, 0L) == new_run_id(t, 1L))
})

test_that("new_run_id leaves .Random.seed untouched", {
  set.seed(1)
  before <- .Random.seed
  new_run_id()
  expect_identical(.Random.seed, before)
})

test_that("store_create_run gives distinct directories for the same time", {
  tmp <- withr::local_tempdir()
  store <- file.path(tmp, ".mlxtrx")
  t <- Sys.time()
  a <- store_create_run(store, t)
  b <- store_create_run(store, t)
  expect_true(dir.exists(a$run_dir))
  expect_true(dir.exists(b$run_dir))
  expect_false(a$run_id == b$run_id)
  expect_identical(dirname(a$run_dir), file.path(store, "runs"))
})

test_that("store_root_dir is the folder holding the store", {
  expect_identical(store_root_dir("/a/b/.mlxtrx"), "/a/b")
})

test_that("store_write_json round-trips, refuses overwrite, leaves no tmp", {
  tmp <- withr::local_tempdir()
  p <- file.path(tmp, "x.json")
  store_write_json(list(a = 1, b = NULL), p)
  expect_identical(jsonlite::read_json(p), list(a = 1L, b = NULL))
  expect_error(store_write_json(list(a = 2), p), "already exists")
  expect_identical(list.files(tmp, pattern = "\\.tmp-"), character(0))
  expect_identical(jsonlite::read_json(p)$a, 1L)
})

test_that("path records round-trip", {
  tmp <- withr::local_tempdir()
  tmp <- as.character(fs::path_real(tmp))
  rel <- path_to_record(file.path(tmp, "sub/../sub/x.txt"), tmp)
  expect_identical(rel, list(path = "sub/x.txt", absolute = FALSE))
  expect_identical(
    path_from_record(rel, tmp),
    as.character(fs::path_norm(file.path(tmp, "sub/x.txt")))
  )

  out <- withr::local_tempdir()
  out <- as.character(fs::path_real(out))
  abs_rec <- path_to_record(file.path(out, "y.txt"), tmp)
  expect_true(abs_rec$absolute)
  expect_identical(path_from_record(abs_rec, tmp), file.path(out, "y.txt"))

  b <- path_to_record("lib:oral1_1cpt_kaVCl.txt", tmp)
  expect_identical(
    b,
    list(path = "lib:oral1_1cpt_kaVCl.txt", absolute = FALSE, builtin = TRUE)
  )
  expect_identical(path_from_record(b, tmp), "lib:oral1_1cpt_kaVCl.txt")
})

test_that("file_record hashes files and skips built-ins", {
  tmp <- withr::local_tempdir()
  tmp <- as.character(fs::path_real(tmp))
  f <- file.path(tmp, "a.txt")
  writeLines("hello", f)
  rec <- file_record(f, tmp)
  expect_identical(rec$path, "a.txt")
  expect_identical(rec$sha256, calculate_sha256(f))
  expect_identical(rec$size, as.numeric(file.size(f)))
  expect_match(rec$mtime, "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$")
  b <- file_record("lib:foo.txt", tmp)
  expect_true(b$builtin)
  expect_null(b$sha256)
})

test_that("claim_completion lets exactly one claimant win", {
  d <- withr::local_tempdir()
  expect_identical(claim_completion(d), file.path(d, "completion"))
  expect_null(claim_completion(d))
  expect_identical(
    claim_completion(d, retry = TRUE),
    file.path(d, "completion-2")
  )
  expect_identical(
    claim_completion(d, retry = TRUE),
    file.path(d, "completion-3")
  )
})

test_that("cmd_software_version maps commands", {
  expect_identical(cmd_software_version("mono24"), "2024R1")
  expect_identical(
    cmd_software_version("/opt/monolix/MonolixSuite2024R1/bin/monolix.sh"),
    "2024R1"
  )
  expect_true(is.na(cmd_software_version("foo")))
})

test_that("default_store errors with setup help when the option is unset", {
  withr::local_options(mlxtrx.store = NULL)
  err <- expect_error(default_store(), "No run store set")
  expect_match(conditionMessage(err), "options(mlxtrx.store = ", fixed = TRUE)
  expect_match(conditionMessage(err), ".Rprofile", fixed = TRUE)
})

test_that("default_store returns the option as an absolute path", {
  tmp <- withr::local_tempdir()
  withr::local_options(mlxtrx.store = file.path(tmp, ".mlxtrx"))
  expect_identical(default_store(), file.path(tmp, ".mlxtrx"))
})

test_that("default_store resolves a relative option against the working directory", {
  tmp <- withr::local_tempdir()
  withr::local_dir(tmp)
  withr::local_options(mlxtrx.store = ".mlxtrx")
  expect_identical(
    default_store(),
    file.path(as.character(fs::path_real(tmp)), ".mlxtrx")
  )
})

test_that("default_store rejects an option that is not a single path", {
  withr::local_options(mlxtrx.store = c("a", "b"))
  expect_error(default_store(), "must be a single path")
  withr::local_options(mlxtrx.store = "")
  expect_error(default_store(), "must be a single path")
  withr::local_options(mlxtrx.store = NA_character_)
  expect_error(default_store(), "must be a single path")
  withr::local_options(mlxtrx.store = 1)
  expect_error(default_store(), "must be a single path")
})

test_that("reads without a store or option error and create nothing", {
  tmp <- withr::local_tempdir()
  withr::local_dir(tmp)
  withr::local_options(mlxtrx.store = NULL)
  expect_error(runs_data(), "No run store set")
  expect_length(list.files(tmp, all.files = TRUE, no.. = TRUE), 0)
})

test_that("claim_completion errors on I/O failure instead of reporting a lost claim", {
  missing <- file.path(withr::local_tempdir(), "no-such-run")
  expect_error(claim_completion(missing), "Could not create completion folder")
  expect_error(
    claim_completion(missing, retry = TRUE),
    "Could not create completion folder"
  )
})
