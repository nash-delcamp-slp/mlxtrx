# A root holding a copy of the theoph fixture and one run whose run.json
# points at it. `submitted_at` is one hour before now.
local_theoph_run <- function(.env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .env)
  file.copy(
    test_path("fixtures", "theoph"),
    root,
    recursive = TRUE
  )
  project <- file.path(root, "theoph", "theoph.mlxtran")
  out_dir <- file.path(root, "theoph", "theoph")
  submitted <- Sys.time() - 3600
  # Fixture files predate the run; make the summary a fresh output.
  Sys.setFileTime(file.path(out_dir, "summary.txt"), Sys.time() - 1800)
  run <- fake_run_json(
    "r1",
    project = file_record(project, root),
    submitted_at = format_utc(submitted)
  )
  s <- local_store_with_runs(
    list(list(id = "r1", run = run)),
    root = root,
    .env = .env
  )
  c(s, list(out_dir = out_dir, run_dir = s$run_dirs[["r1"]]))
}

run_state <- function(s) {
  store_read(s$store)$runs
}

test_that("records outputs, summary and done", {
  s <- local_theoph_run()
  expect_equal(record_completion(s$run_dir, s$store), "recorded")
  res <- store_read(s$store)
  expect_equal(res$runs$state, "recorded")
  outs <- res$files[res$files$io_type == "output", ]
  expect_equal(nrow(outs), 1L)
  expect_equal(outs$name, "summary.txt")
  expect_equal(
    outs$sha256,
    calculate_sha256(file.path(s$out_dir, "summary.txt"))
  )
  expect_equal(res$summary$n_individuals, 12L)
  expect_equal(res$summary$n_doses, 12L)
  expect_equal(res$observations$n_observations[res$observations$obs_type == "CONC"], 132L)
  done <- jsonlite::read_json(file.path(s$run_dir, "completion", "done.json"))
  expect_false(done$recorded_late)
  expect_length(done$late_flags, 0L)
  expect_match(done$recorded_by, "@")
})

test_that("outputs older than submitted_at are not recorded", {
  s <- local_theoph_run()
  old <- file.path(s$out_dir, "old.txt")
  writeLines("x", old)
  Sys.setFileTime(old, Sys.time() - 7200)
  record_completion(s$run_dir, s$store)
  res <- store_read(s$store)
  outs <- res$files[res$files$io_type == "output", ]
  expect_equal(outs$name, "summary.txt")
})

test_that("a second call backs off and changes nothing", {
  s <- local_theoph_run()
  record_completion(s$run_dir, s$store)
  snap <- function() {
    f <- list.files(s$run_dir, recursive = TRUE, full.names = TRUE)
    file.info(f)$mtime
  }
  before <- snap()
  expect_message(
    res <- record_completion(s$run_dir, s$store),
    "already"
  )
  expect_equal(res, "claimed_elsewhere")
  expect_equal(snap(), before)
})

test_that("late recordings flag outputs written long after the summary", {
  s <- local_theoph_run()
  extra <- file.path(s$out_dir, "extra.txt")
  writeLines("x", extra)
  Sys.setFileTime(extra, Sys.time() + 1800)
  record_completion(s$run_dir, s$store, late = TRUE)
  res <- store_read(s$store)
  expect_equal(res$runs$state, "recorded_late")
  flags <- res$runs$late_flags[[1]]
  expect_equal(basename(flags), "extra.txt")
  expect_length(flags, 1L)
})

test_that("retry records into completion-2 and is always late", {
  s <- local_theoph_run()
  dir.create(file.path(s$run_dir, "completion"))
  expect_equal(record_completion(s$run_dir, s$store, retry = TRUE), "recorded")
  expect_true(file.exists(file.path(s$run_dir, "completion-2", "done.json")))
  expect_equal(run_state(s)$state, "recorded_late")
})

test_that("a summary.txt older than submitted_at is not parsed or recorded", {
  s <- local_theoph_run()
  Sys.setFileTime(file.path(s$out_dir, "summary.txt"), Sys.time() - 7200)
  expect_equal(record_completion(s$run_dir, s$store), "recorded")
  expect_false(file.exists(file.path(s$run_dir, "completion", "summary.json")))
  expect_true(file.exists(file.path(s$run_dir, "completion", "done.json")))
  res <- store_read(s$store)
  expect_equal(nrow(res$summary), 0L)
})

test_that("an edited summary.txt is flagged against the median mtime", {
  s <- local_theoph_run()
  t0 <- Sys.time() - 1800
  for (nm in c("a.txt", "b.txt", "c.txt")) {
    f <- file.path(s$out_dir, nm)
    writeLines("x", f)
    Sys.setFileTime(f, t0)
  }
  Sys.setFileTime(file.path(s$out_dir, "summary.txt"), t0 + 3600)
  record_completion(s$run_dir, s$store, late = TRUE)
  flags <- store_read(s$store)$runs$late_flags[[1]]
  expect_equal(basename(flags), "summary.txt")
})

test_that("a malformed submitted_at errors before the completion is claimed", {
  s <- local_theoph_run()
  run <- jsonlite::read_json(file.path(s$run_dir, "run.json"))
  run$submitted_at <- "yesterday"
  unlink(file.path(s$run_dir, "run.json"))
  store_write_json(run, file.path(s$run_dir, "run.json"))
  expect_error(record_completion(s$run_dir, s$store), "submitted_at")
  expect_length(list.files(s$run_dir, pattern = "^completion"), 0L)
})
