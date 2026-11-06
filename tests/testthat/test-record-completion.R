# A substage holding a copy of the theoph fixture and one run whose run.json
# points at it. `submitted_at` is one hour before now.
local_theoph_run <- function(.env = parent.frame()) {
  substage <- withr::local_tempdir(.local_envir = .env)
  file.copy(
    test_path("fixtures", "theoph"),
    substage,
    recursive = TRUE
  )
  project <- file.path(substage, "theoph", "theoph.mlxtran")
  out_dir <- file.path(substage, "theoph", "theoph")
  submitted <- Sys.time() - 3600
  # Fixture files predate the run; make the summary a fresh output.
  Sys.setFileTime(file.path(out_dir, "summary.txt"), Sys.time() - 1800)
  run <- fake_run_json(
    "r1",
    project = file_record(project, substage),
    submitted_at = format_utc(submitted)
  )
  s <- local_store_with_runs(
    list(list(id = "r1", run = run)),
    substage = substage,
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
