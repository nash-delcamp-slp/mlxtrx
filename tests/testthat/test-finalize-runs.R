# One unrecorded run on a copy of the theoph fixture, with the fake `sq` on
# PATH. The queue holds the given job IDs. The run belongs to `user`.
local_finalize_env <- function(
  queue = character(),
  job_id = 7L,
  user = Sys.info()[["user"]],
  .env = parent.frame()
) {
  root <- withr::local_tempdir(.local_envir = .env)
  file.copy(test_path("fixtures", "theoph"), root, recursive = TRUE)
  project <- file.path(root, "theoph", "theoph.mlxtran")
  out_dir <- file.path(root, "theoph", "theoph")
  Sys.setFileTime(file.path(out_dir, "summary.txt"), Sys.time() - 1800)
  run <- fake_run_json(
    "r1",
    project = file_record(project, root),
    submitted_at = format_utc(Sys.time() - 3600),
    job_id = job_id,
    user = user
  )
  s <- local_store_with_runs(
    list(list(id = "r1", run = run)),
    root = root,
    .env = .env
  )
  fake <- withr::local_tempdir(.local_envir = .env)
  writeLines(as.character(queue), file.path(fake, "queue"))
  withr::local_path(
    normalizePath(test_path("fixtures", "bin")),
    .local_envir = .env
  )
  withr::local_envvar(MLXTRX_FAKE_DIR = fake, .local_envir = .env)
  c(s, list(fake = fake, out_dir = out_dir, run_dir = s$run_dirs[["r1"]]))
}

test_that("sq_job_ids() lists the queued job IDs", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = c("11", "22"))
  expect_equal(sq_job_ids(), c("11", "22"))
})

test_that("sq_job_ids() treats a header-only listing as an empty queue", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = character())
  expect_equal(sq_job_ids(), character())
  expect_no_warning(result <- job_in_sq(7))
  expect_false(result)
})

test_that("sq_job_ids() errors when sq fails", {
  skip_on_os("windows")
  e <- local_finalize_env()
  file.create(file.path(e$fake, "sq_fail"))
  expect_error(sq_job_ids(), "sq failed")
})

test_that("sq_job_ids() errors when sq is missing", {
  withr::local_path(character(), action = "replace")
  expect_error(sq_job_ids(), "sq failed")
})

test_that("job_in_sq() keeps warning and returning FALSE when sq fails", {
  skip_on_os("windows")
  e <- local_finalize_env()
  file.create(file.path(e$fake, "sq_fail"))
  expect_warning(result <- job_in_sq(7), "sq failed")
  expect_false(result)
})

test_that("an unrecorded run whose job left the queue is recorded late", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = "999999")
  expect_message(res <- finalize_runs(e$store), "recorded_late")
  expect_equal(res, data.frame(run_id = "r1", action = "recorded_late"))
  runs <- store_read(e$store)$runs
  expect_equal(runs$state, "recorded_late")
  expect_true(runs$recorded_late)
})

test_that("a run whose job is still queued is left alone", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = c("7", "999999"))
  suppressMessages(res <- finalize_runs(e$store))
  expect_equal(res$action, "still_queued")
  expect_false(dir.exists(file.path(e$run_dir, "completion")))
  expect_equal(store_read(e$store)$runs$state, "unrecorded")
})

test_that("finalize_runs() stops and writes nothing when sq fails", {
  skip_on_os("windows")
  e <- local_finalize_env()
  file.create(file.path(e$fake, "sq_fail"))
  expect_error(finalize_runs(e$store), "sq failed")
  expect_false(dir.exists(file.path(e$run_dir, "completion")))
})

test_that("a run without a job ID needs summary.txt in its output dir", {
  skip_on_os("windows")
  e <- local_finalize_env(job_id = NULL)
  file.create(file.path(e$fake, "sq_fail"))
  summary <- file.path(e$out_dir, "summary.txt")
  file.rename(summary, paste0(summary, ".hidden"))
  suppressMessages(res <- finalize_runs(e$store))
  expect_equal(res$action, "no_summary_yet")
  expect_false(dir.exists(file.path(e$run_dir, "completion")))
  file.rename(paste0(summary, ".hidden"), summary)
  suppressMessages(res <- finalize_runs(e$store))
  expect_equal(res$action, "recorded_late")
})

test_that("a crashed completion is reported and retried on request", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = "999999")
  dir.create(file.path(e$run_dir, "completion"))
  suppressMessages(res <- finalize_runs(e$store))
  expect_equal(res$action, "completion_crashed")
  suppressMessages(res <- finalize_runs(e$store, retry_crashed = TRUE))
  expect_equal(res$action, "recorded_late")
  expect_true(file.exists(
    file.path(e$run_dir, "completion-2", "done.json")
  ))
  expect_equal(store_read(e$store)$runs$state, "recorded_late")
})

test_that("recorded runs are not in the result", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = "999999")
  record_completion(e$run_dir, e$store)
  expect_message(res <- finalize_runs(e$store), "No runs to finalize")
  expect_equal(nrow(res), 0L)
  expect_named(res, c("run_id", "action"))
  expect_equal(store_read(e$store)$runs$state, "recorded")
})

test_that("run_id limits which runs are finalized", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = "999999")
  suppressMessages(res <- finalize_runs(e$store, run_id = "other"))
  expect_equal(nrow(res), 0L)
  expect_equal(store_read(e$store)$runs$state, "unrecorded")
})

test_that("a failed submission is skipped when named", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = "999999")
  failed <- fake_run_json(
    "bad",
    job_id = NULL,
    submission = list(ok = FALSE, output = list("nope"))
  )
  dir.create(file.path(e$store, "runs", "bad"))
  store_write_json(failed, file.path(e$store, "runs", "bad", "run.json"))
  suppressMessages(res <- finalize_runs(e$store))
  expect_equal(res$run_id, "r1")
  suppressMessages(res <- finalize_runs(e$store, run_id = "bad"))
  expect_equal(res$action, "skipped_submit_failed")
})

test_that("a late recording does not take a later run's outputs", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = "999999")
  # Run B of the same project was submitted after A and is recorded; its
  # summary.txt (30 min ago) overwrote A's outputs.
  later <- fake_run_json(
    "r2",
    project = file_record(file.path(e$root, "theoph", "theoph.mlxtran"), e$root),
    submitted_at = format_utc(Sys.time() - 2400),
    job_id = 8L
  )
  dir.create(file.path(e$store, "runs", "r2", "completion"), recursive = TRUE)
  store_write_json(later, file.path(e$store, "runs", "r2", "run.json"))
  store_write_json(
    fake_done_json(),
    file.path(e$store, "runs", "r2", "completion", "done.json")
  )
  suppressMessages(res <- finalize_runs(e$store))
  expect_equal(res$action, "recorded_late")
  expect_false(file.exists(file.path(e$run_dir, "completion", "summary.json")))
  done <- jsonlite::read_json(file.path(e$run_dir, "completion", "done.json"))
  flagged <- vapply(done$late_flags, function(x) x$path, character(1))
  expect_equal(flagged, "theoph/theoph/summary.txt")
  res <- store_read(e$store)
  expect_false("r1" %in% res$summary$run_id)
})

test_that("another user's runs are left for their owner to finalize", {
  skip_on_os("windows")
  e <- local_finalize_env(queue = "999999", user = "someone_else")
  msgs <- character()
  withCallingHandlers(
    res <- finalize_runs(e$store),
    message = function(m) {
      msgs <<- c(msgs, conditionMessage(m))
      invokeRestart("muffleMessage")
    }
  )
  expect_equal(res, data.frame(run_id = "r1", action = "other_user"))
  expect_true(any(grepl("other_user = 1", msgs, fixed = TRUE)))
  expect_true(any(grepl("finalized by their owner: r1", msgs, fixed = TRUE)))
  expect_false(dir.exists(file.path(e$run_dir, "completion")))
  expect_false("all_users" %in% names(formals(finalize_runs)))
})
