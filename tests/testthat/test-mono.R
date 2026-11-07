# A root with two copies of the theoph fixture, a store path, and the fake
# `mono24`/`sq` scripts on PATH driven by a fresh MLXTRX_FAKE_DIR.
local_mono_env <- function(.env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .env)
  for (sub in c("a", "b")) {
    dir.create(file.path(root, sub))
    file.copy(test_path("fixtures", "theoph"), file.path(root, sub), recursive = TRUE)
    unlink(file.path(root, sub, "theoph", "theoph"), recursive = TRUE)
  }
  fake <- withr::local_tempdir(.local_envir = .env)
  file.copy(
    test_path("fixtures", "theoph", "theoph", "summary.txt"),
    file.path(fake, "summary.txt")
  )
  # A shared grid queue is rarely empty: an unrelated job keeps `sq` output to
  # a header plus rows.
  writeLines("999999", file.path(fake, "queue"))
  withr::local_path(
    normalizePath(test_path("fixtures", "bin")),
    .local_envir = .env
  )
  withr::local_envvar(MLXTRX_FAKE_DIR = fake, .local_envir = .env)
  list(
    root = root,
    store = file.path(root, ".mlxtrx"),
    fake = fake,
    paths = file.path(root, c("a", "b"), "theoph", "theoph.mlxtran")
  )
}

read_run_json <- function(store, run_id) {
  jsonlite::read_json(file.path(store, "runs", run_id, "run.json"))
}

test_that("mono() records launch and completion of each job", {
  skip_on_os("windows")
  e <- local_mono_env()
  suppressMessages(
    ids <- mono(e$paths, store = e$store, require_charts_data = FALSE)
  )
  expect_type(ids, "character")
  expect_length(ids, 2L)
  for (i in seq_along(ids)) {
    run <- read_run_json(e$store, ids[i])
    expect_equal(run$schema, 1L)
    expect_equal(run$run_id, ids[i])
    expect_equal(run$launcher, "cli")
    expect_equal(run$cmd, "mono24")
    expect_equal(run$job_id, i)
    expect_equal(run$software_version, "2024R1")
    expect_true(run$submission$ok)
    expect_equal(unlist(run$submission$output), paste("Submitted batch job", i))
    expect_equal(run$user, Sys.info()[["user"]])
    expect_equal(run$host, Sys.info()[["nodename"]])
    expect_equal(run$mlxtrx_version, as.character(utils::packageVersion("mlxtrx")))
    expect_equal(
      run$project$path,
      file.path(c("a", "b")[i], "theoph", "theoph.mlxtran")
    )
    expect_equal(run$project$sha256, calculate_sha256(e$paths[i]))
    roles <- vapply(run$inputs, function(x) x$role, character(1))
    expect_equal(roles, c("data", "model"))
    expect_equal(run$inputs[[2]]$path, "lib:oral1_1cpt_kaVCl.txt")
    expect_true(run$inputs[[2]]$builtin)
    expect_named(
      run$settings,
      c("output_dir", "thread", "tool", "mode", "config")
    )
    expect_true(all(vapply(run$settings, is.null, logical(1))))
    expect_match(run$submitted_at, "^\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z$")
  }
  runs <- store_read(e$store)$runs
  expect_setequal(runs$run_id, ids)
  expect_equal(runs$state, c("recorded", "recorded"))
})

test_that("settings given to mono() are recorded", {
  skip_on_os("windows")
  e <- local_mono_env()
  suppressMessages(
    id <- mono(
      e$paths[1],
      thread = 4L,
      mode = "none",
      store = e$store,
      require_charts_data = FALSE
    )
  )
  run <- read_run_json(e$store, id)
  expect_equal(run$settings$thread, 4L)
  expect_equal(run$settings$mode, "none")
  expect_null(run$settings$tool)
  expect_null(run$settings$output_dir)
  expect_null(run$settings$config)
})

test_that("run.json is written before monitoring starts", {
  skip_on_os("windows")
  e <- local_mono_env()
  local_mocked_bindings(monitor_jobs = function(...) stop("boom"))
  expect_error(
    suppressMessages(mono(e$paths, store = e$store, require_charts_data = FALSE)),
    "boom"
  )
  runs <- store_read(e$store)$runs
  expect_equal(nrow(runs), 2L)
  expect_equal(runs$state, c("unrecorded", "unrecorded"))
})

test_that("a failed submission is recorded and not monitored", {
  skip_on_os("windows")
  e <- local_mono_env()
  file.create(file.path(e$fake, "garbage"))
  expect_warning(
    id <- mono(e$paths[1], store = e$store, require_charts_data = FALSE),
    "Could not extract job ID"
  )
  run <- read_run_json(e$store, id)
  expect_false(run$submission$ok)
  expect_equal(unlist(run$submission$output), "nope")
  expect_null(run$job_id)
  expect_equal(store_read(e$store)$runs$state, "submit_failed")
})

test_that("mono() writes only to the store", {
  skip_on_os("windows")
  e <- local_mono_env()
  suppressMessages(
    mono(e$paths, store = e$store, require_charts_data = FALSE)
  )
  expect_true(dir.exists(e$store))
  found <- list.files(
    file.path(e$root, c("a", "b")),
    pattern = "^\\.mlxtrx$",
    recursive = TRUE,
    all.files = TRUE,
    include.dirs = TRUE
  )
  expect_length(found, 0L)
})

test_that("a failing project stops the batch before it is submitted", {
  skip_on_os("windows")
  e <- local_mono_env()
  # A data path that is a folder cannot be hashed.
  data_b <- file.path(e$root, "b", "theoph", "data", "theoph.csv")
  unlink(data_b)
  dir.create(data_b)
  expect_error(
    suppressMessages(mono(e$paths, store = e$store, require_charts_data = FALSE)),
    "Runs already submitted: "
  )
  expect_equal(readLines(file.path(e$fake, "next_job")), "2")
  runs <- store_read(e$store)$runs
  expect_equal(nrow(runs), 1L)
  expect_equal(runs$state, "unrecorded")
  expect_length(list.files(file.path(e$store, "runs")), 1L)
})

test_that("a failed completion warns and the other runs are recorded", {
  skip_on_os("windows")
  e <- local_mono_env()
  real <- record_completion
  calls <- 0L
  local_mocked_bindings(record_completion = function(run_dir, store, ...) {
    calls <<- calls + 1L
    if (calls == 1L) stop("disk full")
    real(run_dir, store, ...)
  })
  w <- expect_warning(
    suppressMessages(
      ids <- mono(e$paths, store = e$store, require_charts_data = FALSE)
    ),
    "disk full"
  )
  expect_match(conditionMessage(w), ids[1], fixed = TRUE)
  runs <- store_read(e$store)$runs
  expect_equal(runs$state[runs$run_id == ids[1]], "unrecorded")
  expect_equal(runs$state[runs$run_id == ids[2]], "recorded")
})

# A fake local runner named monolix.sh in its own temp dir. It copies
# $MLXTRX_FAKE_DIR/summary.txt into the project's export folder, prints 30
# lines of log, and exits 0; with $MLXTRX_FAKE_DIR/fail present it
# exits 255 without writing anything.
local_fake_monolix_sh <- function(.env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = .env)
  cmd <- file.path(dir, "monolix.sh")
  writeLines(
    c(
      "#!/bin/sh",
      "fake=\"$MLXTRX_FAKE_DIR\"",
      "i=1",
      "while [ $i -le 30 ]; do echo \"log line $i\"; i=$((i + 1)); done",
      "if [ -e \"$fake/fail\" ]; then echo 'Error: invalid project'; exit 255; fi",
      "project=\"\"",
      "while [ $# -gt 0 ]; do",
      "  case \"$1\" in",
      "    -p) project=\"$2\"; shift 2 ;;",
      "    *) shift ;;",
      "  esac",
      "done",
      "dir=$(dirname \"$project\")",
      "export_path=$(sed -n \"s/^[[:space:]]*exportpath[[:space:]]*=[[:space:]]*['\\\"]\\(.*\\)['\\\"].*/\\1/p\" \"$project\")",
      "mkdir -p \"$dir/$export_path\"",
      "cp \"$fake/summary.txt\" \"$dir/$export_path/summary.txt\"",
      "exit 0"
    ),
    cmd
  )
  Sys.chmod(cmd, "755")
  cmd
}

test_that("a local runner that exits 0 is recorded without a warning", {
  skip_on_os("windows")
  e <- local_mono_env()
  cmd <- local_fake_monolix_sh()
  expect_no_warning(
    suppressMessages(
      id <- mono(e$paths[1], cmd = cmd, store = e$store, require_charts_data = FALSE)
    )
  )
  run <- read_run_json(e$store, id)
  expect_true(run$submission$ok)
  expect_null(run$job_id)
  res <- store_read(e$store)
  expect_equal(res$runs$state, "recorded")
  expect_equal(nrow(res$summary), 1L)
  expect_equal(res$summary$n_individuals, 12L)
})

test_that("a local runner that exits non-zero is a failed submission", {
  skip_on_os("windows")
  e <- local_mono_env()
  cmd <- local_fake_monolix_sh()
  file.create(file.path(e$fake, "fail"))
  w <- expect_warning(
    id <- mono(e$paths[1], cmd = cmd, store = e$store, require_charts_data = FALSE),
    "exited with status 255"
  )
  expect_match(conditionMessage(w), "invalid project")
  expect_no_match(conditionMessage(w), "log line 1\n", fixed = TRUE)
  run <- read_run_json(e$store, id)
  expect_false(run$submission$ok)
  expect_length(run$submission$output, 31L)
  expect_equal(store_read(e$store)$runs$state, "submit_failed")
})

test_that("a failing sq leaves runs unrecorded instead of finished", {
  skip_on_os("windows")
  e <- local_mono_env()
  file.create(file.path(e$fake, "hold"))
  file.create(file.path(e$fake, "sq_fail"))
  real <- monitor_jobs
  local_mocked_bindings(monitor_jobs = function(...) {
    real(..., poll_interval = 0, sq_failure_limit = 2)
  })
  warnings <- character()
  msgs <- character()
  withCallingHandlers(
    ids <- mono(e$paths, store = e$store, require_charts_data = FALSE),
    warning = function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    },
    message = function(m) {
      msgs <<- c(msgs, conditionMessage(m))
      invokeRestart("muffleMessage")
    }
  )
  expect_length(warnings, 2L)
  expect_match(warnings, "sq failed")
  expect_true(any(grepl("finalize_runs()", msgs, fixed = TRUE)))
  expect_equal(store_read(e$store)$runs$state, c("unrecorded", "unrecorded"))
})
