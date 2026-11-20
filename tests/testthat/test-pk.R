# A root with a copy of the synthetic PKanalix fixture without its results, a
# store path, and the fake `pk24`/`sq` scripts on PATH driven by a fresh
# MLXTRX_FAKE_DIR whose pkx_results folder holds the fixture results.
local_pk_env <- function(.env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .env)
  file.copy(test_path("fixtures", "pkx"), root, recursive = TRUE)
  unlink(file.path(root, "pkx", "pk"), recursive = TRUE)
  fake <- withr::local_tempdir(.local_envir = .env)
  dir.create(file.path(fake, "pkx_results"))
  file.copy(
    test_path("fixtures", "pkx", "pk", "PKanalix"),
    file.path(fake, "pkx_results"),
    recursive = TRUE
  )
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
    path = file.path(root, "pkx", "project.pkx")
  )
}

# A fake local runner named pkanalix.sh in its own temp dir. It writes its
# arguments to $MLXTRX_FAKE_DIR/args, copies $MLXTRX_FAKE_DIR/pkx_results
# into the project's export folder and exits 0; with $MLXTRX_FAKE_DIR/fail
# present it exits 255 without writing results.
local_fake_pkanalix_sh <- function(.env = parent.frame()) {
  dir <- withr::local_tempdir(.local_envir = .env)
  cmd <- file.path(dir, "pkanalix.sh")
  writeLines(
    c(
      "#!/bin/sh",
      "fake=\"$MLXTRX_FAKE_DIR\"",
      "echo \"$@\" > \"$fake/args\"",
      "if [ -e \"$fake/fail\" ]; then echo 'Error: project not found'; exit 255; fi",
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
      "cp -R \"$fake/pkx_results/.\" \"$dir/$export_path/\"",
      "exit 0"
    ),
    cmd
  )
  Sys.chmod(cmd, "755")
  cmd
}

test_that("pk() submits with pk24 and records the run and its results", {
  skip_on_os("windows")
  e <- local_pk_env()
  suppressMessages(
    id <- pk(e$path, store = e$store, require_charts_data = FALSE)
  )
  run <- jsonlite::read_json(file.path(e$store, "runs", id, "run.json"))
  expect_equal(run$cmd, "pk24")
  expect_equal(run$job_id, 1L)
  expect_equal(run$software_version, "2024R1")
  expect_equal(run$output_dir, list(path = "pkx/pk", absolute = FALSE))
  roles <- vapply(run$inputs, function(x) x$role, character(1))
  expect_equal(roles, c("data", "model"))
  res <- store_read(e$store)
  expect_equal(res$runs$state, "recorded")
  expect_equal(res$summary$tasks, "nca,ca,be")
  expect_equal(nrow(res$bioequivalence), 2L)
})

test_that("pk() passes thread and mode to the command", {
  skip_on_os("windows")
  e <- local_pk_env()
  cmd <- local_fake_pkanalix_sh()
  suppressMessages(
    id <- pk(e$path, thread = 2L, mode = "none", cmd = cmd, store = e$store, require_charts_data = FALSE)
  )
  args <- readLines(file.path(e$fake, "args"))
  expect_match(args, "--thread 2", fixed = TRUE)
  expect_match(args, "--mode none", fixed = TRUE)
})

test_that("a local pkanalix.sh that exits 0 is recorded with --no-gui", {
  skip_on_os("windows")
  e <- local_pk_env()
  cmd <- local_fake_pkanalix_sh()
  expect_no_warning(
    suppressMessages(
      id <- pk(e$path, cmd = cmd, store = e$store, require_charts_data = FALSE)
    )
  )
  expect_match(readLines(file.path(e$fake, "args")), "^--no-gui -p ")
  run <- jsonlite::read_json(file.path(e$store, "runs", id, "run.json"))
  expect_true(run$submission$ok)
  expect_null(run$job_id)
  res <- store_read(e$store)
  expect_equal(res$runs$state, "recorded")
  expect_equal(res$summary$n_individuals, 3L)
})

test_that("a local pkanalix.sh that exits 255 is a failed submission", {
  skip_on_os("windows")
  e <- local_pk_env()
  cmd <- local_fake_pkanalix_sh()
  file.create(file.path(e$fake, "fail"))
  expect_warning(
    pk(e$path, cmd = cmd, store = e$store, require_charts_data = FALSE),
    "exited with status 255"
  )
  expect_equal(store_read(e$store)$runs$state, "submit_failed")
})

test_that("pk() errors before submitting when no store is set", {
  skip_on_os("windows")
  e <- local_pk_env()
  withr::local_options(mlxtrx.store = NULL)
  expect_error(pk(e$path, require_charts_data = FALSE), "No run store set")
  expect_false(file.exists(file.path(e$fake, "next_job")))
  expect_false(dir.exists(file.path(e$root, "pkx", "pk")))
})

test_that("PKanalix commands map to their lixoft version folders", {
  expect_equal(cmd_version_dir("pk24"), "pkanalix/pkanalix2024R1")
  expect_equal(cmd_version_dir("/usr/local/bin/pk23"), "pkanalix/pkanalix2023R1")
  expect_equal(cmd_version_dir("pk21"), "pkanalix/pkanalix2021R1")
  expect_equal(cmd_version_dir("pk20"), "pkanalix/pkanalix2020R1")
  expect_equal(
    cmd_version_dir("/srv/Lixoft/MonolixSuite2024R2/bin/pkanalix.sh"),
    "pkanalix/pkanalix2024R2"
  )
  expect_equal(cmd_software_version("pk24"), "2024R1")
  expect_equal(
    cmd_version_dir("/srv/Lixoft/MonolixSuite2024R1/bin/monolix.sh"),
    "monolix/monolix2024R1"
  )
})
