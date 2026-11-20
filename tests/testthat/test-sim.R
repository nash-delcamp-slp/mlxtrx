# A root holding a copy of the synthetic Simulx fixture, a store path, and a
# fake local runner named simulx.sh that records its arguments in `args` next
# to it and writes Simulation/simulatedData.txt into the export folder.
local_sim_env <- function(.env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .env)
  file.copy(test_path("fixtures", "smlx"), root, recursive = TRUE)
  bin <- withr::local_tempdir(.local_envir = .env)
  cmd <- file.path(bin, "simulx.sh")
  writeLines(c(
    "#!/bin/sh",
    sprintf("echo \"$@\" > '%s'", file.path(bin, "args")),
    # File timestamps come from a coarse clock; a short wait keeps the output
    # from being dated before the run's submission time.
    "sleep 1",
    sprintf("mkdir -p '%s'", file.path(root, "smlx", "sim", "Simulation")),
    sprintf("echo x > '%s'", file.path(root, "smlx", "sim", "Simulation", "simulatedData.txt")),
    "exit 0"
  ), cmd)
  Sys.chmod(cmd, "755")
  list(
    root = root,
    store = file.path(root, ".mlxtrx"),
    cmd = cmd,
    args = file.path(bin, "args"),
    path = file.path(root, "smlx", "project.smlx")
  )
}

test_that("a local simulx.sh run gets --no-gui and records inputs and tasks", {
  skip_on_os("windows")
  e <- local_sim_env()
  suppressMessages(
    id <- sim(e$path, cmd = e$cmd, store = e$store)
  )
  expect_match(readLines(e$args), "^--no-gui -p ")
  run <- jsonlite::read_json(file.path(e$store, "runs", id, "run.json"))
  expect_equal(
    vapply(run$inputs, `[[`, "", "role"),
    c("data", "model", "other")
  )
  res <- store_read(e$store)
  expect_equal(res$runs$state, "recorded")
  expect_equal(res$summary$tasks, "simulation")
  expect_equal(run_status(e$path, store = e$store)$condition, "clean")
})
