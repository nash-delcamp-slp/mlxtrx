# A temporary HOME holding ~/lixoft/<version_dir>/config/config.ini with the
# given [misc] lines. `~` expands from HOME on Linux and macOS only.
local_config_ini <- function(version_dir, misc, .env = parent.frame()) {
  home <- withr::local_tempdir(.local_envir = .env)
  dir <- file.path(home, "lixoft", version_dir, "config")
  dir.create(dir, recursive = TRUE)
  writeLines(c("[misc]", misc), file.path(dir, "config.ini"))
  withr::local_envvar(HOME = home, .local_envir = .env)
  home
}

test_that("PKanalix needs export-charts-datasets but not exportVPCSimulations", {
  skip_on_os("windows")
  local_config_ini("pkanalix/pkanalix2024R1", "exportChartsData=true")
  err <- expect_error(
    suppressMessages(check_config_ini("pk24", offer_fix = FALSE)),
    "export-charts-datasets=true"
  )
  expect_no_match(conditionMessage(err), "exportVPCSimulations")
  expect_no_match(conditionMessage(err), "exportChartsData")
})

test_that("PKanalix config with both chart settings passes", {
  skip_on_os("windows")
  local_config_ini(
    "pkanalix/pkanalix2024R1",
    c("exportChartsData=true", "export-charts-datasets=true")
  )
  expect_true(suppressMessages(check_config_ini("pk24", offer_fix = FALSE)))
})

test_that("Monolix still needs exportVPCSimulations", {
  skip_on_os("windows")
  local_config_ini(
    "monolix/monolix2024R1",
    c("exportChartsData=true", "export-charts-datasets=true")
  )
  expect_error(
    suppressMessages(check_config_ini("mono24", offer_fix = FALSE)),
    "exportVPCSimulations=true"
  )
})

test_that("Simulx still needs all three chart settings", {
  skip_on_os("windows")
  local_config_ini("simulx/simulx2024R1", "exportChartsData=true")
  err <- expect_error(
    suppressMessages(check_config_ini("sim24", offer_fix = FALSE)),
    "export-charts-datasets=true"
  )
  expect_match(conditionMessage(err), "exportVPCSimulations=true")
})
