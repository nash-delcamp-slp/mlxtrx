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

test_that("config_ini_path() locates the tool's config.ini or gives NA", {
  skip_on_os("windows")
  home <- local_config_ini("monolix/monolix2024R1", "exportChartsData=true")
  expect_equal(
    config_ini_path("mono24"),
    normalizePath(
      file.path(home, "lixoft/monolix/monolix2024R1/config/config.ini")
    )
  )
  expect_identical(config_ini_path("nonexistent-cmd"), NA_character_)
})
