local_report_store <- function(.env = parent.frame()) {
  local_theoph_store(
    list(
      list(id = "r1", submitted_at = Sys.time() - 7200),
      list(
        id = "r2",
        submitted_at = Sys.time() - 3600,
        recorded_late = TRUE,
        flag_summary = TRUE
      )
    ),
    .env = .env
  )
}

test_that("generate_project_report counts late completions and outputs", {
  s <- local_report_store()
  report <- generate_project_report(
    s$root,
    output_files = s$summary,
    store = s$store
  )
  expect_equal(report$summary$total_project_files, 1L)
  expect_equal(report$summary$project_files_completed, 1L)
  expect_equal(report$summary$project_files_completed_late, 1L)
  expect_equal(report$summary$output_files_late, 0L)
  expect_equal(report$summary$output_files_suspect, 1L)
  expect_equal(report$summary$output_files_alignment_issues, 1L)
})

test_that("generate_project_report counts outputs recorded late", {
  s <- local_theoph_store(list(list(id = "r1", recorded_late = TRUE)))
  report <- generate_project_report(s$root, store = s$store)
  expect_equal(report$summary$output_files_late, 1L)
  expect_equal(report$summary$output_files_suspect, 0L)
})

test_that("print_report_summary prints late recordings", {
  s <- local_report_store()
  expect_output(
    print_report_summary(s$root, store = s$store),
    "of which recorded late: 1",
    fixed = TRUE
  )
  expect_output(
    print_report_summary(s$root, store = s$store),
    "Outputs flagged suspect: 1",
    fixed = TRUE
  )
  s2 <- local_theoph_store(list(list(id = "r1", recorded_late = TRUE)))
  expect_output(
    print_report_summary(s2$root, store = s2$store),
    "Outputs recorded late: 1",
    fixed = TRUE
  )
})

test_that("a run without results is counted and printed apart from incomplete", {
  root <- local_theoph_root()
  spec <- theoph_run_spec(root, "r1")
  spec$completions$completion$summary <- NULL
  s <- local_store_with_runs(list(spec), root = root)
  report <- generate_project_report(root, store = s$store)
  expect_equal(report$summary$project_files_no_results, 1L)
  expect_equal(report$summary$project_files_incomplete, 0L)
  expect_equal(report$summary$project_files_completed, 0L)
  expect_output(
    print_report_summary(root, store = s$store),
    "Finished without results: 1",
    fixed = TRUE
  )
})
