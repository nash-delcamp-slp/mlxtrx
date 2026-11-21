# A theoph root with one clean model and a second, never-run model.
local_two_model_store <- function(.env = parent.frame()) {
  s <- local_theoph_store(list(list(id = "r1")), .env = .env)
  other <- file.path(dirname(s$project), "other.mlxtran")
  writeLines(
    sub("exportpath = 'theoph'", "exportpath = 'other'", readLines(s$project), fixed = TRUE),
    other
  )
  s
}

report_lines <- function(...) {
  capture.output(print_report_summary(...))
}

test_that("the counts block lists each condition with padded names", {
  s <- local_two_model_store()
  out <- report_lines(s$root, store = s$store)
  expect_equal(out[[1]], "=== RUN STATUS REPORT ===")
  expect_match(out[[2]], paste0("^Path: ", s$root), fixed = FALSE)
  expect_match(out[[3]], "^Generated: \\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2} ")
  expect_true("Models: 2" %in% out)
  expect_true("  never_run  1" %in% out)
  expect_true("  clean      1" %in% out)
})

test_that("a not-clean model shows other conditions and details", {
  s <- local_theoph_store(list(list(id = "r1")))
  writeLines("changed", s$data)
  writeLines("changed", s$summary)
  out <- report_lines(s$root, store = s$store)
  expect_true("Needs attention:" %in% out)
  expect_true(
    "  theoph/theoph.mlxtran  inputs_changed (output_edited)" %in% out
  )
  expect_true(
    "    theoph/data/theoph.csv modified; theoph/theoph/summary.txt modified" %in%
      out
  )
})

test_that("a model without details has no details line", {
  s <- local_two_model_store()
  out <- report_lines(s$root, store = s$store)
  expect_equal(out[[length(out)]], "  theoph/other.mlxtran  never_run")
  expect_false(any(grepl("^  theoph/theoph.mlxtran", out)))
})

test_that("all clean prints one line and no attention block", {
  s <- local_theoph_store(list(list(id = "r1")))
  out <- report_lines(s$root, store = s$store)
  expect_true("All models clean." %in% out)
  expect_false(any(grepl("Needs attention", out)))
})

test_that("an empty directory reports no models", {
  root <- withr::local_tempdir()
  s <- local_store_with_runs(list(), root = root)
  out <- report_lines(root, store = s$store)
  expect_true("Models: 0" %in% out)
  expect_true("No models found." %in% out)
})

test_that("untracked paths are shown by path", {
  s <- local_theoph_store(list(list(id = "r1")))
  stray <- file.path(s$root, "notes.txt")
  writeLines("x", stray)
  out <- report_lines(stray, store = s$store)
  expect_true(any(grepl("notes.txt  untracked$", out)))
})

test_that("the invisible return equals run_status()", {
  s <- local_two_model_store()
  capture.output(r <- print_report_summary(s$root, store = s$store))
  expect_equal(r, run_status(s$root, store = s$store))
  capture.output(v <- withVisible(print_report_summary(s$root, store = s$store)))
  expect_false(v$visible)
})

test_that("untracked results are counted and listed as needing attention", {
  root <- local_theoph_root()
  s <- local_store_with_runs(list(), root = root)
  out <- report_lines(root, store = s$store)
  expect_true("  untracked_results  1" %in% out)
  expect_true("Needs attention:" %in% out)
  expect_true(any(grepl("^  theoph/theoph.mlxtran  untracked_results", out)))
})
