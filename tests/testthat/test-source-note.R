# Root with two copies of the theoph project (theoph/, theoph2/), one run each.
local_two_project_store <- function(.env = parent.frame()) {
  root <- local_theoph_root(.env = .env)
  dir.create(file.path(root, "theoph2"))
  file.copy(
    list.files(file.path(root, "theoph"), full.names = TRUE),
    file.path(root, "theoph2"),
    recursive = TRUE
  )
  r1 <- theoph_run_spec(root, "r1", submitted_at = Sys.time() - 7200)
  r2 <- theoph_run_spec(root, "r2")
  r2 <- rapply(r2, function(x) sub("^theoph/", "theoph2/", x), how = "replace")
  s <- local_store_with_runs(list(r1, r2), root = root, .env = .env)
  p <- theoph_paths(root)
  p2 <- lapply(p, function(x) sub("/theoph/", "/theoph2/", x, fixed = TRUE))
  list(store = s$store, p1 = p, p2 = p2)
}

test_that("source_note names the project and run of a clean file", {
  s <- local_theoph_store(list(list(id = "r1")))
  expect_no_warning(note <- source_note(s$summary, store = s$store))
  expect_equal(note, "Source: theoph/theoph.mlxtran (run r1)")
})

test_that("source_note lists each run once, in file order", {
  s <- local_two_project_store()
  files <- c(s$p2$summary, s$p1$summary, s$p2$summary)
  expect_equal(
    source_note(files, store = s$store),
    "Source: theoph2/theoph.mlxtran (run r2); theoph/theoph.mlxtran (run r1)"
  )
})

test_that("source_note warns about modified files but still names the run", {
  s <- local_theoph_store(list(list(id = "r1")))
  cat("x", file = s$summary, append = TRUE)
  expect_warning(
    note <- source_note(s$summary, store = s$store),
    "summary.txt.*modified"
  )
  expect_equal(note, "Source: theoph/theoph.mlxtran (run r1)")
})

test_that("source_note flags files with no run record", {
  s <- local_theoph_store(list(list(id = "r1")))
  expect_warning(
    note <- source_note(c(s$project, s$summary), store = s$store),
    "no_run_record"
  )
  expect_equal(
    note,
    paste0(
      "Source: ", s$project, " (no run record); ",
      "theoph/theoph.mlxtran (run r1)"
    )
  )
})

test_that("source_note warns once listing every non-clean file", {
  s <- local_two_project_store()
  cat("x", file = s$p1$summary, append = TRUE)
  cat("x", file = s$p2$summary, append = TRUE)
  warnings <- character()
  withCallingHandlers(
    source_note(c(s$p1$summary, s$p2$summary), store = s$store),
    warning = function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    }
  )
  expect_length(warnings, 1L)
  expect_match(warnings, "theoph/theoph/summary.txt", fixed = TRUE)
  expect_match(warnings, "theoph2/theoph/summary.txt", fixed = TRUE)
})
