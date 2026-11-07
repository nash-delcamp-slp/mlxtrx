test_that("check_output_files: unchanged, then modified after an edit", {
  s <- local_theoph_store(list(list(id = "r1")))
  res <- check_output_files(s$summary, store = s$store)
  expect_equal(res$status, "unchanged")
  expect_identical(res$run_id, "r1")
  expect_false(res$recorded_late)
  expect_equal(res$recorded_sha256, calculate_sha256(s$summary))
  cat("x", file = s$summary, append = TRUE)
  res <- check_output_files(s$summary, store = s$store)
  expect_equal(res$status, "modified")
})

test_that("check_output_files: late runs are never plain unchanged", {
  s <- local_theoph_store(list(list(id = "r1", recorded_late = TRUE)))
  res <- check_output_files(s$summary, store = s$store)
  expect_equal(res$status, "unchanged_late")
  expect_true(res$recorded_late)

  s <- local_theoph_store(list(
    list(id = "r1", recorded_late = TRUE, flag_summary = TRUE)
  ))
  expect_equal(check_output_files(s$summary, store = s$store)$status, "suspect")
})

test_that("check_output_files compares against the newest run that recorded the file", {
  s <- local_theoph_store(list(
    list(id = "r1", submitted_at = Sys.time() - 7200, output_sha256 = "0ld"),
    list(id = "r2", submitted_at = Sys.time() - 3600)
  ))
  res <- check_output_files(s$summary, store = s$store)
  expect_identical(res$run_id, "r2")
  expect_equal(res$status, "unchanged")
})

test_that("check_output_files reports files no run recorded", {
  s <- local_theoph_store(list(list(id = "r1")))
  res <- check_output_files(s$project, store = s$store)
  expect_equal(res$status, "no_run_record")
  expect_true(is.na(res$run_id))
})

test_that("check_output_file_status marks late and flagged outputs", {
  root <- local_theoph_root()
  s <- local_store_with_runs(
    list(
      theoph_run_spec(root, "r1"),
      theoph_run_spec(root, "r2", recorded_late = TRUE),
      theoph_run_spec(root, "r3", recorded_late = TRUE, flag_summary = TRUE)
    ),
    root = root
  )
  res <- check_output_file_status(store = s$store)
  expect_equal(res$run_id, c("r1", "r2", "r3"))
  expect_equal(res$status, c("unchanged", "unchanged_late", "suspect"))
  expect_equal(res$recorded_late, c(FALSE, TRUE, TRUE))
  expect_equal(check_output_file_status("r2", store = s$store)$run_id, "r2")
  expect_equal(nrow(check_output_file_status(character(), store = s$store)), 0L)
})

test_that("check_input_file_status: data unchanged, library model unchanged, then modified", {
  s <- local_theoph_store(list(list(id = "r1")))
  res <- check_input_file_status(store = s$store)
  expect_equal(nrow(res), 2L)
  expect_equal(res$status, c("unchanged", "unchanged"))
  expect_true("recorded_late" %in% names(res))
  data_row <- res[res$file_name == "theoph.csv", ]
  expect_equal(data_row$recorded_sha256, calculate_sha256(s$data))
  cat("x", file = s$data, append = TRUE)
  res <- check_input_file_status("r1", store = s$store)
  expect_equal(res$status[res$file_name == "theoph.csv"], "modified")
})

test_that("check_project_file_status: unchanged, then modified after an edit", {
  s <- local_theoph_store(list(list(id = "r1")))
  res <- check_project_file_status(s$root, store = s$store)
  expect_equal(res$status, "unchanged")
  expect_identical(res$run_id, "r1")
  cat("\n", file = s$project, append = TRUE)
  res <- check_project_file_status(s$root, store = s$store)
  expect_equal(res$status, "modified")
})

test_that("check_completion_status: no_runs for a never-run project, without writing", {
  root <- local_theoph_root()
  store <- file.path(root, ".mlxtrx")
  res <- check_completion_status(root, store = store)
  expect_equal(res$status, "no_runs")
  expect_false(res$has_run)
  expect_false(dir.exists(store))
})

test_that("check_completion_status: completed, completed_late and incomplete", {
  s <- local_theoph_store(list(list(id = "r1")))
  res <- check_completion_status(s$root, store = s$store)
  expect_equal(res$status, "completed")
  expect_identical(res$latest_run_id, "r1")
  expect_false(res$recorded_late)

  s <- local_theoph_store(list(
    list(id = "r1", submitted_at = Sys.time() - 7200),
    list(id = "r2", submitted_at = Sys.time() - 3600, recorded_late = TRUE)
  ))
  res <- check_completion_status(s$root, store = s$store)
  expect_equal(res$status, "completed_late")
  expect_identical(res$latest_run_id, "r2")
  expect_true(res$recorded_late)

  root <- local_theoph_root()
  spec <- theoph_run_spec(root, "r1")
  spec$completions <- NULL
  s <- local_store_with_runs(list(spec), root = root)
  res <- check_completion_status(root, store = s$store)
  expect_equal(res$status, "incomplete")
  expect_false(res$has_successful_run)
})

test_that("check_project_file_status compares against the latest submitted completed run", {
  root <- local_theoph_root()
  now <- Sys.time()
  a <- theoph_run_spec(root, "a", submitted_at = now - 5000)
  a$run$project$sha256 <- "v1"
  a$completions$completion$done$completed_at <- format_utc(now - 1000)
  b <- theoph_run_spec(root, "b", submitted_at = now - 3000)
  b$completions$completion$done$completed_at <- format_utc(now - 2000)
  s <- local_store_with_runs(list(a, b), root = root)
  res <- check_project_file_status(root, store = s$store)
  expect_identical(res$run_id, "b")
  expect_equal(res$status, "unchanged")
})

test_that("check_completion_status: a latest run without results is no_results", {
  root <- local_theoph_root()
  spec <- theoph_run_spec(root, "r1")
  spec$completions$completion$summary <- NULL
  s <- local_store_with_runs(list(spec), root = root)
  res <- check_completion_status(root, store = s$store)
  expect_equal(res$status, "no_results")
  expect_false(res$has_successful_run)
  expect_true(res$has_run)
})
