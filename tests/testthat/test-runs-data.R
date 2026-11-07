test_that("a missing store gives no runs and is not created", {
  store <- file.path(withr::local_tempdir(), ".mlxtrx")
  expect_message(res <- runs_data(store), "No runs found")
  expect_null(res)
  expect_false(dir.exists(store))
})

test_that("runs_data returns run columns with sha256 and late state", {
  s <- local_theoph_store(list(list(id = "r1")))
  d <- runs_data(s$store)
  expect_equal(nrow(d), 1L)
  expect_identical(d$run_id, "r1")
  expect_equal(d$state, "recorded")
  expect_false(d$recorded_late)
  expect_equal(d$project_file_sha256, calculate_sha256(s$project))
  expect_false("project_file_md5_checksum" %in% names(d))
  expect_equal(attr(d$submitted_at, "tzone"), store_tz())
})

test_that("include_summary adds summary stats and n_obs_ columns", {
  s <- local_theoph_store(list(list(id = "r1")))
  d <- runs_data(s$store, include_summary = TRUE)
  expect_equal(d$ofv, 300.5)
  expect_equal(d$n_obs_CONC, 132L)
  expect_equal(d$n_individuals, 12L)
})

test_that("latest_only keeps one row per project; ignore_failed_runs drops unfinished", {
  root <- local_theoph_root()
  s <- local_store_with_runs(
    list(
      theoph_run_spec(root, "r1", submitted_at = Sys.time() - 7200),
      theoph_run_spec(root, "r2", submitted_at = Sys.time() - 3600),
      list(id = "r3")
    ),
    root = root
  )
  all <- runs_data(s$store)
  expect_setequal(all$run_id, c("r1", "r2", "r3"))
  latest <- runs_data(s$store, latest_only = TRUE)
  expect_equal(nrow(latest), 2L)
  expect_equal(anyDuplicated(latest$project_path), 0L)
  expect_true("r2" %in% latest$run_id)
  expect_false("r1" %in% latest$run_id)
  done <- runs_data(s$store, ignore_failed_runs = TRUE)
  expect_setequal(done$run_id, c("r1", "r2"))
})

test_that("include_files gives one row per input and output file with sha256", {
  s <- local_theoph_store(list(list(id = "r1")))
  d <- runs_data(s$store, include_files = TRUE)
  expect_setequal(d$io_type, c("input", "output"))
  out <- d[d$io_type == "output", ]
  expect_equal(out$file_name, "summary.txt")
  expect_equal(out$sha256, calculate_sha256(s$summary))
  expect_false("md5_checksum" %in% names(d))
})

test_that("runs_table returns a gt table", {
  s <- local_theoph_store(list(list(id = "r1")))
  expect_s3_class(runs_table(s$store), "gt_tbl")
  expect_s3_class(runs_table(s$store, include_files = TRUE), "gt_tbl")
  expect_s3_class(runs_table(s$store, include_summary = TRUE), "gt_tbl")
})

test_that("get_run_files returns the files of the given runs", {
  root <- local_theoph_root()
  s <- local_store_with_runs(
    list(theoph_run_spec(root, "r1"), theoph_run_spec(root, "r2")),
    root = root
  )
  f <- get_run_files("r1", s$store)
  expect_true(all(f$run_id == "r1"))
  expect_setequal(f$io_type, c("project", "input", "output"))
  expect_true("sha256" %in% names(f))
  expect_equal(nrow(get_run_files(character(), s$store)), 0L)
})

test_that("ignore_failed_runs drops runs that finished without results", {
  root <- local_theoph_root()
  bare <- theoph_run_spec(root, "r2", submitted_at = Sys.time() - 3600)
  bare$completions$completion$summary <- NULL
  s <- local_store_with_runs(
    list(theoph_run_spec(root, "r1", submitted_at = Sys.time() - 7200), bare),
    root = root
  )
  all <- runs_data(s$store)
  expect_setequal(all$run_id, c("r1", "r2"))
  expect_equal(all$state[all$run_id == "r2"], "no_results")
  expect_identical(runs_data(s$store, ignore_failed_runs = TRUE)$run_id, "r1")
})

test_that("observation types become syntactic n_obs_ column names", {
  s <- local_store_with_runs(list(list(
    id = "r1",
    completions = list(completion = list(
      summary = list(observations = list("obsid 1" = 10L, "obsid 2" = 20L)),
      done = fake_done_json()
    ))
  )))
  d <- runs_data(s$store, include_summary = TRUE)
  expect_identical(d$n_obs_obsid_1, 10L)
  expect_identical(d$n_obs_obsid_2, 20L)
  expect_false(any(grepl(" ", names(d))))
})

test_that("observation types that collapse to one name stay distinct columns", {
  s <- local_store_with_runs(list(list(
    id = "r1",
    completions = list(completion = list(
      summary = list(observations = list("a b" = 1L, "a-b" = 2L)),
      done = fake_done_json()
    ))
  )))
  d <- runs_data(s$store, include_summary = TRUE)
  obs <- grep("^n_obs_", names(d), value = TRUE)
  expect_length(obs, 2L)
  expect_equal(anyDuplicated(obs), 0L)
})
