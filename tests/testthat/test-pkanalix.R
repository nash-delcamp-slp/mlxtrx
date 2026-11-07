test_that("a PKanalix run records its result statistics", {
  s <- local_pkx_run()
  expect_equal(record_completion(s$run_dir, s$store), "recorded")
  res <- store_read(s$store)
  expect_equal(res$runs$state, "recorded")
  expect_equal(nrow(res$files[res$files$io_type == "output", ]), 5L)
  expect_equal(res$summary$tasks, "nca,ca,be")
  expect_equal(res$summary$n_individuals, 3L)
  expect_equal(res$summary$ofv, 120.25)
  expect_equal(res$summary$aic, 130.25)
  expect_equal(res$summary$bic, 135.75)
  expect_equal(res$summary$ca_cost, 1.5)
  expect_true(is.na(res$summary$bicc))
  expect_true(is.na(res$summary$exploratory_iterations))
  expect_equal(nrow(res$observations), 0L)
})

test_that("a PKanalix result file older than submitted_at is not parsed", {
  s <- local_pkx_run()
  Sys.setFileTime(
    file.path(s$results_dir, "ca", "cost.txt"),
    Sys.time() - 7200
  )
  record_completion(s$run_dir, s$store)
  res <- store_read(s$store)
  expect_equal(res$summary$tasks, "nca,ca,be")
  expect_true(is.na(res$summary$ofv))
  expect_true(is.na(res$summary$ca_cost))
  expect_false("cost.txt" %in% res$files$name)
})

test_that("a PKanalix run without result files is no_results", {
  s <- local_pkx_run()
  unlink(file.path(s$results_dir, c("nca", "ca", "be")), recursive = TRUE)
  expect_equal(record_completion(s$run_dir, s$store), "recorded")
  expect_false(file.exists(file.path(s$run_dir, "completion", "summary.json")))
  res <- store_read(s$store)
  expect_equal(res$runs$state, "no_results")
  expect_equal(nrow(res$summary), 0L)
})

test_that("a late recording skips a flagged PKanalix result file", {
  s <- local_pkx_run()
  Sys.setFileTime(
    file.path(s$results_dir, "be", "confidenceInterval_Test.txt"),
    Sys.time() + 1800
  )
  record_completion(s$run_dir, s$store, late = TRUE)
  res <- store_read(s$store)
  expect_equal(res$runs$state, "recorded_late")
  expect_equal(basename(res$runs$late_flags[[1]]), "confidenceInterval_Test.txt")
  expect_equal(res$summary$tasks, "nca,ca")
  expect_equal(res$summary$ofv, 120.25)
})

test_that("store_read returns bioequivalence rows of PKanalix runs", {
  s <- local_pkx_run()
  record_completion(s$run_dir, s$store)
  be <- store_read(s$store)$bioequivalence
  expect_named(
    be,
    c("run_id", "formulation", "parameter", "ratio", "ci_lower", "ci_upper", "bioequivalent")
  )
  expect_equal(be$run_id, c("r1", "r1"))
  expect_equal(be$formulation, c("Test", "Test"))
  expect_equal(be$parameter, c("AUC_TAU", "Cmax"))
  expect_equal(be$ratio, c(104.2, 133.9))
  expect_equal(be$ci_lower, c(98.1, 122.3))
  expect_equal(be$ci_upper, c(110.6, 146.6))
  expect_identical(be$bioequivalent, c(TRUE, FALSE))
})

test_that("runs_data gives one be_ column per parameter and statistic", {
  s <- local_pkx_run()
  record_completion(s$run_dir, s$store)
  d <- runs_data(s$store, include_summary = TRUE)
  expect_equal(d$tasks, "nca,ca,be")
  expect_equal(d$ca_cost, 1.5)
  expect_equal(d$n_individuals, 3L)
  expect_equal(d$be_AUC_TAU_ratio, 104.2)
  expect_equal(d$be_AUC_TAU_ci_lower, 98.1)
  expect_equal(d$be_AUC_TAU_ci_upper, 110.6)
  expect_identical(d$be_AUC_TAU_bioequivalent, TRUE)
  expect_equal(d$be_Cmax_ratio, 133.9)
  expect_identical(d$be_Cmax_bioequivalent, FALSE)
  expect_false(any(grepl("^n_obs_", names(d))))
})

test_that("be_ columns name the formulation when a run has several", {
  s <- local_pkx_run()
  be_dir <- file.path(s$results_dir, "be")
  ci <- readLines(file.path(be_dir, "confidenceInterval_Test.txt"), warn = FALSE)
  ci[2] <- sub("104.2", "99.5", ci[2], fixed = TRUE)
  writeLines(ci, file.path(be_dir, "confidenceInterval_Test 2.txt"))
  Sys.setFileTime(file.path(be_dir, "confidenceInterval_Test 2.txt"), Sys.time() - 1800)
  record_completion(s$run_dir, s$store)
  d <- runs_data(s$store, include_summary = TRUE)
  expect_equal(d$be_Test_AUC_TAU_ratio, 104.2)
  expect_equal(d$be_Test_2_AUC_TAU_ratio, 99.5)
  expect_identical(d$be_Test_2_Cmax_bioequivalent, FALSE)
  expect_false("be_AUC_TAU_ratio" %in% names(d))
})

test_that("Monolix and PKanalix runs leave each other's columns NA", {
  s <- local_pkx_run()
  record_completion(s$run_dir, s$store)
  theoph <- local_theoph_root()
  file.copy(file.path(theoph, "theoph"), s$root, recursive = TRUE)
  spec <- theoph_run_spec(s$root, "r2")
  cdir <- file.path(s$store, "runs", "r2", "completion")
  dir.create(cdir, recursive = TRUE)
  store_write_json(spec$run, file.path(s$store, "runs", "r2", "run.json"))
  for (part in c("outputs", "summary", "done")) {
    store_write_json(spec$completions$completion[[part]], file.path(cdir, paste0(part, ".json")))
  }
  d <- runs_data(s$store, include_summary = TRUE)
  mlx <- d[d$run_id == "r2", ]
  pkx <- d[d$run_id == "r1", ]
  expect_true(is.na(mlx$tasks))
  expect_true(is.na(mlx$ca_cost))
  expect_true(is.na(mlx$be_AUC_TAU_ratio))
  expect_equal(mlx$n_obs_CONC, 132L)
  expect_true(is.na(pkx$n_obs_CONC))
  expect_true(is.na(pkx$bicc))
  expect_true(is.na(pkx$n_doses))
})
