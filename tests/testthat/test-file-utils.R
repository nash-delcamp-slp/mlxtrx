test_that("calculate_sha256 hashes file contents", {
  path <- withr::local_tempfile()
  writeBin(charToRaw("abc"), path)
  expect_identical(
    calculate_sha256(path),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
  )
})

test_that("calculate_sha256 returns NA for a missing file", {
  expect_identical(
    calculate_sha256(withr::local_tempfile()),
    NA_character_
  )
})

test_that("theoph fixture summary parses to the documented values", {
  summary_path <- test_path("fixtures", "theoph", "theoph", "summary.txt")
  parsed <- MonolixHandler$new()$parse_summary(summary_path)
  stats <- parsed$summary_stats
  expect_identical(stats$exploratory_iterations, 200L)
  expect_identical(stats$smoothing_iterations, 100L)
  expect_true(stats$fisher_matrix_estimation)
  expect_equal(stats$eigenvalue_min, 0.01)
  expect_equal(stats$eigenvalue_max, 5)
  expect_equal(stats$ofv, 300.5)
  expect_equal(stats$aic, 316.5)
  expect_equal(stats$bicc, 320.1)
  expect_equal(stats$bic, 318.2)
  expect_identical(stats$n_individuals, 12L)
  expect_identical(stats$n_doses, 12L)
  expect_identical(parsed$observations, list(CONC = 132L))
})

eigen_stats <- function(eigen_line) {
  path <- withr::local_tempfile(fileext = ".txt", .local_envir = parent.frame())
  writeLines(
    c("ESTIMATION OF THE FISHER INFORMATION MATRIX ____", eigen_line),
    path
  )
  MonolixHandler$new()$parse_summary(path)$summary_stats
}

test_that("eigenvalue_min and eigenvalue_max are the smallest and largest listed", {
  stats <- eigen_stats("Eigen values           :     0.099       1.9        19")
  expect_equal(stats$eigenvalue_min, 0.099)
  expect_equal(stats$eigenvalue_max, 19)
})

test_that("eigenvalues in scientific notation are read whole", {
  stats <- eigen_stats("Eigen values           :   3.2e-06   0.5   1.4e+03")
  expect_equal(stats$eigenvalue_min, 3.2e-06)
  expect_equal(stats$eigenvalue_max, 1400)
})

test_that("eigenvalues stay NA when Monolix cannot compute them", {
  stats <- eigen_stats(
    "WARNING : Impossible to compute the eigen values of the correlation matrix."
  )
  expect_true(is.na(stats$eigenvalue_min))
  expect_true(is.na(stats$eigenvalue_max))
})
