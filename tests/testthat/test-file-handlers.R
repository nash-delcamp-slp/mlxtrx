test_that("Monolix result files are summary.txt in the output dir", {
  out <- withr::local_tempdir()
  handler <- MonolixHandler$new()
  expect_length(handler$result_files(out), 0L)
  writeLines("x", file.path(out, "summary.txt"))
  writeLines("x", file.path(out, "other.txt"))
  expect_equal(handler$result_files(out), file.path(out, "summary.txt"))
  files <- file.path(out, c("other.txt", "summary.txt", "sub/summary.txt"))
  expect_equal(handler$result_files(out, files), files[2])
  expect_length(handler$result_files(out, character()), 0L)
})

# A root holding a copy of the synthetic PKanalix fixture.
local_pkx_root <- function(.env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .env)
  file.copy(test_path("fixtures", "pkx"), root, recursive = TRUE)
  file.path(root, "pkx")
}

pkx_results_dir <- function(dir) {
  file.path(dir, "pk", "PKanalix", "IndividualParameters")
}

test_that(".pkx projects get the PKanalix handler", {
  expect_s3_class(get_file_handler("pkx"), "PkanalixHandler")
  expect_true(get_file_handler("pkx")$expects_summary())
})

test_that("PKanalix input files are the data file and the model", {
  dir <- local_pkx_root()
  path <- file.path(dir, "project.pkx")
  handler <- PkanalixHandler$new()
  files <- handler$get_input_files(handler$parse_file(path), dir)
  expect_equal(files$data_file, normalizePath(file.path(dir, "data", "pk.csv")))
  expect_equal(files$model_file, "lib:oral1_1cpt_kaVCl.txt")
})

test_that("the PKanalix output dir is the exportpath, else the project name", {
  dir <- local_pkx_root()
  path <- file.path(dir, "project.pkx")
  handler <- PkanalixHandler$new()
  expect_equal(
    normalizePath(handler$get_output_dir(path)),
    normalizePath(file.path(dir, "pk"))
  )
  lines <- readLines(path)
  writeLines(lines[!grepl("exportpath", lines)], file.path(dir, "other.pkx"))
  expect_equal(
    handler$get_output_dir(file.path(dir, "other.pkx")),
    file.path(dir, "other")
  )
})

test_that("PKanalix result files are the parameter, cost and CI tables", {
  dir <- local_pkx_root()
  out <- file.path(dir, "pk")
  res <- pkx_results_dir(dir)
  expected <- file.path(
    res,
    c(
      "be/confidenceInterval_Test.txt",
      "ca/caIndividualParameters.txt",
      "ca/cost.txt",
      "nca/ncaIndividualParameters.txt"
    )
  )
  handler <- PkanalixHandler$new()
  expect_setequal(handler$result_files(out), expected)
  candidates <- c(file.path(res, "nca", "summary.txt"), expected[c(1, 3)])
  expect_equal(handler$result_files(out, candidates), expected[c(1, 3)])
  expect_length(handler$result_files(file.path(dir, "nope")), 0L)
})

test_that("PKanalix results give tasks, individuals, CA criteria and BE", {
  dir <- local_pkx_root()
  handler <- PkanalixHandler$new()
  parsed <- handler$parse_results(handler$result_files(file.path(dir, "pk")))
  stats <- parsed$summary_stats
  expect_equal(stats$tasks, "nca,ca,be")
  expect_identical(stats$n_individuals, 3L)
  expect_equal(stats$ofv, 120.25)
  expect_equal(stats$aic, 130.25)
  expect_equal(stats$bic, 135.75)
  expect_equal(stats$ca_cost, 1.5)
  be <- parsed$bioequivalence
  expect_length(be, 2L)
  expect_equal(be[[1]]$formulation, "Test")
  expect_equal(be[[1]]$parameter, "AUC_TAU")
  expect_equal(be[[1]]$ratio, 104.2)
  expect_equal(be[[1]]$ci_lower, 98.1)
  expect_equal(be[[1]]$ci_upper, 110.6)
  expect_identical(be[[1]]$bioequivalent, TRUE)
  expect_equal(be[[2]]$parameter, "Cmax")
  expect_identical(be[[2]]$bioequivalent, FALSE)
})

test_that("individuals come from CA parameters without NCA ones", {
  dir <- local_pkx_root()
  handler <- PkanalixHandler$new()
  files <- handler$result_files(file.path(dir, "pk"))
  parsed <- handler$parse_results(files[!grepl("/nca/", files)])
  expect_equal(parsed$summary_stats$tasks, "ca,be")
  expect_identical(parsed$summary_stats$n_individuals, 2L)
  only_be <- handler$parse_results(files[grepl("/be/", files)])
  expect_equal(only_be$summary_stats$tasks, "be")
  expect_identical(only_be$summary_stats$n_individuals, NA_integer_)
  expect_identical(only_be$summary_stats$ofv, NA_real_)
})

test_that("each confidence interval file gives its formulation", {
  dir <- local_pkx_root()
  be_dir <- file.path(pkx_results_dir(dir), "be")
  file.copy(
    file.path(be_dir, "confidenceInterval_Test.txt"),
    file.path(be_dir, "confidenceInterval_Test 2.txt")
  )
  handler <- PkanalixHandler$new()
  parsed <- handler$parse_results(handler$result_files(file.path(dir, "pk")))
  formulations <- vapply(parsed$bioequivalence, `[[`, "", "formulation")
  expect_setequal(formulations, c("Test", "Test 2"))
  expect_length(formulations, 4L)
})

test_that("empty or non-numeric PKanalix cells parse to NA without a warning", {
  dir <- local_pkx_root()
  results <- pkx_results_dir(dir)
  ci <- file.path(results, "be", "confidenceInterval_Test.txt")
  lines <- readLines(ci, warn = FALSE)
  lines[2] <- "AUC_TAU,500.5,3,480.25,3,0.0413,-0.02,0.1,-nan(ind),,,-nan(ind)"
  writeLines(lines, ci)
  writeLines(c("Cost,-2LL,AIC,BIC", "-nan(ind),120.25,,135.75"), file.path(results, "ca", "cost.txt"))
  handler <- PkanalixHandler$new()
  expect_no_warning(
    parsed <- handler$parse_results(handler$result_files(file.path(dir, "pk")))
  )
  be <- parsed$bioequivalence[[1]]
  expect_identical(be$ratio, NA_real_)
  expect_identical(be$ci_lower, NA_real_)
  expect_identical(be$ci_upper, NA_real_)
  expect_identical(be$bioequivalent, NA)
  expect_identical(parsed$summary_stats$ca_cost, NA_real_)
  expect_identical(parsed$summary_stats$aic, NA_real_)
  expect_equal(parsed$bioequivalence[[2]]$ratio, 133.9)
})

test_that("only confidence interval files directly in be/ are result files", {
  dir <- local_pkx_root()
  out <- file.path(dir, "pk")
  nested <- file.path(pkx_results_dir(dir), "be", "confidenceInterval_X", "copy.txt")
  handler <- PkanalixHandler$new()
  expect_length(handler$result_files(out, nested), 0L)
})
