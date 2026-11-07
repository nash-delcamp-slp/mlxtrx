out_rec <- function(path, mtime = "2026-11-06T15:00:00.000Z") {
  list(path = path, sha256 = "ccc", size = 5L, mtime = mtime)
}

summary_json <- function(ofv = 100.5) {
  list(
    summary_stats = list(
      exploratory_iterations = 200L,
      smoothing_iterations = 100L,
      fisher_matrix_estimation = TRUE,
      eigenvalue_min = 0.1,
      eigenvalue_max = 9.5,
      ofv = ofv,
      aic = 110.5,
      bicc = 120.5,
      bic = 115.5,
      n_individuals = 12L,
      n_doses = 132L
    ),
    observations = list(y1 = 132L, y2 = 40L),
    summary_extracted_at = "2026-11-06T15:02:45.000Z"
  )
}

full_completion <- function(done = fake_done_json(), ofv = 100.5) {
  list(
    outputs = list(out_rec("out/run001/summary.txt"), out_rec("/abs/x.csv")),
    summary = summary_json(ofv),
    done = done
  )
}

test_that("a missing store gives zero-row frames and creates nothing", {
  tmp <- withr::local_tempdir()
  store <- file.path(tmp, "nope", ".mlxtrx")
  res <- store_read(store)
  expect_named(res, c("runs", "files", "summary", "observations"))
  expect_true(all(vapply(res, nrow, integer(1)) == 0L))
  expect_s3_class(res$runs$submitted_at, "POSIXct")
  expect_type(res$runs$late_flags, "list")
  expect_identical(
    names(res$summary),
    c(
      "run_id", "exploratory_iterations", "smoothing_iterations",
      "fisher_matrix_estimation", "eigenvalue_min", "eigenvalue_max", "ofv",
      "aic", "bicc", "bic", "n_individuals", "n_doses", "summary_extracted_at"
    )
  )
  expect_false(dir.exists(store))
  expect_false(dir.exists(file.path(tmp, "nope")))
})

test_that("an existing store without runs/ is empty and stays untouched", {
  tmp <- withr::local_tempdir()
  store <- file.path(tmp, ".mlxtrx")
  dir.create(store)
  res <- store_read(store)
  expect_equal(nrow(res$runs), 0L)
  expect_identical(list.files(store, all.files = TRUE, no.. = TRUE), character(0))
})

test_that("each run state is derived from the files present", {
  s <- local_store_with_runs(list(
    list(
      id = "a-failed",
      run = fake_run_json(
        "a-failed",
        job_id = NULL,
        submission = list(ok = FALSE, output = list("boom"))
      )
    ),
    list(id = "b-unrecorded"),
    list(
      id = "c-crashed",
      completions = list(completion = list(outputs = list(out_rec("o.txt"))))
    ),
    list(id = "d-recorded", completions = list(completion = full_completion())),
    list(
      id = "e-late",
      completions = list(
        completion = full_completion(
          fake_done_json(
            recorded_late = TRUE,
            late_flags = list(list(path = "out/run001/late.txt", absolute = FALSE))
          )
        )
      )
    )
  ))
  runs <- store_read(s$store)$runs
  expect_identical(
    runs$state,
    c("submit_failed", "unrecorded", "completion_crashed", "recorded", "recorded_late")
  )
  expect_identical(runs$recorded_late, c(NA, NA, NA, FALSE, TRUE))
  expect_identical(runs$submission_ok, c(FALSE, TRUE, TRUE, TRUE, TRUE))
  expect_identical(runs$run_id, names(s$run_ids))
  expect_identical(runs$run_dir, unname(s$run_dirs))
  expect_true(is.na(runs$completed_at[2]))
  expect_s3_class(runs$completed_at, "POSIXct")
  expect_identical(
    runs$late_flags[[5]],
    file.path(s$root, "out/run001/late.txt")
  )
  expect_identical(runs$late_flags[[4]], character(0))
})

test_that("run.json fields are mapped to columns", {
  s <- local_store_with_runs(list(
    list(id = "r1", completions = list(completion = full_completion()))
  ))
  r <- store_read(s$store)$runs
  expect_identical(r$launcher, "cli")
  expect_identical(r$job_id, 123456L)
  expect_identical(r$project_path, file.path(s$root, "models/run001.mlxtran"))
  expect_identical(r$project_name, "run001.mlxtran")
  expect_identical(r$data_file, file.path(s$root, "data/pk.csv"))
  expect_identical(r$model_file, "lib:oral1_1cpt_kaVCl.txt")
  expect_identical(r$thread, 4L)
  expect_true(is.na(r$tool))
  expect_identical(r$output_dir, file.path(s$root, "models/run001"))
  expect_identical(r$cmd, "mono24")
  expect_identical(r$software_version, "2024R1")
  expect_identical(r$user, "tester")
  expect_identical(r$host, "gridlogin01")
  expect_identical(r$project_file_sha256, "aaa")
  expect_equal(
    as.numeric(r$submitted_at),
    as.numeric(as.POSIXct("2026-11-06 14:23:01", tz = "UTC"))
  )
  expect_equal(
    as.numeric(r$completed_at),
    as.numeric(as.POSIXct("2026-11-06 15:02:44", tz = "UTC"))
  )
  expect_equal(
    as.numeric(r$project_file_timestamp),
    as.numeric(as.POSIXct("2026-11-06 14:20:11", tz = "UTC"))
  )
  expect_identical(attr(r$submitted_at, "tzone"), Sys.timezone())
})

test_that("files frame lists project, inputs and outputs of the used attempt", {
  s <- local_store_with_runs(list(
    list(id = "r1", completions = list(completion = full_completion()))
  ))
  f <- store_read(s$store)$files
  expect_identical(f$io_type, c("project", "input", "input", "output", "output"))
  expect_identical(f$run_id, rep("r1", 5))
  expect_identical(f$builtin, c(FALSE, FALSE, TRUE, FALSE, FALSE))
  expect_identical(
    f$path,
    c(
      file.path(s$root, "models/run001.mlxtran"),
      file.path(s$root, "data/pk.csv"),
      "lib:oral1_1cpt_kaVCl.txt",
      file.path(s$root, "out/run001/summary.txt"),
      "/abs/x.csv"
    )
  )
  expect_identical(f$name[4], "summary.txt")
  expect_identical(f$sha256[c(1, 4)], c("aaa", "ccc"))
  expect_true(is.na(f$sha256[3]))
  expect_identical(f$size[1], 10)
  expect_type(f$size, "double")
  expect_s3_class(f$timestamp, "POSIXct")
})

test_that("the highest completion attempt with done.json is used", {
  s <- local_store_with_runs(list(
    list(
      id = "r1",
      completions = list(
        completion = list(outputs = list(out_rec("old.txt"))),
        `completion-2` = list(
          outputs = list(out_rec("new.txt")),
          summary = summary_json(ofv = 7),
          done = fake_done_json(recorded_late = TRUE)
        )
      )
    )
  ))
  res <- store_read(s$store)
  expect_identical(res$runs$state, "recorded_late")
  expect_true(res$runs$recorded_late)
  expect_identical(res$files$name[res$files$io_type == "output"], "new.txt")
  expect_equal(res$summary$ofv, 7)
})

test_that("unreadable run folders warn once each and the rest are returned", {
  s <- local_store_with_runs(list(
    list(id = "good"),
    list(id = "badjson", run = "{ not json"),
    list(id = "tmponly", run = NULL, files = "run.json.tmp-abcd1234")
  ))
  res <- NULL
  expect_warning(
    expect_warning(
      res <- store_read(s$store),
      "Skipping unreadable run folder badjson"
    ),
    "Skipping unreadable run folder tmponly"
  )
  expect_identical(res$runs$run_id, "good")
})

test_that("relative paths resolve against the root", {
  s <- local_store_with_runs(list(
    list(id = "r1", completions = list(completion = full_completion()))
  ))
  copy <- withr::local_tempdir()
  file.copy(list.files(s$root, all.files = TRUE, full.names = TRUE, no.. = TRUE),
            copy, recursive = TRUE)
  res <- store_read(file.path(copy, ".mlxtrx"))
  expect_identical(res$runs$project_path, file.path(copy, "models/run001.mlxtran"))
  expect_identical(res$runs$run_dir, file.path(copy, ".mlxtrx/runs/r1"))
})

test_that("summary and observations come from summary.json", {
  s <- local_store_with_runs(list(
    list(id = "r1", completions = list(completion = full_completion())),
    list(id = "r2")
  ))
  res <- store_read(s$store)
  expect_identical(res$summary$run_id, "r1")
  expect_identical(res$summary$ofv, 100.5)
  expect_identical(res$summary$n_individuals, 12L)
  expect_true(res$summary$fisher_matrix_estimation)
  expect_s3_class(res$summary$summary_extracted_at, "POSIXct")
  expect_identical(res$observations$run_id, c("r1", "r1"))
  expect_identical(res$observations$obs_type, c("y1", "y2"))
  expect_identical(res$observations$n_observations, c(132L, 40L))
})

test_that("a parseable run.json that is malformed warns for its folder only", {
  bad <- fake_run_json("nopro")
  bad$project <- NULL
  multi <- fake_run_json("multi", cmd = list("a", "b"))
  s <- local_store_with_runs(list(
    list(id = "good"),
    list(id = "nopro", run = bad),
    list(id = "multi", run = multi)
  ))
  res <- NULL
  expect_warning(
    expect_warning(
      res <- store_read(s$store),
      "Skipping unreadable run folder nopro"
    ),
    "Skipping unreadable run folder multi"
  )
  expect_identical(res$runs$run_id, "good")
})
