# A root holding a copy of the synthetic PKanalix fixture and one run whose
# run.json points at it. `submitted_at` is one hour before now and every
# fixture result file is a fresh output (30 minutes old).
local_pkx_run <- function(.env = parent.frame()) {
  root <- withr::local_tempdir(.local_envir = .env)
  file.copy(test_path("fixtures", "pkx"), root, recursive = TRUE)
  project <- file.path(root, "pkx", "project.pkx")
  out_dir <- file.path(root, "pkx", "pk")
  results_dir <- file.path(out_dir, "PKanalix", "IndividualParameters")
  outputs <- list.files(out_dir, recursive = TRUE, full.names = TRUE)
  Sys.setFileTime(outputs, Sys.time() - 1800)
  run <- fake_run_json(
    "r1",
    project = file_record(project, root),
    output_dir = path_to_record(out_dir, root),
    submitted_at = format_utc(Sys.time() - 3600)
  )
  s <- local_store_with_runs(
    list(list(id = "r1", run = run)),
    root = root,
    .env = .env
  )
  c(s, list(
    out_dir = out_dir,
    results_dir = results_dir,
    run_dir = s$run_dirs[["r1"]]
  ))
}
