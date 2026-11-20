# One unrecorded run on a copy of the theoph fixture, with the fake `sq` on
# PATH. The queue holds the given job IDs. The run belongs to `user` and was
# launched with `cmd`.
local_finalize_env <- function(
  queue = character(),
  job_id = 7L,
  user = Sys.info()[["user"]],
  cmd = "mono24",
  .env = parent.frame()
) {
  root <- withr::local_tempdir(.local_envir = .env)
  file.copy(test_path("fixtures", "theoph"), root, recursive = TRUE)
  project <- file.path(root, "theoph", "theoph.mlxtran")
  out_dir <- file.path(root, "theoph", "theoph")
  Sys.setFileTime(file.path(out_dir, "summary.txt"), Sys.time() - 1800)
  run <- fake_run_json(
    "r1",
    project = file_record(project, root),
    output_dir = path_to_record(out_dir, root),
    submitted_at = format_utc(Sys.time() - 3600),
    job_id = job_id,
    user = user,
    cmd = cmd
  )
  s <- local_store_with_runs(
    list(list(id = "r1", run = run)),
    root = root,
    .env = .env
  )
  fake <- withr::local_tempdir(.local_envir = .env)
  writeLines(as.character(queue), file.path(fake, "queue"))
  withr::local_path(
    normalizePath(test_path("fixtures", "bin")),
    .local_envir = .env
  )
  withr::local_envvar(MLXTRX_FAKE_DIR = fake, .local_envir = .env)
  c(s, list(fake = fake, out_dir = out_dir, run_dir = s$run_dirs[["r1"]]))
}
