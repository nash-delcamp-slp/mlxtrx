# A theoph store with one run "r1" whose run.json records a config.ini
# (kept outside the root) and whose run folder holds its copy.
local_archive_store <- function(.env = parent.frame()) {
  root <- local_theoph_root(.env = .env)
  ini_dir <- withr::local_tempdir(.local_envir = .env)
  ini <- file.path(ini_dir, "config.ini")
  writeLines(c("[misc]", "exportChartsData=true"), ini)
  spec <- theoph_run_spec(root, "r1")
  spec$run$config_ini <- c(file_record(ini, root), list(copy = "config.ini"))
  s <- local_store_with_runs(list(spec), root = root, .env = .env)
  file.copy(ini, file.path(s$run_dirs[["r1"]], "config.ini"))
  c(s, theoph_paths(root), list(ini = ini))
}
