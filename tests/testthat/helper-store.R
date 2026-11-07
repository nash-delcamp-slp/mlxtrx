# Minimal run.json content; `...` overrides top-level fields.
fake_run_json <- function(run_id, ...) {
  base <- list(
    schema = 1L,
    run_id = run_id,
    launcher = "cli",
    project = list(
      path = "models/run001.mlxtran",
      sha256 = "aaa",
      size = 10L,
      mtime = "2026-11-06T14:20:11.000Z"
    ),
    inputs = list(
      list(
        role = "data",
        path = "data/pk.csv",
        sha256 = "bbb",
        size = 99L,
        mtime = "2026-11-06T14:00:00.000Z"
      ),
      list(role = "model", path = "lib:oral1_1cpt_kaVCl.txt", builtin = TRUE)
    ),
    settings = list(
      output_dir = NULL,
      thread = 4L,
      tool = NULL,
      mode = NULL,
      config = NULL
    ),
    output_dir = list(path = "models/run001", absolute = FALSE),
    cmd = "mono24",
    job_id = 123456L,
    software_version = "2024R1",
    submitted_at = "2026-11-06T14:23:01.000Z",
    submission = list(ok = TRUE, output = list("Submitted batch job 123456")),
    user = "tester",
    host = "gridlogin01",
    mlxtrx_version = "0.0.0.9000"
  )
  over <- list(...)
  base[names(over)] <- over
  base
}

# Minimal done.json content; `...` overrides fields.
fake_done_json <- function(...) {
  base <- list(
    completed_at = "2026-11-06T15:02:44.000Z",
    recorded_at = "2026-11-06T15:02:50.000Z",
    recorded_by = "tester@gridlogin01",
    recorded_late = FALSE,
    late_flags = list()
  )
  over <- list(...)
  base[names(over)] <- over
  base
}

# Builds a temp root holding `<root>/.mlxtrx/runs/<id>/...` from R
# lists, written with store_write_json(). Removed when `.env` exits.
#
# runs: unnamed list of run specs. Each spec is a list with
#   id          run folder name (default: generated "2026110614230<i>Z-000<i>")
#   run         run.json content as a list, a character string written
#               verbatim (e.g. invalid JSON), or NULL for no run.json
#               (default: fake_run_json(id))
#   completions named list, one entry per attempt folder ("completion",
#               "completion-2", ...); each entry is a list of outputs,
#               summary, done (list content, or NULL to omit the file)
#   files       character vector of extra file names created empty in the
#               run folder (e.g. "run.json.tmp-abcd1234")
# root: existing directory to use instead of a new temp dir.
#
# Returns list(store, root, run_ids, run_dirs); the last two are named
# by run id in spec order.
#
# Example:
#   s <- local_store_with_runs(list(
#     list(id = "r1"),
#     list(
#       id = "r2",
#       completions = list(
#         completion = list(
#           outputs = list(list(path = "out/a.txt", sha256 = "c", size = 1L,
#                               mtime = "2026-11-06T15:00:00.000Z")),
#           done = fake_done_json()
#         )
#       )
#     )
#   ))
#   store_read(s$store)
local_store_with_runs <- function(
  runs = list(),
  root = NULL,
  .env = parent.frame()
) {
  if (is.null(root)) {
    root <- withr::local_tempdir(.local_envir = .env)
  }
  store <- file.path(root, ".mlxtrx")
  runs_dir <- file.path(store, "runs")
  dir.create(runs_dir, recursive = TRUE)
  ids <- character()
  dirs <- character()
  for (i in seq_along(runs)) {
    spec <- runs[[i]]
    id <- spec$id %||% sprintf("2026110614230%dZ-000%d", i %% 10, i %% 10)
    run_dir <- file.path(runs_dir, id)
    dir.create(run_dir)
    ids[id] <- id
    dirs[id] <- run_dir
    run <- if ("run" %in% names(spec)) spec$run else fake_run_json(id)
    if (is.character(run)) {
      writeLines(run, file.path(run_dir, "run.json"))
    } else if (!is.null(run)) {
      store_write_json(run, file.path(run_dir, "run.json"))
    }
    for (nm in names(spec$completions)) {
      cdir <- file.path(run_dir, nm)
      dir.create(cdir)
      for (part in c("outputs", "summary", "done")) {
        content <- spec$completions[[nm]][[part]]
        if (!is.null(content)) {
          store_write_json(content, file.path(cdir, paste0(part, ".json")))
        }
      }
    }
    for (f in spec$files) {
      file.create(file.path(run_dir, f))
    }
  }
  list(store = store, root = root, run_ids = ids, run_dirs = dirs)
}
