# A substage holding a copy of the theoph fixture, with no store.
local_theoph_substage <- function(.env = parent.frame()) {
  substage <- withr::local_tempdir(.local_envir = .env)
  file.copy(test_path("fixtures", "theoph"), substage, recursive = TRUE)
  substage
}

theoph_paths <- function(substage) {
  list(
    project = file.path(substage, "theoph", "theoph.mlxtran"),
    data = file.path(substage, "theoph", "data", "theoph.csv"),
    summary = file.path(substage, "theoph", "theoph", "summary.txt")
  )
}

# Run spec for local_store_with_runs() whose records hash the theoph files in
# `substage` as they are now: project, data input, library model, summary.txt
# as the one output, and its parsed summary.
#   recorded_late  written to done.json
#   flag_summary   whether summary.txt is in done.json's late_flags
#   output_sha256  recorded hash of summary.txt in place of the real one
theoph_run_spec <- function(
  substage,
  id,
  submitted_at = Sys.time() - 3600,
  recorded_late = FALSE,
  flag_summary = FALSE,
  output_sha256 = NULL
) {
  p <- theoph_paths(substage)
  output <- file_record(p$summary, substage)
  if (!is.null(output_sha256)) {
    output$sha256 <- output_sha256
  }
  summary <- get_file_handler("mlxtran")$parse_summary(p$summary)
  summary$summary_extracted_at <- format_utc(Sys.time())
  late_flags <- if (flag_summary) {
    list(path_to_record(p$summary, substage))
  } else {
    list()
  }
  list(
    id = id,
    run = fake_run_json(
      id,
      project = file_record(p$project, substage),
      inputs = list(
        c(list(role = "data"), file_record(p$data, substage)),
        list(role = "model", path = "lib:oral1_1cpt_kaVCl.txt", builtin = TRUE)
      ),
      submitted_at = format_utc(submitted_at)
    ),
    completions = list(
      completion = list(
        outputs = list(output),
        summary = summary,
        done = fake_done_json(
          recorded_late = recorded_late,
          late_flags = late_flags
        )
      )
    )
  )
}

# A theoph substage with a store holding the given theoph_run_spec() runs.
# `specs` is a list of argument lists for theoph_run_spec() (minus substage).
local_theoph_store <- function(specs, .env = parent.frame()) {
  substage <- local_theoph_substage(.env = .env)
  runs <- lapply(specs, function(a) do.call(theoph_run_spec, c(list(substage), a)))
  s <- local_store_with_runs(runs, substage = substage, .env = .env)
  c(s, theoph_paths(substage))
}
