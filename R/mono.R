#' Submit Monolix and Simulx jobs to grid and record runs in the run store
#'
#' Executes one or more commands that submit Monolix/Simulx jobs to the grid and
#' records each run in the run store.
#'
#' @param path Character vector. Paths to existing Monolix or Simulx project files.
#' @param output_dir Character vector or `NULL`. Directory for
#'   output files relative to the project path.
#'   Passed as `--output-dir` argument to the Monolix command.
#'   If `NULL` (default), Monolix uses the export path from the project file.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param thread Integer vector or `NULL`. Number of threads used by Monolix/Simulx.
#'   Passed as `--thread` argument to the Monolix and Simulx commands.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param tool Character vector or `NULL`. Tool to launch assessment
#'   (`"monolix"`, `"modelBuilding"`, `"bootstrap"`).
#'   Passed as `--tool` argument to the Monolix command.
#'   If `NULL` (default), Monolix uses `"monolix"`.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param mode Character vector or `NULL`. Console mode
#'   (`"none"`, `"basic"`, or `"complete"`).
#'   Passed as `--mode` argument to the Monolix and Simulx commands.
#'   If `NULL` (default), Monolix and Simulx use `"basic"`.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param config Character vector or `NULL`. Configuration file path.
#'   Passed as `--config` argument to the Monolix command.
#'   Should be same length as `path`, length 1, or `NULL`.
#' @param cmd Character vector. The Monolix or Simulx command to execute. Must be
#'   identifiable by `Sys.which()` and contain "mono" or "sim" in the name.
#'   Default is "mono24". Should be same length as `path` or length 1.
#' @param store Character scalar. The run store directory (see
#'   [default_store()]). Defaults to `default_store()`. Created when the first
#'   run is recorded.
#' @param require_charts_data A logical indicating whether charts data must
#'   be exported. This requires the relevant config.ini file includes options
#'   `exportChartsData=true`, `export-charts-datasets=true`, and
#'   `exportVPCSimulations=true`.
#'
#' @return Character vector. The run IDs of all jobs, in the order of `path`.
#'
#' @details
#' Jobs are submitted one at a time. For each job, this function:
#' \enumerate{
#'   \item Records the project and input files (path, SHA-256, size,
#'     modification time) and the settings passed to the command, and creates
#'     a run folder `<store>/runs/<run_id>/`
#'   \item Submits the job using `system2()` and extracts the job ID from the
#'     submission output
#'   \item Writes `run.json` straight away: the records above, the command,
#'     job ID, software version, submission time and output, user and host
#' }
#'
#' If a step fails, no further jobs are submitted and the error names the
#' runs already submitted.
#'
#' A submission that yields no job ID and no local result is recorded with
#' `submission.ok = false` and is not monitored.
#'
#' Once all jobs are submitted, the function polls the job queue until every
#' submitted job has finished, then records each run's completion in a
#' `completion/` folder of the run: the output files written since submission
#' (`outputs.json`), the parsed `summary.txt` (`summary.json`) and, last,
#' `done.json`.
#'
#' Paths inside the store are relative to the folder that holds the store
#' when the file is inside it, and absolute otherwise.
#'
#' @examples
#' \dontrun{
#' # Basic usage with default settings
#' mono("path/to/project.mlxtran")
#'
#' # Multiple jobs
#' mono(c("project1.mlxtran", "project2.mlxtran"))
#'
#' # Specify custom output directory and thread count
#' mono(
#'   path = "project.mlxtran",
#'   output_dir = "custom-project-output",
#'   thread = 4
#' )
#'
#' # Record runs in a specific store
#' mono("project.mlxtran", store = "path/to/monolix/.mlxtrx")
#' }
#'
#' @export
mono <- function(
  path,
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL,
  cmd = getOption("mlxtrx.monolix_cmd", "mono24"),
  store = default_store(),
  require_charts_data = TRUE
) {
  assertthat::assert_that(
    all(file.exists(path)),
    msg = "All `path`s should be paths to existing Monolix or Simulx project files."
  )

  assertthat::assert_that(
    is.character(cmd),
    all(Sys.which(cmd) != ""),
    all(grepl("mono|sim", cmd, ignore.case = TRUE)),
    msg = "All `cmd` commands must be 'mono' or 'sim' commands that can be identified by `Sys.which()`."
  )

  assertthat::assert_that(
    assertthat::is.string(store),
    msg = "`store` must be a path to the run store directory."
  )

  if (require_charts_data) {
    for (.cmd in unique(cmd)) {
      check_config_ini(cmd = Sys.which(.cmd))
    }
  }

  # Recycle all arguments to match length of path
  recycle <- function(x) if (is.null(x)) NULL else rep_len(x, length(path))
  cmd_recycled <- rep_len(cmd, length(path))
  output_dir_recycled <- recycle(output_dir)
  thread_recycled <- recycle(thread)
  tool_recycled <- recycle(tool)
  mode_recycled <- recycle(mode)
  config_recycled <- recycle(config)

  root <- store_root_dir(store)
  run_ids <- character(length(path))
  run_dirs <- character(length(path))
  job_ids <- rep(NA_integer_, length(path))

  submitted <- 0L
  tryCatch(
    for (i in seq_along(path)) {
      # Everything that reads the project or the store happens before the
      # submission, so a failure here stops the batch before this job runs.
      project <- file_record(path[i], root)
      inputs <- input_records(path[i], root)
      settings <- list(
        output_dir = output_dir_recycled[i],
        thread = thread_recycled[i],
        tool = tool_recycled[i],
        mode = mode_recycled[i],
        config = resolve_config_path(config_recycled[i], path[i])
      )
      run <- store_create_run(store)

      submitted_at <- Sys.time()
      submission <- execute_job(
        path = path[i],
        output_dir = output_dir_recycled[i],
        thread = thread_recycled[i],
        tool = tool_recycled[i],
        mode = mode_recycled[i],
        config = config_recycled[i],
        cmd = cmd_recycled[i]
      )
      submitted <- i
      run_ids[i] <- run$run_id
      run_dirs[i] <- run$run_dir
      job_ids[i] <- submission$job_id

      store_write_json(
        list(
          schema = 1L,
          run_id = run$run_id,
          launcher = "cli",
          project = project,
          inputs = inputs,
          settings = settings,
          cmd = cmd_recycled[i],
          job_id = if (is.na(submission$job_id)) NULL else submission$job_id,
          software_version = na_to_null(cmd_software_version(cmd_recycled[i])),
          submitted_at = format_utc(submitted_at),
          submission = list(ok = submission$ok, output = I(submission$output)),
          user = Sys.info()[["user"]],
          host = Sys.info()[["nodename"]],
          mlxtrx_version = as.character(utils::packageVersion("mlxtrx"))
        ),
        file.path(run$run_dir, "run.json")
      )
    },
    error = function(e) {
      if (submitted == 0L) {
        stop(e)
      }
      stop(
        conditionMessage(e),
        "\nRuns already submitted: ",
        paste(run_ids[seq_len(submitted)], collapse = ", "),
        call. = FALSE
      )
    }
  )

  monitor_jobs(
    path = path,
    run_id = run_ids,
    run_dir = run_dirs,
    job_id = job_ids,
    output_dir = output_dir_recycled,
    cmd = cmd_recycled,
    store = store
  )

  return(run_ids)
}

#' @rdname mono
#' @export
sim <- function(
  path,
  thread = NULL,
  mode = NULL,
  cmd = getOption("mlxtrx.simulx_cmd", "sim24"),
  store = default_store(),
  require_charts_data = FALSE
) {
  mono(
    path = path,
    thread = thread,
    mode = mode,
    cmd = cmd,
    store = store,
    require_charts_data = require_charts_data
  )
}

na_to_null <- function(x) {
  if (length(x) == 0L || is.na(x)) NULL else x
}

# File records of a project's data and model files, each with its role. Files
# that do not exist are left out; built-in library models are kept.
input_records <- function(path, root) {
  handler <- get_file_handler(fs::path_ext(path))
  input_files <- handler$get_input_files(handler$parse_file(path), dirname(path))
  roles <- c(data_file = "data", model_file = "model")
  records <- list()
  for (nm in names(roles)) {
    file <- input_files[[nm]]
    if (is.null(file) || is.na(file)) {
      next
    }
    if (file.exists(file) || is_builtin_path(file)) {
      records[[length(records) + 1L]] <- c(
        list(role = roles[[nm]]),
        file_record(file, root)
      )
    }
  }
  records
}

# Config file path as an absolute path; a relative path that does not exist
# from the working directory is looked up next to the project file.
resolve_config_path <- function(config, path) {
  if (is.null(config)) {
    return(NULL)
  }
  if (!fs::is_absolute_path(config) && !file.exists(config)) {
    config_built <- file.path(dirname(path), config)
    if (file.exists(config_built)) {
      config <- config_built
    }
  }
  normalizePath(config, mustWork = FALSE)
}

#' Execute a single Monolix job
#'
#' Submits a single Monolix job to the grid and extracts the job ID.
#'
#' @param path Character scalar. Path to a single existing Monolix project file.
#' @param output_dir Character scalar or `NULL`. Directory for output files.
#' @param thread Integer scalar or `NULL`. Number of threads used by Monolix.
#' @param tool Character scalar or `NULL`. Tool to launch assessment.
#' @param mode Character scalar or `NULL`. Console mode.
#' @param config Character scalar or `NULL`. Configuration file path.
#' @param cmd Character scalar. The Monolix command to execute.
#'
#' @return A list with `job_id` (integer, `NA` when no job ID was found),
#'   `ok` (logical) and `output` (character, the command's output). For the
#'   local runners `monolix.sh` and `simulx.sh`, `ok` is `TRUE` when the
#'   command exits with status 0. For other commands, `ok` is `TRUE` when a
#'   job ID was found or the command reports that results have been loaded.
#'
#' @keywords internal
execute_job <- function(
  path,
  output_dir = NULL,
  thread = NULL,
  tool = NULL,
  mode = NULL,
  config = NULL,
  cmd = "mono24"
) {
  # A non-zero exit status is reported below from the "status" attribute, so
  # system2()'s own warning about it is muffled.
  result <- suppressWarnings(system2(
    cmd,
    args = c(
      if (basename(cmd) == "monolix.sh") "--no-gui" else NULL,
      paste("-p", shQuote(normalizePath(path))),
      if (!is.null(output_dir)) {
        paste("--output-dir", shQuote(output_dir))
      } else {
        NULL
      },
      if (!is.null(thread)) paste("--thread", thread) else NULL,
      if (!is.null(tool)) paste("--tool", tool) else NULL,
      if (!is.null(mode)) paste("--mode", mode) else NULL,
      if (!is.null(config)) paste("--config", config) else NULL
    ),
    stdout = TRUE,
    stderr = TRUE
  ))
  output <- as.character(result)

  # Local runners execute the project in the foreground: success is exit
  # status 0. system2() sets the "status" attribute only when it is non-zero.
  if (basename(cmd) %in% c("monolix.sh", "simulx.sh")) {
    status <- attr(result, "status") %||% 0L
    if (status == 0L) {
      message("Ran ", cmd, " for file: ", path)
      return(list(job_id = NA_integer_, ok = TRUE, output = output))
    }
    warning(
      cmd,
      " exited with status ",
      status,
      " for file: ",
      path,
      ". Last lines of output:\n",
      paste(utils::tail(output, 20), collapse = "\n")
    )
    return(list(job_id = NA_integer_, ok = FALSE, output = output))
  }

  job_id <- NA_integer_
  if (length(output) > 0) {
    job_match <- stringr::str_extract(
      output[1],
      "(?<=Submitted batch job )\\d+"
    )
    if (!is.na(job_match)) {
      job_id <- as.integer(job_match)
    }
  }

  if (is.na(job_id)) {
    if (
      any(stringr::str_detect(
        utils::head(output, 10),
        "Results have been successfully loaded"
      ))
    ) {
      message("Submitted ", cmd, " for file: ", path)
      return(list(job_id = NA_integer_, ok = TRUE, output = output))
    }
    warning(
      "Could not extract job ID from ",
      cmd,
      " output: ",
      paste(output, collapse = "\n")
    )
    return(list(job_id = NA_integer_, ok = FALSE, output = output))
  }

  message("Submitted ", cmd, " job ", job_id, " for file: ", path)
  list(job_id = job_id, ok = TRUE, output = output)
}

#' Monitor multiple jobs and record completed ones
#'
#' Polls every `poll_interval` seconds until each submitted job has finished,
#' then records its completion in the run store with `record_completion()`. A
#' job with a job ID has finished when it is no longer in the queue; a job
#' run by the local runners `monolix.sh` or `simulx.sh` has finished already;
#' any other job without a job ID has finished when its `summary.txt` exists.
#' Runs whose
#' submission failed are skipped. A run whose completion cannot be recorded
#' gives a warning naming it, and the other runs are still monitored and
#' recorded.
#'
#' The queue is read with `sq` once per poll. When `sq` fails, a warning is
#' given and the jobs with a job ID count as still queued for that poll. After
#' `sq_failure_limit` consecutive failures those runs are no longer monitored
#' and are left unrecorded, for [finalize_runs()] to record once they finish.
#'
#' @param path Character vector. Paths to Monolix project files.
#' @param run_id Character vector. Run IDs.
#' @param run_dir Character vector. Run folders in the store.
#' @param job_id Integer vector. External job IDs (from job scheduler).
#' @param output_dir Character vector or `NULL`. Output directories.
#' @param cmd Character vector. The commands used for each job.
#' @param store Character scalar. The run store directory (see [default_store()]).
#' @param poll_interval Numeric. Seconds between polls.
#' @param sq_failure_limit Integer. Consecutive `sq` failures after which runs
#'   with a job ID are no longer monitored.
#'
#' @return NULL (invisible). Called for side effects.
#'
#' @keywords internal
monitor_jobs <- function(
  path,
  run_id,
  run_dir,
  job_id,
  output_dir = NULL,
  cmd,
  store,
  poll_interval = 5,
  sq_failure_limit = 12
) {
  assertthat::assert_that(
    length(path) == length(run_id) &&
      length(run_id) == length(run_dir) &&
      length(run_id) == length(job_id),
    msg = "`path`, `run_id`, `run_dir`, and `job_id` must have the same length"
  )

  if (!is.null(output_dir)) {
    assertthat::assert_that(
      length(output_dir) == length(path),
      msg = "`output_dir` must be NULL or same length as `path`"
    )
  }

  submitted_ok <- vapply(
    run_dir,
    function(dir) {
      run <- jsonlite::read_json(file.path(dir, "run.json"))
      !isFALSE(run$submission$ok)
    },
    logical(1),
    USE.NAMES = FALSE
  )

  # Track which jobs are still running
  running_jobs <- which(submitted_ok)

  sq_failures <- 0L

  while (length(running_jobs) > 0) {
    # Check which running jobs have completed
    completed_now <- c()

    # NULL when sq failed: every job with a job ID counts as still queued.
    queued <- character()
    if (any(!is.na(job_id[running_jobs]))) {
      queued <- tryCatch(
        sq_job_ids(),
        error = function(e) {
          warning(conditionMessage(e), call. = FALSE)
          NULL
        }
      )
      sq_failures <- if (is.null(queued)) sq_failures + 1L else 0L
    }

    for (i in running_jobs) {
      job_completed <- FALSE

      if (!is.na(job_id[i])) {
        job_completed <- !is.null(queued) &&
          !as.character(job_id[i]) %in% queued
      } else if (basename(cmd[i]) %in% c("monolix.sh", "simulx.sh")) {
        # Local runners have finished when execute_job() returns.
        job_completed <- TRUE
      } else {
        ext <- fs::path_ext(path[i])
        handler <- get_file_handler(ext)

        if (!handler$can_monitor()) {
          warning(
            "Can't currently monitor .",
            ext,
            " jobs that are not submitted to the grid."
          )
          job_completed <- TRUE
        } else {
          summary_file <- handler$get_summary_file_path(path[i], output_dir[i])
          job_completed <- file.exists(summary_file)
        }
      }

      if (job_completed) {
        completed_now <- c(completed_now, i)
      }
    }

    for (i in completed_now) {
      tryCatch(
        record_completion(run_dir[i], store),
        error = function(e) {
          warning(
            "Could not record completion of run ",
            run_id[i],
            ": ",
            conditionMessage(e),
            call. = FALSE
          )
        }
      )
    }
    running_jobs <- setdiff(running_jobs, completed_now)

    if (sq_failures >= sq_failure_limit) {
      abandoned <- running_jobs[!is.na(job_id[running_jobs])]
      running_jobs <- setdiff(running_jobs, abandoned)
      message(
        "sq failed ",
        sq_failures,
        " times in a row; no longer monitoring runs ",
        paste(run_id[abandoned], collapse = ", "),
        ". Record them with finalize_runs() once they finish."
      )
    }

    # Sleep before next check if there are still running jobs
    if (length(running_jobs) > 0) {
      Sys.sleep(poll_interval)
    }
  }

  invisible(NULL)
}
