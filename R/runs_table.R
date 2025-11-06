#' Extract data about executed Monolix runs
#'
#' Queries the database to retrieve information about Monolix jobs that have
#' been executed and recorded. Returns a data frame with basic job information
#' or detailed file tracking data.
#'
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#' @param include_files Logical scalar. Whether to include details about input and
#'   output files in the data. Default is `FALSE`.
#'
#' @return A data frame with run information. Contains columns: `run_id`, `job_id`,
#'   `project_name`, `project_path`, `data_file`, `model_file`, `thread`, `tool`, `mode`,
#'   `config`, `cmd`, `project_file_timestamp`, `project_file_md5_checksum`,
#'   `submitted_at`, `completed_at`.
#'   When `include_files = TRUE`, additionally contains file details.
#'
#' @export
runs_data <- function(
  db_conn = default_db_conn(),
  include_files = FALSE
) {
  assertthat::assert_that(
    inherits(db_conn, "DBIObject"),
    msg = "`db_conn` must be a valid database connection"
  )
  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Query the runs data
  if (include_files) {
    runs_data <- DBI::dbGetQuery(
      db_conn,
      "
      SELECT 
        r.run_id,
        r.job_id,
        pf.path as project_path,
        pf.name as project_name,
        df.path as data_file,
        mf.path as model_file,
        r.thread,
        r.tool,
        r.mode,
        r.config,
        r.cmd,
        r.submitted_at,
        r.completed_at,
        rf.io_type,
        f.path as file_path,
        f.name as file_name,
        rf.timestamp as file_timestamp,
        rf.md5_checksum,
        prf.timestamp as project_file_timestamp,
        prf.md5_checksum as project_file_md5_checksum
      FROM run r
      LEFT JOIN file pf ON r.project_file_id = pf.id
      LEFT JOIN file df ON r.data_file_id = df.id
      LEFT JOIN file mf ON r.model_file_id = mf.id
      LEFT JOIN run_file prf ON r.run_id = prf.run_id AND r.project_file_id = prf.file_id AND prf.io_type = 'project'
      LEFT JOIN run_file rf ON r.run_id = rf.run_id AND rf.io_type IN ('input', 'output')
      LEFT JOIN file f ON rf.file_id = f.id
      ORDER BY r.submitted_at DESC, rf.io_type, f.path
      "
    )
  } else {
    runs_data <- DBI::dbGetQuery(
      db_conn,
      "
      SELECT 
        r.run_id,
        r.job_id,
        pf.path as project_path,
        pf.name as project_name,
        df.path as data_file,
        mf.path as model_file,
        r.thread,
        r.tool,
        r.mode,
        r.config,
        r.cmd,
        r.submitted_at,
        r.completed_at,
        prf.timestamp as project_file_timestamp,
        prf.md5_checksum as project_file_md5_checksum
      FROM run r
      LEFT JOIN file pf ON r.project_file_id = pf.id
      LEFT JOIN file df ON r.data_file_id = df.id
      LEFT JOIN file mf ON r.model_file_id = mf.id
      LEFT JOIN run_file prf ON r.run_id = prf.run_id AND r.project_file_id = prf.file_id AND prf.io_type = 'project'
      ORDER BY r.submitted_at DESC
      "
    )
  }

  # Return early if no data
  if (nrow(runs_data) == 0) {
    message("No runs found in database")
    return(invisible(NULL))
  }

  # Format the data with timezone conversion
  if (include_files) {
    runs_formatted <- runs_data |>
      dplyr::mutate(
        submitted_at = as.POSIXct(.data[["submitted_at"]], tz = "UTC") |>
          lubridate::with_tz(Sys.timezone()),
        completed_at = as.POSIXct(.data[["completed_at"]], tz = "UTC") |>
          lubridate::with_tz(Sys.timezone()),
        project_file_timestamp = as.POSIXct(
          .data[["project_file_timestamp"]],
          tz = "UTC"
        ) |>
          lubridate::with_tz(Sys.timezone()),
        file_timestamp = as.POSIXct(.data[["file_timestamp"]], tz = "UTC") |>
          lubridate::with_tz(Sys.timezone())
      ) |>
      dplyr::select(dplyr::any_of(c(
        "run_id",
        "job_id",
        "project_path",
        "project_name",
        "data_file",
        "model_file",
        "thread",
        "tool",
        "mode",
        "config",
        "project_file_timestamp",
        "project_file_md5_checksum",
        "io_type",
        "file_name",
        "file_path",
        "file_timestamp",
        "md5_checksum",
        "cmd",
        "submitted_at",
        "completed_at"
      )))
  } else {
    runs_formatted <- runs_data |>
      dplyr::mutate(
        submitted_at = as.POSIXct(.data[["submitted_at"]], tz = "UTC") |>
          lubridate::with_tz(Sys.timezone()),
        completed_at = as.POSIXct(.data[["completed_at"]], tz = "UTC") |>
          lubridate::with_tz(Sys.timezone()),
        project_file_timestamp = as.POSIXct(
          .data[["project_file_timestamp"]],
          tz = "UTC"
        ) |>
          lubridate::with_tz(Sys.timezone())
      ) |>
      dplyr::select(dplyr::any_of(c(
        "run_id",
        "job_id",
        "project_name",
        "project_path",
        "data_file",
        "model_file",
        "thread",
        "tool",
        "mode",
        "config",
        "project_file_timestamp",
        "project_file_md5_checksum",
        "cmd",
        "submitted_at",
        "completed_at"
      )))
  }

  runs_formatted
}

#' Generate a table with details about executed Monolix runs
#'
#' Creates a formatted table displaying information about Monolix jobs that have
#' been executed and recorded in the database. The table can include basic job
#' information or detailed file tracking data.
#'
#' @param db_conn A database connection object inheriting from `DBIObject`.
#'   Defaults to a connection created by `default_db_conn()`.
#' @param format Character scalar. Output format for the table.
#'   Either `"gt"` for a formatted gt table (default) or `"csv"` for a data frame.
#' @param output_file Character scalar or `NULL`. File path to save output.
#' @param include_files Logical scalar. Whether to include details about input and
#'   output files in the table. Default is `FALSE`.
#'
#' @return When `format = "gt"`, returns a `gt_tbl` object. When `format = "csv"`
#'   and `output_file` is specified, returns the data frame invisibly after writing
#'   to file. When `format = "csv"` and `output_file` is `NULL`, returns the data frame.
#'   Returns `NULL` invisibly if no runs are found in the database.
#'
#' @examples
#' \dontrun{
#' # Table of all runs
#' runs_table()
#'
#' # Table with input/output file information
#' runs_table(include_files = TRUE)
#'
#' # Export to CSV
#' runs_table(format = "csv", output_file = "runs_summary.csv")
#'
#' # Use custom database connection
#' conn <- DBI::dbConnect(duckdb::duckdb(), "custom.db")
#' runs_table(db_conn = conn)
#' }
#'
#' @seealso
#' \code{\link{runs_data}} for extracting the underlying data,
#' \code{\link{mono}} for submitting Monolix jobs and recording runs,
#' \code{\link{default_db_conn}} for default database connections
#'
#' @export
runs_table <- function(
  db_conn = default_db_conn(),
  format = "gt",
  output_file = NULL,
  include_files = FALSE
) {
  format <- match.arg(format, c("gt", "csv"))

  if (missing(db_conn)) {
    on.exit(DBI::dbDisconnect(db_conn), add = TRUE)
  }

  # Get the data using runs_data()
  runs_formatted <- runs_data(db_conn = db_conn, include_files = include_files)

  # Return early if no data
  if (is.null(runs_formatted)) {
    return(invisible(NULL))
  }

  # Handle output format
  if (format == "csv") {
    if (!is.null(output_file)) {
      readr::write_csv(runs_formatted, output_file)
      message("CSV written to: ", output_file)
      return(invisible(runs_formatted))
    } else {
      return(runs_formatted)
    }
  } else {

    # Create gt table
    if (include_files) {
      gt_table <- runs_formatted |>
        dplyr::mutate(
          run_id = sprintf(
            "%s '%s' (%d)",
            .data[["cmd"]],
            .data[["project_path"]],
            .data[["run_id"]]
          ),
          job_id = NULL,
          project_path = NULL,
          project_name = NULL,
          cmd = NULL
        ) |>
        gt::gt(groupname_col = "run_id") |>
        gt::tab_header(
          title = "Executed Runs with File Details",
          subtitle = paste("Total runs:", length(unique(runs_formatted$run_id)))
        ) |>
        gt::cols_label(
          io_type = "Type",
          file_name = "File",
          file_path = "Path",
          file_timestamp = "File Modified",
          md5_checksum = "MD5",
          data_file = "Data File",
          model_file = "Model File",
          thread = "Threads",
          tool = "Tool",
          mode = "Mode",
          config = "Config File",
          project_file_timestamp = "Project Modified",
          project_file_md5_checksum = "Project MD5",
          submitted_at = "Submitted",
          completed_at = "Completed"
        ) |>
        gt::fmt_datetime(
          columns = dplyr::any_of(c(
            "submitted_at",
            "completed_at",
            "project_file_timestamp",
            "file_timestamp"
          )),
          date_style = "yMMMd",
          time_style = "Hm"
        )
    } else {
      gt_table <- runs_formatted |>
        gt::gt() |>
        gt::tab_header(
          title = "Executed Runs",
          subtitle = paste("Total runs:", nrow(runs_formatted))
        ) |>
        gt::cols_label(
          run_id = "Run ID",
          project_name = "Project",
          project_path = "Path",
          data_file = "Data File",
          model_file = "Model File",
          thread = "Threads",
          tool = "Tool",
          mode = "Mode",
          config = "Config File",
          project_file_timestamp = "Project Modified",
          project_file_md5_checksum = "Project MD5",
          submitted_at = "Submitted",
          completed_at = "Completed"
        ) |>
        gt::fmt_datetime(
          columns = dplyr::any_of(c(
            "submitted_at",
            "completed_at",
            "project_file_timestamp"
          )),
          date_style = "yMMMd",
          time_style = "Hm"
        )
    }

    gt_table <- gt_table |>
      gt::tab_style(
        style = gt::cell_text(font = gt::google_font("JetBrains Mono")),
        locations = gt::cells_body(
          columns = dplyr::contains(c("job_id", "path", "md5"))
        )
      ) |>
      gt::tab_options(
        table.font.size = 12,
        heading.title.font.size = 16
      )

    if (!is.null(output_file)) {
      gt::gtsave(gt_table, output_file)
      message("HTML written to: ", output_file)
      return(invisible(gt_table))
    } else {
      return(gt_table)
    }
  }
}
