# function to generate a table with details about executed runs
runs_table <- function(
  db_conn = default_db_conn(),
  format = "gt",
  output_file = NULL,
  include_files = FALSE
) {
  format <- match.arg(format, c("gt", "csv"))

  assertthat::assert_that(
    inherits(db_conn, "DBIObject"),
    msg = "`db_conn` must be a valid database connection"
  )
  on.exit(DBI::dbDisconnect(db_conn), add = TRUE)

  # Query the runs data
  if (include_files) {
    runs_data <- DBI::dbGetQuery(
      db_conn,
      "
      SELECT 
        j.job_id,
        j.path,
        j.cmd,
        j.submitted_at,
        'input' as file_type,
        i.file_path,
        i.file_timestamp,
        i.md5_checksum
      FROM mono_jobs j
      LEFT JOIN input_files i ON j.job_id = i.job_id
      
      UNION ALL
      
      SELECT 
        j.job_id,
        j.path,
        j.cmd,
        j.submitted_at,
        'output' as file_type,
        o.file_path,
        o.file_timestamp,
        o.md5_checksum
      FROM mono_jobs j
      LEFT JOIN output_files o ON j.job_id = o.job_id
      
      ORDER BY j.submitted_at DESC, j.job_id, file_type
      "
    )
  } else {
    runs_data <- DBI::dbGetQuery(
      db_conn,
      "
      SELECT 
        job_id,
        path,
        cmd,
        submitted_at
      FROM mono_jobs 
      ORDER BY submitted_at DESC
      "
    )
  }

  # Return early if no data
  if (nrow(runs_data) == 0) {
    message("No runs found in database")
    return(invisible(NULL))
  }

  # Format the data
  if (include_files) {
    runs_formatted <- runs_data |>
      dplyr::mutate(
        project_name = basename(path),
        submitted_at = as.POSIXct(submitted_at),
        file_timestamp = as.POSIXct(file_timestamp),
        file_name = basename(file_path)
      ) |>
      dplyr::select(
        job_id,
        path,
        project_name,
        file_type,
        file_name,
        file_path,
        file_timestamp,
        md5_checksum,
        cmd,
        submitted_at
      )
  } else {
    runs_formatted <- runs_data |>
      dplyr::mutate(
        project_name = basename(path),
        submitted_at = as.POSIXct(submitted_at)
      ) |>
      dplyr::select(job_id, project_name, path, cmd, submitted_at)
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
          job_id = sprintf("%s '%s' (%d)", cmd, path, job_id),
          path = NULL,
          project_name = NULL,
          cmd = NULL
        ) |>
        gt::gt(groupname_col = "job_id") |>
        gt::tab_header(
          title = "Executed Runs with File Details",
          subtitle = paste("Total runs:", length(unique(runs_formatted$job_id)))
        ) |>
        gt::cols_label(
          file_type = "Type",
          file_name = "File",
          file_path = "Path",
          file_timestamp = "File Modified",
          md5_checksum = "MD5",
          submitted_at = "Submitted"
        ) |>
        gt::fmt_datetime(
          columns = c(submitted_at, file_timestamp),
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
          job_id = "Job ID",
          project_name = "Project",
          path = "Path",
          submitted_at = "Submitted"
        ) |>
        gt::fmt_datetime(
          columns = submitted_at,
          date_style = "yMMMd",
          time_style = "Hm"
        )
    }

    gt_table <- gt_table |>
      gt::tab_style(
        style = gt::cell_text(font = gt::google_font("JetBrains Mono")),
        locations = gt::cells_body(
          columns = contains(c("job_id", "path", "md5"))
        )
      ) |>
      gt::tab_options(
        table.font.size = 12,
        heading.title.font.size = 16
      )

    return(gt_table)
  }
}
