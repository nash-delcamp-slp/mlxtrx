#' One-line source footnote for a table, figure or listing
#'
#' Names the runs, and their project files, that produced the plot or table
#' data files behind a table, figure or listing (TFL), so the TFL can be
#' traced back to its run IDs. Intended as a footnote. Each run appears once,
#' in the order its first file appears in `files`. Project paths and the paths
#' of files no run recorded are relative to the folder that holds the store
#' where possible. A file no run recorded is listed as
#' `<file> (no run record)`. An empty `files` is an error.
#'
#' A warning lists every file whose current state is not `"unchanged"` or
#' `"unchanged_late"` (see [check_output_files()]), with its status, so a
#' footnote is not trusted for files that were edited, deleted or never
#' recorded.
#'
#' @param files Character vector. Paths to the plot or table data files the
#'   TFL was made from.
#' @param store Character scalar. The run store directory (see [default_store()]).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return A character scalar such as
#'   `"Source: theoph/theoph.mlxtran (run 20260101T120000Z-ab12)"`.
#'
#' @examples
#' \dontrun{
#' note <- source_note(c("theoph/theoph/summary.txt"))
#' ggplot2::ggplot(df, ggplot2::aes(time, conc)) +
#'   ggplot2::geom_line() +
#'   ggplot2::labs(caption = note)
#' }
#'
#' @export
source_note <- function(files, store = default_store()) {
  files <- as.character(files)
  if (length(files) == 0L) {
    stop("`files` is empty", call. = FALSE)
  }
  checked <- check_output_files(files, store)
  root <- path_key(fs::path_abs(store_root_dir(store)))

  no_record <- is.na(checked$run_id)
  entries <- ifelse(
    no_record,
    paste0(rel_to(path_key(files), root), " (no run record)"),
    paste0(
      rel_to(path_key(checked$project_path), root),
      " (run ", checked$run_id, ")"
    )
  )

  changed <- !checked$status %in% c("unchanged", "unchanged_late")
  if (any(changed)) {
    warning(
      "Some source files are not as recorded: ",
      paste0(files[changed], " (", checked$status[changed], ")", collapse = ", "),
      call. = FALSE
    )
  }

  paste0("Source: ", paste(unique(entries), collapse = "; "))
}
