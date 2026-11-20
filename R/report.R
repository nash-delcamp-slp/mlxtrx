#' Print a run status report
#'
#' Prints a plain-text report of [run_status()] for `path`: the path, the time
#' of the report, how many models are in each condition, and, for every model
#' that is not `"clean"`, its condition (with any other conditions in
#' parentheses) and its details. It is a printed view of [run_status()], which
#' it calls once, so the two always agree.
#'
#' Conditions are listed most serious first, as in [run_status()]. Project
#' files are shown relative to `path` when `path` is a directory and they are
#' under it; untracked paths are shown as given.
#'
#' @param path Character vector. Files or directories to report on, as in
#'   [run_status()]. Defaults to `"."`.
#' @param store Character scalar. The run store directory (see [default_store()]).
#'   Defaults to `default_store()`. Nothing is written to it.
#'
#' @return The data frame from [run_status()], invisibly.
#'
#' @seealso [run_status()]
#' @export
print_report_summary <- function(path = ".", store = default_store()) {
  status <- run_status(path, store)
  roots <- status_path_key(path)
  roots <- roots[dir.exists(roots)]

  order_of <- c(
    "never_run", "failed", "unfinished", "inputs_changed", "output_edited",
    "output_suspect", "clean_late", "clean", "untracked"
  )
  cat("=== RUN STATUS REPORT ===\n")
  cat("Path: ", paste(status_path_key(path), collapse = ", "), "\n", sep = "")
  cat("Generated: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z"), "\n\n", sep = "")
  cat("Models: ", nrow(status), "\n", sep = "")
  if (nrow(status) == 0L) {
    cat("No models found.\n")
    return(invisible(status))
  }

  counts <- table(factor(status$condition, levels = order_of))
  counts <- counts[counts > 0L]
  cat(
    paste0("  ", formatC(names(counts), width = -max(nchar(names(counts)))),
           "  ", counts, "\n"),
    sep = ""
  )

  attention <- status[status$condition != "clean", , drop = FALSE]
  if (nrow(attention) == 0L) {
    cat("\nAll models clean.\n")
    return(invisible(status))
  }
  label <- ifelse(is.na(attention$project_file), attention$path, attention$project_file)
  inside <- vapply(label, function(l) any(fs::path_has_parent(l, roots)), logical(1))
  label[inside] <- vapply(label[inside], function(l) {
    root <- roots[fs::path_has_parent(l, roots)][[1]]
    as.character(fs::path_rel(l, root))
  }, character(1))
  ord <- order(match(attention$condition, order_of), label)
  attention <- attention[ord, , drop = FALSE]
  label <- label[ord]

  cat("\nNeeds attention:\n")
  for (i in seq_len(nrow(attention))) {
    other <- attention$other_conditions[[i]]
    cat("  ", label[[i]], "  ", attention$condition[[i]],
        if (!is.na(other)) paste0(" (", other, ")"), "\n", sep = "")
    if (!is.na(attention$details[[i]])) {
      cat("    ", attention$details[[i]], "\n", sep = "")
    }
  }
  invisible(status)
}
