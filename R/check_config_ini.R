#' Check Monolix/Simulx config file
#'
#' Checks if the relevant config.ini file contains required export settings
#' for charts data.
#' In interactive sessions, offers to fix missing settings.
#'
#' @param cmd Monolix or Simulx command being executed. Defaults to `"mono24"`
#' @param offer_fix Whether to offer interactive fixes. Defaults to `interactive()`
#'
#' @return Returns TRUE if all settings are present
#' @export
#'
#' @examples
#' \dontrun{
#' check_config_ini()
#' }
check_config_ini <- function(
  cmd = getOption("mlxtrx.monolix_cmd", "mono24"),
  offer_fix = interactive()
) {
  required_settings <- list(
    "exportChartsData" = "true",
    "export-charts-datasets" = "true",
    "exportVPCSimulations" = "true"
  )

  cmd_ext <- fs::path_ext(cmd)

  if (cmd_ext == "") {
    version_dir <- switch(
      basename(cmd),
      mono24 = "monolix/monolix2024R1",
      sim24 = "simulx/simulx2024R1",
      mono23 = "monolix/monolix2023R1",
      sim23 = "simulx/simulx2023R1",
      mono21 = "monolix/monolix2021R1",
      sim21 = "simulx/simulx2021R1",
      mono20 = "monolix/monolix2020R1",
      sim20 = "simulx/simulx2020R1",
      ""
    )
  } else if (basename(cmd) == "monolix.sh") {
    version_dir <- file.path(
      "monolix",
      stringr::str_replace(
        cmd,
        ".*M(onolix)Suite(20\\d\\dR\\d)/.*",
        "m\\1\\2"
      )
    )
  } else if (basename(cmd) == "simulx.sh") {
    version_dir <- file.path(
      "simulx",
      stringr::str_replace(
        cmd,
        ".*MonolixSuite(20\\d\\dR\\d)/.*",
        "simulx\\1"
      )
    )
  }

  ini_path <- file.path(
    "~/lixoft",
    version_dir,
    "config/config.ini"
  )

  ini_path <- normalizePath(ini_path, mustWork = FALSE)

  if (!file.exists(ini_path)) {
    stop("config.ini file not found for cmd: ", cmd, call. = FALSE)
  }

  # Parse the ini file to extract sections and settings
  parsed_content <- parse_ini_file(ini_path)

  # Check if [misc] section exists
  if (!"misc" %in% names(parsed_content)) {
    stop("Missing [misc] section in config.ini", call. = FALSE)
  }

  misc_section <- parsed_content$misc

  # Check which settings are missing or incorrect
  missing_settings <- character(0)
  incorrect_settings <- character(0)

  for (setting_name in names(required_settings)) {
    required_value <- required_settings[[setting_name]]

    if (!setting_name %in% names(misc_section)) {
      missing_settings <- c(
        missing_settings,
        paste0(setting_name, "=", required_value)
      )
    } else if (misc_section[[setting_name]] != required_value) {
      incorrect_settings <- c(
        incorrect_settings,
        paste0(setting_name, "=", required_value)
      )
    }
  }

  if (length(missing_settings) == 0 && length(incorrect_settings) == 0) {
    return(TRUE)
  }

  # Create error message for missing/incorrect settings
  error_parts <- character(0)
  if (length(missing_settings) > 0) {
    error_parts <- c(
      error_parts,
      paste0(
        "Missing required config settings:\n",
        paste0("  - ", missing_settings, collapse = "\n")
      )
    )
  }
  if (length(incorrect_settings) > 0) {
    error_parts <- c(
      error_parts,
      paste0(
        "Incorrect config settings:\n",
        paste0("  - ", incorrect_settings, collapse = "\n")
      )
    )
  }

  error_msg <- paste(error_parts, collapse = "\n\n")

  if (!offer_fix) {
    stop(error_msg, call. = FALSE)
  }

  # offer to fix
  warning(error_msg, "\n\n", sep = "", immediate. = TRUE, call. = FALSE)

  # Create modified version
  ini_lines <- readLines(ini_path, warn = FALSE)
  modified_lines <- fix_ini_settings(
    ini_lines,
    missing_settings,
    incorrect_settings
  )

  # Write to temp file
  temp_file <- file.path(tempdir(), "config_modified.ini")
  writeLines(modified_lines, temp_file)

  # Show diff
  cat("Proposed changes:\n")
  show_ini_diff(ini_lines, modified_lines, missing_settings, incorrect_settings)

  # Ask user if they want to replace
  response <- readline(
    "Replace existing config.ini file with the modified version? (y/N): "
  )

  if (tolower(trimws(response)) %in% c("y", "yes")) {
    # Replace original
    file.copy(temp_file, ini_path, overwrite = TRUE)
    cat("config.ini file updated.\n")

    return(TRUE)
  } else {
    cat("File not modified. You can manually copy from:", temp_file, "\n")
    stop(error_msg, call. = FALSE)
  }
}

#' Parse ini file into structured list
#' @param ini_path Path to ini file
#' @return Named list with sections as top-level elements
#' @keywords internal
parse_ini_file <- function(ini_path) {
  lines <- readLines(ini_path, warn = FALSE)
  lines <- trimws(lines)
  lines <- lines[lines != ""] # Remove empty lines

  result <- list()
  current_section <- NULL

  for (line in lines) {
    # Check for section headers [SECTION]
    if (grepl("^\\[.*\\]$", line)) {
      section_name <- gsub("^\\[|\\]$", "", line)
      current_section <- section_name
      result[[section_name]] <- list()
    } else if (grepl("=", line) && !is.null(current_section)) {
      # Key-value pair within a section
      parts <- stringr::str_split(line, "\\s*=\\s*", 2)[[1]]
      if (length(parts) == 2) {
        key <- trimws(parts[1])
        value <- trimws(parts[2])
        result[[current_section]][[key]] <- value
      }
    }
  }

  return(result)
}

#' Fix INI file settings by adding missing and replacing incorrect ones
#' @param ini_lines Original INI file lines
#' @param missing_settings Vector of missing settings to add
#' @param incorrect_settings Vector of settings to replace
#' @return Modified INI file lines
#' @keywords internal
fix_ini_settings <- function(ini_lines, missing_settings, incorrect_settings) {
  modified_lines <- ini_lines

  # Find the [misc] section boundaries
  misc_start <- which(grepl("^\\s*\\[misc\\]\\s*$", modified_lines))
  if (length(misc_start) == 0) {
    stop("Could not find [misc] section in config.ini", call. = FALSE)
  }
  misc_start <- misc_start[1]

  # Find the end of [misc] section (next section or end of file)
  next_section <- which(grepl("^\\s*\\[.*\\]\\s*$", modified_lines))
  next_section <- next_section[next_section > misc_start]
  misc_end <- if (length(next_section) > 0) {
    next_section[1] - 1
  } else {
    length(modified_lines)
  }

  # Replace incorrect settings within [misc] section
  for (setting in incorrect_settings) {
    setting_parts <- stringr::str_split(setting, "=", 2)[[1]]
    setting_key <- setting_parts[1]

    # Find the line with this setting in [misc] section
    misc_lines <- (misc_start + 1):misc_end
    setting_line_idx <- which(grepl(
      paste0("^\\s*", setting_key, "\\s*="),
      modified_lines[misc_lines]
    ))

    if (length(setting_line_idx) > 0) {
      actual_line_idx <- misc_lines[setting_line_idx[1]]
      modified_lines[actual_line_idx] <- setting
    }
  }

  # Add missing settings to the end of [misc] section
  if (length(missing_settings) > 0) {
    # Insert missing settings before the next section (or at the end)
    insert_pos <- misc_end + 1

    # Add missing settings
    new_lines <- c(
      modified_lines[1:(insert_pos - 1)],
      missing_settings,
      modified_lines[insert_pos:length(modified_lines)]
    )

    modified_lines <- new_lines
  }

  return(modified_lines)
}

#' Show differences between original and modified INI files
#' @param original_lines Original INI lines
#' @param modified_lines Modified INI lines
#' @param missing_settings Missing settings that were added
#' @param incorrect_settings Incorrect settings that were replaced
#' @keywords internal
show_ini_diff <- function(
  original_lines,
  modified_lines,
  missing_settings,
  incorrect_settings
) {
  cat("Original file has", length(original_lines), "lines\n")
  cat("Proposed file has", length(modified_lines), "lines\n\n")

  if (length(missing_settings) > 0) {
    cat("Added lines to [misc] section:\n")
    for (setting in missing_settings) {
      cat("+", setting, "\n")
    }
    if (length(incorrect_settings) > 0) cat("\n")
  }

  if (length(incorrect_settings) > 0) {
    cat("Replaced lines in [misc] section:\n")
    for (setting in incorrect_settings) {
      setting_key <- stringr::str_split(setting, "=", 2)[[1]][1]
      # Find original value to show what's being replaced
      misc_start <- which(grepl("^\\s*\\[misc\\]\\s*$", original_lines))[1]
      next_section <- which(grepl("^\\s*\\[.*\\]\\s*$", original_lines))
      next_section <- next_section[next_section > misc_start]
      misc_end <- if (length(next_section) > 0) {
        next_section[1] - 1
      } else {
        length(original_lines)
      }

      misc_lines <- (misc_start + 1):misc_end
      old_setting_idx <- which(grepl(
        paste0("^\\s*", setting_key, "\\s*="),
        original_lines[misc_lines]
      ))

      if (length(old_setting_idx) > 0) {
        old_line <- original_lines[misc_lines[old_setting_idx[1]]]
        cat("-", trimws(old_line), "\n")
        cat("+", setting, "\n")
      }
    }
  }
}
