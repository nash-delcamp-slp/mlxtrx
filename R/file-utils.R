# helper function to calculate MD5 checksum
calculate_md5 <- function(file_path) {
  if (!file.exists(file_path)) {
    return(NA_character_)
  }
  digest::digest(file_path, algo = "md5", file = TRUE)
}

# helper function to get file timestamp
get_file_timestamp <- function(file_path) {
  if (!file.exists(file_path)) {
    return(NA)
  }
  file.info(file_path)$mtime
}

# helper function to parse an mlxtran file to extract details in a structured format
parse_mlxtran <- function(path) {
  lines <- readLines(path, warn = FALSE)
  lines <- trimws(lines)
  lines <- lines[lines != ""]

  result <- list()
  current_path <- character(0)

  for (line in lines) {
    # Check for different header levels
    if (grepl("^<.*>$", line)) {
      # Header Level 1: <HEADER>
      header_name <- gsub("^<|>$", "", line)
      current_path <- header_name
      result[[header_name]] <- list()
    } else if (grepl("^\\[.*\\]$", line)) {
      # Header Level 2: [HEADER]
      header_name <- gsub("^\\[|\\]$", "", line)
      current_path <- c(current_path[1], header_name) # Keep level 1, add level 2
      result[[current_path[1]]][[header_name]] <- list()
    } else if (grepl(":$", line)) {
      # Header Level 3: HEADER:
      header_name <- gsub(":$", "", line)
      current_path <- c(current_path[1], current_path[2], header_name)
      result[[current_path[1]]][[current_path[2]]][[header_name]] <- list()
    } else if (grepl("=", line)) {
      # Key-value pair: item = 'value' or item = "value"
      parts <- stringr::str_split(line, "\\s*=\\s*", 2)[[1]]
      if (length(parts) == 2) {
        key <- trimws(parts[1])
        value <- trimws(parts[2])

        # Further parse path when wrapped in {}
        if (stringr::str_detect(value, "\\{path\\s*=")) {
          value <- gsub("^\\{path\\s*=\\s*|\\}$", "", value)
        }

        # Remove quotes from value if present
        value <- gsub("^['\"]|['\"]$", "", value)

        # Add to the appropriate level in the structure
        if (length(current_path) == 1) {
          result[[current_path[1]]][[key]] <- value
        } else if (length(current_path) == 2) {
          result[[current_path[1]]][[current_path[2]]][[key]] <- value
        } else if (length(current_path) == 3) {
          result[[current_path[1]]][[current_path[2]]][[current_path[3]]][[
            key
          ]] <- value
        }
      }
    }
  }

  return(result)
}

#' Resolve output directory for a Monolix project
#'
#' @param path Character scalar. Path to Monolix project file.
#' @param output_dir Character scalar or NULL. Override output directory.
#'
#' @return Character scalar. Resolved output directory path.
#'
#' @keywords internal
resolve_output_dir <- function(path, output_dir = NULL) {
  path_content <- parse_mlxtran(path)

  current_output_dir <- output_dir %||%
    file.path(
      dirname(path),
      path_content$MONOLIX$SETTINGS$GLOBAL$exportpath
    )

  # Resolve relative output directory path
  if (
    !fs::is_absolute_path(current_output_dir) &&
      !file.exists(current_output_dir)
  ) {
    current_output_dir_built <- file.path(
      dirname(path),
      current_output_dir
    )
    if (file.exists(current_output_dir_built)) {
      current_output_dir <- current_output_dir_built
    }
  }

  current_output_dir
}
