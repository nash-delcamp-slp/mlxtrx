#' Base file handler class
#' @keywords internal
FileHandler <- R6::R6Class(
  "FileHandler",
  public = list(
    #' @description Parse project file and extract metadata
    #' @param path Path to project file
    parse_file = function(path) {
      stop("parse_file must be implemented by subclass")
    },

    #' @description Get input files from parsed content
    #' @param parsed_content Result from parse_file()
    #' @param project_dir Directory containing the project file
    get_input_files = function(parsed_content, project_dir) {
      stop("get_input_files must be implemented by subclass")
    },

    #' @description Get expected output directory
    #' @param path Path to project file
    #' @param output_dir User-specified output directory (may be NULL)
    get_output_dir = function(path, output_dir = NULL) {
      stop("get_output_dir must be implemented by subclass")
    },

    #' @description Check if job monitoring is supported
    can_monitor = function() {
      TRUE
    },

    #' @description Parse summary file
    #' @param summary_file_path Path to summary.txt file
    parse_summary = function(summary_file_path) {
      NULL
    },

    #' @description Get path to summary file
    #' @param project_path Path to project file
    #' @param output_dir Optional output directory override
    get_summary_file_path = function(project_path, output_dir = NULL) {
      output_dir_resolved <- self$get_output_dir(project_path, output_dir)
      file.path(output_dir_resolved, "summary.txt")
    }
  )
)

#' Monolix file handler
#' @keywords internal
MonolixHandler <- R6::R6Class(
  "MonolixHandler",
  inherit = FileHandler,
  public = list(
    #' @description Parse project file and extract metadata
    #' @param path Path to project file
    parse_file = function(path) {
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
    },
    #' @description Get input files from parsed content
    #' @param parsed_content Result from parse_file()
    #' @param project_dir Directory containing the project file
    get_input_files = function(parsed_content, project_dir) {
      files <- list()

      # Data file
      data_file <- parsed_content$DATAFILE$FILEINFO$file
      if (!is.null(data_file)) {
        files$data_file <- resolve_file_path(data_file, project_dir)
      }

      # Model file
      model_file <- parsed_content$MODEL$LONGITUDINAL$file
      if (!is.null(model_file)) {
        files$model_file <- resolve_file_path(model_file, project_dir)
      }

      files
    },
    #' @description Get expected output directory
    #' @param path Path to project file
    #' @param output_dir User-specified output directory (may be NULL)
    get_output_dir = function(path, output_dir = NULL) {
      path_content <- self$parse_file(path)
      path_based_output_dir <- file.path(
        dirname(path),
        path_content$MONOLIX$SETTINGS$GLOBAL$exportpath
      )
      current_output_dir <- output_dir %||%
        path_based_output_dir
      resolve_output_dir(path = path, output_dir = current_output_dir)
    },

    #' @description Parse Monolix summary file
    #' @param summary_file_path Path to summary.txt file
    parse_summary = function(summary_file_path) {
      if (!file.exists(summary_file_path)) {
        return(NULL)
      }

      tryCatch(
        {
          summary_lines <- readLines(summary_file_path)

          # Initialize summary data structure
          summary_data <- list(
            summary_stats = list(
              exploratory_iterations = NA_integer_,
              smoothing_iterations = NA_integer_,
              fisher_matrix_estimation = FALSE,
              eigenvalue_min = NA_real_,
              eigenvalue_max = NA_real_,
              ofv = NA_real_,
              aic = NA_real_,
              bicc = NA_real_,
              bic = NA_real_,
              n_individuals = NA_integer_,
              n_doses = NA_integer_
            ),

            # Flexible observations data
            observations = list()
          )

          # Parse iterations
          exp_match <- stringr::str_extract(
            summary_lines,
            "(Exploratory phase iterations\\s*:\\s*)(\\d*)",
            group = 2
          )
          summary_data$summary_stats$exploratory_iterations <- as.integer(exp_match[
            !is.na(exp_match)
          ][1])

          smooth_match <- stringr::str_extract(
            summary_lines,
            "(Smoothing phase iterations\\s*:\\s*)(\\d+)",
            group = 2
          )
          summary_data$summary_stats$smoothing_iterations <- as.integer(smooth_match[
            !is.na(smooth_match)
          ][1])

          # Check for Fisher matrix estimation
          fisher_line <- any(stringr::str_detect(
            summary_lines,
            "ESTIMATION OF THE FISHER INFORMATION MATRIX"
          ))
          summary_data$summary_stats$fisher_matrix_estimation <- fisher_line

          # Parse eigenvalues if Fisher matrix exists
          if (fisher_line) {
            eigen_lines <- summary_lines[stringr::str_detect(
              summary_lines,
              "Eigen values"
            )]
            if (length(eigen_lines) > 0) {
              eigen_numbers <- stringr::str_extract_all(
                eigen_lines[1],
                "\\d+\\.?\\d*"
              )[[1]]
              if (length(eigen_numbers) >= 2) {
                summary_data$summary_stats$eigenvalue_min <- as.numeric(eigen_numbers[
                  1
                ])
                summary_data$summary_stats$eigenvalue_max <- as.numeric(eigen_numbers[
                  2
                ])
              }
            }
          }

          # Parse statistical criteria
          ofv_match <- stringr::str_extract(
            summary_lines,
            "(-2 x log-likelihood\\s*\\(OFV\\)\\s*:\\s*)([-+]?\\d*\\.?\\d+)",
            group = 2
          )
          summary_data$summary_stats$ofv <- as.numeric(ofv_match[
            !is.na(ofv_match)
          ][1])

          aic_match <- stringr::str_extract(
            summary_lines,
            "(Akaike Information Criteria\\s*\\(AIC\\)\\s*:\\s*)([-+]?\\d*\\.?\\d+)",
            group = 2
          )
          summary_data$summary_stats$aic <- as.numeric(aic_match[
            !is.na(aic_match)
          ][1])

          bicc_match <- stringr::str_extract(
            summary_lines,
            "(Corrected Bayesian Information Criteria\\s*\\(BICc\\)\\s*:\\s*)([-+]?\\d*\\.?\\d+)",
            group = 2
          )
          summary_data$summary_stats$bicc <- as.numeric(bicc_match[
            !is.na(bicc_match)
          ][1])

          bic_match <- stringr::str_extract(
            summary_lines,
            "(Bayesian Information Criteria\\s*\\(BIC\\)\\s*:\\s*)([-+]?\\d*\\.?\\d+)",
            group = 2
          )
          summary_data$summary_stats$bic <- as.numeric(bic_match[
            !is.na(bic_match)
          ][1])

          # Parse dataset information
          indiv_match <- stringr::str_extract(
            summary_lines,
            "(Number of individuals\\s*:\\s*)(\\d+)",
            group = 2
          )
          summary_data$summary_stats$n_individuals <- as.integer(indiv_match[
            !is.na(indiv_match)
          ][1])

          doses_match <- stringr::str_extract(
            summary_lines,
            "(Number of doses\\s*:\\s*)(\\d+)",
            group = 2
          )
          summary_data$summary_stats$n_doses <- as.integer(doses_match[
            !is.na(doses_match)
          ][1])

          # Parse observations
          obs_lines <- summary_lines[stringr::str_detect(
            summary_lines,
            "Number of observations"
          )]

          for (obs_line in obs_lines) {
            obs_match <- stringr::str_match(
              obs_line,
              "Number of observations\\s*\\((.+?)\\):\\s*(\\d+)"
            )
            if (!is.na(obs_match[1])) {
              obs_type <- stringr::str_trim(obs_match[2])
              obs_count <- as.integer(obs_match[3])
              summary_data$observations[[obs_type]] <- obs_count
            }
          }

          return(summary_data)
        },
        error = function(e) {
          warning(
            "Failed to parse summary file ",
            summary_file_path,
            ": ",
            e$message
          )
          return(NULL)
        }
      )
    }
  )
)

#' Simulx file handler
#' @keywords internal
SimulxHandler <- R6::R6Class(
  "SimulxHandler",
  inherit = FileHandler,
  public = list(
    #' @description Parse project file and extract metadata
    #' @param path Path to project file
    parse_file = function(path) {
      monolix_suite_path <- getOption(
        "mlxtrx.monolix_suite",
        "/srv/Lixoft/MonolixSuite2024R1/"
      )
      if (!dir.exists(monolix_suite_path)) {
        warning(
          "Monolix Suite installation not detected in the `mlxtrx.monolix_suite` option.",
          call. = FALSE
        )
        return(list())
      }
      suppressMessages({
        lixoftConnectors::initializeLixoftConnectors(
          "simulx",
          path = monolix_suite_path,
          force = TRUE
        )
      })
      loaded <- lixoftConnectors::loadProject(path)
      if (loaded) {
        # for the sake of matching parsed mlxtran output, return a similar structure.
        result <- list(
          MODEL = list(
            LONGITUDINAL = list(
              file = lixoftConnectors::getStructuralModel()
            )
          )
        )
        # initialize to another software to unload the project.
        suppressMessages({
          lixoftConnectors::initializeLixoftConnectors(
            "monolix",
            path = monolix_suite_path,
            force = TRUE
          )
          lixoftConnectors::initializeLixoftConnectors(
            "simulx",
            path = monolix_suite_path,
            force = TRUE
          )
        })
      } else {
        return(list())
        warning(
          "Could not load project for input extraction: ",
          path,
          call. = FALSE
        )
      }
      result
    },
    #' @description Get input files from parsed content
    #' @param parsed_content Result from parse_file()
    #' @param project_dir Directory containing the project file
    get_input_files = function(parsed_content, project_dir) {
      # Simulx has different input file structure
      files <- list()

      # Model file
      model_file <- parsed_content$MODEL$LONGITUDINAL$file
      if (!is.null(model_file)) {
        files$model_file <- resolve_file_path(model_file, project_dir)
      }

      files
    },
    #' @description Get expected output directory
    #' @param path Path to project file
    #' @param output_dir User-specified output directory (may be NULL)
    get_output_dir = function(path, output_dir = NULL) {
      path_based_output_dir <- file.path(
        dirname(path),
        fs::path_ext_remove(basename(path))
      )
      current_output_dir <- output_dir %||%
        path_based_output_dir
      resolve_output_dir(path = path, output_dir = current_output_dir)
    }
  )
)

# Helper function to resolve file paths
resolve_file_path <- function(file_path, project_dir) {
  if (fs::is_absolute_path(file_path) || file.exists(file_path)) {
    return(normalizePath(file_path, mustWork = FALSE))
  }

  candidate_path <- file.path(project_dir, file_path)
  if (file.exists(candidate_path)) {
    return(normalizePath(candidate_path))
  }

  file_path # Return original if not found
}

#' Get appropriate file handler for a given file extension
#' @param ext File extension (without dot)
#' @keywords internal
get_file_handler <- function(ext) {
  switch(
    tolower(ext),
    "mlxtran" = MonolixHandler$new(),
    "smlx" = SimulxHandler$new(),
    {
      # Default handler for unknown types
      message(
        "Using generic handler for .",
        ext,
        " files. Limited functionality available."
      )
      GenericHandler$new()
    }
  )
}

#' Generic file handler for unknown file types
#' @keywords internal
GenericHandler <- R6::R6Class(
  "GenericHandler",
  inherit = FileHandler,
  public = list(
    #' @description Parse project file and extract metadata
    #' @param path Path to project file
    parse_file = function(path) {
      list()
    },
    #' @description Get input files from parsed content
    #' @param parsed_content Result from parse_file()
    #' @param project_dir Directory containing the project file
    get_input_files = function(parsed_content, project_dir) {
      list()
    },
    #' @description Get expected output directory
    #' @param path Path to project file
    #' @param output_dir User-specified output directory (may be NULL)
    get_output_dir = function(path, output_dir = NULL) {
      if (!is.null(output_dir)) {
        return(file.path(dirname(path), output_dir))
      }
      file.path(dirname(path), fs::path_ext_remove(basename(path)))
    },
    #' @description Check if job monitoring is supported
    can_monitor = function() {
      FALSE
    }
  )
)

#' Resolve output directory for a project file
#'
#' @param path Character scalar. Path to project file.
#' @param output_dir Character scalar or NULL. Override output directory.
#'
#' @return Character scalar. Resolved output directory path.
#'
#' @keywords internal
resolve_output_dir <- function(path, output_dir = NULL) {
  if (!is.null(output_dir)) {
    if (
      !fs::is_absolute_path(output_dir) &&
        !file.exists(output_dir)
    ) {
      output_dir_built <- file.path(
        dirname(path),
        output_dir
      )
      if (file.exists(output_dir_built)) {
        output_dir <- output_dir_built
      }
    }
  } else {
    output_dir_built <- file.path(
      dirname(path),
      fs::path_ext_remove(basename(path))
    )
    if (file.exists(output_dir_built)) {
      output_dir <- output_dir_built
    }
  }
  output_dir
}
