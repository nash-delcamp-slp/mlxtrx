# helper function to calculate SHA-256 checksum
calculate_sha256 <- function(file_path) {
  if (!file.exists(file_path)) {
    return(NA_character_)
  }
  digest::digest(file_path, algo = "sha256", file = TRUE)
}

# helper function to get file timestamp
get_file_timestamp <- function(file_path) {
  if (!file.exists(file_path)) {
    return(NA)
  }
  file.info(file_path)$mtime
}
