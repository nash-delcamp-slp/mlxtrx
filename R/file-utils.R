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
