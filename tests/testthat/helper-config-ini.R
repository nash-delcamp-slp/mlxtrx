# A temporary HOME holding ~/lixoft/<version_dir>/config/config.ini with the
# given [misc] lines. `~` expands from HOME on Linux and macOS only.
local_config_ini <- function(version_dir, misc, .env = parent.frame()) {
  home <- withr::local_tempdir(.local_envir = .env)
  dir <- file.path(home, "lixoft", version_dir, "config")
  dir.create(dir, recursive = TRUE)
  writeLines(c("[misc]", misc), file.path(dir, "config.ini"))
  withr::local_envvar(HOME = home, .local_envir = .env)
  home
}
