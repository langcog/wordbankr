#' @keywords internal
"_PACKAGE"

## usethis namespace: start
#' @importFrom dplyr %>%
#' @importFrom rlang .data :=
#' @importFrom glue glue
## usethis namespace: end
NULL

# enable rlang::on_load
.onLoad <- function(lib, pkg) {
  rlang::run_on_load()
}

# config file path
cnf_path <- NULL
# on package load, call config setup and update config file path
rlang::on_load({
  cnf_path <- setup_cnf()
})

setup_cnf <- function() {
  # find path of config file
  cnf_path <- system.file("wordbankr.cnf", package = "wordbankr")
  # find path of pem file
  pem_path <- system.file("global-bundle.pem", package = "wordbankr")
  # write config file pointing to path of pem file
  writeLines(c("[client]", paste0("ssl-ca=", pem_path)), con = cnf_path)
  return(cnf_path)
}
