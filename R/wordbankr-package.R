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
