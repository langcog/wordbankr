#' Fit quantiles to vocabulary sizes using quantile regression
#'
#' @param vocab_data A data frame returned by \code{get_administration_data}.
#' @param measure A column of \code{vocab_data} with vocabulary values
#'   (\code{production} or \code{comprehension}).
#' @param group (Optional) A column of \code{vocab_data} to group by.
#' @param quantiles Either one of "standard" (default), "deciles", "quintiles",
#'   "quartiles", "median", or a numeric vector of quantile values.
#'
#' @importFrom quantregGrowth ps
#'
#' @return A data frame with the columns "language", "form", "age", \code{group}
#'   (if specified), "quantile", and \code{measure}, where \code{measure} is the
#'   fit vocabulary value for that quantile at that age.
#' @export
#'
#' @examples
#' \donttest{
#' eng_wg <- get_administration_data(language = "English (American)",
#'                                   form = "WG",
#'                                   include_demographic_info = TRUE)
#' if (!is.null(eng_wg)) {
#'   vocab_quantiles <- fit_vocab_quantiles(eng_wg, production)
#'   vocab_quantiles_sex <- fit_vocab_quantiles(eng_wg, production, sex)
#'   vocab_quartiles <- fit_vocab_quantiles(eng_wg, production, quantiles = "quartiles")
#' }
#' }
fit_vocab_quantiles <- function(vocab_data, measure, group,
                                quantiles = "standard") {

  lifecycle::deprecate_warn(
    when = "1.0.0", what = "fit_vocab_quantiles()",
    details = "Please use the vocabulary norms shiny app at http://wordbank.stanford.edu/analyses?name=vocab_norms")

  quantile_opts <- list(
    standard = c(0.10, 0.25, 0.50, 0.75, 0.90),
    deciles = c(0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90),
    quintiles = c(0.20, 0.40, 0.60, 0.80),
    quartiles = c(0.25, 0.50, 0.75),
    median = c(0.5)
  )
  if (is.numeric(quantiles)) {
    if (any(quantiles >= 1, quantiles <= 0))
      stop("Numeric quantiles must be between 0 and 1")
    num_quantiles <- quantiles
  } else if (is.character(quantiles) & length(quantiles) == 1) {
    if (!(quantiles %in% names(quantile_opts)))
      stop("Character quantiles must be one of ",
           paste(names(quantile_opts), collapse = ", "))
    num_quantiles <- quantile_opts[[quantiles]]
  } else {
    stop("Quantiles must be a numeric vector or a character vector of length 1")
  }

  vocab_data <- vocab_data %>% dplyr::group_by(.data$language, .data$form)

  if (!missing(group)) {
    vocab_data <- vocab_data %>%
      dplyr::filter((dplyr::if_any({{ group }}, ~!is.na(.x)))) %>%
      dplyr::group_by({{ group }}, .add = TRUE)
  }

  vocab_models <- vocab_data %>%
    dplyr::rename(vocab = {{ measure }}) %>%
    tidyr::nest() %>%
    dplyr::mutate(group_label = paste(.data$language, .data$form, {{ group }})) %>%
    dplyr::mutate(model = purrr::map2(
      .data$group_label, .data$data,
      function(gl, df) {
        tryCatch(
          suppressWarnings(
            quantregGrowth::gcrq(vocab ~ ps(age, monotone = 1, lambda = 1000),
                                 tau = num_quantiles, data = df)
          ),
          error = function(e) {
            message(glue("Unable to fit model for {gl}"))
            return(NULL)
          })
      })) %>%
    dplyr::select(-"group_label") %>%
    dplyr::filter(purrr::map_lgl(.data$model, ~!is.null(.))) %>%
    dplyr::ungroup()
  if (nrow(vocab_models) == 0) return(NULL)

  ages <- data.frame(age = min(vocab_data$age):max(vocab_data$age))
  get_predicted <- function(vocab_model) {
    vocab_fits <- stats::predict(vocab_model, newdata = ages)
    if (length(vocab_model$taus) == 1)
      vocab_fits <- rlang::set_names(list(vocab_fits), vocab_model$taus)
    vocab_fits %>%
      dplyr::as_tibble() %>%
      dplyr::mutate(age = ages$age) %>%
      tidyr::gather("quantile", "predicted", -.data$age)
  }

  vocab_fits <- vocab_models %>%
    dplyr::mutate(predicted = purrr::map(.data$model, get_predicted)) %>%
    dplyr::select(-"data", -"model") %>%
    tidyr::unnest(cols = "predicted") %>%
    dplyr::rename("{{measure}}" := .data$predicted) %>%
    dplyr::mutate(quantile = factor(.data$quantile))

  return(vocab_fits)

}
