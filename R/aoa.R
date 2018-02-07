#' Fit age of acquisition estimates for Wordbank data
#'
#' For each item in the input data, estimate its age of acquisition as the
#' earliest age (in months) at which the proportion of children who
#' understand/produce the item is greater than some threshold. The proportions
#' used can be empirical or first smoothed by a model.
#'
#' @param instrument_data A data frame returned by \code{get_instrument_data},
#'   which must have an "age" column and a "num_item_id" column.
#' @param measure One of "produces" or "understands" (defaults to "produces").
#' @param method A string indicating which smoothing method to use:
#'   \code{empirical} to use empirical proportions,  \code{glm} to fit a
#'   logistic linear model, \code{glmrob} a robust logistic linear model
#'   (defaults to \code{glm}).
#' @param proportion A number between 0 and 1 indicating threshold proportion of
#'   children.
#'
#' @return A data frame where every row is an item, the item-level columns from
#'   the input data are preserved, and the \code{aoa} column contains the age of
#'   acquisition estimates.
#'
#' @examples
#' \dontrun{
#' eng_ws_data <- get_instrument_data(language = "English (American)",
#'                                    form = "WS",
#'                                    items = c("item_1", "item_42"),
#'                                    administrations = TRUE)
#' eng_ws_aoa <- fit_aoa(eng_ws_data)
#' }
#' @export
fit_aoa <- function(instrument_data, measure = "produces", method = "glm",
                    proportion = 0.5) {

  assertthat::assert_that(is.element("age", colnames(instrument_data)))
  assertthat::assert_that(is.element("num_item_id", colnames(instrument_data)))

  instrument_summary <- instrument_data %>%
    dplyr::filter(!is.na(.data$age)) %>%
    dplyr::mutate(
      produces = !is.na(.data$value) & .data$value == "produces",
      understands = !is.na(.data$value) &
        (.data$value == "understands" | .data$value == "produces")
    ) %>%
    dplyr::select(-.data$value) %>%
    tidyr::gather("measure_name", "value",
                  .data$produces, .data$understands) %>%
    dplyr::filter(.data$measure_name == measure) %>%
    dplyr::group_by(.data$age, .data$num_item_id) %>%
    dplyr::summarise(num_true = sum(.data$value),
                     num_false = n() - .data$num_true)

  inv_logit <- function(x) 1 / (exp(-x) + 1)
  ages <- dplyr::data_frame(
    age = min(instrument_summary$age):max(instrument_summary$age)
  )

  fit_methods <- list(
    "empirical" = function(item_data) {
      item_data %>% dplyr::mutate(
        prop = .data$num_true / (.data$num_true + .data$num_false)
      )
    },
    "glm" = function(item_data) {
      model <- stats::glm(cbind(num_true, num_false) ~ age, item_data,
                          family = "binomial")
      ages %>% dplyr::mutate(prop = inv_logit(stats::predict(model, ages)))
    },
    "glmrob" = function(item_data) {
      model <- robustbase::glmrob(cbind(num_true, num_false) ~ age, item_data,
                                  family = "binomial")
      ages %>% dplyr::mutate(prop = inv_logit(stats::predict(model, ages)))
    },
    "bayes" = function(item_data) {
      # TODO
    }
  )

  compute_aoa <- function(fit_data) {
    acq <- fit_data %>% dplyr::filter(.data$prop > proportion)
    if (nrow(acq)) min(acq$age) else NA
  }

  instrument_aoa <- instrument_summary %>%
    dplyr::group_by(.data$num_item_id) %>%
    tidyr::nest() %>%
    dplyr::mutate(fit_data = .data$data %>%
                    purrr::map(fit_methods[[method]])) %>%
    dplyr::mutate(aoa = .data$fit_data %>% purrr::map_dbl(compute_aoa)) %>%
    dplyr::select(-.data$data, -.data$fit_data)


  item_cols <- c("num_item_id", "item_id", "definition", "type", "category",
                 "lexical_category", "lexical_class", "uni_lemma",
                 "complexity_category") %>%
    purrr::keep(~.x %in% colnames(instrument_data))

  item_data <- instrument_data %>%
    dplyr::select(!!!item_cols) %>%
    dplyr::distinct()

  instrument_aoa %>% dplyr::left_join(item_data, by = "num_item_id")

}
