#' Get the uni_lemmas available in Wordbank
#'
#' @inheritParams connect_to_wordbank
#' @return A data frame with the column \code{uni_lemma}.
#'
#' @examples
#' \dontrun{
#' uni_lemmas <- get_crossling_items()
#' }
#' @export
get_crossling_items <- function(db_args = NULL) {

  src <- connect_to_wordbank(db_args)

  uni_lemmas <- get_common_table(src, "uni_lemma") %>%
    dplyr::collect()

  DBI::dbDisconnect(src)

  return(uni_lemmas)
}


#' Get item-by-age summary statistics
#'
#' @param item_data A dataframe as returned by \code{get_item_data()}.
#' @inheritParams connect_to_wordbank
#' @return A dataframe with a row for each combination of item and age, and
#'   columns for summary statistics for the group: number of children
#'   (\code{n_children}), means (\code{comprehension}, \code{production}),
#'   standard deviations (\code{comprehension_sd}, \code{production_sd}); also
#'   retains item-level variables from \code{lang_items} (\code{item_id},
#'   \code{item_definition}, \code{uni_lemma}, \code{lexical_category}).
#'
#' @examples
#' \dontrun{
#' italian_dog <- get_item_data(language = "Italian", form = "WG") %>%
#'   dplyr::filter(uni_lemma == "dog")
#' italian_dog_summary <- summarise_items(italian_dog)
#' }
#' @export
summarise_items <- function(item_data, db_args = NULL) {
  message(sprintf("Getting data for %s...", unique(item_data$language)))

  get_instrument_data(language = unique(item_data$language),
                      form = unique(item_data$form),
                      items = item_data$item_id,
                      administration_info = TRUE,
                      item_info = item_data,
                      db_args = db_args) %>%
    dplyr::group_by(.data$language, .data$form, .data$item_id,
                    .data$item_definition, .data$uni_lemma,
                    .data$lexical_category, .data$age) %>%
    dplyr::summarise(
      n_children = dplyr::n(),
      comprehension = sum(.data$understands) / .data$n_children,
      production = sum(.data$produces) / .data$n_children,
      comprehension_sd = stats::sd(.data$understands) / .data$n_children,
      production_sd = stats::sd(.data$produces) / .data$n_children
    ) %>%
    dplyr::ungroup()

}


#' Get item-by-age summary statistics for items across languages
#'
#' @param uni_lemmas A character vector of uni_lemmas.
#' @inheritParams connect_to_wordbank
#' @return A dataframe with a row for each combination of language, item, and
#'   age, and columns for summary statistics for the group: number of children
#'   (\code{n_children}), means (\code{comprehension}, \code{production}),
#'   standard deviations (\code{comprehension_sd}, \code{production_sd}); and
#'   item-level variables (\code{item_id}, \code{definition}, \code{uni_lemma},
#'   \code{lexical_category}, \code{lexical_class}).

#' @examples
#' \dontrun{
#' crossling_data <- get_crossling_data(uni_lemmas = c("hat", "nose"))
#' }
#' @export
get_crossling_data <- function(uni_lemmas, db_args = NULL) {

  src <- connect_to_wordbank(db_args)

  item_data <- get_item_data(db_args = db_args) %>%
    dplyr::filter(.data$uni_lemma %in% uni_lemmas) %>%
    dplyr::select(.data$language, .data$form, .data$form_type, .data$item_id,
                  .data$item_definition, .data$uni_lemma,
                  .data$lexical_category) %>%
    dplyr::mutate(lang = language, frm = form) %>%
    tidyr::nest(data = -c(lang, frm)) %>%
    dplyr::transmute(summary = data %>%
                       purrr::map(~summarise_items(.x, db_args = db_args))) %>%
    tidyr::unnest(summary)

  DBI::dbDisconnect(src)

  return(item_data)

}
