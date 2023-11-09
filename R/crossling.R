#' Get the uni_lemmas available in Wordbank
#'
#' @inheritParams connect_to_wordbank
#' @return A data frame with the column \code{uni_lemma}.
#'
#' @examples
#' \donttest{
#' uni_lemmas <- get_crossling_items()
#' }
#' @export
get_crossling_items <- function(db_args = NULL) {

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  uni_lemmas <- get_common_table(src, "uni_lemma") %>% dplyr::collect()

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
#' \donttest{
#' italian_items <- get_item_data(language = "Italian", form = "WG")
#' if (!is.null(italian_items)) {
#'   italian_dog <- dplyr::filter(italian_items, uni_lemma == "dog")
#'   italian_dog_summary <- summarise_items(italian_dog)
#' }
#' }
#' @export
summarise_items <- function(item_data, db_args = NULL) {
  lang <- unique(item_data$language)
  frm <- unique(item_data$form)
  message(glue("Getting data for {lang} {frm}"))

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  instrument_data <- get_instrument_data(language = lang,
                                         form = frm,
                                         items = item_data$item_id,
                                         administration_info = TRUE,
                                         item_info = item_data,
                                         db_args = db_args)
  if (is.null(instrument_data)) return()
  comp <- !all(is.na(instrument_data$understands))
  item_summary <- instrument_data %>%
    dplyr::group_by(.data$language, .data$form, .data$item_id,
                    .data$item_definition, .data$uni_lemma,
                    .data$lexical_category, .data$age) %>%
    dplyr::summarise(
      n_children = dplyr::n(),
      comprehension = if (comp) sum(.data$understands, na.rm = TRUE) / .data$n_children else NA,
      production = sum(.data$produces, na.rm = TRUE) / .data$n_children,
      comprehension_sd = if (comp) stats::sd(.data$understands, na.rm = TRUE) / .data$n_children else NA,
      production_sd = stats::sd(.data$produces, na.rm = TRUE) / .data$n_children
    ) %>%
    dplyr::ungroup()

  suppressWarnings(DBI::dbDisconnect(src))

  return(item_summary)

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
#' \donttest{
#' crossling_data <- get_crossling_data(uni_lemmas = "dog")
#' }
#' @export
get_crossling_data <- function(uni_lemmas, db_args = NULL) {

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  item_data <- get_item_data(db_args = db_args)
  if (is.null(item_data)) return()
  item_data <- item_data %>%
    dplyr::filter(.data$uni_lemma %in% uni_lemmas) %>%
    dplyr::select("language", "form", "form_type", "item_id", "item_kind",
                  "item_definition", "uni_lemma", "lexical_category")
  if (nrow(item_data) == 0) {
    message("No items found for uni_lemma")
    return()
  }

  safe_summarise_items <- purrr::safely(summarise_items, quiet = FALSE,
                                        otherwise = dplyr::tibble())

  item_summary <- item_data %>%
    dplyr::mutate(lang = .data$language, frm = .data$form) %>%
    tidyr::nest(df = -c("lang", "frm")) %>%
    dplyr::transmute(summary = .data$df %>%
                       purrr::map(~safe_summarise_items(., db_args)$result)) %>%
    tidyr::unnest(cols = "summary")

  suppressWarnings(DBI::dbDisconnect(src))
  return(item_summary)

}
