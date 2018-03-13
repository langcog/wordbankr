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
get_crossling_items <- function(mode = "remote") {

  src <- connect_to_wordbank(mode = mode)

  uni_lemmas <- get_common_table(src, "itemmap") %>%
    dplyr::collect()

  DBI::dbDisconnect(src)

  return(uni_lemmas)
}


#' Get item-by-age summary statistics
#'
#' @param lang_items A dataframe as returned by \code{get_item_data()}.
#' @inheritParams connect_to_wordbank
#' @return A dataframe with a row for each combination of item and age, and
#'   columns for summary statistics for the group: number of children
#'   (\code{n_children}), means (\code{comprehension}, \code{production}),
#'   standard deviations (\code{comprehension_sd}, \code{production_sd}); also
#'   retains item-level variables from \code{lang_items} (\code{item_id},
#'   \code{definition}, \code{uni_lemma}, \code{lexical_category},
#'   \code{lexical_class}).
#'
#' @examples
#' \dontrun{
#' italian_dog <- get_item_data(language = "Italian", form = "WG") %>%
#'   dplyr::filter(uni_lemma == "dog")
#' italian_dog_summary <- summarise_items(italian_dog)
#' }
#' @export
summarise_items <- function(lang_items, mode = "remote") {
  message(sprintf("Getting data for %s...", unique(lang_items$language)))

  get_instrument_data(language = unique(lang_items$language),
                      form = unique(lang_items$form),
                      items = lang_items$item_id,
                      administrations = TRUE,
                      iteminfo = lang_items,
                      mode = mode) %>%
    dplyr::mutate(understands = !is.na(.data$value) &
                    .data$value %in% c("understands", "produces"),
                  produces = !is.na(.data$value) &
                    .data$value == "produces") %>%
    dplyr::group_by(.data$language, .data$item_id, .data$definition,
                    .data$uni_lemma, .data$lexical_category,
                    .data$lexical_class, .data$age) %>%
    dplyr::summarise(
      n_children = n(),
      comprehension = sum(.data$understands) / .data$n_children,
      production = sum(.data$produces) / .data$n_children,
      comprehension_sd = stats::sd(.data$understands) / .data$n_children,
      production_sd = stats::sd(.data$produces) / .data$n_children
    )

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
get_crossling_data <- function(uni_lemmas, mode = "remote") {

  src <- connect_to_wordbank(mode = mode)

  item_data <- get_item_data(form = "WG", mode = mode) %>%
    dplyr::filter(.data$uni_lemma %in% uni_lemmas) %>%
    split(.$language) %>%
    purrr::map_df(~summarise_items(.x, mode))

  DBI::dbDisconnect(src)

  return(item_data)

}
