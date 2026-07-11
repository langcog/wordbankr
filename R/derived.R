#' Get cached age-of-acquisition estimates
#'
#' Age-of-acquisition estimates for every word item on every instrument,
#' precomputed with \code{\link{fit_aoa}} (glm method, 50% threshold) at each
#' data release. \code{aoa} is \code{NA} for items that do not reach the
#' threshold within the instrument's age range.
#'
#' @param language An optional string specifying which language's estimates to
#'   retrieve.
#' @param form An optional string specifying which form's estimates to
#'   retrieve.
#' @param measure An optional string (\code{"produces"} or
#'   \code{"understands"}) to filter by measure.
#' @inheritParams check_db_args
#' @return A data frame with one row per instrument item and measure:
#'   \code{language}, \code{form}, \code{item_id}, \code{item_definition},
#'   \code{category}, \code{uni_lemma}, \code{measure}, \code{aoa}.
#'
#' @examples
#' \dontrun{
#' danish_aoa <- get_aoa(language = "Danish", form = "WS")
#' }
#' @export
get_aoa <- function(language = NULL, form = NULL, measure = NULL,
                    db_args = NULL) {
  check_db_args(db_args)
  aoa <- wb_table("aoa")
  if (is.null(aoa)) return(invisible(NULL))
  aoa <- filter_language_form(aoa, language, form)
  if (!is.null(measure)) {
    aoa <- dplyr::filter(aoa, .data$measure %in% !!measure)
  }
  aoa
}

#' Get multilingual item embeddings
#'
#' Semantic embeddings for every unique word item definition, computed with
#' Google's multilingual \code{gemini-embedding-001} model (768 dimensions).
#' All languages share one embedding space, so cosine similarities are
#' meaningful both within and across languages.
#'
#' @param language An optional string specifying which language's embeddings
#'   to retrieve.
#' @inheritParams check_db_args
#' @return A data frame with one row per unique item definition:
#'   \code{language}, \code{item_definition}, and \code{embedding} (a
#'   list-column of numeric vectors).
#'
#' @examples
#' \dontrun{
#' danish_embeddings <- get_embeddings(language = "Danish")
#' }
#' @export
get_embeddings <- function(language = NULL, db_args = NULL) {
  check_db_args(db_args)
  emb <- wb_table("item_embeddings")
  if (is.null(emb)) return(invisible(NULL))
  if (!is.null(language)) {
    emb <- dplyr::filter(emb, .data$language %in% !!language)
  }
  # embeddings are stored as JSON array strings; parse to a list-column
  if (is.character(emb$embedding)) {
    emb <- dplyr::mutate(
      emb, embedding = purrr::map(.data$embedding,
                                  \(x) as.numeric(jsonlite::fromJSON(x))))
  }
  emb
}
