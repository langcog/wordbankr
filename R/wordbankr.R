.wb_env <- new.env(parent = emptyenv())

#' Deprecated database arguments
#'
#' As of wordbankr 2.0, data are retrieved from the versioned Wordbank dataset
#' on Redivis rather than a MySQL database; `db_args` and
#' `connect_to_wordbank()` are deprecated and ignored.
#'
#' @param db_args Deprecated, ignored.
#' @keywords internal
check_db_args <- function(db_args) {
  if (!is.null(db_args)) {
    warning("As of wordbankr 2.0, data come from Redivis and `db_args` is ",
            "ignored. Install wordbankr 1.x to access a MySQL mirror.",
            call. = FALSE)
  }
}

#' @rdname check_db_args
#' @export
connect_to_wordbank <- function(db_args = NULL) {
  .Deprecated(msg = paste("wordbankr now reads from Redivis;",
                          "connect_to_wordbank() is deprecated and returns",
                          "the dataset reference."))
  wb_dataset()
}

#' @rdname check_db_args
#' @export
get_wordbank_args <- function() {
  .Deprecated(msg = "wordbankr now reads from Redivis; see wb_dataset().")
  list(organization = "datapages", dataset = "wordbank", version = "current")
}

#' The Wordbank dataset on Redivis
#'
#' Returns a reference to the Wordbank Redivis dataset
#' (\url{https://redivis.com/datapages/datasets/wordbank}).
#'
#' @param version A string specifying which version of the Wordbank dataset
#'   to use, e.g. \code{"v1.2"} to pin a released version for reproducibility.
#'   Defaults to \code{"current"}, the most recent release.
#' @return A redivis dataset reference.
#' @export
wb_dataset <- function(version = "current") {
  if (!requireNamespace("redivis", quietly = TRUE)) {
    stop("wordbankr needs the `redivis` package to access Wordbank data.\n",
         "Install it with:\n",
         '  install.packages("redivis", repos = c("https://langcog.r-universe.dev", "https://cloud.r-project.org"))',
         call. = FALSE)
  }
  redivis::redivis$organization("datapages")$dataset("wordbank:627v",
                                                     version = version)
}

# CRAN policy requires graceful failure on unavailable internet resources:
# transient errors are retried with backoff, then produce a message and
# NULL -- never an error
wb_try <- function(expr, tries = 3) {
  expr <- substitute(expr)
  env <- parent.frame()
  for (i in seq_len(tries)) {
    result <- tryCatch(eval(expr, env), error = function(e) {
      if (i < tries) {
        message("Redivis request failed (attempt ", i, "/", tries,
                "), retrying...")
        Sys.sleep(2^i)
      } else {
        message("Could not retrieve data from Redivis. Please check your ",
                "internet connection. If this error persists please contact ",
                "wordbank-contact@stanford.edu.\n(", conditionMessage(e), ")")
      }
      NULL
    })
    if (!is.null(result)) return(result)
  }
  NULL
}

# fetch a whole table as a tibble, cached per session + version
wb_table <- function(name, version = "current") {
  key <- paste(version, name)
  if (is.null(.wb_env[[key]])) {
    .wb_env[[key]] <- wb_try(wb_dataset(version)$table(name)$to_tibble())
  }
  .wb_env[[key]]
}

# run a SQL query against the dataset (server-side filtering for big tables)
wb_query <- function(sql, version = "current") {
  wb_try(wb_dataset(version)$query(sql)$to_tibble())
}

quote_sql <- function(x) paste0("'", gsub("'", "''", x), "'")

filter_language_form <- function(tbl, language = NULL, form = NULL) {
  if (!is.null(language)) tbl <- dplyr::filter(tbl, .data$language %in% !!language)
  if (!is.null(form)) tbl <- dplyr::filter(tbl, .data$form %in% !!form)
  tbl
}

#' Get the Wordbank instruments
#'
#' @inheritParams wb_dataset
#' @return A data frame where each row is a CDI instrument and each column is
#'   a variable about the instrument (\code{instrument_id}, \code{language},
#'   \code{form}, \code{form_type}, \code{age_min}, \code{age_max},
#'   \code{has_grammar}, \code{unilemma_coverage}).
#'
#' @examples
#' \dontrun{
#' instruments <- get_instruments()
#' }
#' @export
get_instruments <- function(version = "current") {
  instruments <- wb_table("instruments:7qxp", version)
  if (is.null(instruments)) return(invisible(NULL))
  dplyr::arrange(instruments, .data$instrument_id)
}

#' Get the Wordbank data sources
#'
#' @param language An optional string specifying which language's datasets to
#'   retrieve.
#' @param form An optional string specifying which form's datasets to
#'   retrieve.
#' @param admin_data A logical indicating whether to include the number of
#'   administrations in the dataset.
#' @inheritParams wb_dataset
#' @return A data frame where each row is a particular dataset and its
#'   characteristics.
#'
#' @examples
#' \dontrun{
#' english_ws_datasets <- get_datasets("English (American)", "WS")
#' }
#' @export
get_datasets <- function(language = NULL, form = NULL, admin_data = FALSE,
                         version = "current") {
  datasets <- wb_table("datasets:newe", version)
  if (is.null(datasets)) return(invisible(NULL))
  datasets <- datasets |>
    filter_language_form(language, form) |>
    dplyr::arrange(.data$dataset_id)
  if (!admin_data) datasets <- dplyr::select(datasets, -"n_admins")
  datasets
}

# demographic factor codings, mirroring wordbankr 1.x
factor_demographics <- function(admins) {
  caregiver_levels <- c("None", "Primary", "Some Secondary", "Secondary",
                        "Some College", "College", "Some Graduate", "Graduate")
  admins |>
    dplyr::mutate(
      sex = factor(.data$sex, levels = c("Female", "Male", "Other")),
      ethnicity = factor(.data$ethnicity,
                         levels = c("Hispanic", "Non-Hispanic")),
      race = factor(.data$race, levels = c("Asian", "Black", "Other", "White")),
      birth_order = factor(.data$birth_order,
                           levels = c("First", "Second", "Third", "Fourth",
                                      "Fifth", "Sixth", "Seventh", "Eighth")),
      caregiver_education = factor(.data$caregiver_education,
                                   levels = caregiver_levels))
}

#' Get the Wordbank by-administration data
#'
#' @param language An optional string specifying which language's
#'   administrations to retrieve.
#' @param form An optional string specifying which form's administrations to
#'   retrieve.
#' @param filter_age A logical indicating whether to filter the
#'   administrations to ones in the instrument's age range.
#' @param include_demographic_info A logical indicating whether to include the
#'   child's demographic information (\code{birth_order},
#'   \code{caregiver_education}, \code{ethnicity}, \code{race}, \code{sex}).
#' @param include_birth_info A logical indicating whether to include the
#'   child's birth information (\code{birth_weight}, \code{born_early_or_late},
#'   \code{gestational_age}, \code{zygosity}).
#' @param include_health_conditions A logical indicating whether to include
#'   the child's health condition information (a nested dataframe under
#'   \code{health_conditions} with the column \code{health_condition_name}).
#' @param include_language_exposure A logical indicating whether to include
#'   the child's language exposure information at time of administration (a
#'   nested dataframe under \code{language_exposures} with the columns
#'   \code{language}, \code{exposure_proportion},
#'   \code{age_of_first_exposure}).
#' @param include_study_internal_id A logical indicating whether to include
#'   the child's ID in the original study data.
#' @inheritParams wb_dataset
#' @return A data frame where each row is a CDI administration and each column
#'   is a variable about the administration or the corresponding child.
#'
#' @examples
#' \dontrun{
#' english_ws_admins <- get_administration_data("English (American)", "WS")
#' }
#' @export
get_administration_data <- function(language = NULL, form = NULL,
                                    filter_age = TRUE,
                                    include_demographic_info = FALSE,
                                    include_birth_info = FALSE,
                                    include_health_conditions = FALSE,
                                    include_language_exposure = FALSE,
                                    include_study_internal_id = FALSE,
                                    version = "current") {
  admins <- wb_table("administrations:xb60", version)
  if (is.null(admins)) return(invisible(NULL))
  admins <- filter_language_form(admins, language, form)

  if (filter_age) admins <- dplyr::filter(admins, .data$in_age_range)

  keep <- c("data_id", "date_of_test", "age", "comprehension", "production",
            "is_norming", "dataset_name", "dataset_origin_name", "language",
            "form", "form_type", "child_id")
  if (include_study_internal_id) keep <- c(keep, "study_internal_id")
  if (include_demographic_info) {
    keep <- c(keep, "birth_order", "caregiver_education", "ethnicity", "race",
              "sex")
  }
  if (include_birth_info) {
    keep <- c(keep, "birth_weight", "born_early_or_late", "gestational_age",
              "zygosity")
  }
  admins <- dplyr::select(admins, dplyr::any_of(keep))

  if (include_demographic_info) admins <- factor_demographics(admins)

  if (include_language_exposure) {
    language_exposures <- wb_table("language_exposures:wpv7", version) |>
      dplyr::semi_join(admins, by = "data_id") |>
      tidyr::nest(language_exposures = -"data_id")
    admins <- dplyr::left_join(admins, language_exposures, by = "data_id")
  }

  if (include_health_conditions) {
    health_conditions <- wb_table("health_conditions:dy4k", version) |>
      dplyr::semi_join(admins, by = "child_id") |>
      tidyr::nest(health_conditions = -"child_id")
    admins <- dplyr::left_join(admins, health_conditions, by = "child_id")
  }

  admins
}

#' Get the Wordbank by-item data
#'
#' @param language An optional string specifying which language's items to
#'   retrieve.
#' @param form An optional string specifying which form's items to retrieve.
#' @inheritParams wb_dataset
#' @return A data frame where each row is a CDI item and each column is a
#'   variable about it: \code{item_id}, \code{item_kind},
#'   \code{item_definition}, \code{english_gloss}, \code{language},
#'   \code{form}, \code{form_type}, \code{category}, \code{lexical_category},
#'   \code{lexical_class}, \code{complexity_category}, \code{uni_lemma}.
#'
#' @examples
#' \dontrun{
#' english_ws_items <- get_item_data("English (American)", "WS")
#' }
#' @export
get_item_data <- function(language = NULL, form = NULL, version = "current") {
  items <- wb_table("items:1mzm", version)
  if (is.null(items)) return(invisible(NULL))
  filter_language_form(items, language, form)
}

#' Get the Wordbank administration-by-item data
#'
#' @param language A string of the instrument's language.
#' @param form A string of the instrument's form.
#' @param items A character vector of item ids (e.g. \code{"item_42"}) to
#'   extract. If not supplied, defaults to all the instrument's items.
#' @param administration_info Either a logical indicating whether to include
#'   administration data or a data frame of administration data (as returned
#'   by \code{get_administration_data}).
#' @param item_info Either a logical indicating whether to include item data
#'   or a data frame of item data (as returned by \code{get_item_data}).
#' @param ... Additional arguments, ignored (for backward compatibility).
#' @inheritParams wb_dataset
#' @return A data frame where each row contains the values (\code{value},
#'   \code{produces}, \code{understands}) of a given item (\code{item_id}) for
#'   a given administration (\code{data_id}), with additional columns of
#'   variables about the administration and item, as specified.
#'
#' @examples
#' \dontrun{
#' eng_ws_data <- get_instrument_data(language = "English (American)",
#'                                    form = "WS",
#'                                    items = c("item_1", "item_42"))
#' }
#' @export
get_instrument_data <- function(language, form, items = NULL,
                                administration_info = FALSE,
                                item_info = FALSE, version = "current", ...) {
  item_filter <- if (!is.null(items)) {
    sprintf("AND item_id IN (%s)", paste(quote_sql(items), collapse = ", "))
  } else ""

  instrument_data <- wb_query(sprintf(
    "SELECT data_id, item_id, value, produces, understands
     FROM item_responses
     WHERE language = %s AND form = %s %s",
    quote_sql(language), quote_sql(form), item_filter), version)
  if (is.null(instrument_data)) return(invisible(NULL))
  instrument_data <- instrument_data |>
    dplyr::mutate(data_id = as.numeric(.data$data_id)) |>
    dplyr::arrange(.data$data_id, strip_item_id(.data$item_id))

  if (!is.null(items)) {
    missing <- setdiff(items, unique(instrument_data$item_id))
    if (length(missing) > 0) {
      warning("items not found in instrument: ",
              paste(missing, collapse = ", "), call. = FALSE)
    }
  }

  # legacy join semantics: item metadata attaches first (keeping language,
  # form, form_type), then administration info right-joins with those
  # columns dropped
  if (isTRUE(item_info)) {
    item_info <- get_item_data(language, form, version = version)
  }
  if (is.data.frame(item_info)) {
    item_join <- item_info |>
      dplyr::filter(.data$language == !!language, .data$form == !!form)
    if (!is.null(items)) {
      item_join <- dplyr::filter(item_join, .data$item_id %in% !!items)
    }
    instrument_data <- dplyr::left_join(instrument_data, item_join,
                                        by = "item_id")
  }

  if (isTRUE(administration_info)) {
    administration_info <- get_administration_data(language, form,
                                                    version = version)
  }
  if (is.data.frame(administration_info)) {
    admin_join <- administration_info |>
      dplyr::filter(.data$language == !!language, .data$form == !!form) |>
      dplyr::select(-"language", -"form", -"form_type")
    instrument_data <- dplyr::right_join(instrument_data, admin_join,
                                         by = "data_id")
  }

  instrument_data
}

strip_item_id <- function(item_id) {
  as.numeric(stringr::str_sub(item_id, 6, stringr::str_length(item_id)))
}
