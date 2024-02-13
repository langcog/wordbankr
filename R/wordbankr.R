handle_con <- function(e) {
  message(strwrap(
    prefix = " ", initial = "",
    "Could not retrieve Wordbank connection information. Please check
     your internet connection. If this error persists please contact
     wordbank-contact@stanford.edu."))
}

#' Get database connection arguments
#'
#' @return List of database connection arguments: host, db_name, username,
#'   password
#' @export
#'
#' @examples
#' \donttest{
#' get_wordbank_args()
#' }
get_wordbank_args <- function() {
  tryCatch(jsonlite::fromJSON("http://wordbank.stanford.edu/db_args"),
           error = handle_con)
}

#' Connect to the Wordbank database
#'
#' @param db_args List with arguments to connect to wordbank mysql database
#'   (host, dbname, user, and password).
#' @return A \code{src} object which is connection to the Wordbank database.
#'
#' @examples
#' \donttest{
#' src <- connect_to_wordbank()
#' }
#' @export
connect_to_wordbank <- function(db_args = NULL) {

  if (is.null(db_args)) {
    db_args <- get_wordbank_args()
    if (is.null(db_args)) return()
  }

  tryCatch(error = handle_con, {
    src <- DBI::dbConnect(RMySQL::MySQL(),
                          host = db_args$host, dbname = db_args$dbname,
                          user = db_args$user, password = db_args$password)
    enc <- DBI::dbGetQuery(src, "SELECT @@character_set_database")
    DBI::dbSendQuery(src, glue::glue("SET CHARACTER SET {enc}"))
    return(src)
  })

}

# safe_tbl <- function(src, ...) {
#   purrr::safely(dplyr::tbl)
# }

#' Connect to an instrument's Wordbank table
#'
#' @keywords internal
#'
#' @param src A connection to the Wordbank database.
#' @param language A string of the instrument's language (insensitive to case
#'   and whitespace).
#' @param form A string of the instrument's form (insensitive to case and
#'   whitespace).
#' @return A \code{tbl} object containing the instrument's data.
get_instrument_table <- function(src, language, form) {
  san_string <- function(s) {
    s %>%
      tolower() %>%
      stringr::str_replace_all("[[:punct:]]", "") %>%
      stringr::str_split(" ") %>%
      unlist()
  }
  table_name <- paste(c("instruments", san_string(language), san_string(form)),
                      collapse = "_")
  tryCatch(dplyr::tbl(src, table_name), error = handle_con)
}


#' Connect to a single Wordbank common table
#'
#' @keywords internal
#'
#' @param src A connection to the Wordbank database.
#' @param name A string indicating the name of a common table.
#' @return A \code{tbl} object.
get_common_table <- function(src, name) {
  suppressWarnings(
    tryCatch(dplyr::tbl(src, paste("common", name, sep = "_")),
             error = handle_con)
  )
}


#' Get the Wordbank instruments
#'
#' @return A data frame where each row is a CDI instrument and each column is a
#'   variable about the instrument (\code{instrument_id}, \code{language},
#'   \code{form}, \code{age_min}, \code{age_max}, \code{has_grammar}).
#' @inheritParams connect_to_wordbank
#'
#' @examples
#' \donttest{
#' instruments <- get_instruments()
#' }
#' @export
get_instruments <- function(db_args = NULL) {

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  suppressWarnings(
    instruments <- get_common_table(src, name = "instrument") %>%
      dplyr::rename(instrument_id = "id") %>%
      dplyr::collect()
  )

  DBI::dbDisconnect(src)

  return(instruments)

}

#' Get the Wordbank data sources
#'
#' @param language An optional string specifying which language's datasets to
#'   retrieve.
#' @param form An optional string specifying which form's datasets to retrieve.
#' @param admin_data A logical indicating whether to include summary-level
#'   statistics on the administrations within a dataset.
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a particular dataset and its
#'   characteristics: \code{dataset_id}, \code{dataset_name},
#'   \code{dataset_origin_name} (unique identifier for groups of datasets that
#'   may share children), \code{language}, \code{form}, \code{form_type},
#'   \code{contributor} (contributor name and affiliated institution),
#'   \code{citation}, \code{license}, \code{longitudinal} (whether dataset
#'   includes longitudinal participants). Also includes summary statistics on a
#'   dataset if the \code{admin_data} flag is \code{TRUE}: number of
#'   administrations (\code{n_admins}).
#'
#' @examples
#' \donttest{
#' english_ws_datasets <- get_datasets(language = "English (American)",
#'                                     form = "WS",
#'                                     admin_data = TRUE)
#' }
#' @export
get_datasets <- function(language = NULL, form = NULL, admin_data = FALSE,
                         db_args = NULL) {

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  instruments_tbl <- get_instruments(db_args = db_args) %>%
    dplyr::select("instrument_id", "language", "form", "form_type")
  if (is.null(instruments_tbl)) return()

  datasets <- get_common_table(src, "dataset") %>% dplyr::collect()
  if (is.null(datasets)) return()

  dataset_data <- datasets %>%
    dplyr::left_join(instruments_tbl, by = "instrument_id")

  input_language <- language
  input_form <- form
  if (!is.null(language) | !is.null(form)) {
    if (!is.null(language)) {
      dataset_data <- dataset_data %>%
        dplyr::filter(.data$language == input_language)
    }
    if (!is.null(form)) {
      dataset_data <- dataset_data %>%
        dplyr::filter(.data$form == input_form)
    }
    assertthat::assert_that(nrow(dataset_data) > 0)
  }

  dataset_data <- dataset_data %>%
    dplyr::rename(dataset_id = .data$id,
                  dataset_origin_name = .data$dataset_origin_id) %>%
    dplyr::mutate(longitudinal = as.logical(.data$longitudinal)) %>%
    dplyr::select(dplyr::starts_with("dataset"), dplyr::everything()) %>%
    dplyr::select(-"instrument_id")

  if (admin_data) {
    admins_tbl <- get_common_table(src, "administration")
    if (is.null(admins_tbl)) return()

    suppressWarnings(
      admins <- admins_tbl %>%
        dplyr::group_by(.data$dataset_id) %>%
        dplyr::summarise(n_admins = dplyr::n_distinct(.data$data_id)) %>%
        dplyr::collect()
    )
    if (is.null(admins)) return()

    dataset_data <- dataset_data %>%
      dplyr::left_join(admins, by = "dataset_id")
  }

  DBI::dbDisconnect(src)

  return(dataset_data)

}


filter_query <- function(filter_language = NULL, filter_form = NULL,
                         db_args = NULL) {
  if (!is.null(filter_language) | !is.null(filter_form)) {
    instruments <- get_instruments(db_args = db_args)
    if (!is.null(filter_language)) {
      instruments <- instruments %>%
        dplyr::filter(.data$language == filter_language)
    }
    if (!is.null(filter_form)) {
      instruments <- instruments %>%
        dplyr::filter(.data$form == filter_form)
    }
    assertthat::assert_that(nrow(instruments) > 0)
    instrument_ids <- instruments$instrument_id
    return(sprintf("WHERE instrument_id IN (%s)",
                   paste(instrument_ids, collapse = ", ")))
  } else {
    return("")
  }
}


#' Get the Wordbank by-administration data
#'
#' @param language An optional string specifying which language's
#'   administrations to retrieve.
#' @param form An optional string specifying which form's administrations to
#'   retrieve.
#' @param filter_age A logical indicating whether to filter the administrations
#'   to ones in the valid age range for their instrument.
#' @param include_demographic_info A logical indicating whether to include the
#'   child's demographic information (\code{birth_order}, \code{ethnicity},
#'   \code{race}, \code{sex}, \code{caregiver_education}).
#' @param include_birth_info A logical indicating whether to include the child's
#'   birth information (\code{birth_weight}, \code{born_early_or_late},
#'   \code{gestational_age}, \code{zygosity}).
#' @param include_health_conditions A logical indicating whether to include the
#'   child's health condition information (a nested dataframe under
#'   \code{health_conditions} with the column \code{health_condition_name}).
#' @param include_language_exposure A logical indicating whether to include the
#'   child's language exposure information at time of administration (a nested
#'   dataframe under \code{language_exposures} with the columns \code{language},
#'   \code{exposure_proportion}, \code{age_of_first_exposure}).
#' @param include_study_internal_id A logical indicating whether to include
#'   the child's ID in the original study data.
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a CDI administration and each column
#'   is a variable about the administration (\code{data_id},
#'   \code{date_of_test}, \code{age}, \code{comprehension}, \code{production},
#'   \code{is_norming}), the dataset it's from (\code{dataset_name},
#'   \code{dataset_origin_name}, \code{language}, \code{form},
#'   \code{form_type}), and information about the child as described in the
#'   parameter specification.
#'
#' @examples
#' \donttest{
#' english_ws_admins <- get_administration_data("English (American)", "WS")
#' all_admins <- get_administration_data()
#' }
#' @export
get_administration_data <- function(language = NULL, form = NULL,
                                    filter_age = TRUE,
                                    include_demographic_info = FALSE,
                                    include_birth_info = FALSE,
                                    include_health_conditions = FALSE,
                                    include_language_exposure = FALSE,
                                    include_study_internal_id = FALSE,
                                    db_args = NULL) {

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  datasets_tbl <- get_datasets(db_args = db_args) %>%
    dplyr::select("dataset_id", "dataset_name", "dataset_origin_name",
                  "language", "form", "form_type")
  if (is.null(datasets_tbl)) return()

  select_cols <- c("data_id", "date_of_test", "age", "comprehension",
                   "production", "is_norming",
                   "child_id", "dataset_id", "age_min", "age_max")

  if (include_study_internal_id) select_cols <- c(select_cols, "study_internal_id")

  demo_cols <- c("birth_order", "ethnicity", "race",
                 "sex", "caregiver_education_id")
  if (include_demographic_info) select_cols <- c(select_cols, demo_cols)

  birth_cols <- c("birth_weight", "born_early_or_late", "gestational_age",
                  "zygosity")
  if (include_birth_info) select_cols <- c(select_cols, birth_cols)

  select_str <- paste(select_cols, collapse = ', ')

  admin_query <- glue(
    "SELECT common_administration.id AS administration_id, {select_str}
    FROM common_administration
    LEFT JOIN common_instrument
    ON common_administration.instrument_id = common_instrument.id
    LEFT JOIN common_child
    ON common_administration.child_id = common_child.id\n",
    {filter_query(language, form, db_args)}
  )

  suppressWarnings(
    admins_tbl <- dplyr::tbl(src, dbplyr::sql(admin_query))
  )
  if (is.null(admins_tbl)) return()

  suppressWarnings(
    admins <- admins_tbl %>%
      dplyr::collect() %>%
      dplyr::mutate(data_id = as.numeric(.data$data_id),
                    is_norming = as.logical(.data$is_norming)) %>%
      dplyr::left_join(datasets_tbl, by = "dataset_id") %>%
      dplyr::select(-"dataset_id") %>%
      dplyr::select("data_id", "date_of_test", "age", "comprehension",
                    "production", "is_norming", dplyr::starts_with("dataset"),
                    "language", "form", "form_type", dplyr::everything())
  )

  if (include_demographic_info) {
    caregiver_education_tbl <- get_common_table(src, "caregiver_education")
    if (is.null(caregiver_education_tbl)) return()

    caregiver_education <- caregiver_education_tbl %>%
      dplyr::collect() %>%
      dplyr::rename(caregiver_education_id = .data$id) %>%
      dplyr::arrange(.data$education_order) %>%
      dplyr::mutate(caregiver_education = factor(
        .data$education_level, levels = .data$education_level)
      ) %>%
      dplyr::select("caregiver_education_id", "caregiver_education")

    admins <- admins %>%
      dplyr::left_join(caregiver_education, by = "caregiver_education_id") %>%
      dplyr::select(-"caregiver_education_id") %>%
      dplyr::relocate(.data$caregiver_education, .after = .data$birth_order) %>%
      dplyr::mutate(sex = factor(.data$sex, levels = c("F", "M", "O"),
                                 labels = c("Female", "Male", "Other")),
                    ethnicity = factor(.data$ethnicity,
                                  levels = c("H", "N"),
                                  labels = c("Hispanic", "Non-Hispanic")),
                    race = factor(.data$race,
                                       levels = c("A", "B", "O", "W"),
                                       labels = c("Asian", "Black", "Other",
                                                  "White")),
                    birth_order = factor(.data$birth_order,
                                         levels = c(1, 2, 3, 4, 5, 6, 7, 8),
                                         labels = c("First", "Second", "Third",
                                                    "Fourth", "Fifth", "Sixth",
                                                    "Seventh", "Eighth")))
  }

  if (include_language_exposure) {
    language_exposure_tbl <- get_common_table(src, "language_exposure")
    if (is.null(language_exposure_tbl)) return()

    language_exposures <- language_exposure_tbl %>%
      dplyr::semi_join(admins_tbl, by = "administration_id") %>%
      dplyr::select(-"id") %>%
      dplyr::collect() %>%
      tidyr::nest(language_exposures = -"administration_id")
    admins <- admins %>%
      dplyr::left_join(language_exposures, by = "administration_id")
  }

  if (include_health_conditions) {
    health_condition_tbl <- get_common_table(src, "health_condition")
    if (is.null(health_condition_tbl)) return()

    child_health_conditions_tbl <- get_common_table(src, "child_health_conditions")
    if (is.null(child_health_conditions_tbl)) return()

    child_health_conditions <- child_health_conditions_tbl %>%
      dplyr::semi_join(admins_tbl, by = "child_id") %>%
      dplyr::left_join(health_condition_tbl,
                       by = c("healthcondition_id" = "id")) %>%
      dplyr::select(-"id", -"healthcondition_id") %>%
      dplyr::collect() %>%
      tidyr::nest(health_conditions = -"child_id")
    admins <- admins %>%
      dplyr::left_join(child_health_conditions, by = "child_id")
  }

  DBI::dbDisconnect(src)

  if (filter_age) admins <- admins %>%
    dplyr::filter(.data$age >= .data$age_min, .data$age <= .data$age_max)

  admins <- admins %>%
    dplyr::select(-"age_min", -"age_max", -"administration_id")
  return(admins)

}


strip_item_id <- function(item_id) {
  as.numeric(stringr::str_sub(item_id, 6, stringr::str_length(item_id)))
}


#' Get the Wordbank by-item data
#'
#' @param language An optional string specifying which language's items to
#'   retrieve.
#' @param form An optional string specifying which form's items to retrieve.
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a CDI item and each column is a
#'   variable about it: \code{item_id}, \code{item_kind} (e.g. word, gestures,
#'   word_endings), \code{item_definition}, \code{english_gloss},
#'   \code{language}, \code{form}, \code{form_type}, \code{category}
#'   (meaning-based group as shown on the CDI form), \code{lexical_category},
#'   \code{lexical_class}, \code{complexity_category}, \code{uni_lemma}).
#'
#' @examples
#' \donttest{
#' english_ws_items <- get_item_data("English (American)", "WS")
#' all_items <- get_item_data()
#' }
#' @export
get_item_data <- function(language = NULL, form = NULL, db_args = NULL) {

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  item_tbl <- get_common_table(src, "item")
  if (is.null(item_tbl)) return()

  item_query <- paste(
    "SELECT item_id, language, form, form_type, item_kind, category,
    item_definition, english_gloss, uni_lemma, lexical_category, complexity_category
    FROM common_item
    LEFT JOIN common_instrument
    ON common_item.instrument_id = common_instrument.id
    LEFT JOIN common_item_category
    ON common_item.item_category_id = common_item_category.id
    LEFT JOIN common_uni_lemma
    ON common_item.uni_lemma_id = common_uni_lemma.id",
    filter_query(language, form, db_args),
    sep = "\n")

  items <- dplyr::tbl(src, dbplyr::sql(item_query)) %>%
    dplyr::collect()

  DBI::dbDisconnect(src)

  return(items)

}


#' Get the Wordbank administration-by-item data
#'
#' @param language A string of the instrument's language (insensitive to case
#'   and whitespace).
#' @param form A string of the instrument's form (insensitive to case and
#'   whitespace).
#' @param items A character vector of column names of \code{instrument_table} of
#'   items to extract. If not supplied, defaults to all the columns of
#'   \code{instrument_table}.
#' @param administration_info Either a logical indicating whether to include
#'   administration data or a data frame of administration data (as returned by
#'   \code{get_administration_data}).
#' @param item_info Either a logical indicating whether to include item data or
#'   a data frame of item data (as returned by \code{get_item_data}).
#' @param ... <[`dynamic-dots`][rlang::dyn-dots]> Arguments passed to
#'   \code{get_administration_data()}.
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row contains the values (\code{value},
#'   \code{produces}, \code{understands}) of a given item (\code{item_id}) for a
#'   given administration (\code{data_id}), with additional columns of variables
#'   about the administration and item, as specified.
#'
#' @examples
#' \donttest{
#' eng_ws_data <- get_instrument_data(language = "English (American)",
#'                                    form = "WS",
#'                                    items = c("item_1", "item_42"),
#'                                    item_info = TRUE)
#' }
#' @export
get_instrument_data <- function(language, form, items = NULL,
                                administration_info = FALSE, item_info = FALSE,
                                db_args = NULL, ...) {

  items_quo <- rlang::enquo(items)
  input_language <- language
  input_form <- form

  src <- connect_to_wordbank(db_args)
  if (is.null(src)) return()

  instrument_tbl <- get_instrument_table(src, language, form)
  if (is.null(instrument_tbl)) return()
  columns <- colnames(instrument_tbl)

  if (is.null(items)) {
    items <- columns[2:length(columns)]
    items_quo <- rlang::enquo(items)
  } else {
    assertthat::assert_that(all(items %in% columns))
    names(items) <- NULL
  }

  if ("logical" %in% class(administration_info)) {
    if (administration_info) {
      administration_info <- get_administration_data(language, form,
                                                     db_args = db_args,
                                                     ...)
    } else {
      administration_info <- NULL
    }
  }
  if (!is.null(administration_info)) {
    administration_info <- administration_info %>%
      dplyr::filter(.data$language == input_language,
                    .data$form == input_form) %>%
      dplyr::select(-"language", -"form", -"form_type")
  }

  if ("logical" %in% class(item_info)) {
    item_data <- get_item_data(language, form, db_args = db_args)
  } else {
    item_data <- item_info
  }

  item_data <- item_data %>%
    dplyr::filter(.data$language == input_language, .data$form == input_form,
                  is.element(.data$item_id, items)) %>%
    dplyr::mutate(num_item_id = strip_item_id(.data$item_id)) %>%
    dplyr::select(-"item_id")

  item_data_cols <- colnames(item_data)

  produces_vals <- c("produces", "produce")
  understands_vals <- c("understands", "underst")
  sometimes_vals <- c("sometimes", "sometim")
  na_vals <- c(NA, "NA")

  instrument_data <- instrument_tbl %>%
    dplyr::select("basetable_ptr_id", !!items_quo) %>%
    dplyr::collect() %>%
    dplyr::mutate(data_id = as.numeric(.data$basetable_ptr_id)) %>%
    dplyr::select(-"basetable_ptr_id") %>%
    tidyr::gather("item_id", "value", !!items_quo) %>%
    dplyr::mutate(num_item_id = strip_item_id(.data$item_id)) %>%
    dplyr::left_join(item_data, by = "num_item_id") %>%
    dplyr::mutate(
      .after = .data$value,
      # recode value for single-char values
      value = dplyr::case_when(.data$value %in% produces_vals ~ "produces",
                               .data$value %in% understands_vals ~ "understands",
                               .data$value %in% sometimes_vals ~ "sometimes",
                               .data$value %in% na_vals ~ NA,
                               .default = .data$value),
      # code value as produces only for words
      produces = .data$value == "produces",
      produces = dplyr::if_else(.data$item_kind == "word", .data$produces, NA),
      # code value as understands only for words in WG-type forms
      understands = .data$value == "understands" | .data$value == "produces",
      understands = dplyr::if_else(
        .data$form_type == "WG" & .data$item_kind == "word", .data$understands, NA
      )
    )

  if (!is.null(administration_info)) {
    instrument_data <- instrument_data %>%
      dplyr::right_join(administration_info, by = "data_id")
  }

  if ("logical" %in% class(item_info) && !item_info) {
    instrument_data <- instrument_data %>% dplyr::select(-{{ item_data_cols }})
  } else {
    instrument_data <- instrument_data %>% dplyr::select(-"num_item_id")
  }

  DBI::dbDisconnect(src)

  return(instrument_data)

}
