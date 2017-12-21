if (getRversion() >= "2.15.1") utils::globalVariables(
  c(".", "n")
)

#' @importFrom dplyr "%>%"
#' @importFrom rlang .data
NULL

#' Connect to the Wordbank database
#'
#' @param mode A string indicating connection mode: one of \code{"local"},
#'   or \code{"remote"} (defaults to \code{"remote"})
#' @return A \code{src} object which is connection to the Wordbank database
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' src <- connect_to_wordbank()
#' DBI::dbDisconnect(src)
#' }
connect_to_wordbank <- function(mode = "remote") {
  
  assertthat::assert_that(is.element(mode, c("local", "remote")))
  address <- switch(mode,
                    local = "localhost",
                    remote = "server.wordbank.stanford.edu")
  
  DBI::dbConnect(RMySQL::MySQL(),
                 host = address, dbname = "wordbank",
                 user = "wordbank", password = "wordbank")
  
}


#' Connect to an instrument's Wordbank table
#'
#' @param src A connection to the Wordbank database
#' @param language A string of the instrument's language (insensitive to case
#'   and whitespace)
#' @param form A string of the instrument's form (insensitive to case and
#'   whitespace)
#' @return A \code{tbl} object containing the instrument's data
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' src <- connect_to_wordbank()
#' eng_ws <- get_instrument_table(src, "English (American)", "WS")
#' DBI::dbDisconnect(src)
#' }
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
  instrument_table <- dplyr::tbl(src, table_name)
  return(instrument_table)
}


#' Connect to a single Wordbank common table
#'
#' @param src A connection to the Wordbank database
#' @param name A string indicating the name of a common table
#' @return A \code{tbl} object
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' src <- connect_to_wordbank()
#' instruments <- get_common_table(src, "instrument")
#' DBI::dbDisconnect(src)
#' }
get_common_table <- function(src, name) {
  common_table <- dplyr::tbl(src, paste("common", name, sep = "_"))
  return(common_table)
}


#' Get the Wordbank instruments
#'
#' @return A data frame
#' @inheritParams connect_to_wordbank
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' instruments <- get_instruments()
#' }
#' @export
get_instruments <- function(mode = "remote") {
  
  src <- connect_to_wordbank(mode = mode)
  
  instruments <- get_common_table(src, name = "instrument") %>%
    dplyr::rename_(instrument_id = "id") %>%
    dplyr::collect()
  
  DBI::dbDisconnect(src)
  
  return(instruments)
  
}


filter_query <- function(filter_language = NULL, filter_form = NULL,
                         mode = "remote") {
  if (!is.null(filter_language) | !is.null(filter_form)) {
    instruments <- get_instruments(mode = mode)
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
#'   to ones in the valid age range for their instrument
#' @param original_ids A logical indicating whether to include the original ids provided
#'   by data contributors. Wordbank provides no guarantees about the structure or 
#'   uniqueness of these ids. Use at your own risk!    
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a CDI administration and each column 
#'   is a variable about the administration (\code{data_id}, \code{age}, 
#'   \code{comprehension}, \code{production}), its instrument (\code{language}, 
#'   \code{form}), its child (\code{birth_order}, \code{ethnicity}, \code{sex}, 
#'   \code{mom_ed}), or its dataset source (\code{norming},
#'   \code{longitudinal}). Also includes an \code{original_id} column if the
#'   \code{original_ids} flag is \code{TRUE}.
#'   
#' @examples
#' \dontrun{
#' english_ws_admins <- get_administration_data("English (American)", "WS")
#' all_admins <- get_administration_data()
#' }
#' @export
get_administration_data <- function(language = NULL, form = NULL,
                                    filter_age = TRUE, original_ids = FALSE,
                                    mode = "remote") {
  
  src <- connect_to_wordbank(mode = mode)
  
  mom_ed <- get_common_table(src, "momed") %>%
    dplyr::collect() %>%
    dplyr::rename(momed_id = .data$id, momed_level = .data$level, momed_order = .data$order) %>%
    dplyr::arrange(.data$momed_order) %>%
    dplyr::transmute(momed_id = as.numeric(.data$momed_id),
                     mom_ed = factor(.data$momed_level, levels = .data$momed_level))
  
  sources <- get_common_table(src, "source") %>%
    dplyr::collect() %>%
    dplyr::rename(source_id = .data$id) %>%
    dplyr::mutate(source_name = ifelse(nchar(.data$dataset) > 0,
                                paste0(.data$name, " (", .data$dataset, ")"),
                                .data$name),
           longitudinal = as.logical(.data$longitudinal)) %>%
    dplyr::select(.data$source_id, .data$longitudinal, .data$source_name, .data$license)
  
  admin_query <- paste(
    "SELECT data_id, age, comprehension, production, language, form,
    birth_order, ethnicity, sex, momed_id, zygosity, study_id as original_id,
    age_min, age_max, norming, source_id
    FROM common_administration
    LEFT JOIN common_instrument
    ON common_administration.instrument_id = common_instrument.id
    LEFT JOIN common_child
    ON common_administration.child_id = common_child.id",
    filter_query(language, form, mode = mode),
    sep = "\n")
  
  admins <- dplyr::tbl(src, dplyr::sql(admin_query)) %>%
    dplyr::collect() %>%
    dplyr::mutate(data_id = as.numeric(.data$data_id), norming = as.logical(.data$norming)) %>%
    dplyr::left_join(mom_ed, by = "momed_id") %>%
    dplyr::select(-.data$momed_id) %>%
    dplyr::left_join(sources, by = "source_id") %>%
    dplyr::select(-.data$source_id) %>%
    dplyr::mutate(sex = factor(.data$sex, levels = c("F", "M", "O"),
                               labels = c("Female", "Male", "Other")),
                  ethnicity = factor(.data$ethnicity,
                                     levels = c("A", "B", "O", "W", "H"),
                                     labels = c("Asian", "Black", "Other",
                                                "White", "Hispanic")),
                  birth_order = factor(.data$birth_order,
                                       levels = c(1, 2, 3, 4, 5, 6, 7, 8),
                                       labels = c("First", "Second", "Third",
                                                  "Fourth", "Fifth", "Sixth",
                                                  "Seventh", "Eighth")))
  if (!original_ids)
    admins <- admins %>% dplyr::select(-.data$original_id)
  
  DBI::dbDisconnect(src)
  
  if (filter_age) admins <- admins %>%
    dplyr::filter(.data$age >= .data$age_min, .data$age <= .data$age_max)
  
  admins <- admins %>%
    dplyr::select(-.data$age_min, -.data$age_max)
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
#'   variable about it (\code{language}, \code{form}, \code{type},
#'   \code{lexical_category}, \code{category}, \code{uni_lemma}, \code{item},
#'   \code{definition}, \code{num_item_id}).
#'
#' @examples
#' \dontrun{
#' english_ws_items <- get_item_data("English (American)", "WS")
#' all_items <- get_item_data()
#' }
#' @export
get_item_data <- function(language = NULL, form = NULL, mode = "remote") {
  
  src <- connect_to_wordbank(mode = mode)
  
  item_query <- paste(
    "SELECT item_id, definition, language, form, type, name AS category,
    lexical_category, lexical_class, uni_lemma, complexity_category
    FROM common_iteminfo
    LEFT JOIN common_instrument
    ON common_iteminfo.instrument_id = common_instrument.id
    LEFT JOIN common_category
    ON common_iteminfo.category_id = common_category.id
    LEFT JOIN common_itemmap
    ON common_iteminfo.map_id = common_itemmap.uni_lemma",
    filter_query(language, form, mode = mode),
    sep = "\n")
  
  items <- dplyr::tbl(src, dplyr::sql(item_query)) %>%
    dplyr::collect() %>%
    dplyr::mutate(num_item_id = strip_item_id(.data$item_id))
  
  DBI::dbDisconnect(src)
  
  return(items)
  
}


#' Get the Wordbank administration-by-item data
#'
#' @param language A string of the instrument's language (insensitive
#'   to case and whitespace)
#' @param form A string of the instrument's form (insensitive to case
#'   and whitespace)
#' @param items A character vector of column names of \code{instrument_table} of
#'   items to extract. If not supplied, defaults to all the columns of
#'   \code{instrument_table}
#' @param administrations Either a logical indicating whether to include
#'   administration data or a data frame of administration data (from
#'   \code{get_administration_data})
#' @param iteminfo Either a logical indicating whether to include item data or a
#'   data frame of item data (from \code{get_item_data})
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is the result (\code{value}) of a given
#'   item (\code{num_item_id}) for a given administration (\code{data_id})
#'
#' @examples
#' \dontrun{
#' eng_ws_data <- get_instrument_data(language = "English (American)",
#'                                    form = "WS",
#'                                    items = c("item_1", "item_42"))
#' }
#' @export
get_instrument_data <- function(language, form,
                                items = NULL, administrations = FALSE,
                                iteminfo = FALSE, mode = "remote") {
  
  items_quo <- rlang::enquo(items)

  src <- connect_to_wordbank(mode = mode)
  instrument_table <- get_instrument_table(src, language, form)
  
  columns <- colnames(instrument_table)

  if (is.null(items)) {
    items <- columns[2:length(columns)]
    items_quo <- rlang::enquo(items)
  } else {
    assertthat::assert_that(all(items %in% columns))
    names(items) <- NULL
  }
  
  if ("logical" %in% class(administrations)) {
    if (administrations) {
      administrations <- get_administration_data(language, form, mode = mode)
    }
  } else {
    administrations <- administrations %>%
      dplyr::filter(.data$language == language, .data$form == form)
  }
  
  if ("logical" %in% class(iteminfo)) {
    if (iteminfo) {
      iteminfo <- get_item_data(language, form, mode = mode) %>%
        dplyr::select(-.data$language, -.data$form)
    }
  } else {
    iteminfo <- iteminfo %>%
      dplyr::filter(.data$language == language, .data$form == form,
                    is.element(.data$item_id, .data$items)) %>%
      dplyr::select(-.data$language, -.data$form)
  }
  
  instrument_data <- instrument_table %>%
    dplyr::select(.data$basetable_ptr_id, !!items_quo) %>%
    dplyr::collect() %>%
    dplyr::mutate(data_id = as.numeric(.data$basetable_ptr_id)) %>%
    dplyr::select(-.data$basetable_ptr_id) %>%
    tidyr::gather("item_id", "value", !!items_quo) %>%
    dplyr::mutate(num_item_id = strip_item_id(.data$item_id)) %>%
    dplyr::select(-.data$item_id)
  
  if ("data.frame" %in% class(administrations)) {
    instrument_data <- dplyr::right_join(instrument_data, administrations,
                                         by = "data_id")
  }
  
  if ("data.frame" %in% class(iteminfo)) {
    instrument_data <- dplyr::right_join(instrument_data, iteminfo,
                                         by = "num_item_id")
  }
  
  DBI::dbDisconnect(src)
  
  return(instrument_data)
  
}


#' Fit age of acquisition estimates for Wordbank data
#' 
#' For each item in the input data, estimate its age of acquisition as the 
#' earliest age (in months) at which the proportion of children who 
#' understand/produce the item is greater than some threshold. The proportions 
#' used can be empirical or first smoothed by a model.
#' 
#' @param instrument_data A data frame returned by \code{get_instrument_data}, 
#'   which must have an "age" column and a "num_item_id" column
#' @param measure One of "produces" or "understands" (defaults to "produces")
#' @param method A string indicating which smoothing method to use: 
#'   \code{empirical} to use empirical proportions,  \code{glm} to fit a 
#'   logistic linear model, \code{glmrob} a robust logistic linear model 
#'   (defaults to \code{glm})
#' @param proportion A number between 0 and 1 indicating threshold proportion of
#'   children
#'   
#' @return A data frame where every row is an item, the item-level columns from
#'   the input data are preserved, and the \code{aoa} column contains the age of
#'   acquisition estimates
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
    dplyr::mutate(produces = !is.na(.data$value) & .data$value == "produces",
                  understands = !is.na(.data$value) &
                    (.data$value == "understands" | .data$value == "produces")) %>%
    dplyr::select(-.data$value) %>%
    tidyr::gather("measure_name", "value", .data$produces, .data$understands) %>%
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
      item_data %>% dplyr::mutate(prop = .data$num_true / (.data$num_true + .data$num_false))
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
    dplyr::mutate(fit_data = .data$data %>% purrr::map(fit_methods[[method]])) %>%
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


#' Connect to the Wordbank database
#' @param x A 1-row dataframe passed on from \code{"match_crossling_items"} 
#'   with the following variables:
#' @param mode A string indicating connection mode: one of \code{"local"},
#'   or \code{"remote"} (defaults to \code{"remote"})
#' @return A dataframe describing the instrument (\code{language}), its variables (\code{item_id}, 
#'   \code{definition}, \code{uni_lemma}, \code{lexical_category}, \code{lexical_class}),
#'   its agegroup (\code{age}, \code{n_children}), and the group's performance (\code{comprehension}, 
#'   \code{production}, \code{comprehension_sd}, \code{production_sd}).
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' dog_unilemma <- get_item_data(language = "Italian", form = "WG") %>%
#'   filter(uni_lemma == "dog")
#' dog_in_italian <- find_matches(dog_unilemma)
#' }
find_matches <- function(x, mode = "remote") {
  
  match_data <- get_instrument_data(language = x$language[1], 
                                    form = x$form[1], 
                                    items = x$item_id, administrations = TRUE, 
                                    iteminfo = TRUE, mode = mode) %>%
    dplyr::filter(.data$item_id %in% x$item_id) %>%
    dplyr::group_by(.data$language, .data$item_id, .data$definition, .data$uni_lemma,
                    .data$lexical_category, .data$lexical_class, .data$age) %>%
    dplyr::summarise(
      n_children = n(),
      comprehension = sum(.data$value %in% c("understands", "produces"),
                          na.rm = TRUE) / .data$n_children,
      production = sum(.data$value == "produces", na.rm = TRUE) / .data$n_children,
      comprehension_sd = stats::sd(.data$value %in% c("understands", "produces"),
                                   na.rm = TRUE) / .data$n_children,
      production_sd = stats::sd(.data$value == "produces", na.rm = TRUE) / .data$n_children
    )
  
  return(match_data)
  
}


#' Get a list of unilemmas available at Wordbank
#'
#' @param mode A string indicating connection mode: one of \code{"local"},
#'   or \code{"remote"} (defaults to \code{"remote"})
#' @return A data frame
#' @inheritParams connect_to_wordbank
#'
#' @examples
#' \dontrun{
#' unilemmas <- get_crossling_items()
#' }
#' @export
get_crossling_items <- function(mode = "remote") {
  src <- connect_to_wordbank(mode = "remote")
  unilemmas <- get_common_table(src, "itemmap") %>% 
    dplyr::collect()
  
  DBI::dbDisconnect(src)
  
  return(unilemmas)
}


#' Match unilemmas to their Wordbank group-by-item data
#'
#' @param unilemmas A character vector of unilemmas to get group-by-item data for
#' @param mode A string indicating connection mode: one of \code{"local"},
#'   or \code{"remote"} (defaults to \code{"remote"})
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a particular CDI item with a set of 
#' variables describing its instrument (\code{language}), its variables (\code{item_id}, 
#'  \code{definition}, \code{uni_lemma}, \code{lexical_category}, \code{lexical_class}),
#'  its agegroup (\code{age}, \code{n_children}), and the group's performance (\code{comprehension}, 
#'  \code{production}, \code{comprehension_sd}, \code{production_sd}).
#'
#' @examples
#' \dontrun{
#' crossling_words <- match_crossling_items(unilemmas = c("hat", "nose"))
#' }
#' @export
match_crossling_items <- function(unilemmas, mode = "remote") {
  src <- connect_to_wordbank(mode = mode)
  item_data <- get_item_data(mode = mode) %>%
    dplyr::filter(.data$uni_lemma %in% unilemmas, .data$form == "WG") %>%
    split(.$language) %>%
    purrr::map_df(function(x) find_matches(x, mode))
  
  DBI::dbDisconnect(src)
  
  return(item_data)
  
}


#' Get the Wordbank source data
#'
#' @param language An optional string specifying which language's 
#'   datasets to retrieve.
#' @param form An optional string specifying which form's datasets to 
#'   retrieve.
#' @param administrations Either a logical indicating whether to include
#'   summary-level statistics on the administrations within a dataset.
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a particular dataset and its 
#' characteristics: dataset id and name (\code{source_id}, \code{name},
#' \code{dataset}), language (\code{instrument_language}), 
#' form (\code{instrument_form}), contributor and affiliated institution 
#' (\code{contributor}), provided citation (\code{citation}), whether
#' dataset includes longitudinal participants (\code{longitudinal}),
#' and licensing information (\code{license}). Also includes summary
#' statistics on a dataset if the (\code{administrations}) flag is \code{TRUE},
#' including number of children (\code{n_children}) and age range
#' (\code{age_min}, \code{age_max}).
#'
#' @examples
#' \dontrun{
#' english_ws_sources <- get_source_data(language = "English (American)",
#'                                    form = "WS",
#'                                    administrations = TRUE)
#' }
#' @export
get_source_data <- function(language = NULL, form = NULL, 
                            administrations = FALSE, mode = "remote") {
  
  src <- connect_to_wordbank(mode = mode)
  instrument_table <- get_instruments(mode = mode)
  
  source_data <- get_common_table(src, "source") %>%
    dplyr::collect()
  
  if (!is.null(language) | !is.null(form)) {
    if (!is.null(language)) {
      source_data <- source_data %>%
        dplyr::filter(.data$instrument_language == language)
    }
    if (!is.null(form)) {
      source_data <- source_data %>%
        dplyr::filter(.data$instrument_form == form)
    }
    assertthat::assert_that(nrow(source_data) > 0)
  }
  
  source_data <- source_data %>%
    dplyr::rename(source_id = .data$id) %>%
    dplyr::mutate(longitudinal = as.logical(.data$longitudinal),
                  instrument_form = factor(.data$instrument_form, 
                                           levels = c("WS", "WG", "TC", "TEDS Twos", 
                                                      "TEDS Threes", "FormA", "FormBOne",
                                                      "FormBTwo", "FormC", "IC", "Oxford CDI"),
                                           labels = c("Words & Sentences", "Words & Gestures", 
                                                      "Toddler Checklist", "TEDS Twos", 
                                                      "TEDS Threes", "FormA", "FormBOne",
                                                      "FormBTwo", "FormC", "Infant Checklist",
                                                      "Oxford CDI")),
                  license = factor(.data$license,
                                   levels = c("CC-BY", "CC-BY-NC"),
                                   labels = c("Creative Commons Attribution 4.0 International", 
                                              "Creative Commons Attribution-NonCommercial 4.0 International")))
  
  if (administrations) {
    admins <- get_common_table(src, "administration") %>% 
      dplyr::collect() %>%
      dplyr::filter(.data$source_id %in% source_data$source_id) %>%
      dplyr::group_by(.data$source_id) %>%
      dplyr::summarise(n_children = dplyr::n_distinct(.data$child_id),
                       age_min = min(.data$age, na.rm = TRUE),
                       age_max = max(.data$age, na.rm = TRUE))
    
    source_data <- source_data %>%
      dplyr::left_join(admins, by = "source_id")
  }
  
  DBI::dbDisconnect(src)
  
  return(source_data)
  
}
