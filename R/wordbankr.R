#' Connect to the Wordbank database
#'
#' @param mode A string indicating connection mode: one of \code{"local"},
#'   or \code{"remote"} (defaults to \code{"remote"})
#' @return A \code{src} object which is connection to the Wordbank database
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' wordbank <- connect_to_wordbank()
#' rm(wordbank)
#' }
connect_to_wordbank <- function(mode = "remote") {

  assertthat::assert_that(is.element(mode, c("local", "remote")))
  address <- switch(mode,
                    local = "",
                    remote = "54.200.225.86")

  src <- dplyr::src_mysql(host = address, dbname = "wordbank",
                          user = "wordbank", password = "wordbank")
  return(src)
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
#' eng_ws <- get_instrument_table(src, "english", "ws")
#' rm(src, eng_ws)
#' }
get_instrument_table <- function(src, language, form) {
  table_name <- paste(unlist(c("instruments",
                               strsplit(tolower(language), " "),
                               strsplit(tolower(form), " "))),
                      collapse = "_")
  instrument_table <- tbl(src, table_name) %>%
    rename_(data_id = "basetable_ptr_id")
  return(instrument_table)
}


#' Connect to all the Wordbank common tables
#'
#' @param src A connection to the Wordbank database
#' @return A list whose names are common table names and whose values
#' are \code{tbl} objects
#' @keywords internal
#'
#' @examples
#' \dontrun{
#' src <- connect_to_wordbank()
#' common_tables <- get_common_tables(src)
#' rm(src, common_tables)
#' }
get_common_tables <- function(src) {
  names <- Filter(function(tbl) substr(tbl, 1, 7) == "common_",
                  dplyr::src_tbls(src))
  names(names) <- lapply(names, function(name) substr(name, 8, nchar(name)))
  common_tables <- lapply(names, function(name) dplyr::tbl(src, name))
  return(common_tables)
}


#' Connect to a single Wordbank common tables
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
#' rm(src, instruments)
#' }
get_common_table <- function(src, name) {
  common_table <- dplyr::tbl(src, paste("common", name, sep = "_"))
  return(common_table)
}


#' Get the Wordbank by-administration data
#'
#' @param filter_age A logical indicating whether to filter the administrations
#'   to ones in the valid age range for their instrument
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a CDI administration and each column
#'   is a variable about the administration (\code{data_id}, \code{age},
#'   \code{comprehension}, \code{production}), its instrument (\code{language},
#'   \code{form}), or its child (\code{birth_order}, \code{ethnicity},
#'   \code{sex}, \code{momed}).
#'
#' @examples
#' \dontrun{
#' admins <- get_administration_data()
#' }
get_administration_data <- function(filter_age = TRUE, mode = "remote") {

  src <- connect_to_wordbank(mode = mode)

  mom_ed <- get_common_table(src, "momed") %>%
    as.data.frame() %>%
    rename_(momed_id = "id", momed_level = "level", momed_order = "order") %>%
    arrange_("momed_order") %>%
    transmute_(momed_id = ~as.numeric(momed_id),
               momed = ~factor(momed_level, levels = momed_level))

  children <- get_common_table(src, "child") %>%
    as.data.frame() %>%
    select_("id", "birth_order", "ethnicity", "sex", "momed_id") %>%
    rename_(child_id = "id") %>%
    mutate_(child_id = ~as.numeric(child_id),
            momed_id = ~as.numeric(momed_id)) %>%
    left_join(mom_ed) %>%
    select_("-momed_id")

  instruments <- get_common_table(src, "instrument") %>%
    as.data.frame() %>%
    rename_(instrument_id = "id") %>%
    mutate_(instrument_id = ~as.numeric(instrument_id))

  admins <- get_common_table(src, "administration") %>%
    as.data.frame() %>%
    select_("data_id", "child_id", "age", "instrument_id", "comprehension",
            "production") %>%
    mutate_(data_id = ~as.numeric(data_id),
            child_id = ~as.numeric(child_id)) %>%
    left_join(instruments) %>%
    select_("-instrument_id") %>%
    left_join(children) %>%
    select_("-child_id")

  rm(src)
  gc()

  if (filter_age) admins <- filter_(admins, .dots = list(~age >= age_min,
                                                        ~age <= age_max))
  admins <- admins %>%
    select_(.dots = list("-age_min", "-age_max"))
  return(admins)

}


#' Get the Wordbank by-item data
#'
#' @inheritParams connect_to_wordbank
#' @return A data frame where each row is a CDI item and each column is a
#'   variable about it (\code{language}, \code{form}, \code{type},
#'   \code{lexical_category}, \code{category}, \code{uni_lemma}, \code{item},
#'   \code{definition}, \code{num_item_id}).
#'
#' @examples
#' \dontrun{
#' items <- get_item_data()
#' }
get_item_data <- function(mode = "remote") {

  src <- connect_to_wordbank(mode = mode)

  instruments <- get_common_table(src, "instrument") %>%
    rename_(instrument_id = "id") %>%
    select_("instrument_id", "language", "form") %>%
    as.data.frame()

  categories <- get_common_table(src, "category") %>%
    rename_(category_id = "id", category = "name") %>%
    as.data.frame()

  maps <- get_common_table(src, "itemmap") %>%
    as.data.frame()

  iteminfo <- get_common_table(src, "iteminfo") %>%
    select_("-id") %>%
    rename_(uni_lemma = "map_id") %>%
    as.data.frame() %>%
    left_join(instruments) %>%
    #    select_("-instrument_id") %>%
    left_join(categories) %>%
    select_("-category_id") %>%
    left_join(maps) %>%
    #as.data.frame() %>%
    mutate_(num_item_id = ~as.numeric(substr(item_id, 6, nchar(item_id)))) %>%
    select_("item_id", "instrument_id", "language", "form", "type",
            "lexical_category", "lexical_class", "category", "uni_lemma", "item",
            "definition", "num_item_id")

  rm(src)
  gc()
  return(iteminfo)

}


#' Get the Wordbank administration-by-item data
#'
#' @param instrument_language A string of the instrument's language (insensitive
#'   to case and whitespace)
#' @param instrument_form A string of the instrument's form (insensitive to case
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
#' eng_ws_data <- get_instrument_data(instrument_language = "English",
#'                                    instrument_form = "WS",
#'                                    items = c("item_1", "item_42"))
#' }
get_instrument_data <- function(instrument_language, instrument_form, items,
                                administrations = FALSE, iteminfo = FALSE,
                                mode = "remote") {

  src <- connect_to_wordbank(mode = mode)
  instrument_table <- get_instrument_table(src, instrument_language,
                                           instrument_form)

  if (is.null(items)) {
    columns <- instrument_table$select
    items <- as.character(columns)[2:length(columns)]
  } else {
    assertthat::assert_that(all(items %in% instrument_table$select))
    names(items) <- NULL
  }

  if (class(administrations) == "logical" && administrations) {
    administrations <- get_administration_data()
  }

  if (class(iteminfo) == "logical" && iteminfo) {
    iteminfo <- get_item_data()
  }

  instrument_data <- instrument_table %>%
    select_(.dots = as.list(c("data_id", items))) %>%
    as.data.frame %>%
    mutate_(data_id = ~as.numeric(data_id)) %>%
    tidyr::gather_("item_id", "value", items, convert = TRUE) %>%
    mutate_(num_item_id = ~as.numeric(substr(item_id, 6, nchar(item_id)))) %>%
    select_("-item_id") #%>%
  #mutate(value = ifelse(is.na(value), "", value))

  if (class(administrations) == "data.frame") {
    instrument_data <- left_join(instrument_data, administrations)
  }

  if (class(iteminfo) == "data.frame") {
    instrument_data <- left_join(instrument_data, iteminfo)
  }

  rm(src, instrument_table)
  gc()
  return(instrument_data)

}
