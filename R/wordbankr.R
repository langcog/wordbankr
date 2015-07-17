#' Connect to the Wordbank database
#' 
#' @param mode A string indicating connection mode: one of \code{"local"},
#'   \code{"prod"}, or \code{"dev"} (defaults to \code{"prod"})
#' @return A \code{src} object which is connection to the Wordbank database
#'   
#' @examples
#' wordbank <- connect_to_wordbank()
connect_to_wordbank <- function(mode = "prod") {
  
  assertthat::assert_that(is.element(mode, c("local", "prod", "dev")))
  address <- switch(mode,
                    local = "",
                    prod = "54.200.225.86",
                    dev = "54.149.39.46")
  
  dplyr::src_mysql(host = address, dbname = "wordbank",
                   user = "wordbank", password = "wordbank")
}

#' Connect to an instrument's Wordbank table
#' 
#' @param language A string of the instrument's language (insensitive to case
#'   and whitespace)
#' @param form A string of the instrument's form (insensitive to case and
#'   whitespace)
#' @inheritParams connect_to_wordbank
#'   
#' @return A \code{tbl} object containing the instrument's data
#'   
#' @examples
#' eng_ws <- get_instrument_table("english", "ws")
get_instrument_table <- function(language, form, mode = "prod") {
  src <- connect_to_wordbank(mode = mode)
  table_name <- paste(unlist(c("instruments",
                               strsplit(tolower(language), " "),
                               strsplit(tolower(form), " "))),
                      collapse = "_")
  instrument_table <- tbl(src, table_name)
  rm(src)
  instrument_table %>%
    rename_(data_id = "basetable_ptr_id")
}


#' Connect to all the Wordbank common tables
#' 
#' @inheritParams connect_to_wordbank
#' @return A list whose names are common table names and whose values
#' are \code{tbl} objects
#'
#' @examples
#' common_tables <- get_common_tables()
get_common_tables <- function(mode = "prod") {
  src <- connect_to_wordbank(mode = mode)
  names <- Filter(function(tbl) substr(tbl, 1, 7) == "common_", dplyr::src_tbls(src))
  names(names) <- lapply(names, function(name) substr(name, 8, nchar(name)))  
  lapply(names, function(name) dplyr::tbl(src, name))
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
#' admins <- get_administration_data()
get_administration_data <- function(filter_age = TRUE, mode = "prod") {
  
  common_tables <- get_common_tables()
  
  mom_ed <- as.data.frame(common_tables$momed) %>%
    rename_(momed_id = "id", momed_level = "level", momed_order = "order") %>%
    arrange_("momed_order") %>%
    transmute_(momed_id = ~as.numeric(momed_id),
               momed = ~factor(momed_level, levels = momed_level))
  
  children <- as.data.frame(common_tables$child) %>%
    select_("id", "birth_order", "ethnicity", "sex", "momed_id") %>%
    rename_(child_id = "id") %>%
    mutate_(child_id = ~as.numeric(child_id),
            momed_id = ~as.numeric(momed_id)) %>%
    left_join(mom_ed) %>%
    select_("-momed_id")
  
  instruments <- as.data.frame(common_tables$instrument) %>%
    rename_(instrument_id = "id") %>%
    mutate_(instrument_id = ~as.numeric(instrument_id))
  
  admins <- as.data.frame(common_tables$administration) %>%
    select_("data_id", "child_id", "age", "instrument_id", "comprehension",
            "production") %>%
    mutate_(data_id = ~as.numeric(data_id),
            child_id = ~as.numeric(child_id)) %>%
    left_join(instruments) %>%
    select_("-instrument_id") %>%
    left_join(children) %>%
    select_("-child_id")
  
  if(filter_age) admins <- filter_(admins, .dots = list(~age >= age_min,
                                                        ~age <= age_max))
  admins %>%
    select_(.dots = list("-age_min", "-age_max"))
  
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
#' items <- get_item_data()
get_item_data <- function(mode = "prod") {
  
  common_tables <- get_common_tables()
  
  instruments <- common_tables$instrument %>%
    rename_(instrument_id = "id") %>%
    select_("instrument_id", "language", "form")
  
  categories <- common_tables$category %>%
    rename_(category_id = "id", category = "name")
  
  maps <- common_tables$itemmap
  
  iteminfo <- common_tables$iteminfo %>%
    select_("-id") %>%
    rename_(uni_lemma = "map_id") %>%
    left_join(instruments) %>%
    select_("-instrument_id") %>%
    left_join(categories) %>%
    select_("-category_id") %>%
    left_join(maps)

  iteminfo %>%
    as.data.frame() %>%
    mutate_(num_item_id = ~as.numeric(substr(item_id, 6, nchar(item_id)))) %>%
    select_("item_id", "language", "form", "type", "lexical_category", "category",
            "uni_lemma", "item", "definition", "num_item_id")
  
}


#' Get the Wordbank administration-by-item data
#' 
#' @param instrument_table A Wordbank instrument table as returned by 
#'   \code{get_instrument_table}
#' @param items A character vector of column names of \code{instrument_table} of
#'   items to extract. If not supplied, defaults to all the columns of 
#'   \code{instrument_table}
#' @param administrations Either a logical indicating whether to include
#'   administration data or a data frame of administration data (from \code{get_administration_data})
#' @param iteminfo Either a logical indicating whether to include
#'   item data or a data frame of item data (from \code{get_item_data})
#' @return A data frame where each row is the result (\code{value}) of a given
#'   item (\code{num_item_id}) for a given administration (\code{data_id})
#'   
#' @examples
#' eng_ws_table <- get_instrument_table("English", "WS")
#' eng_ws_data <- get_instrument_data(eng_ws_table, c("item_1", "item_42"))
get_instrument_data <- function(instrument_table, items,
                                administrations = FALSE,
                                iteminfo = FALSE) {
  
  if(is.null(items)) {
    columns <- instrument_table$select
    items <- as.character(columns)[2:length(columns)]
  } else {
    assertthat::assert_that(all(items %in% instrument_table$select))
  }
  
  if(class(administrations) == "logical" && administrations) {
    administrations <- get_administration_data()
  }

  if(class(iteminfo) == "logical" && iteminfo) {
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
  
  if(class(administrations) == "data.frame") {
    instrument_data <- left_join(instrument_data, administrations)
  }

  if(class(iteminfo) == "data.frame") {
    instrument_data <- left_join(instrument_data, iteminfo)
  }
  
  instrument_data
  
}