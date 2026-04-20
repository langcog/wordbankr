NUMBER_OF_INSTRUMENTS = 78
NUMBER_OF_LANGUAGES = 38
NUMBER_OF_DATASETS = 132
NUMBER_OF_FORMS = 20
NUMBER_OF_FORM_TYPES = 2

test_that("connect_to_wordbank() works", {
  con <- connect_to_wordbank()
  testthat::expect_true(class(con) == "MySQLConnection")
})

test_that("get_instruments() works", {
  instruments <- get_instruments()
  testthat::expect_true(is.data.frame(instruments))
  testthat::expect_equal(nrow(instruments), NUMBER_OF_INSTRUMENTS)
  testthat::expect_length(unique(instruments$language), NUMBER_OF_LANGUAGES)
  testthat::expect_length(unique(instruments$form), NUMBER_OF_FORMS)
  testthat::expect_length(unique(instruments$form_type), NUMBER_OF_FORM_TYPES)
  res <- apply(dplyr::select(instruments, age_min, age_max, has_grammar, unilemma_coverage), 2, function(x) sum(is.na(x))>0)
  testthat::expect_false(all(res), label = paste(paste(names(which(res)), collapse=", "), "contain(s) NA(s)"))
  testthat::expect_contains(instruments$language, "English (American)") # spot-check...could check against expected list
})

# Test the get_datasets function
test_that("get_datasets() works", {
  datasets <- get_datasets()
  testthat::expect_true(is.data.frame(datasets))
  testthat::expect_equal(nrow(datasets), NUMBER_OF_DATASETS)
  testthat::expect_equal(length(unique(datasets$language)), NUMBER_OF_LANGUAGES)
  testthat::expect_equal(length(unique(datasets$form)), NUMBER_OF_FORMS)
  testthat::expect_equal(length(unique(datasets$form_type)), NUMBER_OF_FORM_TYPES)
})

# Test the get_administration_data function
test_that("get_administration_data() works", {
  languages_to_test <- c("English (American)", "Spanish (Mexican)", "Croatian") # testing all would be v slow
  for(lang in languages_to_test) {
    admins <- get_administration_data(language = lang)
    testthat::expect_true(is.data.frame(admins))
    res <- apply(dplyr::select(admins, age, comprehension, production), 2, function(x) sum(is.na(x))>0)
    testthat::expect_false(all(res), label = paste(paste(names(which(res)), collapse=", "), "contain(s) NA(s) in ",lang))
  }
})

# Test the get_item_data function
test_that("get_item_data() works", {
  items <- get_item_data(language = "English (American)", form = "WS") # should we test more?
  testthat::expect_true(is.data.frame(items))
  testthat::expect_true(unique(items$form)=="WS")
  testthat::expect_length(unique(items$item_id), nrow(items))
  testthat::expect_equal(nrow(items), 797)
  testthat::expect_equal(nrow(subset(items, item_kind=="word")), 680)
  # test for expected number per item_kind / category / lexical_category, or number of defined uni_lemmas?
})

# Test the get_instrument_data function
test_that("get_instrument_data() works", {
  instrument_data <- get_instrument_data(language = "English (American)", form = "WG")
  testthat::expect_true(is.data.frame(instrument_data))
  testthat::expect_length(unique(instrument_data$item_id), 492)
  # compare unique data_ids to number of admins?
  testthat::expect_setequal(unique(instrument_data$produces), c(NA, FALSE, TRUE))
  testthat::expect_setequal(unique(instrument_data$understands), c(NA, FALSE, TRUE))
  # what are allowable values?
  # unique(instrument_data$value) # "yes" NA "no" "" "understands" "never" "often" "sometimes"  "sometimes/o" "produces" "not yet"
})


# should probably test that calculating production / comprehension from
# get_instrument_data matches production and comprehension from get_administration_data