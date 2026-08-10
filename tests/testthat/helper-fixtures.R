# Characterization-test helpers: compare redivis-backend output against
# golden fixtures generated from wordbankr 1.0.3 running on the MySQL
# database (see data-raw/make_fixtures.R).

skip_if_no_redivis <- function() {
  # network tests never run on CRAN; locally/CI they need a Redivis token
  skip_on_cran()
  skip_if(Sys.getenv("REDIVIS_API_TOKEN") == "",
          "REDIVIS_API_TOKEN not set")
}

load_fixture <- function(name) {
  readRDS(test_path("fixtures", paste0(name, ".rds")))
}

# normalize a tibble for comparison: align column order to the fixture,
# sort rows by all non-list columns, drop rownames/grouping
normalize <- function(x, col_order, sort_cols) {
  x <- dplyr::ungroup(x)
  x <- x[, col_order]
  x <- dplyr::arrange(x, dplyr::across(dplyr::all_of(sort_cols)))
  x <- as.data.frame(x)
  rownames(x) <- NULL
  x
}

# fixtures predate the dataset_version column added to every get_* result;
# check it separately, then drop it so the rest of the comparison is unchanged
check_and_drop_dataset_version <- function(x, fixture_name) {
  if (!"dataset_version" %in% names(x)) return(x)
  expect_true(all(x$dataset_version == TEST_VERSION),
             label = paste0(fixture_name, " dataset_version"))
  dplyr::select(x, -"dataset_version")
}

# deliberate 2.0 changes, applied to the legacy (1.0.3) fixtures so the
# rest of the comparison still enforces exact parity:
#   - date_of_test is a Date (dataset v2.0 stores it as a date type, not a
#     string)
#   - language_exposures nested column exposure_proportion is renamed
#     exposure_percentage (dataset v2.0; values unchanged)
#   - ASL CDITwo item ids are normalized "Item_N" -> "item_N" (dataset v2.0)
#   - WS-type comprehension is NA for datasets where the import mirrored
#     production into comprehension (langcog/wordbank#333, dataset v1.5); the
#     signature rule below preserves the few WS datasets that genuinely
#     measured comprehension, exactly as the ETL does
apply_v2_deltas <- function(x) {
  if ("date_of_test" %in% names(x) && is.character(x$date_of_test)) {
    x$date_of_test <- as.Date(x$date_of_test)
  }
  if ("item_id" %in% names(x)) {
    x$item_id <- sub("^Item_", "item_", x$item_id)
  }
  if ("language_exposures" %in% names(x)) {
    x$language_exposures <- purrr::map(x$language_exposures, function(d) {
      if (is.data.frame(d) && "exposure_proportion" %in% names(d)) {
        names(d)[names(d) == "exposure_proportion"] <- "exposure_percentage"
      }
      d
    })
  }
  if (all(c("comprehension", "production", "form_type", "dataset_name")
          %in% names(x))) {
    x <- x |>
      dplyr::group_by(.data$language, .data$form, .data$dataset_name) |>
      dplyr::mutate(comprehension = if (dplyr::first(.data$form_type) == "WS" &&
                                        any(!is.na(.data$comprehension)) &&
                                        all(.data$comprehension ==
                                              .data$production, na.rm = TRUE)) {
        .data$comprehension * NA  # all-NA, preserving the column type
      } else .data$comprehension) |>
      dplyr::ungroup()
  }
  x
}

expect_matches_fixture <- function(actual, fixture_name) {
  actual <- check_and_drop_dataset_version(actual, fixture_name)
  expected <- apply_v2_deltas(load_fixture(fixture_name))

  # same columns, in the same order
  expect_identical(names(actual), names(expected),
                   label = paste0(fixture_name, " column names"))

  sort_cols <- names(expected)[!purrr::map_lgl(expected, is.list)]
  act <- normalize(actual, names(expected), sort_cols)
  exp <- normalize(expected, names(expected), sort_cols)

  # nested list-columns: compare after sorting inner rows
  for (col in names(exp)[purrr::map_lgl(exp, is.list)]) {
    tidy_nested <- function(v) {
      purrr::map(v, function(d) {
        if (is.null(d) || (is.data.frame(d) && nrow(d) == 0)) return(NULL)
        d <- as.data.frame(d)[, sort(names(d)), drop = FALSE]
        d <- d[do.call(order, d), , drop = FALSE]
        rownames(d) <- NULL
        d
      })
    }
    act[[col]] <- tidy_nested(act[[col]])
    exp[[col]] <- tidy_nested(exp[[col]])
  }

  expect_equal(act, exp, label = fixture_name, tolerance = 1e-8)
}

# n_distinct_overrides: named vector of expected distinct counts that
# deliberately differ from the legacy fixture (each use documents why)
expect_matches_shape <- function(actual, fixture_name,
                                 n_distinct_overrides = NULL) {
  actual <- check_and_drop_dataset_version(actual, fixture_name)
  expected <- load_fixture(fixture_name)
  expect_equal(nrow(actual), expected$nrow,
               label = paste0(fixture_name, " nrow"))
  expect_identical(names(actual), expected$names,
                   label = paste0(fixture_name, " names"))
  n_distinct <- expected$n_distinct
  for (nm in names(n_distinct_overrides)) {
    n_distinct[[nm]] <- n_distinct_overrides[[nm]]
  }
  expect_equal(purrr::map_int(actual, dplyr::n_distinct),
               n_distinct,
               label = paste0(fixture_name, " n_distinct"))
}

# fixtures were generated from the database state released as v1.3; the data
# are value-identical through v2.0 modulo the deliberate changes encoded in
# apply_v2_deltas (v2.0 also normalizes the schema — child-level variables in
# children, form_type in instruments — which get_administration_data joins
# back together). Pin so the suite stays green regardless of later data
# releases -- pass `version = TEST_VERSION` to every get_* call under test
TEST_VERSION <- "v2.0"
