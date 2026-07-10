# Characterization-test helpers: compare redivis-backend output against
# golden fixtures generated from wordbankr 1.0.3 running on the MySQL
# database (see data-raw/make_fixtures.R).

skip_if_no_redivis <- function() {
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

expect_matches_fixture <- function(actual, fixture_name) {
  expected <- load_fixture(fixture_name)

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

expect_matches_shape <- function(actual, fixture_name) {
  expected <- load_fixture(fixture_name)
  expect_equal(nrow(actual), expected$nrow,
               label = paste0(fixture_name, " nrow"))
  expect_identical(names(actual), expected$names,
                   label = paste0(fixture_name, " names"))
  expect_equal(purrr::map_int(actual, dplyr::n_distinct),
               expected$n_distinct,
               label = paste0(fixture_name, " n_distinct"))
}
