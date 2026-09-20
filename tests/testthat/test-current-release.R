# The characterization tests pin an immutable dataset version, so they cannot
# notice a new data release that the package decodes badly (e.g. demographic
# labels that no longer match the factor levels and silently become NA).
# These checks run against whatever "current" is.

test_that("current release decodes without losing demographic values", {
  skip_if_no_redivis()
  admins <- get_administration_data(include_demographic_info = TRUE)
  skip_if(is.null(admins), "Wordbank unreachable")
  children <- wb_query("SELECT sex, birth_order, caregiver_education, race, ethnicity FROM children")
  skip_if(is.null(children), "Wordbank unreachable")
  decoded <- factor_demographics(children)
  for (col in c("sex", "birth_order", "caregiver_education", "race", "ethnicity")) {
    lost <- sum(!is.na(children[[col]]) & is.na(decoded[[col]]))
    expect_equal(lost, 0, label = paste0("values of `", col, "` dropped by factor levels"))
  }
})

test_that("current release has unique keys", {
  skip_if_no_redivis()
  admins <- get_administration_data()
  skip_if(is.null(admins), "Wordbank unreachable")
  expect_false(any(duplicated(admins$data_id)))
  instruments <- get_instruments()
  expect_false(any(duplicated(instruments[c("language", "form")])))
})
