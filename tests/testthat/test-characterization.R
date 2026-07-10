# Characterization tests: every get_* call must reproduce the output of
# wordbankr 1.0.3 against the MySQL database (fixtures pinned to Redivis
# dataset v1.2, which was extracted from the same database state).

test_that("get_instruments matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(get_instruments(), "instruments")
})

test_that("get_datasets matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(get_datasets(), "datasets")
  expect_matches_fixture(get_datasets(language = "English (American)"),
                         "datasets_eng")
  expect_matches_fixture(get_datasets(form = "WS", admin_data = TRUE),
                         "datasets_ws_admins")
})

test_that("get_administration_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(
    get_administration_data(language = "Kiswahili", form = "WG"),
    "admins_kiswahili_wg")
  expect_matches_fixture(
    get_administration_data(language = "Kiswahili", form = "WG",
                            include_demographic_info = TRUE,
                            include_birth_info = TRUE,
                            include_health_conditions = TRUE,
                            include_language_exposure = TRUE),
    "admins_kiswahili_wg_full")
  expect_matches_fixture(
    get_administration_data(language = "Danish", form = "WS",
                            include_demographic_info = TRUE),
    "admins_danish_ws_demo")
  expect_matches_fixture(
    get_administration_data(language = "English (American)", form = "WG",
                            filter_age = FALSE),
    "admins_eng_wg_nofilter")
})

test_that("get_administration_data full pull has legacy shape", {
  skip_if_no_redivis()
  expect_matches_shape(get_administration_data(), "admins_all_shape")
})

test_that("get_item_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(get_item_data(language = "Kiswahili", form = "WG"),
                         "items_kiswahili_wg")
  expect_matches_fixture(get_item_data(language = "Danish", form = "WS"),
                         "items_danish_ws")
  expect_matches_shape(get_item_data(), "items_all_shape")
})

test_that("get_instrument_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(
    get_instrument_data(language = "Kiswahili", form = "WG"),
    "instrdata_kiswahili_wg")
  expect_matches_fixture(
    get_instrument_data(language = "Kiswahili", form = "WG",
                        items = c("item_1", "item_10", "item_100")),
    "instrdata_kiswahili_wg_items")
  expect_matches_fixture(
    get_instrument_data(language = "Kiswahili", form = "WG",
                        administration_info = TRUE, item_info = TRUE),
    "instrdata_kiswahili_wg_joined")
  expect_matches_shape(
    get_instrument_data(language = "Danish", form = "WS"),
    "instrdata_danish_ws_shape")
})

test_that("get_crossling_items covers legacy uni-lemmas", {
  skip_if_no_redivis()
  # DELIBERATE CHANGE in 2.0: returns uni_lemmas derived from items (no
  # internal id column, no orphan lemmas unattached to any item)
  new <- get_crossling_items()
  expect_identical(names(new), "uni_lemma")
  legacy <- load_fixture("crossling_items")
  item_lemmas <- unique(get_item_data()$uni_lemma)
  expect_true(all(new$uni_lemma %in% legacy$uni_lemma))
  expect_setequal(new$uni_lemma, item_lemmas[!is.na(item_lemmas)])
})

test_that("get_crossling_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(get_crossling_data(uni_lemmas = "dog"),
                         "crossling_dog")
})

test_that("fit_aoa matches legacy output", {
  instr <- load_fixture("aoa_input")
  expect_matches_fixture(fit_aoa(instr), "aoa_kiswahili_subset")
  expect_matches_fixture(
    fit_aoa(instr, measure = "understands", method = "empirical"),
    "aoa_kiswahili_subset_emp")
})

test_that("fit_vocab_quantiles matches legacy output", {
  admins <- load_fixture("admins_danish_ws_demo")
  expect_matches_fixture(fit_vocab_quantiles(admins, production),
                         "quantiles_danish_ws")
  expect_matches_fixture(fit_vocab_quantiles(admins, production, sex),
                         "quantiles_danish_ws_sex")
})

test_that("get_aoa returns cached estimates", {
  skip_if_no_redivis()
  skip_if(is.null(wb_try(wb_dataset()$table("aoa")$get())),
          "aoa table not yet released")
  aoa <- get_aoa(language = "Danish", form = "WS", measure = "produces")
  expect_true(all(c("language", "form", "item_id", "item_definition",
                    "measure", "aoa") %in% names(aoa)))
  expect_true(nrow(aoa) > 500)
  expect_true(all(aoa$language == "Danish"))
  hund <- aoa$aoa[aoa$item_definition == "hund"]
  expect_true(is.finite(hund) && hund > 10 && hund < 30)
})

test_that("get_embeddings returns parsed vectors", {
  skip_if_no_redivis()
  skip_if(is.null(wb_try(wb_dataset()$table("item_embeddings")$get())),
          "item_embeddings table not yet released")
  emb <- get_embeddings(language = "Danish")
  expect_true(is.list(emb$embedding))
  expect_equal(length(emb$embedding[[1]]), 768)
  expect_true(nrow(emb) > 500)
})
