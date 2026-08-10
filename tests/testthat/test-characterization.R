# Characterization tests: every get_* call must reproduce the output of
# wordbankr 1.0.3 against the MySQL database (fixtures pinned to Redivis
# dataset v1.2, which was extracted from the same database state).

test_that("get_instruments matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(get_instruments(version = TEST_VERSION),
                         "instruments")
})

test_that("get_datasets matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(get_datasets(version = TEST_VERSION), "datasets")
  expect_matches_fixture(
    get_datasets(language = "English (American)", version = TEST_VERSION),
    "datasets_eng")
  expect_matches_fixture(
    get_datasets(form = "WS", admin_data = TRUE, version = TEST_VERSION),
    "datasets_ws_admins")
})

test_that("get_administration_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(
    get_administration_data(language = "Kiswahili", form = "WG",
                            version = TEST_VERSION),
    "admins_kiswahili_wg")
  expect_matches_fixture(
    get_administration_data(language = "Kiswahili", form = "WG",
                            include_demographic_info = TRUE,
                            include_birth_info = TRUE,
                            include_health_conditions = TRUE,
                            include_language_exposure = TRUE,
                            version = TEST_VERSION),
    "admins_kiswahili_wg_full")
  expect_matches_fixture(
    get_administration_data(language = "Danish", form = "WS",
                            include_demographic_info = TRUE,
                            version = TEST_VERSION),
    "admins_danish_ws_demo")
  expect_matches_fixture(
    get_administration_data(language = "English (American)", form = "WG",
                            filter_age = FALSE, version = TEST_VERSION),
    "admins_eng_wg_nofilter")
})

test_that("get_administration_data full pull has legacy shape", {
  skip_if_no_redivis()
  expect_matches_shape(get_administration_data(version = TEST_VERSION),
                       "admins_all_shape")
})

test_that("get_item_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(
    get_item_data(language = "Kiswahili", form = "WG",
                 version = TEST_VERSION),
    "items_kiswahili_wg")
  expect_matches_fixture(
    get_item_data(language = "Danish", form = "WS", version = TEST_VERSION),
    "items_danish_ws")
  expect_matches_shape(get_item_data(version = TEST_VERSION),
                       "items_all_shape")
})

test_that("get_instrument_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(
    get_instrument_data(language = "Kiswahili", form = "WG",
                        version = TEST_VERSION),
    "instrdata_kiswahili_wg")
  expect_matches_fixture(
    get_instrument_data(language = "Kiswahili", form = "WG",
                        items = c("item_1", "item_10", "item_100"),
                        version = TEST_VERSION),
    "instrdata_kiswahili_wg_items")
  expect_matches_fixture(
    get_instrument_data(language = "Kiswahili", form = "WG",
                        administration_info = TRUE, item_info = TRUE,
                        version = TEST_VERSION),
    "instrdata_kiswahili_wg_joined")
  expect_matches_shape(
    get_instrument_data(language = "Danish", form = "WS",
                        version = TEST_VERSION),
    "instrdata_danish_ws_shape")
})

test_that("get_crossling_items covers legacy uni-lemmas", {
  skip_if_no_redivis()
  # DELIBERATE CHANGE in 2.0: returns uni_lemmas derived from items (no
  # internal id column, no orphan lemmas unattached to any item)
  new <- get_crossling_items(version = TEST_VERSION)
  expect_identical(names(new), c("uni_lemma", "dataset_version"))
  expect_true(all(new$dataset_version == TEST_VERSION))
  legacy <- load_fixture("crossling_items")
  item_lemmas <- unique(get_item_data(version = TEST_VERSION)$uni_lemma)
  expect_true(all(new$uni_lemma %in% legacy$uni_lemma))
  expect_setequal(new$uni_lemma, item_lemmas[!is.na(item_lemmas)])
})

test_that("get_crossling_data matches legacy output", {
  skip_if_no_redivis()
  expect_matches_fixture(
    get_crossling_data(uni_lemmas = "dog", version = TEST_VERSION),
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
  skip_if(is.null(wb_try(wb_dataset(TEST_VERSION)$table("aoa")$get())),
          "aoa table not yet released")
  aoa <- get_aoa(language = "Danish", form = "WS", measure = "produces",
                 version = TEST_VERSION)
  expect_true(all(c("language", "form", "item_id", "item_definition",
                    "measure", "aoa", "dataset_version") %in% names(aoa)))
  expect_true(all(aoa$dataset_version == TEST_VERSION))
  expect_true(nrow(aoa) > 500)
  expect_true(all(aoa$language == "Danish"))
  hund <- aoa$aoa[aoa$item_definition == "hund"]
  expect_true(is.finite(hund) && hund > 10 && hund < 30)
})

test_that("get_embeddings returns parsed vectors", {
  skip_if_no_redivis()
  skip_if(is.null(wb_try(wb_dataset(TEST_VERSION)$table("item_embeddings")$get())),
          "item_embeddings table not yet released")
  emb <- get_embeddings(language = "Danish", version = TEST_VERSION)
  expect_true(is.list(emb$embedding))
  expect_equal(length(emb$embedding[[1]]), 768)
  expect_true(nrow(emb) > 500)
})
