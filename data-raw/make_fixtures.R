#!/usr/bin/env Rscript
# Generate characterization fixtures by running the CURRENT (MySQL-backed)
# wordbankr against the live database. The redivis backend is then developed
# against these golden outputs: same call -> same tibble.
#
# Run from the package root with the legacy wordbankr (>= 1.0.3) installed:
#   Rscript data-raw/make_fixtures.R
#
# Small instruments are stored in full; unbounded calls (all languages) are
# stored as "shape" fixtures (dims, names, classes, checksums) to keep the
# repo light.

suppressMessages({
  library(wordbankr)
  library(dplyr)
  library(purrr)
})

stopifnot(packageVersion("wordbankr") >= "1.0.3")
dir.create("tests/testthat/fixtures", recursive = TRUE, showWarnings = FALSE)

save_fixture <- function(x, name) {
  saveRDS(x, file.path("tests/testthat/fixtures", paste0(name, ".rds")),
          version = 2)
  message("wrote ", name, " (", paste(dim(x), collapse = " x "), ")")
}

# summary shape for large results: enough to validate without storing data
shape <- function(x) {
  list(nrow = nrow(x), names = names(x),
       classes = map_chr(x, ~ paste(class(.x), collapse = "/")),
       n_distinct = map_int(x, dplyr::n_distinct))
}
save_shape <- function(x, name) {
  saveRDS(shape(x), file.path("tests/testthat/fixtures", paste0(name, ".rds")),
          version = 2)
  message("wrote shape ", name, " (", nrow(x), " rows)")
}

# ---- instruments / datasets --------------------------------------------------

save_fixture(get_instruments(), "instruments")
save_fixture(get_datasets(), "datasets")
save_fixture(get_datasets(language = "English (American)"), "datasets_eng")
save_fixture(get_datasets(form = "WS", admin_data = TRUE), "datasets_ws_admins")

# ---- administrations ---------------------------------------------------------

save_fixture(get_administration_data(language = "Kiswahili", form = "WG"),
             "admins_kiswahili_wg")
save_fixture(
  get_administration_data(language = "Kiswahili", form = "WG",
                          include_demographic_info = TRUE,
                          include_birth_info = TRUE,
                          include_health_conditions = TRUE,
                          include_language_exposure = TRUE),
  "admins_kiswahili_wg_full")
save_fixture(get_administration_data(language = "Danish", form = "WS",
                                     include_demographic_info = TRUE),
             "admins_danish_ws_demo")
save_fixture(get_administration_data(language = "English (American)",
                                     form = "WG", filter_age = FALSE),
             "admins_eng_wg_nofilter")
save_shape(get_administration_data(), "admins_all_shape")

# ---- items -------------------------------------------------------------------

save_fixture(get_item_data(language = "Kiswahili", form = "WG"),
             "items_kiswahili_wg")
save_fixture(get_item_data(language = "Danish", form = "WS"),
             "items_danish_ws")
save_shape(get_item_data(), "items_all_shape")

# ---- instrument data (child x item) -----------------------------------------

save_fixture(get_instrument_data(language = "Kiswahili", form = "WG"),
             "instrdata_kiswahili_wg")
save_fixture(
  get_instrument_data(language = "Kiswahili", form = "WG",
                      items = c("item_1", "item_10", "item_100")),
  "instrdata_kiswahili_wg_items")
save_fixture(
  get_instrument_data(language = "Kiswahili", form = "WG",
                      administration_info = TRUE, item_info = TRUE),
  "instrdata_kiswahili_wg_joined")
save_shape(get_instrument_data(language = "Danish", form = "WS"),
           "instrdata_danish_ws_shape")

# ---- cross-linguistic --------------------------------------------------------

save_fixture(get_crossling_items(), "crossling_items")
save_fixture(get_crossling_data(uni_lemmas = "dog"), "crossling_dog")

# ---- pure model-fitting functions (no DB; pin behavior on fixture input) ----

instr <- readRDS("tests/testthat/fixtures/instrdata_kiswahili_wg_joined.rds")
save_fixture(fit_aoa(instr), "aoa_kiswahili_wg")
save_fixture(fit_aoa(instr, measure = "understands", method = "empirical"),
             "aoa_kiswahili_wg_emp")

admins <- readRDS("tests/testthat/fixtures/admins_danish_ws_demo.rds")
save_fixture(fit_vocab_quantiles(admins, production), "quantiles_danish_ws")
save_fixture(fit_vocab_quantiles(admins, production, sex),
             "quantiles_danish_ws_sex")

message("done")
