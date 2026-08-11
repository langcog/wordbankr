# wordbankr 2.0.0

* All `get_*` functions (and `summarise_items()`, `wb_dataset()`) gain a
  `version` argument for pinning a data release, replacing the
  `options(wordbankr.dataset_version = "v1.2")` global option.
* The `db_args` argument is removed from all `get_*` functions (it was
  already ignored as of 2.0.0).
* All `get_*` functions now record the data's `dataset_version` in a column
  of their output; when `version = "current"` (the default), this is
  resolved to the actual current version tag (e.g. `"v1.5"`) rather than
  the literal string `"current"`.
* Breaking (with dataset v2.0+): in `get_administration_data()`,
  `date_of_test` is now a `Date` (previously a string), and the nested
  `language_exposures` column `exposure_proportion` is renamed
  `exposure_percentage` (its values were always percentages, 0-100). ASL
  CDITwo item ids are normalized from `"Item_N"` to `"item_N"`.
* The Redivis dataset (v2.0+) uses a normalized schema — child-level
  variables live in the `children` table and instrument-level variables in
  `instruments` — but `get_administration_data()` joins these back together,
  so its flat output shape is unchanged.

# wordbankr 2.0.0

* Data now come from the versioned Wordbank dataset on Redivis
  (https://stanford.redivis.com/datasets/627v-9ewzpdvz0) instead of the MySQL
  database. All `get_*` functions keep their signatures and return the same
  data.
* New: pin analyses to a data release with
  `options(wordbankr.dataset_version = "v1.2")` for full reproducibility.
* `get_administration_data(filter_age = FALSE)` now returns administrations
  outside the instrument age range flagged by the source data (previously
  these were only reachable through the age filter's absence).
* `connect_to_wordbank()`, `get_wordbank_args()`, and the `db_args` argument
  are deprecated and ignored.
* `get_crossling_items()` now returns a single `uni_lemma` column derived
  from item mappings; the internal database `id` column and uni-lemmas
  unattached to any item are no longer included.
* The `redivis` client is a suggested (not imported) dependency, installable
  from `https://langcog.r-universe.dev`; wordbankr prompts with the install
  command if it is missing.
* All network access retries transient failures and then fails gracefully
  (message + `NULL`), and no test or example requires network access on CRAN.
