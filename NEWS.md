# wordbankr 2.0.0

* Data now come from the versioned Wordbank dataset on Redivis
  (https://redivis.com/datapages/datasets/wordbank) instead of the MySQL
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
* All network access retries transient failures and then fails gracefully
  (message + `NULL`), and no test or example requires network access on CRAN.
