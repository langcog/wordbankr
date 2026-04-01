# Get the Wordbank by-administration data

Get the Wordbank by-administration data

## Usage

``` r
get_administration_data(
  language = NULL,
  form = NULL,
  filter_age = TRUE,
  include_demographic_info = FALSE,
  include_birth_info = FALSE,
  include_health_conditions = FALSE,
  include_language_exposure = FALSE,
  include_study_internal_id = FALSE,
  db_args = NULL
)
```

## Arguments

- language:

  An optional string specifying which language's administrations to
  retrieve.

- form:

  An optional string specifying which form's administrations to
  retrieve.

- filter_age:

  A logical indicating whether to filter the administrations to ones in
  the valid age range for their instrument.

- include_demographic_info:

  A logical indicating whether to include the child's demographic
  information (`birth_order`, `ethnicity`, `race`, `sex`,
  `caregiver_education`).

- include_birth_info:

  A logical indicating whether to include the child's birth information
  (`birth_weight`, `born_early_or_late`, `gestational_age`, `zygosity`).

- include_health_conditions:

  A logical indicating whether to include the child's health condition
  information (a nested dataframe under `health_conditions` with the
  column `health_condition_name`).

- include_language_exposure:

  A logical indicating whether to include the child's language exposure
  information at time of administration (a nested dataframe under
  `language_exposures` with the columns `language`,
  `exposure_proportion`, `age_of_first_exposure`).

- include_study_internal_id:

  A logical indicating whether to include the child's ID in the original
  study data.

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A data frame where each row is a CDI administration and each column is a
variable about the administration (`data_id`, `date_of_test`, `age`,
`comprehension`, `production`, `is_norming`), the dataset it's from
(`dataset_name`, `dataset_origin_name`, `language`, `form`,
`form_type`), and information about the child as described in the
parameter specification.

## Examples

``` r
# \donttest{
english_ws_admins <- get_administration_data("English (American)", "WS")
all_admins <- get_administration_data()
# }
```
