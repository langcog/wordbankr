# Get the Wordbank data sources

Get the Wordbank data sources

## Usage

``` r
get_datasets(language = NULL, form = NULL, admin_data = FALSE, db_args = NULL)
```

## Arguments

- language:

  An optional string specifying which language's datasets to retrieve.

- form:

  An optional string specifying which form's datasets to retrieve.

- admin_data:

  A logical indicating whether to include summary-level statistics on
  the administrations within a dataset.

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A data frame where each row is a particular dataset and its
characteristics: `dataset_id`, `dataset_name`, `dataset_origin_name`
(unique identifier for groups of datasets that may share children),
`language`, `form`, `form_type`, `contributor` (contributor name and
affiliated institution), `citation`, `license`, `longitudinal` (whether
dataset includes longitudinal participants). Also includes summary
statistics on a dataset if the `admin_data` flag is `TRUE`: number of
administrations (`n_admins`).

## Examples

``` r
# \donttest{
english_ws_datasets <- get_datasets(language = "English (American)",
                                    form = "WS",
                                    admin_data = TRUE)
# }
```
