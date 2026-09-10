# Get the Wordbank data sources

Get the Wordbank data sources

## Usage

``` r
get_datasets(
  language = NULL,
  form = NULL,
  admin_data = FALSE,
  version = "current"
)
```

## Arguments

- language:

  An optional string specifying which language's datasets to retrieve.

- form:

  An optional string specifying which form's datasets to retrieve.

- admin_data:

  A logical indicating whether to include the number of administrations
  in the dataset.

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A data frame where each row is a particular dataset and its
characteristics, including which `dataset_version` it came from.

## Examples

``` r
if (FALSE) { # \dontrun{
english_ws_datasets <- get_datasets("English (American)", "WS")
} # }
```
