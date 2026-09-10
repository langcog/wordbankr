# Get the Wordbank instruments

Get the Wordbank instruments

## Usage

``` r
get_instruments(version = "current")
```

## Arguments

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A data frame where each row is a CDI instrument and each column is a
variable about the instrument (`instrument_id`, `language`, `form`,
`form_type`, `age_min`, `age_max`, `has_grammar`, `unilemma_coverage`,
`dataset_version`).

## Examples

``` r
if (FALSE) { # \dontrun{
instruments <- get_instruments()
} # }
```
