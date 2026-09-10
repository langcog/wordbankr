# Get the uni_lemmas available in Wordbank

Get the uni_lemmas available in Wordbank

## Usage

``` r
get_crossling_items(version = "current")
```

## Arguments

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A data frame with the columns `uni_lemma` and `dataset_version`.

## Examples

``` r
if (FALSE) { # \dontrun{
uni_lemmas <- get_crossling_items()
} # }
```
