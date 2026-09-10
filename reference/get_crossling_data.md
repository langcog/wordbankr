# Get item-by-age summary statistics for items across languages

Get item-by-age summary statistics for items across languages

## Usage

``` r
get_crossling_data(uni_lemmas, version = "current")
```

## Arguments

- uni_lemmas:

  A character vector of uni_lemmas.

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A dataframe with a row for each combination of language, item, and age,
and columns for summary statistics for the group: number of children
(`n_children`), means (`comprehension`, `production`), standard
deviations (`comprehension_sd`, `production_sd`); and item-level
variables (`item_id`, `definition`, `uni_lemma`, `lexical_category`,
`lexical_class`, `dataset_version`).

## Examples

``` r
if (FALSE) { # \dontrun{
crossling_data <- get_crossling_data(uni_lemmas = "dog")
} # }
```
