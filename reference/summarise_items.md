# Get item-by-age summary statistics

Get item-by-age summary statistics

## Usage

``` r
summarise_items(item_data, version = "current")
```

## Arguments

- item_data:

  A dataframe as returned by
  [`get_item_data()`](https://langcog.github.io/wordbankr/reference/get_item_data.md).

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A dataframe with a row for each combination of item and age, and columns
for summary statistics for the group: number of children (`n_children`),
means (`comprehension`, `production`), standard deviations
(`comprehension_sd`, `production_sd`); also retains item-level variables
from `lang_items` (`item_id`, `item_definition`, `uni_lemma`,
`lexical_category`) and `dataset_version`.

## Examples

``` r
if (FALSE) { # \dontrun{
italian_items <- get_item_data(language = "Italian", form = "WG")
if (!is.null(italian_items)) {
  italian_dog <- dplyr::filter(italian_items, uni_lemma == "dog")
  italian_dog_summary <- summarise_items(italian_dog)
}
} # }
```
