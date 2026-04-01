# Get item-by-age summary statistics

Get item-by-age summary statistics

## Usage

``` r
summarise_items(item_data, db_args = NULL)
```

## Arguments

- item_data:

  A dataframe as returned by
  [`get_item_data()`](http://langcog.github.io/wordbankr/reference/get_item_data.md).

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A dataframe with a row for each combination of item and age, and columns
for summary statistics for the group: number of children (`n_children`),
means (`comprehension`, `production`), standard deviations
(`comprehension_sd`, `production_sd`); also retains item-level variables
from `lang_items` (`item_id`, `item_definition`, `uni_lemma`,
`lexical_category`).

## Examples

``` r
# \donttest{
italian_items <- get_item_data(language = "Italian", form = "WG")
if (!is.null(italian_items)) {
  italian_dog <- dplyr::filter(italian_items, uni_lemma == "dog")
  italian_dog_summary <- summarise_items(italian_dog)
}
#> Getting data for Italian WG
# }
```
