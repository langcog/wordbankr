# Get the Wordbank by-item data

Get the Wordbank by-item data

## Usage

``` r
get_item_data(language = NULL, form = NULL, version = "current")
```

## Arguments

- language:

  An optional string specifying which language's items to retrieve.

- form:

  An optional string specifying which form's items to retrieve.

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A data frame where each row is a CDI item and each column is a variable
about it: `item_id`, `item_kind`, `item_definition`, `english_gloss`,
`language`, `form`, `form_type`, `category`, `lexical_category`,
`lexical_class`, `complexity_category`, `uni_lemma`, `dataset_version`.

## Examples

``` r
if (FALSE) { # \dontrun{
english_ws_items <- get_item_data("English (American)", "WS")
} # }
```
