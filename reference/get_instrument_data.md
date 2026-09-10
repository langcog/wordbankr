# Get the Wordbank administration-by-item data

Get the Wordbank administration-by-item data

## Usage

``` r
get_instrument_data(
  language,
  form,
  items = NULL,
  administration_info = FALSE,
  item_info = FALSE,
  version = "current",
  ...
)
```

## Arguments

- language:

  A string of the instrument's language.

- form:

  A string of the instrument's form.

- items:

  A character vector of item ids (e.g. `"item_42"`) to extract. If not
  supplied, defaults to all the instrument's items.

- administration_info:

  Either a logical indicating whether to include administration data or
  a data frame of administration data (as returned by
  `get_administration_data`).

- item_info:

  Either a logical indicating whether to include item data or a data
  frame of item data (as returned by `get_item_data`).

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

- ...:

  Additional arguments, ignored (for backward compatibility).

## Value

A data frame where each row contains the values (`value`, `produces`,
`understands`) of a given item (`item_id`) for a given administration
(`data_id`), with additional columns of variables about the
administration and item, as specified, and `dataset_version`.

## Examples

``` r
if (FALSE) { # \dontrun{
eng_ws_data <- get_instrument_data(language = "English (American)",
                                   form = "WS",
                                   items = c("item_1", "item_42"))
} # }
```
