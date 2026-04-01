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
  db_args = NULL,
  ...
)
```

## Arguments

- language:

  A string of the instrument's language (insensitive to case and
  whitespace).

- form:

  A string of the instrument's form (insensitive to case and
  whitespace).

- items:

  A character vector of column names of `instrument_table` of items to
  extract. If not supplied, defaults to all the columns of
  `instrument_table`.

- administration_info:

  Either a logical indicating whether to include administration data or
  a data frame of administration data (as returned by
  `get_administration_data`).

- item_info:

  Either a logical indicating whether to include item data or a data
  frame of item data (as returned by `get_item_data`).

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

- ...:

  \<\[\`dynamic-dots\`\]\[rlang::dyn-dots\]\> Arguments passed to
  [`get_administration_data()`](http://langcog.github.io/wordbankr/reference/get_administration_data.md).

## Value

A data frame where each row contains the values (`value`, `produces`,
`understands`) of a given item (`item_id`) for a given administration
(`data_id`), with additional columns of variables about the
administration and item, as specified.

## Examples

``` r
# \donttest{
eng_ws_data <- get_instrument_data(language = "English (American)",
                                   form = "WS",
                                   items = c("item_1", "item_42"),
                                   item_info = TRUE)
# }
```
