# Get the Wordbank by-item data

Get the Wordbank by-item data

## Usage

``` r
get_item_data(language = NULL, form = NULL, db_args = NULL)
```

## Arguments

- language:

  An optional string specifying which language's items to retrieve.

- form:

  An optional string specifying which form's items to retrieve.

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A data frame where each row is a CDI item and each column is a variable
about it: `item_id`, `item_kind` (e.g. word, gestures, word_endings),
`item_definition`, `english_gloss`, `language`, `form`, `form_type`,
`category` (meaning-based group as shown on the CDI form),
`lexical_category`, `lexical_class`, `complexity_category`,
`uni_lemma`).

## Examples

``` r
# \donttest{
english_ws_items <- get_item_data("English (American)", "WS")
all_items <- get_item_data()
# }
```
