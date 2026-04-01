# Connect to an instrument's Wordbank table

Connect to an instrument's Wordbank table

## Usage

``` r
get_instrument_table(src, language, form)
```

## Arguments

- src:

  A connection to the Wordbank database.

- language:

  A string of the instrument's language (insensitive to case and
  whitespace).

- form:

  A string of the instrument's form (insensitive to case and
  whitespace).

## Value

A `tbl` object containing the instrument's data.
