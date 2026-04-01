# Get the Wordbank instruments

Get the Wordbank instruments

## Usage

``` r
get_instruments(db_args = NULL)
```

## Arguments

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A data frame where each row is a CDI instrument and each column is a
variable about the instrument (`instrument_id`, `language`, `form`,
`age_min`, `age_max`, `has_grammar`).

## Examples

``` r
# \donttest{
instruments <- get_instruments()
# }
```
