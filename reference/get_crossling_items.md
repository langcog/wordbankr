# Get the uni_lemmas available in Wordbank

Get the uni_lemmas available in Wordbank

## Usage

``` r
get_crossling_items(db_args = NULL)
```

## Arguments

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A data frame with the column `uni_lemma`.

## Examples

``` r
# \donttest{
uni_lemmas <- get_crossling_items()
# }
```
