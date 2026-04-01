# Connect to the Wordbank database

Connect to the Wordbank database

## Usage

``` r
connect_to_wordbank(db_args = NULL)
```

## Arguments

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A `src` object which is connection to the Wordbank database.

## Examples

``` r
# \donttest{
src <- connect_to_wordbank()
# }
```
