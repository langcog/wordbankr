# Deprecated database arguments

As of wordbankr 2.0, data are retrieved from the versioned Wordbank
dataset on Redivis rather than a MySQL database; \`db_args\` and
\`connect_to_wordbank()\` are deprecated and ignored.

## Usage

``` r
check_db_args(db_args)

connect_to_wordbank(db_args = NULL)

get_wordbank_args()
```

## Arguments

- db_args:

  Deprecated, ignored.
