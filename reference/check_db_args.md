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

## Value

`check_db_args()`: no return value, called for its side effect (a
warning if `db_args` is supplied). `connect_to_wordbank()`: the Wordbank
dataset reference, as returned by
[`wb_dataset()`](https://langcog.github.io/wordbankr/reference/wb_dataset.md).
`get_wordbank_args()`: a list with elements `organization`, `dataset`,
and `version` identifying the Redivis dataset.
