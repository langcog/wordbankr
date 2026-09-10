# The Wordbank dataset on Redivis

Returns a reference to the Wordbank Redivis dataset
(<https://stanford.redivis.com/datasets/627v-9ewzpdvz0>).

## Usage

``` r
wb_dataset(version = "current")
```

## Arguments

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A redivis dataset reference.
