# Get cached age-of-acquisition estimates

Age-of-acquisition estimates for every word item on every instrument,
precomputed with
[`fit_aoa`](https://langcog.github.io/wordbankr/reference/fit_aoa.md)
(glm method, 50 data release. `aoa` is `NA` for items that do not reach
the threshold within the instrument's age range.

## Usage

``` r
get_aoa(language = NULL, form = NULL, measure = NULL, version = "current")
```

## Arguments

- language:

  An optional string specifying which language's estimates to retrieve.

- form:

  An optional string specifying which form's estimates to retrieve.

- measure:

  An optional string (`"produces"` or `"understands"`) to filter by
  measure.

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A data frame with one row per instrument item and measure: `language`,
`form`, `item_id`, `item_definition`, `category`, `uni_lemma`,
`measure`, `aoa`, `dataset_version`.

## Examples

``` r
if (FALSE) { # \dontrun{
danish_aoa <- get_aoa(language = "Danish", form = "WS")
} # }
```
