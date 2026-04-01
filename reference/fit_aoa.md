# Fit age of acquisition estimates for Wordbank data

For each item in the input data, estimate its age of acquisition as the
earliest age (in months) at which the proportion of children who
understand/produce the item is greater than some threshold. The
proportions used can be empirical or first smoothed by a model.

## Usage

``` r
fit_aoa(
  instrument_data,
  measure = "produces",
  method = "glm",
  proportion = 0.5,
  age_min = min(instrument_data$age, na.rm = TRUE),
  age_max = max(instrument_data$age, na.rm = TRUE)
)
```

## Arguments

- instrument_data:

  A data frame returned by `get_instrument_data`, which must have an
  "age" column and a "num_item_id" column.

- measure:

  One of "produces" or "understands" (defaults to "produces").

- method:

  A string indicating which smoothing method to use: `empirical` to use
  empirical proportions, `glm` to fit a logistic linear model, `glmrob`
  a robust logistic linear model (defaults to `glm`).

- proportion:

  A number between 0 and 1 indicating threshold proportion of children.

- age_min:

  The minimum age to allow for an age of acquisition. Defaults to the
  minimum age in `instrument_data`

- age_max:

  The maximum age to allow for an age of acquisition. Defaults to the
  maximum age in `instrument_data`

## Value

A data frame where every row is an item, the item-level columns from the
input data are preserved, and the `aoa` column contains the age of
acquisition estimates.

## Examples

``` r
# \donttest{
eng_ws_data <- get_instrument_data(language = "English (American)",
                                   form = "WS",
                                   items = c("item_1", "item_42"),
                                   administration_info = TRUE)
if (!is.null(eng_ws_data)) eng_ws_aoa <- fit_aoa(eng_ws_data)
# }
```
