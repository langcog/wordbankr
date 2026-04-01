# Fit quantiles to vocabulary sizes using quantile regression

Fit quantiles to vocabulary sizes using quantile regression

## Usage

``` r
fit_vocab_quantiles(vocab_data, measure, group, quantiles = "standard")
```

## Arguments

- vocab_data:

  A data frame returned by `get_administration_data`.

- measure:

  A column of `vocab_data` with vocabulary values (`production` or
  `comprehension`).

- group:

  (Optional) A column of `vocab_data` to group by.

- quantiles:

  Either one of "standard" (default), "deciles", "quintiles",
  "quartiles", "median", or a numeric vector of quantile values.

## Value

A data frame with the columns "language", "form", "age", `group` (if
specified), "quantile", and `measure`, where `measure` is the fit
vocabulary value for that quantile at that age.

## Examples

``` r
# \donttest{
eng_wg <- get_administration_data(language = "English (American)",
                                  form = "WG",
                                  include_demographic_info = TRUE)
if (!is.null(eng_wg)) {
  vocab_quantiles <- fit_vocab_quantiles(eng_wg, production)
  vocab_quantiles_sex <- fit_vocab_quantiles(eng_wg, production, sex)
  vocab_quartiles <- fit_vocab_quantiles(eng_wg, production, quantiles = "quartiles")
}
#> Warning: `fit_vocab_quantiles()` was deprecated in wordbankr 1.0.0.
#> ℹ Please use the vocabulary norms shiny app at
#>   http://wordbank.stanford.edu/analyses?name=vocab_norms
# }
```
