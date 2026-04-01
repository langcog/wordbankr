# Changelog

## wordbankr (development version)

## wordbankr 1.0.3

CRAN release: 2024-03-01

- allow for inclusion of study internal IDs
- correctly handle new database values
- minor bug fixes

## wordbankr 1.0.2

CRAN release: 2023-11-09

- more graceful failure for connection issues

## wordbankr 1.0.1

CRAN release: 2023-10-13

- graceful failure for connection issues

## wordbankr 1.0.0

CRAN release: 2022-09-09

- updates for new structure of the Wordbank database, including
  functionality for data on children’s language exposures and health
  conditions
- renaming of fields and arguments for consistency and clarity
- addition of arguments indicating which sets of child information to
  include in
  [`get_administration_data()`](http://langcog.github.io/wordbankr/reference/get_administration_data.md)
- coding of production/comprehension values in
  [`get_instrument_data()`](http://langcog.github.io/wordbankr/reference/get_instrument_data.md)
- graceful failure for connection issues
- deprecation of quantiles functionality

## wordbankr 0.3.1

CRAN release: 2020-11-13

- new functionality for fitting quantiles of vocabulary sizes
- compatibility with dplyr 2.0 and tidyr 1.0

## wordbankr 0.3.0

CRAN release: 2018-03-14

- compatibility with tidyeval
- new functionality for metadata on data sources
- new functionality for age of acquisition estimates
- new functionality for cross-linguistic mapping
- function and argument naming consistency
- bug fixes and performance improvements
