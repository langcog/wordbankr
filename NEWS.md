# wordbankr 1.0.2
- more graceful failure for connection issues

# wordbankr 1.0.1
- graceful failure for connection issues

# wordbankr 1.0.0
- updates for new structure of the Wordbank database, including functionality for data on children's language exposures and health conditions
- renaming of fields and arguments for consistency and clarity
- addition of arguments indicating which sets of child information to include in `get_administration_data()`
- coding of production/comprehension values in `get_instrument_data()`
- graceful failure for connection issues
- deprecation of quantiles functionality

# wordbankr 0.3.1
- new functionality for fitting quantiles of vocabulary sizes
- compatibility with dplyr 2.0 and tidyr 1.0

# wordbankr 0.3.0
- compatibility with tidyeval
- new functionality for metadata on data sources
- new functionality for age of acquisition estimates
- new functionality for cross-linguistic mapping
- function and argument naming consistency
- bug fixes and performance improvements

# wordbank 0.2.0
- additional fields in get_administration_data(): zygosity, original_id, norming, longitudinal, source_name
- minor bug fixes
- faster queries
