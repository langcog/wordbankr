# wordbankr

[![Travis-CI Build Status](https://travis-ci.org/langcog/wordbankr.svg?branch=master)](https://travis-ci.org/langcog/wordbankr)

R package for accessing the Wordbank database.

Installation
------------

To install the released version on [CRAN](https://cran.r-project.org/package=wordbankr):

```
install.packages("wordbankr")
```

To install the latest development version:

```
# install.packages("devtools")
devtools::install_github("langcog/wordbankr")
```

Usage
-----

See what instruments (languages and forms) are available:
```
instrument <- get_instruments()
```

Get by-administration data:
```
english_ws_admins <- get_administration_data("English (American)", "WS")
all_admins <- get_administration_data()
```

Get by-item data:
```
english_ws_items <- get_item_data("English (American)", "WS")
all_items <- get_item_data()
```

Get administration-by-item data:
```
english_ws_data <- get_instrument_data("English (American)", "WS")
```

For more details, see [this vignette](http://langcog.github.io/wordbankr/) and check out [more complicated usages](http://wordbank.stanford.edu/analyses).
