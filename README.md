# wordbankr

[![Travis-CI Build Status](https://travis-ci.org/langcog/wordbankr.svg?branch=master)](https://travis-ci.org/langcog/wordbankr)

R package for accessing the Wordbank database.

Installation
------------

```
install.packages("devtools")
devtools::install_github("langcog/wordbankr")
```

Usage
-----

See what instruments (languages and forms) are available.
```
instrument <- get_instruments()
```

Get by-administration data.
```
english_ws_admins <- get_administration_data("English", "WS")
all_admins <- get_administration_data()
```

Get by-item data.
```
english_ws_items <- get_items_data("English", "WS")
all_items <- get_items_data()
```

Get administration-by-item data.
```
english_ws_data <- get_instrument_data("English", "WS")
```

For more details, see [this vignette](http://langcog.github.io/wordbankr/) and check out [more complicated usages](http://wordbank.stanford.edu/analyses).
