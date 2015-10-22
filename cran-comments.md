## Test environments
* local OS X install, R 3.2.2
* ubuntu 12.04 (on travis-ci), R 3.2.2
* win-builder (devel and release)

## R CMD check results
There were no ERRORs or WARNINGs.

There were 2 NOTEs:

* checking CRAN incoming feasibility ... NOTE
  Maintainer: ‘Mika Braginsky <mika.br@gmail.com>’
  New submission

  This is my first submission.

* checking dependencies in R code ... NOTE
  Namespace in Imports field not imported from: 'RMySQL'

  RMySQL is required by dplyr to access MySQL databases.
