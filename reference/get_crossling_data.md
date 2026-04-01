# Get item-by-age summary statistics for items across languages

Get item-by-age summary statistics for items across languages

## Usage

``` r
get_crossling_data(uni_lemmas, db_args = NULL)
```

## Arguments

- uni_lemmas:

  A character vector of uni_lemmas.

- db_args:

  List with arguments to connect to wordbank mysql database (host,
  dbname, user, and password).

## Value

A dataframe with a row for each combination of language, item, and age,
and columns for summary statistics for the group: number of children
(`n_children`), means (`comprehension`, `production`), standard
deviations (`comprehension_sd`, `production_sd`); and item-level
variables (`item_id`, `definition`, `uni_lemma`, `lexical_category`,
`lexical_class`).

## Examples

``` r
# \donttest{
crossling_data <- get_crossling_data(uni_lemmas = "dog")
#> Getting data for British Sign Language WG
#> Getting data for Cantonese WS
#> Getting data for Croatian WG
#> Getting data for Croatian WS
#> Getting data for Danish WG
#> Getting data for Danish WS
#> Getting data for English (American) WG
#> Getting data for English (American) WS
#> Getting data for French (Quebecois) WG
#> Getting data for French (Quebecois) WS
#> Getting data for German WS
#> Getting data for Hebrew WG
#> Getting data for Hebrew WS
#> Getting data for Italian WG
#> Getting data for Italian WS
#> Getting data for Mandarin (Beijing) IC
#> Getting data for Mandarin (Beijing) TC
#> Getting data for Mandarin (Beijing) WS
#> Getting data for Norwegian WG
#> Getting data for Norwegian WS
#> Getting data for Russian WG
#> Getting data for Russian WS
#> Getting data for Slovak WG
#> Getting data for Slovak WS
#> Getting data for Spanish (Mexican) WG
#> Getting data for Spanish (Mexican) WS
#> Getting data for Swedish WG
#> Getting data for Swedish WS
#> Getting data for Turkish WG
#> Getting data for Turkish WS
#> Getting data for English (British) TEDS Twos
#> Getting data for American Sign Language FormA
#> Getting data for American Sign Language FormBOne
#> Getting data for American Sign Language FormBTwo
#> Getting data for American Sign Language FormC
#> Getting data for Greek (Cypriot) WS
#> Getting data for Kigiriama WG
#> Getting data for Kigiriama WS
#> Getting data for Kiswahili WG
#> Getting data for Kiswahili WS
#> Getting data for Czech WS
#> Getting data for English (Australian) WS
#> Getting data for English (British) Oxford CDI
#> Getting data for Latvian WG
#> Getting data for Latvian WS
#> Getting data for Korean WG
#> Getting data for Korean WS
#> Getting data for French (French) WG
#> Getting data for French (French) WS
#> Getting data for Spanish (European) WG
#> Getting data for Spanish (European) WS
#> Getting data for Portuguese (European) WG
#> Getting data for Portuguese (European) WS
#> Getting data for Mandarin (Taiwanese) WG
#> Getting data for Mandarin (Taiwanese) WS
#> Getting data for English (Irish) WS
#> Getting data for Irish WS
#> Getting data for Finnish WS
#> Getting data for Dutch Swingley
#> Getting data for Dutch WS
#> Getting data for Dutch WG
#> Getting data for Dutch FormOne
#> Getting data for Dutch FormTwoA
#> Getting data for Hungarian WS
#> Getting data for Spanish (Argentinian) WS
#> Getting data for American Sign Language CDITwo
#> Getting data for Spanish (Chilean) WG
#> Getting data for Spanish (Peruvian) WG
#> Getting data for Spanish (Peruvian) WS
#> Getting data for Persian WS
#> Getting data for Persian WG
#> Getting data for English (American) WSShort
#> Getting data for English (American) WGShort
#> Getting data for Japanese WG
#> Getting data for Japanese WS
#> Getting data for Arabic (Saudi) WS
#> Getting data for Estonian WS
#> Getting data for Catalan WS
#> Getting data for Korean WGComp
#> Getting data for Finnish WGProd
#> Getting data for Finnish WGProdShort
#> Getting data for Catalan WG
# }
```
