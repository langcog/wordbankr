# Get multilingual item embeddings

Semantic embeddings for every unique word item definition, computed with
Google's multilingual `gemini-embedding-001` model (768 dimensions). All
languages share one embedding space, so cosine similarities are
meaningful both within and across languages.

## Usage

``` r
get_embeddings(language = NULL, version = "current")
```

## Arguments

- language:

  An optional string specifying which language's embeddings to retrieve.

- version:

  A string specifying which version of the Wordbank dataset to use, e.g.
  `"v1.2"` to pin a released version for reproducibility. Defaults to
  `"current"`, the most recent release.

## Value

A data frame with one row per unique item definition: `language`,
`item_definition`, `embedding` (a list-column of numeric vectors), and
`dataset_version`.

## Examples

``` r
if (FALSE) { # \dontrun{
danish_embeddings <- get_embeddings(language = "Danish")
} # }
```
