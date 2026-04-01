# Get database connection arguments

Get database connection arguments

## Usage

``` r
get_wordbank_args()
```

## Value

List of database connection arguments: host, db_name, username, password

## Examples

``` r
# \donttest{
get_wordbank_args()
#> $host
#> [1] "wordbank2-prod-20240205.canyiscnpddk.us-west-2.rds.amazonaws.com"
#> 
#> $dbname
#> [1] "wordbank"
#> 
#> $user
#> [1] "wordbank_reader"
#> 
#> $password
#> [1] "ICanOnlyRead@99"
#> 
# }
```
