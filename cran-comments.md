# wordbankr 2.0.0

This is a resubmission of a package that was archived on 2024-01-29
("for repeated policy violation ... On Internet access"). Version 2.0.0 is
a major rewrite that directly addresses the reason for archival.

## What changed regarding internet access

The package accesses Wordbank, an open database of children's vocabulary
development, now hosted as a versioned dataset on Redivis. The 2.0.0
design ensures that CRAN checks perform **zero network access**:

- All examples for functions that access the database are wrapped in
  `\dontrun{}` (they are long-running network operations against a remote
  database).
- The vignette gates all chunk evaluation on `NOT_CRAN`, so it builds
  without evaluation on CRAN machines.
- All tests that touch the network use `skip_on_cran()` via a shared
  helper; fixture-based tests of pure computation still run on CRAN.
- Per CRAN policy on internet resources, every user-facing function fails
  gracefully when the resource is unavailable: transient failures are
  retried with backoff, then the function returns `invisible(NULL)` with
  an informative `message()`, never an error.

We run a continuous "cran-simulation" CI job (R CMD check with no
credentials and `NOT_CRAN=false`) to guarantee these properties hold.

## Suggests package from Additional_repositories

The database client `redivis` is not on CRAN. It is declared in
`Suggests` with a runtime guard that prints installation instructions,
and is available (source and binaries) from the repository declared in
`Additional_repositories: https://langcog.r-universe.dev`.

## Test environments

- local macOS 15 (aarch64), R 4.5
- GitHub Actions: ubuntu-latest (release), cran-simulation job with no
  credentials and NOT_CRAN=false
- win-builder (devel)

## R CMD check results

0 errors | 0 warnings | 1 note

- New submission / package was archived on CRAN: addressed above.
