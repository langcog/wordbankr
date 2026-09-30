# wordbankr 2.0.0 (resubmission)

Thank you for the review. Changes in response:

* Added `\value` to `check_db_args.Rd` (the deprecated `connect_to_wordbank()`
  / `get_wordbank_args()` page); every exported function now documents the
  structure and meaning of its return value.
* Replaced `\dontrun{}` with `\donttest{}` in all examples, except
  `get_crossling_data()`, whose example downloads item-level data from every
  instrument in the database (over five minutes) and remains `\dontrun{}` with
  a comment saying so. No example can be unwrapped: every function downloads
  data from Redivis, which requires a (free) Redivis account, so none can run
  on a machine without credentials.
* To make the `\donttest{}` examples safe to execute anywhere, data functions
  now check for Redivis credentials before contacting the server: in a
  non-interactive session with none available they return `NULL` with a
  message in well under a second, instead of starting the Redivis client's
  browser-based sign-in (which would otherwise wait indefinitely). Verified by
  running `R CMD check --as-cran` (which executes `\donttest{}` examples) with
  no credentials, a clean home directory, and no browser: every example
  completes in under 0.2 s, nothing is written outside the session's
  temporary directory, and the package installs nothing.

## Background: archival, and internet access

This is a resubmission of a package archived on 2024-04-04 ("On Internet
access"). Version 2.0.0 is a rewrite designed so that CRAN checks perform
zero network access: `\donttest{}` examples and all network tests fail
gracefully without credentials (tests additionally use `skip_on_cran()`), the
vignette gates its data chunks on `NOT_CRAN`, transient failures are retried
with backoff and then produce a message and `NULL`, never an error. Acronyms
in the Description: CDI = MacArthur-Bates Communicative Development
Inventories (spelled out there).

## Suggests package from Additional_repositories

The database client `redivis` is not on CRAN. It is in `Suggests`, available
from `Additional_repositories: https://langcog.r-universe.dev` (the check
reports it resolvable), and guarded at runtime: without it, data functions
return `NULL` with installation instructions. `R CMD check --as-cran` with
`redivis` not installed and `_R_CHECK_FORCE_SUGGESTS_=false` passes.

## Test environments

- local macOS 15 (aarch64), R 4.5 — with credentials, without credentials,
  and without the `redivis` package installed
- GitHub Actions: macOS, Windows, and Ubuntu (R release), Ubuntu (R devel and
  oldrel-1); plus a "cran-simulation" job with no credentials and
  NOT_CRAN=false
- win-builder (devel)

## R CMD check results

0 errors | 0 warnings | 1 note

- New submission; package was archived on CRAN: addressed above.
- Suggests not in mainstream repositories: redivis, available from the
  declared Additional_repositories.

## Reverse dependencies

One reverse dependency, cdiWG2WS, lists wordbankr in Suggests only and does
not call any wordbankr function in its code, tests, or vignettes.
