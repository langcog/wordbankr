# No network: these run on CRAN. Data functions must fail fast and quietly
# when there is no way to authenticate with Redivis.

without_credentials <- function(code) {
  old_token <- Sys.getenv("REDIVIS_API_TOKEN", unset = NA)
  old_nb <- Sys.getenv("REDIVIS_DEFAULT_NOTEBOOK", unset = NA)
  old_home <- Sys.getenv("HOME")
  on.exit({
    if (is.na(old_token)) Sys.unsetenv("REDIVIS_API_TOKEN") else Sys.setenv(REDIVIS_API_TOKEN = old_token)
    if (is.na(old_nb)) Sys.unsetenv("REDIVIS_DEFAULT_NOTEBOOK") else Sys.setenv(REDIVIS_DEFAULT_NOTEBOOK = old_nb)
    Sys.setenv(HOME = old_home)
  }, add = TRUE)
  Sys.unsetenv("REDIVIS_API_TOKEN")
  Sys.unsetenv("REDIVIS_DEFAULT_NOTEBOOK")
  # tables fetched earlier in the session are legitimately served from the
  # cache without re-checking credentials; start from an empty cache
  rm(list = ls(.wb_env, all.names = TRUE), envir = .wb_env)
  Sys.setenv(HOME = withr_tempdir <- tempfile("home"))
  dir.create(withr_tempdir)
  force(code)
}

test_that("data functions return NULL with a message when there are no credentials", {
  skip_if(interactive(), "guard only applies to non-interactive sessions")
  # without the redivis package the (equally graceful) install message fires
  # first; CRAN check machines may not have redivis
  without_credentials({
    t0 <- Sys.time()
    expect_message(res <- get_instruments(),
                   "No Redivis credentials|needs the 'redivis' package")
    expect_null(res)
    expect_lt(as.numeric(Sys.time() - t0, units = "secs"), 2)
    expect_message(expect_null(get_administration_data("Danish", "WS")))
    expect_message(expect_null(get_item_data("Danish", "WS")))
    expect_message(expect_null(get_crossling_items()))
    expect_message(expect_null(get_aoa(language = "Danish")))
  })
})

test_that("credential detection sees a token", {
  without_credentials({
    expect_false(wb_credentials_available())
    Sys.setenv(REDIVIS_API_TOKEN = "x")
    expect_true(wb_credentials_available())
  })
})
