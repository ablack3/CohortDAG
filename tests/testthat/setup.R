library(testthat)

# ---------------------------------------------------------------------------
# Test tier helpers
# ---------------------------------------------------------------------------
# Tier 1: Always runs (quick coverage, DuckDB, <5 min)
# Tier 2: RUN_TIER_2=true (full 3000+ cohort library, 30-60 min)
# Tier 3: RUN_TIER_3=true (live database platforms, requires DB env vars)

skip_if_not_tier <- function(tier) {
  if (tier == 1L) return(invisible(NULL))  # Tier 1 always runs
  env_var <- paste0("RUN_TIER_", tier)
  val <- Sys.getenv(env_var, unset = "")
  if (!tolower(val) %in% c("true", "1", "yes")) {
    skip(paste0("Tier ", tier, " tests skipped (set ", env_var, "=true to run)"))
  }
}

# ---------------------------------------------------------------------------
# CDMConnector setup (for integration tests)
# ---------------------------------------------------------------------------
if (rlang::is_installed("CDMConnector")) {
  library(CDMConnector)

  if (!interactive()) {
    withr::local_envvar(
      R_USER_CACHE_DIR = tempfile(),
      .local_envir = teardown_env(),
      EUNOMIA_DATA_FOLDER = Sys.getenv("EUNOMIA_DATA_FOLDER", unset = tempfile())
    )
  }

  tryCatch({
    if (Sys.getenv("skip_eunomia_download_test") != "TRUE") downloadEunomiaData(overwrite = TRUE)
  }, error = function(e) NA)

  local_eunomia_con <- function(..., env = parent.frame()) {
    dbpath <- eunomiaDir(...)
    con <- DBI::dbConnect(duckdb::duckdb(), dbpath)
    withr::defer({
      try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
      unlink(dbpath)
    }, envir = env)
    con
  }

  disconnect <- function(con) {
    if (is.null(con)) return(invisible(NULL))
    DBI::dbDisconnect(con)
  }

  dbToTest <- "duckdb"
  ciTestDbs <- c("duckdb")

  get_connection <- function(dbms, ...) {
    if (dbms == "duckdb") {
      return(DBI::dbConnect(duckdb::duckdb(), eunomiaDir()))
    }
    rlang::abort(paste("Connection for", dbms, "not configured in CohortDAG tests"))
  }

  get_cdm_schema <- function(dbms) {
    if (dbms == "duckdb") return("main")
    ""
  }

  get_write_schema <- function(dbms, prefix = paste0("temp", (floor(as.numeric(Sys.time())*100) %% 100000L + Sys.getpid()) %% 100000L, "_")) {
    if (dbms == "duckdb") return(c(schema = "main"))
    ""
  }
}
