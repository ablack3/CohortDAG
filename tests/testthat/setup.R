library(testthat)

# CohortDAG depends on CDMConnector, so we can use its test helpers
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

  # Helper: create a duckdb connection to an eunomia copy and auto-cleanup
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

  # Database test matrix - only DuckDB for CohortDAG integration tests
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
