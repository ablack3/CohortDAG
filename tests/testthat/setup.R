library(testthat)

# ---------------------------------------------------------------------------
# Test tier helpers
# ---------------------------------------------------------------------------
# Tier 1: Always runs (quick coverage, DuckDB, <5 min)
# Tier 2: RUN_TIER_2=true (full 3000+ cohort library, 30-60 min)
# Tier 3: RUN_TIER_3=true (live database platforms, requires DB env vars)
# Long live-db batch benchmarks: runTier3Test=true

is_truthy <- function(x) {
  tolower(if (is.null(x) || length(x) == 0L) "" else x) %in% c("true", "1", "yes")
}

skip_if_not_tier <- function(tier) {
  if (tier == 1L) return(invisible(NULL))
  env_var <- paste0("RUN_TIER_", tier)
  val <- Sys.getenv(env_var, unset = "")
  if (!is_truthy(val)) {
    skip(paste0("Tier ", tier, " tests skipped (set ", env_var, "=true to run)"))
  }
}

skip_if_not_run_tier3_benchmark <- function() {
  val <- Sys.getenv("runTier3Test", unset = "")
  if (!is_truthy(val)) {
    skip("Live tier 3 batch benchmark skipped (set runTier3Test=true to run)")
  }
}

split_schema_spec <- function(x) {
  if (length(x) == 0L || is.null(x) || any(!nzchar(x))) {
    return("")
  }
  if (length(x) > 1L) {
    return(x)
  }
  parts <- strsplit(x, "\\.")[[1L]]
  if (length(parts) == 1L) return(parts)
  if (length(parts) == 2L) return(c(schema = parts[1L], prefix = parts[2L]))
  c(catalog = parts[1L], schema = parts[2L], prefix = parts[3L])
}

normalize_schema_spec <- function(x, prefix = NULL) {
  if (length(x) == 0L || is.null(x) || any(!nzchar(x))) {
    return("")
  }

  if (length(x) == 1L) {
    out <- c(schema = unname(x[1L]))
  } else if (length(x) >= 2L) {
    nm <- names(x)
    if (is.null(nm) || !all(c("catalog", "schema") %in% nm)) {
      out <- c(catalog = unname(x[1L]), schema = unname(x[2L]))
    } else {
      out <- x[c("catalog", "schema")]
    }
  } else {
    out <- x
  }

  if (!is.null(prefix)) out <- c(out, prefix = prefix)
  out
}

write_prefix <- function() {
  paste0(
    "temp",
    (floor(as.numeric(Sys.time()) * 100) %% 100000L + Sys.getpid()) %% 100000L,
    "_"
  )
}

# ---------------------------------------------------------------------------
# CDMConnector setup (for integration and live platform tests)
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
    tryCatch(DBI::dbDisconnect(con), error = function(e) invisible(NULL))
  }

  get_connection <- function(dbms, DatabaseConnector = FALSE) {
    dbms <- tolower(dbms)

    if (DatabaseConnector) {
      stopifnot(rlang::is_installed("DatabaseConnector"))

      if (dbms == "duckdb") {
        cli::cli_inform("Testing with DatabaseConnector on duckdb")
        return(DatabaseConnector::connect(
          dbms = "duckdb",
          server = eunomiaDir()
        ))
      }

      if (dbms %in% c("postgresql", "postgres")) {
        cli::cli_inform("Testing with DatabaseConnector on postgresql")
        return(DatabaseConnector::connect(
          dbms = "postgresql",
          server = Sys.getenv("CDM5_POSTGRESQL_SERVER"),
          user = Sys.getenv("CDM5_POSTGRESQL_USER"),
          password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD")
        ))
      }

      if (dbms == "redshift") {
        cli::cli_inform("Testing with DatabaseConnector on redshift")
        return(DatabaseConnector::connect(
          dbms = "redshift",
          server = Sys.getenv("CDM5_REDSHIFT_SERVER"),
          user = Sys.getenv("CDM5_REDSHIFT_USER"),
          password = Sys.getenv("CDM5_REDSHIFT_PASSWORD"),
          port = Sys.getenv("CDM5_REDSHIFT_PORT")
        ))
      }

      if (dbms == "sqlserver") {
        cli::cli_inform("Testing with DatabaseConnector on sql server")
        return(DatabaseConnector::connect(
          dbms = "sql server",
          server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
          user = Sys.getenv("CDM5_SQL_SERVER_USER"),
          password = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
          port = Sys.getenv("CDM5_SQL_SERVER_PORT")
        ))
      }

      if (dbms == "snowflake") {
        cli::cli_inform("Testing with DatabaseConnector on snowflake")
        connection_details <- DatabaseConnector::createConnectionDetails(
          dbms = "snowflake",
          connectionString = Sys.getenv("SNOWFLAKE_CONNECTION_STRING"),
          user = Sys.getenv("SNOWFLAKE_USER"),
          password = Sys.getenv("SNOWFLAKE_PASSWORD")
        )
        return(DatabaseConnector::connect(connection_details))
      }

      if (dbms == "spark") {
        cli::cli_inform("Testing with DatabaseConnector on spark")
        connection_details <- DatabaseConnector::createConnectionDetails(
          dbms = "spark",
          user = Sys.getenv("DATABRICKS_USER"),
          password = Sys.getenv("DATABRICKS_TOKEN"),
          connectionString = Sys.getenv("DATABRICKS_CONNECTION_STRING")
        )
        return(DatabaseConnector::connect(connection_details))
      }

      if (dbms == "bigquery") {
        cli::cli_inform("Testing with DatabaseConnector on bigquery")
        options(sqlRenderTempEmulationSchema = Sys.getenv("BIGQUERY_SCRATCH_SCHEMA"))
        connection_details <- DatabaseConnector::createConnectionDetails(
          dbms = "bigquery",
          connectionString = Sys.getenv("BIGQUERY_CONNECTION_STRING"),
          user = "",
          password = ""
        )
        return(DatabaseConnector::connect(connection_details))
      }

      stop("Testing ", dbms, " with DatabaseConnector has not been implemented yet.")
    }

    if (dbms == "duckdb") {
      return(DBI::dbConnect(duckdb::duckdb(dbdir = eunomiaDir())))
    }

    if (dbms == "postgres" && Sys.getenv("CDM5_POSTGRESQL_DBNAME") != "") {
      return(DBI::dbConnect(
        RPostgres::Postgres(),
        dbname = Sys.getenv("CDM5_POSTGRESQL_DBNAME"),
        host = Sys.getenv("CDM5_POSTGRESQL_HOST"),
        user = Sys.getenv("CDM5_POSTGRESQL_USER"),
        password = Sys.getenv("CDM5_POSTGRESQL_PASSWORD"),
        port = as.integer(Sys.getenv("CDM5_POSTGRESQL_PORT", unset = "5432"))
      ))
    }

    if (dbms == "local" && Sys.getenv("LOCAL_POSTGRESQL_DBNAME") != "") {
      return(DBI::dbConnect(
        RPostgres::Postgres(),
        dbname = Sys.getenv("LOCAL_POSTGRESQL_DBNAME"),
        host = Sys.getenv("LOCAL_POSTGRESQL_HOST"),
        user = Sys.getenv("LOCAL_POSTGRESQL_USER"),
        password = Sys.getenv("LOCAL_POSTGRESQL_PASSWORD")
      ))
    }

    if (dbms == "redshift" && Sys.getenv("CDM5_REDSHIFT_DBNAME") != "") {
      return(DBI::dbConnect(
        RPostgres::Redshift(),
        dbname = Sys.getenv("CDM5_REDSHIFT_DBNAME"),
        host = Sys.getenv("CDM5_REDSHIFT_HOST"),
        port = Sys.getenv("CDM5_REDSHIFT_PORT"),
        user = Sys.getenv("CDM5_REDSHIFT_USER"),
        password = Sys.getenv("CDM5_REDSHIFT_PASSWORD")
      ))
    }

    if (dbms == "sqlserver" && Sys.getenv("CDM5_SQL_SERVER_USER") != "") {
      return(DBI::dbConnect(
        odbc::odbc(),
        Driver = Sys.getenv("SQL_SERVER_DRIVER"),
        Server = Sys.getenv("CDM5_SQL_SERVER_SERVER"),
        Database = Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE"),
        UID = Sys.getenv("CDM5_SQL_SERVER_USER"),
        PWD = Sys.getenv("CDM5_SQL_SERVER_PASSWORD"),
        TrustServerCertificate = "yes",
        Port = Sys.getenv("CDM5_SQL_SERVER_PORT")
      ))
    }

    if (dbms == "oracle" && "OracleODBC-19" %in% odbc::odbcListDataSources()$name) {
      return(DBI::dbConnect(odbc::odbc(), "OracleODBC-19"))
    }

    if (dbms == "bigquery" && Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH") != "") {
      bigrquery::bq_auth(path = Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH"))
      return(DBI::dbConnect(
        bigrquery::bigquery(),
        project = Sys.getenv("BIGQUERY_PROJECT_ID"),
        dataset = Sys.getenv("BIGQUERY_CDM_SCHEMA")
      ))
    }

    if (dbms == "snowflake" && Sys.getenv("SNOWFLAKE_USER") != "") {
      return(DBI::dbConnect(
        odbc::odbc(),
        SERVER = Sys.getenv("SNOWFLAKE_SERVER"),
        UID = Sys.getenv("SNOWFLAKE_USER"),
        PWD = Sys.getenv("SNOWFLAKE_PASSWORD"),
        DATABASE = Sys.getenv("SNOWFLAKE_DATABASE"),
        WAREHOUSE = Sys.getenv("SNOWFLAKE_WAREHOUSE"),
        DRIVER = Sys.getenv("SNOWFLAKE_DRIVER")
      ))
    }

    if (dbms == "spark" && Sys.getenv("DATABRICKS_HTTPPATH") != "") {
      message("connecting to databricks")
      return(DBI::dbConnect(
        odbc::databricks(),
        httpPath = Sys.getenv("DATABRICKS_HTTPPATH"),
        useNativeQuery = FALSE,
        bigint = "numeric"
      ))
    }

    rlang::abort("Could not create connection. Are some environment variables missing?")
  }

  get_cdm_schema <- function(dbms) {
    dbms <- tolower(dbms)
    s <- switch(
      dbms,
      "postgres" = Sys.getenv("CDM5_POSTGRESQL_CDM_SCHEMA"),
      "local" = Sys.getenv("LOCAL_POSTGRESQL_CDM_SCHEMA"),
      "redshift" = Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA"),
      "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA"), "\\.")[[1]],
      "oracle" = Sys.getenv("CDM5_ORACLE_CDM_SCHEMA"),
      "duckdb" = "main",
      "bigquery" = Sys.getenv("BIGQUERY_CDM_SCHEMA"),
      "snowflake" = strsplit(Sys.getenv("SNOWFLAKE_CDM_SCHEMA"), "\\.")[[1]],
      "spark" = Sys.getenv("DATABRICKS_CDM_SCHEMA"),
      NULL
    )
    if (length(s) == 0L) s <- ""
    normalize_schema_spec(s, prefix = NULL)
  }

  get_write_schema <- function(dbms, prefix = write_prefix()) {
    dbms <- tolower(dbms)
    s <- switch(
      dbms,
      "postgres" = Sys.getenv("CDM5_POSTGRESQL_SCRATCH_SCHEMA"),
      "local" = Sys.getenv("LOCAL_POSTGRESQL_SCRATCH_SCHEMA"),
      "redshift" = Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA"),
      "sqlserver" = strsplit(Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA"), "\\.")[[1]],
      "oracle" = Sys.getenv("CDM5_ORACLE_SCRATCH_SCHEMA"),
      "duckdb" = "main",
      "bigquery" = Sys.getenv("BIGQUERY_SCRATCH_SCHEMA"),
      "snowflake" = strsplit(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA"), "\\.")[[1]],
      "spark" = Sys.getenv("DATABRICKS_SCRATCH_SCHEMA"),
      NULL
    )
    if (length(s) == 0L) s <- ""
    normalize_schema_spec(s, prefix = prefix)
  }

  if (Sys.getenv("TEST_USING_DATABASE_CONNECTOR") %in% c("TRUE", "FALSE")) {
    testUsingDatabaseConnector <- as.logical(Sys.getenv("TEST_USING_DATABASE_CONNECTOR"))
  } else {
    testUsingDatabaseConnector <- FALSE
  }

  ciTestDbs <- c("duckdb", "postgres", "redshift", "sqlserver", "snowflake", "bigquery", "spark")

  if (Sys.getenv("CI_TEST_DB", unset = "") == "") {
    dbToTest <- ciTestDbs
  } else {
    checkmate::assert_choice(Sys.getenv("CI_TEST_DB"), choices = ciTestDbs)
    dbToTest <- Sys.getenv("CI_TEST_DB")
    print(paste("running CI tests on", dbToTest))
  }

  if ("postgres" %in% dbToTest && Sys.getenv("CDM5_POSTGRESQL_DBNAME") == "") {
    dbToTest <- setdiff(dbToTest, "postgres")
    print("CI tests not run on postgres - CDM5_POSTGRESQL_DBNAME not found")
  }
  if ("redshift" %in% dbToTest && Sys.getenv("CDM5_REDSHIFT_DBNAME") == "") {
    dbToTest <- setdiff(dbToTest, "redshift")
    print("CI tests not run on redshift - CDM5_REDSHIFT_DBNAME not found")
  }
  if ("sqlserver" %in% dbToTest && Sys.getenv("CDM5_SQL_SERVER_USER") == "") {
    dbToTest <- setdiff(dbToTest, "sqlserver")
    print("CI tests not run on sqlserver - CDM5_SQL_SERVER_USER not found")
  }
  if ("snowflake" %in% dbToTest && Sys.getenv("SNOWFLAKE_USER") == "") {
    dbToTest <- setdiff(dbToTest, "snowflake")
    print("CI tests not run on snowflake - SNOWFLAKE_USER not found")
  }
  if ("bigquery" %in% dbToTest && Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH") == "") {
    dbToTest <- setdiff(dbToTest, "bigquery")
    print("CI tests not run on bigquery - BIGQUERY_SERVICE_ACCOUNT_JSON_PATH not found")
  }
  if ("spark" %in% dbToTest && Sys.getenv("DATABRICKS_HTTPPATH") == "") {
    dbToTest <- setdiff(dbToTest, "spark")
    print("CI tests not run on spark/databricks - DATABRICKS_HTTPPATH not found")
  }
  if (!rlang::is_installed("duckdb")) {
    dbToTest <- setdiff(dbToTest, "duckdb")
    print("CI tests not run on duckdb - duckdb package is not installed")
  }
}
