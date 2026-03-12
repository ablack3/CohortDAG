skip_if_not_tier(3L)

test_that("Platform Snowflake: CIRCE vs DAG produce identical cohort rows", {
  skip_on_cran()
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("odbc")
  skip_if_not_installed("dplyr")
  library(CDMConnector)
  library(dplyr)

  # Required env vars
  server    <- Sys.getenv("SNOWFLAKE_SERVER", unset = "")
  user      <- Sys.getenv("SNOWFLAKE_USER", unset = "")
  password  <- Sys.getenv("SNOWFLAKE_PASSWORD", unset = "")
  database  <- Sys.getenv("SNOWFLAKE_DATABASE", unset = "")
  warehouse <- Sys.getenv("SNOWFLAKE_WAREHOUSE", unset = "")
  driver    <- Sys.getenv("SNOWFLAKE_DRIVER", unset = "")
  scratch   <- Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA", unset = "")

  skip_if(server == "",    "SNOWFLAKE_SERVER not set")
  skip_if(user == "",      "SNOWFLAKE_USER not set")
  skip_if(password == "",  "SNOWFLAKE_PASSWORD not set")
  skip_if(database == "",  "SNOWFLAKE_DATABASE not set")
  skip_if(warehouse == "", "SNOWFLAKE_WAREHOUSE not set")
  skip_if(driver == "",    "SNOWFLAKE_DRIVER not set")
  skip_if(scratch == "",   "SNOWFLAKE_SCRATCH_SCHEMA not set")

  # Optional env vars
  cdm_schema_raw <- Sys.getenv("SNOWFLAKE_CDM_SCHEMA", unset = "CDM")

  # Connect
  con <- DBI::dbConnect(
    odbc::odbc(),
    server    = server,
    uid       = user,
    pwd       = password,
    database  = database,
    warehouse = warehouse,
    driver    = driver
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Snowflake schemas may use dot notation (DATABASE.SCHEMA) for CDMConnector
  split_schema <- function(s) {
    parts <- strsplit(s, "\\.")[[1L]]
    if (length(parts) == 2L) return(c(catalog = parts[1L], schema = parts[2L]))
    c(catalog = database, schema = s)
  }

  cdm_schema_spec   <- split_schema(cdm_schema_raw)
  write_schema_spec <- split_schema(scratch)

  # CDM reference
  cdm <- CDMConnector::cdmFromCon(
    con,
    cdmSchema   = cdm_schema_spec,
    writeSchema = write_schema_spec
  )

  # Force dialect
  withr::local_options(list(CohortDAG.force_target_dialect = "snowflake"))

  # Load cohort set
  cohort_set <- platform_test_cohort_set(n_max = 2L)

  # Run CIRCE (baseline)
  t_circe <- system.time({
    cdm <- CDMConnector::generateCohortSet(
      cdm,
      cohortSet = cohort_set,
      name      = "cohort_circe",
      overwrite = TRUE
    )
  })

  # Run DAG (new)
  t_dag <- system.time({
    cdm <- CohortDAG::generateCohortSet2(
      cdm,
      cohortSet = cohort_set,
      name      = "cohort_dag",
      overwrite = TRUE
    )
  })

  # Compare
  cmp <- cohort_tables_identical_sorted(cdm, "cohort_circe", "cohort_dag")
  message("[Snowflake] ", cmp$details)
  message(sprintf(
    "[Snowflake] Timing: CIRCE=%.1fs, DAG=%.1fs, ratio=%.2f",
    t_circe[["elapsed"]], t_dag[["elapsed"]],
    t_dag[["elapsed"]] / max(t_circe[["elapsed"]], 0.001)
  ))
  expect_true(cmp$identical, info = cmp$details)

  # Cleanup
  CDMConnector::dropTable(cdm, name = "cohort_circe")
  CDMConnector::dropTable(cdm, name = "cohort_dag")
})
