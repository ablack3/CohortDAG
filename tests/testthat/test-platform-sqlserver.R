skip_if_not_tier(3L)

test_that("Platform SQL Server: CIRCE vs DAG produce identical cohort rows", {
  skip_on_cran()
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("odbc")
  skip_if_not_installed("dplyr")
  library(CDMConnector)
  library(dplyr)

  # Required env vars
  server       <- Sys.getenv("CDM5_SQL_SERVER_SERVER", unset = "")
  user         <- Sys.getenv("CDM5_SQL_SERVER_USER", unset = "")
  password     <- Sys.getenv("CDM5_SQL_SERVER_PASSWORD", unset = "")
  cdm_database <- Sys.getenv("CDM5_SQL_SERVER_CDM_DATABASE", unset = "")
  cdm_schema   <- Sys.getenv("CDM5_SQL_SERVER_CDM_SCHEMA", unset = "")
  scratch      <- Sys.getenv("CDM5_SQL_SERVER_SCRATCH_SCHEMA", unset = "")

  skip_if(server == "",       "CDM5_SQL_SERVER_SERVER not set")
  skip_if(user == "",         "CDM5_SQL_SERVER_USER not set")
  skip_if(password == "",     "CDM5_SQL_SERVER_PASSWORD not set")
  skip_if(cdm_database == "", "CDM5_SQL_SERVER_CDM_DATABASE not set")
  skip_if(cdm_schema == "",   "CDM5_SQL_SERVER_CDM_SCHEMA not set")
  skip_if(scratch == "",      "CDM5_SQL_SERVER_SCRATCH_SCHEMA not set")

  # Optional env vars
  driver <- Sys.getenv("SQL_SERVER_DRIVER", unset = "ODBC Driver 17 for SQL Server")
  port   <- Sys.getenv("CDM5_SQL_SERVER_PORT", unset = "")

  # Connect
  connect_args <- list(
    drv      = odbc::odbc(),
    driver   = driver,
    server   = server,
    database = cdm_database,
    uid      = user,
    pwd      = password,
    TrustServerCertificate = "yes"
  )
  if (nchar(port) > 0L) connect_args$port <- as.integer(port)

  con <- do.call(DBI::dbConnect, connect_args)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # CDM reference
  cdm <- CDMConnector::cdmFromCon(
    con,
    cdmSchema   = c(catalog = cdm_database, schema = cdm_schema),
    writeSchema = c(catalog = cdm_database, schema = scratch)
  )

  # Force dialect
  withr::local_options(list(CohortDAG.force_target_dialect = "sql server"))

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
  message("[SQL Server] ", cmp$details)
  message(sprintf(
    "[SQL Server] Timing: CIRCE=%.1fs, DAG=%.1fs, ratio=%.2f",
    t_circe[["elapsed"]], t_dag[["elapsed"]],
    t_dag[["elapsed"]] / max(t_circe[["elapsed"]], 0.001)
  ))
  expect_true(cmp$identical, info = cmp$details)

  # Cleanup
  CDMConnector::dropTable(cdm, name = "cohort_circe")
  CDMConnector::dropTable(cdm, name = "cohort_dag")
})
