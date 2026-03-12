skip_if_not_tier(3L)

test_that("Platform Databricks: CIRCE vs DAG produce identical cohort rows", {
  skip_on_cran()
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("odbc")
  skip_if_not_installed("dplyr")
  library(CDMConnector)
  library(dplyr)

  # Required env vars
  host     <- Sys.getenv("DATABRICKS_HOST", unset = "")
  httppath <- Sys.getenv("DATABRICKS_HTTPPATH", unset = "")
  token    <- Sys.getenv("DATABRICKS_TOKEN", unset = "")
  scratch  <- Sys.getenv("DATABRICKS_SCRATCH_SCHEMA", unset = "")

  skip_if(host == "",     "DATABRICKS_HOST not set")
  skip_if(httppath == "", "DATABRICKS_HTTPPATH not set")
  skip_if(token == "",    "DATABRICKS_TOKEN not set")
  skip_if(scratch == "",  "DATABRICKS_SCRATCH_SCHEMA not set")

  # Optional env vars
  cdm_schema <- Sys.getenv("DATABRICKS_CDM_SCHEMA", unset = "cdm")
  driver     <- Sys.getenv("DATABRICKS_DRIVER", unset = "")

  # Connect via odbc::databricks()
  if (nchar(driver) > 0L) {
    con <- DBI::dbConnect(
      odbc::odbc(),
      driver          = driver,
      host            = host,
      HTTPPath        = httppath,
      AuthMech        = 3L,
      UID             = "token",
      PWD             = token,
      ThriftTransport = 2L,
      SSL             = 1L
    )
  } else {
    con <- DBI::dbConnect(
      odbc::databricks(),
      host     = host,
      httpPath = httppath,
      token    = token
    )
  }
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # CDM reference
  cdm <- CDMConnector::cdmFromCon(
    con,
    cdmSchema   = cdm_schema,
    writeSchema = scratch
  )

  # Force dialect
  withr::local_options(list(CohortDAG.force_target_dialect = "spark"))

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
  message("[Databricks] ", cmp$details)
  message(sprintf(
    "[Databricks] Timing: CIRCE=%.1fs, DAG=%.1fs, ratio=%.2f",
    t_circe[["elapsed"]], t_dag[["elapsed"]],
    t_dag[["elapsed"]] / max(t_circe[["elapsed"]], 0.001)
  ))
  expect_true(cmp$identical, info = cmp$details)

  # Cleanup
  CDMConnector::dropTable(cdm, name = "cohort_circe")
  CDMConnector::dropTable(cdm, name = "cohort_dag")
})
