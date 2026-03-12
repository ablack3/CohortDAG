skip_if_not_tier(3L)

test_that("Platform Redshift: CIRCE vs DAG produce identical cohort rows", {
  skip_on_cran()
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("RPostgres")
  skip_if_not_installed("dplyr")
  library(CDMConnector)
  library(dplyr)

  # Required env vars
  host     <- Sys.getenv("CDM5_REDSHIFT_HOST", unset = "")
  user     <- Sys.getenv("CDM5_REDSHIFT_USER", unset = "")
  password <- Sys.getenv("CDM5_REDSHIFT_PASSWORD", unset = "")
  dbname   <- Sys.getenv("CDM5_REDSHIFT_DBNAME", unset = "")
  scratch  <- Sys.getenv("CDM5_REDSHIFT_SCRATCH_SCHEMA", unset = "")

  skip_if(host == "",     "CDM5_REDSHIFT_HOST not set")
  skip_if(user == "",     "CDM5_REDSHIFT_USER not set")
  skip_if(password == "", "CDM5_REDSHIFT_PASSWORD not set")
  skip_if(dbname == "",   "CDM5_REDSHIFT_DBNAME not set")
  skip_if(scratch == "",  "CDM5_REDSHIFT_SCRATCH_SCHEMA not set")

  # Optional env vars
  port       <- Sys.getenv("CDM5_REDSHIFT_PORT", unset = "5439")
  cdm_schema <- Sys.getenv("CDM5_REDSHIFT_CDM_SCHEMA", unset = "cdm")

  # Connect (Redshift uses the PostgreSQL wire protocol via RPostgres)
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    host     = host,
    port     = as.integer(port),
    user     = user,
    password = password,
    dbname   = dbname,
    sslmode  = "require"
  )
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # CDM reference
  cdm <- CDMConnector::cdmFromCon(
    con,
    cdmSchema   = cdm_schema,
    writeSchema = scratch
  )

  # Force dialect
  withr::local_options(list(CohortDAG.force_target_dialect = "redshift"))

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
  message("[Redshift] ", cmp$details)
  message(sprintf(
    "[Redshift] Timing: CIRCE=%.1fs, DAG=%.1fs, ratio=%.2f",
    t_circe[["elapsed"]], t_dag[["elapsed"]],
    t_dag[["elapsed"]] / max(t_circe[["elapsed"]], 0.001)
  ))
  expect_true(cmp$identical, info = cmp$details)

  # Cleanup
  CDMConnector::dropTable(cdm, name = "cohort_circe")
  CDMConnector::dropTable(cdm, name = "cohort_dag")
})
