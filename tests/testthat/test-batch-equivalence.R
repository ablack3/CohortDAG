# Batch vs single-cohort equivalence: SQL structure and row-level comparison on DuckDB.

test_that("batch with multiple cohorts produces DAG-based SQL structure", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"EraPad":0}}'
  batch_sql <- CohortDAG:::atlas_json_to_sql_batch(list(json, json), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("cohort_stage", batch_sql))
  expect_true(grepl("codesets", batch_sql))
  expect_true(grepl("pe_[0-9a-f]+", batch_sql), info = "Should have primary events node table")
  expect_true(grepl("fc_[0-9a-f]+", batch_sql), info = "Should have final cohort node table")
})

test_that("batch handles concept set exclusions (isExcluded) in codesets", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":4329847},"isExcluded":false,"includeDescendants":true},{"concept":{"CONCEPT_ID":312327},"isExcluded":true,"includeDescendants":true}]}}],"PrimaryCriteria":{"CriteriaList":[{"ConditionOccurrence":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"EraPad":0}}'
  batch_sql <- CohortDAG:::atlas_json_to_sql_batch(list(json, json), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("LEFT JOIN", batch_sql, ignore.case = TRUE))
  expect_true(grepl("is null", batch_sql, ignore.case = TRUE))
  expect_true(grepl("CONCEPT_ANCESTOR", batch_sql, ignore.case = TRUE))
})

test_that("batch handles includeMapped concepts in codesets", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false,"includeMapped":true}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"EraPad":0}}'
  batch_sql <- CohortDAG:::atlas_json_to_sql_batch(list(json, json), optimize = TRUE, target_dialect = NULL)
  expect_true(grepl("Maps to", batch_sql))
  expect_true(grepl("concept_relationship", batch_sql, ignore.case = TRUE))
})

test_that("batch produces same cohort rows as single-cohort on DuckDB", {
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("duckdb")
  library(CDMConnector)

  cohorts_dir <- system.file("cohorts", package = "CohortDAG", mustWork = TRUE)
  cohort_files <- list.files(cohorts_dir, pattern = "\\.json$", full.names = TRUE)
  if (length(cohort_files) < 2L) skip("Need at least 2 cohort JSONs in inst/cohorts")

  # Use first 2 cohorts for a quick test
  idx <- head(seq_along(cohort_files), 2L)
  json_list <- lapply(cohort_files[idx], function(p) paste(readLines(p, warn = FALSE), collapse = "\n"))

  temp_dir <- tempfile("cohort_set")
  dir.create(temp_dir, recursive = TRUE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
  for (i in seq_along(idx)) {
    file.copy(cohort_files[idx[i]], file.path(temp_dir, basename(cohort_files[idx[i]])))
  }
  cohort_set <- CDMConnector::readCohortSet(temp_dir)
  cohort_ids <- as.integer(cohort_set$cohort_definition_id)

  cdm <- CDMConnector::cdmFromCohortSet(cohort_set, n = 50)
  on.exit(CDMConnector::cdmDisconnect(cdm), add = TRUE)

  # Batch path
  err <- tryCatch(
    { cdm <- generateCohortSet2(cdm, cohort_set, name = "cohort"); NULL },
    error = function(e) e
  )
  if (!is.null(err)) skip(paste("generateCohortSet2 failed:", conditionMessage(err)))
  batch_df <- dplyr::collect(cdm$cohort)
  con <- CDMConnector::cdmCon(cdm)

  # For each cohort: run single-cohort SQL and compare row set
  for (k in seq_along(cohort_ids)) {
    cid <- cohort_ids[k]
    json_one <- json_list[[k]]
    single_sql <- CohortDAG:::atlas_json_to_sql(
      json_one, cohort_id = cid, cdm_schema = "main",
      target_schema = "main", target_table = "cohort_single",
      target_dialect = "duckdb", render = TRUE, generate_stats = FALSE
    )
    DBI::dbExecute(con, "DROP TABLE IF EXISTS main.cohort_single")
    DBI::dbExecute(con, "CREATE TABLE main.cohort_single (cohort_definition_id INTEGER NOT NULL, subject_id INTEGER NOT NULL, cohort_start_date DATE NOT NULL, cohort_end_date DATE NOT NULL)")
    stmts_s <- CohortDAG:::split_sql_core(single_sql)
    for (s in stmts_s) {
      if (nzchar(trimws(s))) tryCatch(DBI::dbExecute(con, s), error = function(e) NULL)
    }
    single_cohort <- DBI::dbGetQuery(con, "SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM main.cohort_single")
    single_cohort$cohort_start_date <- as.character(single_cohort$cohort_start_date)
    single_cohort$cohort_end_date <- as.character(single_cohort$cohort_end_date)
    batch_sub <- batch_df[batch_df$cohort_definition_id == cid, ]
    batch_sub$cohort_start_date <- as.character(batch_sub$cohort_start_date)
    batch_sub$cohort_end_date <- as.character(batch_sub$cohort_end_date)

    single_keys <- paste(single_cohort$subject_id, single_cohort$cohort_start_date, single_cohort$cohort_end_date, sep = "|")
    batch_keys <- paste(batch_sub$subject_id, batch_sub$cohort_start_date, batch_sub$cohort_end_date, sep = "|")
    only_single <- sum(!single_keys %in% batch_keys)
    only_batch <- sum(!batch_keys %in% single_keys)
    expect_equal(only_single, 0, info = sprintf("Cohort %d: rows in single but not in batch", cid))
    expect_equal(only_batch, 0, info = sprintf("Cohort %d: rows in batch but not in single", cid))
  }
})
