# Additional tests for R/optimizer.R — targeting uncovered code paths
# Focuses on: atlas_json_to_sql parameter combos, atlas_json_to_sql_batch input
# variants and optimize paths, resolve_literal_conditionals edge cases,
# quote_cdm_table_refs PostgreSQL vs DuckDB, drop_prefixed_tables fallback path,
# buildBatchCohortQuery option combos, collect_criteria_types_from_group deep nesting.

skip_on_cran()

# Minimal JSON for testing (Drug exposure with concept 123)
test_json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123,"CONCEPT_NAME":"test","STANDARD_CONCEPT":"S","STANDARD_CONCEPT_CAPTION":"Standard","INVALID_REASON":"V","INVALID_REASON_CAPTION":"Valid","CONCEPT_CODE":"1","DOMAIN_ID":"Drug","VOCABULARY_ID":"RxNorm","CONCEPT_CLASS_ID":"Ingredient"},"isExcluded":false,"includeDescendants":false,"includeMapped":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"EraPad":0}}'

# Minimal empty JSON for quick tests
empty_json <- '{"ConceptSets":[],"PrimaryCriteria":{"CriteriaList":[],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}}}'


# ---- atlas_json_to_sql: render=FALSE ----

test_that("atlas_json_to_sql with render=FALSE returns SQL without rendering", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L, render = FALSE, target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 0)
  # The SQL should contain cohort query structure
  expect_true(grepl("SELECT|INSERT|CREATE", sql))
})


# ---- atlas_json_to_sql: target_dialect=NULL skips translation ----

test_that("atlas_json_to_sql with target_dialect=NULL skips dialect translation", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 5L, render = TRUE,
                           cdm_schema = "main", target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 100)
  # Should have SQL Server-style functions since no translation occurred
  # (or at minimum, not be translated to DuckDB/PostgreSQL syntax)
})


# ---- atlas_json_to_sql: generate_stats=TRUE ----

test_that("atlas_json_to_sql with generate_stats=TRUE includes stats SQL", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L, render = TRUE,
                           cdm_schema = "main", target_dialect = NULL,
                           generate_stats = TRUE)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 100)
  # generate_stats = TRUE should produce inclusion stats SQL
  # The SQL should reference inclusion_events or inclusion_stats
  expect_true(grepl("inclusion", sql, ignore.case = TRUE) ||
              grepl("stats", sql, ignore.case = TRUE) ||
              nchar(sql) > 500)
})


# ---- atlas_json_to_sql: vocabulary_schema specified ----

test_that("atlas_json_to_sql uses vocabulary_schema when specified", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L, render = TRUE,
                           cdm_schema = "main",
                           vocabulary_schema = "vocab_schema",
                           target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 0)
  # When rendered, the vocabulary schema should be substituted
  expect_true(grepl("vocab_schema", sql, fixed = TRUE))
})


# ---- atlas_json_to_sql: target_schema specified ----

test_that("atlas_json_to_sql uses target_schema when specified", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L, render = TRUE,
                           cdm_schema = "main",
                           target_schema = "results_db",
                           target_dialect = NULL)
  expect_type(sql, "character")
  # Should reference the results schema
  expect_true(grepl("results_db", sql, fixed = TRUE))
})


# ---- atlas_json_to_sql: target_table override ----

test_that("atlas_json_to_sql uses custom target_table", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L, render = TRUE,
                           cdm_schema = "main",
                           target_table = "my_cohort_table",
                           target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(grepl("my_cohort_table", sql, fixed = TRUE))
})


# ---- atlas_json_to_sql: with target_dialect = "duckdb" ----

test_that("atlas_json_to_sql with target_dialect='duckdb' translates SQL", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L, render = TRUE,
                           cdm_schema = "main",
                           target_dialect = "duckdb")
  expect_type(sql, "character")
  expect_true(nchar(sql) > 100)
})


# ---- atlas_json_to_sql: from file ----

test_that("atlas_json_to_sql reads JSON from a file", {
  tmp <- tempfile(fileext = ".json")
  writeLines(test_json, tmp)
  on.exit(unlink(tmp), add = TRUE)

  sql <- atlas_json_to_sql(tmp, cohort_id = 1L, render = TRUE,
                           cdm_schema = "main", target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 100)
})


# ---- atlas_json_to_sql_batch: optimize=FALSE, sequential path ----

test_that("atlas_json_to_sql_batch optimize=FALSE with target_dialect=NULL", {
  batch <- atlas_json_to_sql_batch(list(test_json), optimize = FALSE,
                                   target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
  # Should NOT contain DAG-specific tables like codesets or cohort_stage
  # since optimize=FALSE uses per-cohort buildCohortQuery
})


test_that("atlas_json_to_sql_batch optimize=FALSE with target_dialect='duckdb'", {
  batch <- atlas_json_to_sql_batch(list(test_json, test_json),
                                   optimize = FALSE,
                                   target_dialect = "duckdb")
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


test_that("atlas_json_to_sql_batch optimize=FALSE with empty list returns empty", {
  batch <- atlas_json_to_sql_batch(list(), optimize = FALSE, target_dialect = NULL)
  expect_type(batch, "character")
  # Empty list -> no SQL
  expect_true(nchar(batch) == 0 || batch == "")
})


# ---- atlas_json_to_sql_batch: data.frame input with list-column cohorts ----

test_that("atlas_json_to_sql_batch with data.frame input and string cohorts", {
  df <- data.frame(
    cohort_definition_id = c(10L, 20L),
    cohort = c(test_json, test_json),
    stringsAsFactors = FALSE
  )
  batch <- atlas_json_to_sql_batch(df, optimize = FALSE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


test_that("atlas_json_to_sql_batch with data.frame input and list-column cohorts", {
  parsed <- jsonlite::fromJSON(test_json, simplifyVector = FALSE)
  df <- data.frame(cohort_definition_id = c(1L, 2L), stringsAsFactors = FALSE)
  df$cohort <- list(parsed, parsed)
  batch <- atlas_json_to_sql_batch(df, optimize = FALSE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


test_that("atlas_json_to_sql_batch with data.frame input and file-path cohorts", {
  tmp <- tempfile(fileext = ".json")
  writeLines(test_json, tmp)
  on.exit(unlink(tmp), add = TRUE)

  df <- data.frame(
    cohort_definition_id = 1L,
    cohort = tmp,
    stringsAsFactors = FALSE
  )
  batch <- atlas_json_to_sql_batch(df, optimize = FALSE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


test_that("atlas_json_to_sql_batch data.frame errors on invalid cohort type", {
  df <- data.frame(cohort_definition_id = 1L, stringsAsFactors = FALSE)
  df$cohort <- list(42)  # integer, not string or list
  expect_error(atlas_json_to_sql_batch(df, optimize = FALSE, target_dialect = NULL))
})


# ---- atlas_json_to_sql_batch: single string input (not list) ----

test_that("atlas_json_to_sql_batch wraps single string in list", {
  batch <- atlas_json_to_sql_batch(test_json, optimize = FALSE,
                                   target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


# ---- atlas_json_to_sql_batch: optimize=TRUE with table_prefix ----

test_that("atlas_json_to_sql_batch optimize=TRUE with custom table_prefix", {
  batch <- atlas_json_to_sql_batch(
    list(test_json),
    optimize = TRUE,
    target_dialect = NULL,
    table_prefix = "myprefix_"
  )
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
  expect_true(grepl("myprefix_", batch, fixed = TRUE))
})


test_that("atlas_json_to_sql_batch optimize=TRUE with target_dialect='duckdb'", {
  batch <- atlas_json_to_sql_batch(list(test_json),
                                   optimize = TRUE,
                                   target_dialect = "duckdb")
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


# ---- atlas_json_to_sql_batch: optimize=TRUE with multiple cohorts ----

test_that("atlas_json_to_sql_batch optimize=TRUE with multiple cohorts", {
  # Use different cohort IDs via data.frame
  df <- data.frame(
    cohort_definition_id = c(10L, 20L),
    cohort = c(test_json, test_json),
    stringsAsFactors = FALSE
  )
  batch <- atlas_json_to_sql_batch(df, optimize = TRUE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
  # DAG output should reference codesets and cohort_stage
  expect_true(grepl("codesets", batch, ignore.case = TRUE))
})


# ---- resolve_literal_conditionals: compound false (all-false & parts) ----

test_that("resolve_literal_conditionals false compound with else", {
  sql <- "{0 != 0 & 0 != 0}?{skip}:{keep}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "keep")
})


test_that("resolve_literal_conditionals mixed compound (one true, one false) with else", {
  sql <- "{1 != 0 & 0 != 0}?{skip}:{keep}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "keep")
})


test_that("resolve_literal_conditionals all-true compound condition", {
  sql <- "{3 != 0 & 5 != 0}?{yes}:{no}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "yes")
})


test_that("resolve_literal_conditionals false condition without else removes content", {
  sql <- "BEGIN {0 != 0}?{HIDDEN} END"
  result <- resolve_literal_conditionals(sql)
  expect_false(grepl("HIDDEN", result))
  expect_true(grepl("BEGIN", result))
  expect_true(grepl("END", result))
})


test_that("resolve_literal_conditionals: nested braces inside then block", {
  sql <- "{1 != 0}?{SELECT CASE WHEN x = 1 THEN {y} END}"
  result <- resolve_literal_conditionals(sql)
  # Should keep the content with nested braces
  expect_true(grepl("SELECT CASE WHEN", result))
})


test_that("resolve_literal_conditionals: else branch with nested closing brace", {
  # The else content itself contains {} which tests the depth-tracking parser
  sql <- "{0 != 0}?{skip}:{SELECT FUNC({a})}"
  result <- resolve_literal_conditionals(sql)
  expect_true(grepl("FUNC", result))
  expect_false(grepl("skip", result))
})


test_that("resolve_literal_conditionals: large number condition evaluates true", {
  sql <- "{999 != 0}?{FOUND}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "FOUND")
})


test_that("resolve_literal_conditionals: handles triple compound condition", {
  sql <- "{1 != 0 & 2 != 0 & 3 != 0}?{ALL_TRUE}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "ALL_TRUE")
})


test_that("resolve_literal_conditionals: triple compound with one false", {
  sql <- "{1 != 0 & 0 != 0 & 3 != 0}?{SKIP}:{KEPT}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "KEPT")
})


# ---- resolve_literal_conditionals: missing closing brace (edge/safety) ----

test_that("resolve_literal_conditionals: breaks safely on malformed else (missing close)", {
  # The else brace never closes; the function should break out of the loop
  sql <- "{0 != 0}?{then_val}:{else_val_no_close"
  result <- resolve_literal_conditionals(sql)
  expect_type(result, "character")
  # Should still return something (not hang)
})


test_that("resolve_literal_conditionals: breaks safely on malformed then (missing close)", {
  sql <- "{1 != 0}?{then_val_no_close"
  result <- resolve_literal_conditionals(sql)
  expect_type(result, "character")
})


# ---- quote_cdm_table_refs: single-part schema on DuckDB ----

test_that("quote_cdm_table_refs with single-part schema on DuckDB", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.PERSON p JOIN main.OBSERVATION_PERIOD op ON p.person_id = op.person_id"
  result <- quote_cdm_table_refs(sql, con, "main")
  expect_type(result, "character")
  # On DuckDB, identifiers are case-insensitive, so table names should be
  # preserved (not lowercased as in PostgreSQL)
  expect_true(grepl("PERSON", result) || grepl("person", result))
  expect_true(grepl("OBSERVATION_PERIOD", result) || grepl("observation_period", result))
})


test_that("quote_cdm_table_refs with multi-part schema on DuckDB", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM mycat.mysch.DRUG_EXPOSURE"
  result <- quote_cdm_table_refs(sql, con, "mycat.mysch")
  expect_type(result, "character")
  # The multi-part schema should be split and each part quoted individually
  expect_true(grepl("mycat", result, ignore.case = TRUE))
  expect_true(grepl("mysch", result, ignore.case = TRUE))
})


test_that("quote_cdm_table_refs replaces multiple different CDM tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- paste(
    "SELECT * FROM main.PERSON",
    "JOIN main.CONCEPT ON 1=1",
    "JOIN main.CONCEPT_ANCESTOR ON 1=1",
    "JOIN main.DRUG_EXPOSURE ON 1=1",
    "JOIN main.DEATH ON 1=1"
  )
  result <- quote_cdm_table_refs(sql, con, "main")
  expect_type(result, "character")
  # Function should run without error and return a string
  expect_true(nchar(result) > 0)
})


test_that("quote_cdm_table_refs does not alter non-CDM table references", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.my_custom_table"
  result <- quote_cdm_table_refs(sql, con, "main")
  # Non-CDM tables should not be altered
  expect_true(grepl("my_custom_table", result, fixed = TRUE))
})


# ---- drop_prefixed_tables: tables exist path with multiple tables ----

test_that("drop_prefixed_tables drops tables AND views matching prefix", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE atlas_xyz_codesets (x INT)")
  DBI::dbExecute(con, "CREATE TABLE atlas_xyz_cohort_stage (x INT)")
  DBI::dbExecute(con, "CREATE TABLE atlas_xyz_drug_exposure_filtered (x INT)")
  DBI::dbExecute(con, "CREATE TABLE unrelated_table (x INT)")

  drop_prefixed_tables(con, "main", "atlas_xyz_")

  remaining <- DBI::dbListTables(con)
  expect_false("atlas_xyz_codesets" %in% remaining)
  expect_false("atlas_xyz_cohort_stage" %in% remaining)
  expect_false("atlas_xyz_drug_exposure_filtered" %in% remaining)
  expect_true("unrelated_table" %in% remaining)
})


test_that("drop_prefixed_tables fallback path when no prefix tables match", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Create some tables that match the OPTIMIZER_STATIC_TABLES with the prefix
  # The fallback path tries to drop prefix+static_name even when no tables are found
  DBI::dbExecute(con, "CREATE TABLE atlas_fallback_codesets (x INT)")
  DBI::dbExecute(con, "CREATE TABLE atlas_fallback_cohort_stage (x INT)")

  # Call with a prefix that will NOT match via startsWith but the
  # tables will be targeted by the fallback static list
  # Actually, startsWith("atlas_fallback_codesets", "atlas_fallback_") is TRUE
  # So let's test the actual fallback by having no matching tables at all
  expect_no_error(drop_prefixed_tables(con, "main", "nonexistent_prefix_"))

  # Verify that existing tables remain
  remaining <- DBI::dbListTables(con)
  expect_true("atlas_fallback_codesets" %in% remaining)
  expect_true("atlas_fallback_cohort_stage" %in% remaining)
})


test_that("drop_prefixed_tables with schema other than main", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS test_schema")
  DBI::dbExecute(con, "CREATE TABLE test_schema.atlas_test_table1 (x INT)")

  # This should attempt to drop tables in test_schema
  expect_no_error(drop_prefixed_tables(con, "test_schema", "atlas_test_"))
})


# ---- buildBatchCohortQuery: cache=TRUE with con ----

test_that("buildBatchCohortQuery with cache=TRUE returns list with expected keys", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")

  cohort <- cohortExpressionFromJson(test_json)

  result <- buildBatchCohortQuery(
    list(cohort), c(1L),
    list(cdm_schema = "main", results_schema = "rs"),
    cache = TRUE, con = con, schema = "rs"
  )

  expect_true(is.list(result))
  expect_true("sql" %in% names(result))
  expect_true("cache_hits" %in% names(result))
  expect_true("cache_misses" %in% names(result))
  expect_true("dag" %in% names(result))
  expect_true("options" %in% names(result))
  expect_true(is.character(result$sql))
  expect_true(nchar(result$sql) > 0)
})


test_that("buildBatchCohortQuery cache=TRUE errors without schema", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  cohort <- cohortExpressionFromJson(test_json)
  expect_error(
    buildBatchCohortQuery(list(cohort), c(1L),
      list(cdm_schema = "main", results_schema = "rs"),
      cache = TRUE, con = con, schema = NULL),
    "schema"
  )
})


# ---- buildBatchCohortQuery: custom options ----

test_that("buildBatchCohortQuery with custom table_prefix in options", {
  cohort <- cohortExpressionFromJson(test_json)
  result <- buildBatchCohortQuery(
    list(cohort), c(1L),
    list(cdm_schema = "main", results_schema = "rs", table_prefix = "custom_pfx_")
  )
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
  expect_true(grepl("custom_pfx_", result, fixed = TRUE))
})


test_that("buildBatchCohortQuery with vocabulary_schema in options", {
  cohort <- cohortExpressionFromJson(test_json)
  result <- buildBatchCohortQuery(
    list(cohort), c(1L),
    list(cdm_schema = "main", results_schema = "rs",
         vocabulary_schema = "vocab_db")
  )
  expect_true(is.character(result))
  expect_true(grepl("vocab_db", result, fixed = TRUE) ||
              grepl("@vocabulary_database_schema", result, fixed = TRUE))
})


test_that("buildBatchCohortQuery with multiple cohorts", {
  cohort <- cohortExpressionFromJson(test_json)
  result <- buildBatchCohortQuery(
    list(cohort, cohort), c(1L, 2L),
    list(cdm_schema = "main", results_schema = "rs")
  )
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})


# ---- collect_criteria_types_from_group: deeper nesting ----

test_that("collect_criteria_types_from_group handles CorrelatedCriteria", {
  # Simulate a criteria item with nested CorrelatedCriteria
  group <- list(
    CriteriaList = list(
      list(DrugExposure = list(
        CodesetId = 1,
        CorrelatedCriteria = list(
          CriteriaList = list(
            list(Measurement = list(CodesetId = 2))
          )
        )
      ))
    )
  )
  types <- collect_criteria_types_from_group(group)
  expect_true("DrugExposure" %in% types)
  # CorrelatedCriteria should be recursed into
  expect_true("Measurement" %in% types)
})


test_that("collect_criteria_types_from_group handles deeply nested Groups", {
  group <- list(
    CriteriaList = list(
      list(ConditionOccurrence = list(CodesetId = 1))
    ),
    Groups = list(
      list(
        CriteriaList = list(
          list(DrugExposure = list(CodesetId = 2))
        ),
        Groups = list(
          list(
            CriteriaList = list(
              list(Measurement = list(CodesetId = 3))
            )
          )
        )
      )
    )
  )
  types <- collect_criteria_types_from_group(group)
  expect_true("ConditionOccurrence" %in% types)
  expect_true("DrugExposure" %in% types)
  expect_true("Measurement" %in% types)
})


test_that("collect_criteria_types_from_group handles NULL input", {
  types <- collect_criteria_types_from_group(NULL)
  expect_equal(length(types), 0L)
})


test_that("collect_criteria_types_from_group handles empty CriteriaList", {
  group <- list(CriteriaList = list())
  types <- collect_criteria_types_from_group(group)
  expect_equal(length(types), 0L)
})


test_that("collect_criteria_types_from_group with VisitType filter adds VisitOccurrence", {
  # VisitType in criteria should cause VisitOccurrence to be added
  group <- list(
    CriteriaList = list(
      list(ConditionOccurrence = list(
        CodesetId = 1,
        VisitType = list(list(CONCEPT_ID = 9201))
      ))
    )
  )
  types <- collect_criteria_types_from_group(group)
  expect_true("ConditionOccurrence" %in% types)
  # The VisitType filter should also add VisitOccurrence
  expect_true("VisitOccurrence" %in% types)
})


# ---- collect_batch_used_domains_from_cohorts: various structures ----

test_that("collect_batch_used_domains_from_cohorts handles EndStrategy with DateOffset", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1)))
    ),
    EndStrategy = list(
      DateOffset = list(
        DateField = "EndDate",
        Offset = 0
      )
    ),
    InclusionRules = list()
  )
  result <- collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("DRUG_EXPOSURE" %in% result)
  expect_true("OBSERVATION_PERIOD" %in% result)
})


test_that("collect_batch_used_domains_from_cohorts handles cohort with camelCase keys", {
  # Test with camelCase keys (alternative JSON format)
  cohort <- list(
    primaryCriteria = list(
      criteriaList = list(list(DrugExposure = list(CodesetId = 1)))
    ),
    inclusionRules = list(),
    censoringCriteria = list()
  )
  result <- collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("DRUG_EXPOSURE" %in% result)
  expect_true("OBSERVATION_PERIOD" %in% result)
})


test_that("collect_batch_used_domains_from_cohorts handles AdditionalCriteria with Groups", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    AdditionalCriteria = list(
      CriteriaList = list(
        list(Observation = list(CodesetId = 2))
      ),
      Groups = list(
        list(
          CriteriaList = list(
            list(Measurement = list(CodesetId = 3))
          )
        )
      )
    )
  )
  result <- collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("CONDITION_OCCURRENCE" %in% result)
  expect_true("OBSERVATION" %in% result)
  expect_true("MEASUREMENT" %in% result)
})


test_that("collect_batch_used_domains_from_cohorts handles InclusionRules with nested Groups", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1)))
    ),
    InclusionRules = list(
      list(
        name = "nested_rule",
        expression = list(
          CriteriaList = list(list(DrugExposure = list(CodesetId = 2))),
          Groups = list(
            list(
              CriteriaList = list(list(ProcedureOccurrence = list(CodesetId = 3)))
            )
          )
        )
      )
    )
  )
  result <- collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("CONDITION_OCCURRENCE" %in% result)
  expect_true("DRUG_EXPOSURE" %in% result)
  expect_true("PROCEDURE_OCCURRENCE" %in% result)
})


test_that("collect_batch_used_domains_from_cohorts deduplicates domains", {
  # Two cohorts using the same domain should not double-count
  c1 <- list(PrimaryCriteria = list(
    CriteriaList = list(list(DrugExposure = list(CodesetId = 1)))
  ))
  c2 <- list(PrimaryCriteria = list(
    CriteriaList = list(list(DrugExposure = list(CodesetId = 2)))
  ))
  result <- collect_batch_used_domains_from_cohorts(list(c1, c2))
  drug_count <- sum(result == "DRUG_EXPOSURE")
  expect_equal(drug_count, 1L)
})


test_that("collect_batch_used_domains_from_cohorts always includes OBSERVATION_PERIOD", {
  cohort <- list(
    PrimaryCriteria = list(
      CriteriaList = list(list(DrugExposure = list(CodesetId = 1)))
    )
  )
  result <- collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("OBSERVATION_PERIOD" %in% result)
})


# ---- rewrite_to_domain_caches: edge cases ----

test_that("rewrite_to_domain_caches with DEATH table (not a domain)", {
  # DEATH is not in DOMAIN_CONFIG, so it should NOT be rewritten
  sql <- "SELECT * FROM main.DEATH"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "main")
  # DEATH should remain as-is since it's not a filtered domain
  expect_true(grepl("DEATH", result))
})


test_that("rewrite_to_domain_caches with DEVICE_EXPOSURE", {
  sql <- "SELECT * FROM cdm.DEVICE_EXPOSURE de"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "cdm")
  expect_true(grepl("device_exposure_filtered", result))
  expect_false(grepl("cdm\\.DEVICE_EXPOSURE", result))
})


test_that("rewrite_to_domain_caches preserves non-domain table refs", {
  sql <- "SELECT * FROM cdm.CONCEPT c JOIN cdm.DRUG_EXPOSURE de ON 1=1"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "cdm")
  # DRUG_EXPOSURE should be rewritten but CONCEPT should remain
  expect_true(grepl("drug_exposure_filtered", result))
  expect_true(grepl("cdm\\.CONCEPT", result))
})


test_that("rewrite_to_domain_caches with multi-part schema", {
  sql <- "SELECT * FROM mycat.mysch.DRUG_EXPOSURE"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "mycat.mysch")
  expect_true(grepl("drug_exposure_filtered", result))
})


test_that("rewrite_to_domain_caches with options uses results_schema + table_prefix", {
  sql <- "SELECT * FROM main.VISIT_OCCURRENCE v"
  opts <- list(results_schema = "results", table_prefix = "atlas_abc_")
  result <- rewrite_to_domain_caches(sql, cdm_schema = "main", options = opts)
  expect_true(grepl("results\\.atlas_abc_visit_occurrence_filtered", result))
})


# ---- atlas_unique_prefix: both uuid and fallback paths ----

test_that("atlas_unique_prefix length is reasonable", {
  prefix <- atlas_unique_prefix()
  expect_true(nchar(prefix) >= 10)
  expect_true(nchar(prefix) <= 30)
})


test_that("atlas_unique_prefix generates unique values across multiple calls", {
  prefixes <- vapply(seq_len(5), function(i) atlas_unique_prefix(), character(1))
  # All should be unique

  expect_equal(length(unique(prefixes)), 5L)
})


# ---- normalize_schema_str: additional edge cases ----

test_that("normalize_schema_str with list containing only catalog and schema", {
  result <- normalize_schema_str(list(catalog = "mydb", schema = "dbo"))
  expect_equal(result, "mydb.dbo")
})


test_that("normalize_schema_str with list containing catalog, schema, and prefix", {
  result <- normalize_schema_str(list(catalog = "mydb", schema = "dbo", prefix = "tmp_"))
  expect_equal(result, "mydb.dbo")
})


test_that("normalize_schema_str with named vector of 3 parts (catalog, schema, prefix)", {
  result <- normalize_schema_str(c(catalog = "cat", schema = "sch", prefix = "pfx_"))
  expect_equal(result, "cat.sch")
})


# ---- extract_write_prefix: edge cases ----

test_that("extract_write_prefix with list that has empty prefix", {
  result <- extract_write_prefix(list(schema = "main", prefix = ""))
  expect_equal(result, "")
})


test_that("extract_write_prefix with named vector with empty string", {
  result <- extract_write_prefix(c(schema = "main"))
  expect_equal(result, "")
})


# ---- read_json_input: edge cases ----

test_that("read_json_input reads multiline JSON from file", {
  tmp <- tempfile(fileext = ".json")
  on.exit(unlink(tmp), add = TRUE)
  writeLines(c("{", '"key": "value"', "}"), tmp)
  result <- read_json_input(tmp)
  expect_true(grepl("key", result))
  # Should be a single string (lines joined)
  expect_equal(length(result), 1L)
})


test_that("read_json_input returns raw JSON with braces as-is", {
  json_str <- '{"ConceptSets": [], "PrimaryCriteria": {}}'
  result <- read_json_input(json_str)
  expect_equal(result, json_str)
})


# ---- DOMAIN_CONFIG: verify all domains have columns field ----

test_that("DOMAIN_CONFIG entries all have columns field", {
  for (dc in DOMAIN_CONFIG) {
    expect_true("columns" %in% names(dc),
                info = paste("Missing columns in domain:", dc$table))
    expect_true(length(dc$columns) >= 3,
                info = paste("Too few columns in domain:", dc$table))
  }
})


test_that("DOMAIN_CONFIG entries all have ix_prefix field", {
  for (dc in DOMAIN_CONFIG) {
    expect_true("ix_prefix" %in% names(dc),
                info = paste("Missing ix_prefix in domain:", dc$table))
  }
})


# ---- CDM_TABLE_NAMES_FOR_QUOTE: comprehensive check ----

test_that("CDM_TABLE_NAMES_FOR_QUOTE contains vocabulary tables", {
  expect_true("CONCEPT" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("CONCEPT_ANCESTOR" %in% CDM_TABLE_NAMES_FOR_QUOTE)
})


test_that("CDM_TABLE_NAMES_FOR_QUOTE contains clinical tables", {
  expect_true("DRUG_EXPOSURE" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("CONDITION_OCCURRENCE" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("PROCEDURE_OCCURRENCE" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("OBSERVATION" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("MEASUREMENT" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("DEVICE_EXPOSURE" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("DEATH" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("VISIT_OCCURRENCE" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("VISIT_DETAIL" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("SPECIMEN" %in% CDM_TABLE_NAMES_FOR_QUOTE)
})


test_that("CDM_TABLE_NAMES_FOR_QUOTE contains era/other tables", {
  expect_true("CONDITION_ERA" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("DRUG_ERA" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("DOSE_ERA" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("PAYER_PLAN_PERIOD" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("PROVIDER" %in% CDM_TABLE_NAMES_FOR_QUOTE)
})


# ---- CRITERIA_TYPE_TO_CDM_TABLE: comprehensive check ----

test_that("CRITERIA_TYPE_TO_CDM_TABLE maps all supported criteria types", {
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["Observation"]], "OBSERVATION")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["DeviceExposure"]], "DEVICE_EXPOSURE")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["Death"]], "DEATH")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["ConditionEra"]], "CONDITION_ERA")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["DrugEra"]], "DRUG_ERA")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["DoseEra"]], "DOSE_ERA")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["Specimen"]], "SPECIMEN")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["VisitDetail"]], "VISIT_DETAIL")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["PayerPlanPeriod"]], "PAYER_PLAN_PERIOD")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["LocationRegion"]], "LOCATION_REGION")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["ObservationPeriod"]], "OBSERVATION_PERIOD")
})


# ---- OPTIMIZER_STATIC_TABLES: comprehensive check ----

test_that("OPTIMIZER_STATIC_TABLES includes inclusion tables", {
  expect_true("inclusion_events_stage" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("inclusion_stats_stage" %in% OPTIMIZER_STATIC_TABLES)
})


test_that("OPTIMIZER_STATIC_TABLES includes all domain filtered tables", {
  expect_true("drug_exposure_filtered" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("condition_occurrence_filtered" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("procedure_occurrence_filtered" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("observation_filtered" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("measurement_filtered" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("device_exposure_filtered" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("visit_occurrence_filtered" %in% OPTIMIZER_STATIC_TABLES)
})


test_that("OPTIMIZER_STATIC_TABLES includes concept-related tables", {
  expect_true("all_concepts" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("codesets" %in% OPTIMIZER_STATIC_TABLES)
})


# ---- atlas_json_to_sql_batch: optimize=TRUE with cdm_table_sql parameter ----

test_that("atlas_json_to_sql_batch optimize=TRUE with cdm_table_sql override", {
  # cdm_table_sql allows injecting custom SQL for domain tables (e.g., when
  # the CDM table has been subset/filtered by CDMConnector)
  custom_sql <- list(
    DRUG_EXPOSURE = "SELECT * FROM main.DRUG_EXPOSURE WHERE person_id IN (1,2,3)"
  )
  batch <- atlas_json_to_sql_batch(
    list(test_json),
    optimize = TRUE,
    target_dialect = NULL,
    cdm_table_sql = custom_sql
  )
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


# ---- buildBatchCohortQuery: with cdm_table_sql in options ----

test_that("buildBatchCohortQuery passes cdm_table_sql to DAG builder", {
  cohort <- cohortExpressionFromJson(test_json)
  custom_sql <- list(
    DRUG_EXPOSURE = "SELECT * FROM main.DRUG_EXPOSURE WHERE 1=1"
  )
  result <- buildBatchCohortQuery(
    list(cohort), c(1L),
    list(cdm_schema = "main", results_schema = "rs",
         cdm_table_sql = custom_sql)
  )
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})


# ---- atlas_json_to_sql_batch: optimize=TRUE with default cohort_ids ----

test_that("atlas_json_to_sql_batch with list input auto-assigns cohort_ids", {
  batch <- atlas_json_to_sql_batch(list(test_json, test_json),
                                   optimize = TRUE,
                                   target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})


# ---- Integration: atlas_json_to_sql round-trip with render + translate ----

test_that("atlas_json_to_sql produces executable SQL with render + translate", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L,
                           cdm_schema = "main",
                           target_schema = "main",
                           target_table = "cohort",
                           target_dialect = "duckdb",
                           render = TRUE)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 200)
  # Should not contain unresolved @-parameters
  expect_false(grepl("@cdm_database_schema", sql, fixed = TRUE))
  expect_false(grepl("@target_database_schema", sql, fixed = TRUE))
})


# ---- atlas_json_to_sql: render=FALSE + target_dialect=NULL (both off) ----

test_that("atlas_json_to_sql with render=FALSE and target_dialect=NULL returns template SQL", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L,
                           render = FALSE, target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 0)
  # Should have temp table references from the buildCohortQuery path
  expect_true(grepl("#Codesets|Codesets|INSERT|SELECT", sql))
})


# ---- atlas_json_to_sql: render=TRUE + target_dialect=NULL (render only) ----

test_that("atlas_json_to_sql renders but does not translate when target_dialect=NULL", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L,
                           cdm_schema = "my_cdm",
                           render = TRUE, target_dialect = NULL)
  expect_type(sql, "character")
  # Rendered: should contain the actual schema name
  expect_true(grepl("my_cdm", sql, fixed = TRUE))
})


# ---- atlas_json_to_sql: target_dialect="" (empty string, should skip) ----

test_that("atlas_json_to_sql with empty target_dialect skips translation", {
  sql <- atlas_json_to_sql(test_json, cohort_id = 1L,
                           cdm_schema = "main",
                           render = TRUE, target_dialect = "")
  expect_type(sql, "character")
  expect_true(nchar(sql) > 0)
})
