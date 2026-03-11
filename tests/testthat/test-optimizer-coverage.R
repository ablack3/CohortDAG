# Tests for R/optimizer.R
# Mix of pure function tests and DuckDB integration tests.

# ---- DOMAIN_CONFIG ----

test_that("DOMAIN_CONFIG is a list of domain configurations", {
  expect_true(is.list(DOMAIN_CONFIG))
  expect_true(length(DOMAIN_CONFIG) >= 7L)
  for (dc in DOMAIN_CONFIG) {
    expect_true("table" %in% names(dc))
    expect_true("alias" %in% names(dc))
    expect_true("std_col" %in% names(dc))
    expect_true("src_col" %in% names(dc))
    expect_true("filtered" %in% names(dc))
  }
})

test_that("DOMAIN_CONFIG contains expected domains", {
  tables <- vapply(DOMAIN_CONFIG, `[[`, character(1), "table")
  expect_true("DRUG_EXPOSURE" %in% tables)
  expect_true("CONDITION_OCCURRENCE" %in% tables)
  expect_true("PROCEDURE_OCCURRENCE" %in% tables)
  expect_true("OBSERVATION" %in% tables)
  expect_true("MEASUREMENT" %in% tables)
  expect_true("VISIT_OCCURRENCE" %in% tables)
})

# ---- rewrite_to_domain_caches ----

test_that("rewrite_to_domain_caches rewrites schema.TABLE to filtered tables", {
  sql <- "SELECT * FROM cdm.DRUG_EXPOSURE WHERE 1=1"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "cdm")
  expect_true(grepl("drug_exposure_filtered", result))
  expect_false(grepl("cdm.DRUG_EXPOSURE", result, fixed = TRUE))
})

test_that("rewrite_to_domain_caches rewrites OBSERVATION_PERIOD", {
  sql <- "SELECT * FROM cdm.OBSERVATION_PERIOD"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "cdm")
  expect_true(grepl("atlas_observation_period", result))
})

test_that("rewrite_to_domain_caches with custom schema", {
  sql <- "SELECT * FROM main.DRUG_EXPOSURE"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "main")
  expect_true(grepl("drug_exposure_filtered", result))
})

test_that("rewrite_to_domain_caches with options uses qualify_table", {
  sql <- "SELECT * FROM cdm.CONDITION_OCCURRENCE"
  opts <- list(results_schema = "rs", table_prefix = "pfx_")
  result <- rewrite_to_domain_caches(sql, cdm_schema = "cdm", options = opts)
  expect_true(grepl("rs.pfx_condition_occurrence_filtered", result))
})

test_that("rewrite_to_domain_caches is case insensitive", {
  sql <- "SELECT * FROM cdm.drug_exposure"
  result <- rewrite_to_domain_caches(sql, cdm_schema = "cdm")
  expect_true(grepl("drug_exposure_filtered", result))
})

# ---- resolve_literal_conditionals ----

test_that("resolve_literal_conditionals expands true condition", {
  sql <- "{1 != 0}?{SELECT 1}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "SELECT 1")
})

test_that("resolve_literal_conditionals removes false condition", {
  sql <- "{0 != 0}?{SELECT 1}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "")
})

test_that("resolve_literal_conditionals with else branch (true)", {
  sql <- "{1 != 0}?{YES}:{NO}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "YES")
})

test_that("resolve_literal_conditionals with else branch (false)", {
  sql <- "{0 != 0}?{YES}:{NO}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "NO")
})

test_that("resolve_literal_conditionals handles multiple conditions", {
  sql <- "A {1 != 0}?{B} C {0 != 0}?{D} E"
  result <- resolve_literal_conditionals(sql)
  expect_true(grepl("B", result))
  expect_false(grepl("D", result))
  expect_true(grepl("A", result))
  expect_true(grepl("C", result))
  expect_true(grepl("E", result))
})

test_that("resolve_literal_conditionals passes through normal SQL", {
  sql <- "SELECT * FROM table WHERE id = 1"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, sql)
})

test_that("resolve_literal_conditionals handles compound conditions", {
  sql <- "{1 != 0 & 2 != 0}?{BOTH_TRUE}"
  result <- resolve_literal_conditionals(sql)
  expect_equal(result, "BOTH_TRUE")
})

# ---- CDM_TABLE_NAMES_FOR_QUOTE ----

test_that("CDM_TABLE_NAMES_FOR_QUOTE contains expected tables", {
  expect_true("OBSERVATION_PERIOD" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("PERSON" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("CONCEPT" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("CONCEPT_ANCESTOR" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("DRUG_EXPOSURE" %in% CDM_TABLE_NAMES_FOR_QUOTE)
  expect_true("CONDITION_OCCURRENCE" %in% CDM_TABLE_NAMES_FOR_QUOTE)
})

# ---- atlas_unique_prefix ----

test_that("atlas_unique_prefix returns string with atlas_ prefix", {
  prefix <- atlas_unique_prefix()
  expect_true(is.character(prefix))
  expect_true(startsWith(prefix, "atlas_"))
  expect_true(endsWith(prefix, "_"))
})

test_that("atlas_unique_prefix generates unique values", {
  p1 <- atlas_unique_prefix()
  p2 <- atlas_unique_prefix()
  expect_false(p1 == p2)
})

# ---- OPTIMIZER_STATIC_TABLES ----

test_that("OPTIMIZER_STATIC_TABLES contains expected tables", {
  expect_true("cohort_stage" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("codesets" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("atlas_observation_period" %in% OPTIMIZER_STATIC_TABLES)
  expect_true("drug_exposure_filtered" %in% OPTIMIZER_STATIC_TABLES)
})

# ---- read_json_input ----

test_that("read_json_input reads JSON string directly", {
  json <- '{"ConceptSets":[]}'
  result <- read_json_input(json)
  expect_equal(result, json)
})

test_that("read_json_input reads from file", {
  tmp <- tempfile(fileext = ".json")
  writeLines('{"key":"value"}', tmp)
  on.exit(unlink(tmp))
  result <- read_json_input(tmp)
  expect_true(grepl("key", result))
})

test_that("read_json_input errors on non-character", {
  expect_error(read_json_input(123))
  expect_error(read_json_input(c("a", "b")))
})

# ---- normalize_schema_str ----

test_that("normalize_schema_str returns default for NULL", {
  expect_equal(normalize_schema_str(NULL), "main")
  expect_equal(normalize_schema_str(NULL, default = "mydb"), "mydb")
})

test_that("normalize_schema_str returns single string as-is", {
  expect_equal(normalize_schema_str("myschema"), "myschema")
})

test_that("normalize_schema_str handles list with schema element", {
  expect_equal(normalize_schema_str(list(schema = "myschema")), "myschema")
})

test_that("normalize_schema_str strips prefix from list", {
  result <- normalize_schema_str(list(catalog = "cat", schema = "sch", prefix = "pfx_"))
  expect_equal(result, "cat.sch")
})

test_that("normalize_schema_str handles named vector", {
  result <- normalize_schema_str(c(catalog = "cat", schema = "sch", prefix = "pfx_"))
  expect_equal(result, "cat.sch")
})

test_that("normalize_schema_str handles named vector without prefix", {
  result <- normalize_schema_str(c(schema = "sch"))
  expect_equal(unname(result), "sch")
})

# ---- extract_write_prefix ----

test_that("extract_write_prefix returns empty for NULL", {
  expect_equal(extract_write_prefix(NULL), "")
})

test_that("extract_write_prefix extracts from list", {
  expect_equal(extract_write_prefix(list(schema = "s", prefix = "pfx_")), "pfx_")
})

test_that("extract_write_prefix extracts from named vector", {
  expect_equal(extract_write_prefix(c(schema = "s", prefix = "pfx_")), "pfx_")
})

test_that("extract_write_prefix returns empty if no prefix", {
  expect_equal(extract_write_prefix(list(schema = "s")), "")
  expect_equal(extract_write_prefix(c(schema = "s")), "")
})

# ---- CRITERIA_TYPE_TO_CDM_TABLE ----

test_that("CRITERIA_TYPE_TO_CDM_TABLE maps known types", {
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["ConditionOccurrence"]], "CONDITION_OCCURRENCE")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["DrugExposure"]], "DRUG_EXPOSURE")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["ProcedureOccurrence"]], "PROCEDURE_OCCURRENCE")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["Measurement"]], "MEASUREMENT")
  expect_equal(CRITERIA_TYPE_TO_CDM_TABLE[["VisitOccurrence"]], "VISIT_OCCURRENCE")
})

# ---- collect_criteria_types_from_group ----

test_that("collect_criteria_types_from_group extracts types from criteria list", {
  group <- list(
    CriteriaList = list(
      list(DrugExposure = list(CodesetId = 1)),
      list(ConditionOccurrence = list(CodesetId = 2))
    )
  )
  types <- collect_criteria_types_from_group(group)
  expect_true("DrugExposure" %in% types)
  expect_true("ConditionOccurrence" %in% types)
})

test_that("collect_criteria_types_from_group handles empty group", {
  types <- collect_criteria_types_from_group(list())
  expect_equal(length(types), 0L)
})

test_that("collect_criteria_types_from_group handles non-list input", {
  types <- collect_criteria_types_from_group(NULL)
  expect_equal(length(types), 0L)
})

# ---- collect_batch_used_domains_from_cohorts ----

test_that("collect_batch_used_domains_from_cohorts finds domains", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"EraPad":0}}'
  cohort <- cohortExpressionFromJson(json)
  tables <- collect_batch_used_domains_from_cohorts(list(cohort))
  expect_true("DRUG_EXPOSURE" %in% tables)
  expect_true("OBSERVATION_PERIOD" %in% tables)
})

# ---- atlas_json_to_sql ----

test_that("atlas_json_to_sql returns SQL string", {
  json <- '{"ConceptSets":[],"PrimaryCriteria":{"CriteriaList":[],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}}}'
  sql <- atlas_json_to_sql(json, cohort_id = 1L, render = FALSE, target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 0)
})

test_that("atlas_json_to_sql with render substitutes parameters", {
  json <- '{"ConceptSets":[],"PrimaryCriteria":{"CriteriaList":[],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}}}'
  sql <- atlas_json_to_sql(json, cohort_id = 1L, cdm_schema = "main",
                           render = TRUE, target_dialect = NULL)
  expect_type(sql, "character")
  expect_true(nchar(sql) > 0)
})

# ---- atlas_json_to_sql_batch ----

test_that("atlas_json_to_sql_batch with optimize=FALSE returns concatenated SQL", {
  json <- '{"ConceptSets":[],"PrimaryCriteria":{"CriteriaList":[],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}}}'
  batch <- atlas_json_to_sql_batch(list(json, json), optimize = FALSE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})

test_that("atlas_json_to_sql_batch with optimize=TRUE produces DAG SQL", {
  json <- '{"ConceptSets":[{"id":1,"name":"x","expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"CollapseType":"ERA","EraPad":0}}'
  batch <- atlas_json_to_sql_batch(list(json), optimize = TRUE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
  expect_true(grepl("codesets", batch))
  expect_true(grepl("cohort_stage", batch))
})

test_that("atlas_json_to_sql_batch accepts data frame input", {
  json <- '{"ConceptSets":[],"PrimaryCriteria":{"CriteriaList":[],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}}}'
  df <- data.frame(cohort_definition_id = c(1L, 2L), cohort = c(json, json), stringsAsFactors = FALSE)
  batch <- atlas_json_to_sql_batch(df, optimize = FALSE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})

test_that("atlas_json_to_sql_batch accepts single string input", {
  json <- '{"ConceptSets":[],"PrimaryCriteria":{"CriteriaList":[],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}}}'
  batch <- atlas_json_to_sql_batch(json, optimize = FALSE, target_dialect = NULL)
  expect_type(batch, "character")
  expect_true(nchar(batch) > 0)
})

# ---- buildBatchCohortQuery ----

test_that("buildBatchCohortQuery with cache=FALSE returns string", {
  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"EraPad":0}}'
  cohort <- cohortExpressionFromJson(json)
  result <- buildBatchCohortQuery(list(cohort), c(1L),
    list(cdm_schema = "main", results_schema = "rs"))
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

test_that("buildBatchCohortQuery with cache=TRUE returns list", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")

  json <- '{"ConceptSets":[{"id":1,"expression":{"items":[{"concept":{"CONCEPT_ID":123},"isExcluded":false,"includeDescendants":false}]}}],"PrimaryCriteria":{"CriteriaList":[{"DrugExposure":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"QualifiedLimit":{"Type":"First"},"ExpressionLimit":{"Type":"All"},"InclusionRules":[],"EndStrategy":{},"CollapseSettings":{"EraPad":0}}'
  cohort <- cohortExpressionFromJson(json)

  result <- buildBatchCohortQuery(list(cohort), c(1L),
    list(cdm_schema = "main", results_schema = "rs"),
    cache = TRUE, con = con, schema = "rs")

  expect_true(is.list(result))
  expect_true("sql" %in% names(result))
  expect_true("cache_hits" %in% names(result))
  expect_true("cache_misses" %in% names(result))
  expect_true("dag" %in% names(result))
  expect_true(is.character(result$sql))
  expect_true(nchar(result$sql) > 0)
})

test_that("buildBatchCohortQuery errors on empty cohort list", {
  expect_error(buildBatchCohortQuery(list(), integer(0)))
})

test_that("buildBatchCohortQuery errors on mismatched lengths", {
  json <- '{"ConceptSets":[],"PrimaryCriteria":{"CriteriaList":[],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}}}'
  cohort <- cohortExpressionFromJson(json)
  expect_error(buildBatchCohortQuery(list(cohort), c(1L, 2L)))
})

# ---- quote_cdm_table_refs (needs DuckDB) ----

test_that("quote_cdm_table_refs quotes table names", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  sql <- "SELECT * FROM main.DRUG_EXPOSURE WHERE 1=1"
  result <- quote_cdm_table_refs(sql, con, "main")
  expect_true(grepl("drug_exposure", tolower(result)))
})

# ---- drop_prefixed_tables (needs DuckDB) ----

test_that("drop_prefixed_tables drops matching tables", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE TABLE atlas_test_mytable (x INT)")
  DBI::dbExecute(con, "CREATE TABLE other_table (x INT)")

  drop_prefixed_tables(con, "main", "atlas_test_")

  tables <- DBI::dbListTables(con)
  expect_false("atlas_test_mytable" %in% tables)
  expect_true("other_table" %in% tables)
})
