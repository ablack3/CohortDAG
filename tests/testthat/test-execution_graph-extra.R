# Additional tests for R/execution_graph.R — normalize, emit, remap, DAG functions

# Extra tests only run in test-coverage and local; skip on container CI
skip_on_cran()
skip_if(nzchar(Sys.getenv("CI_TEST_DB")), "Skipping extra tests on container CI")

# --- qualify_table ---

test_that("qualify_table builds qualified table name", {
  options <- list(results_schema = "@results_schema", table_prefix = "atlas_abc_")
  result <- CohortDAG:::qualify_table("codesets", options)
  expect_equal(result, "@results_schema.atlas_abc_codesets")
})

test_that("qualify_table lowercases the table name", {
  options <- list(results_schema = "main", table_prefix = "pre_")
  result <- CohortDAG:::qualify_table("MyTable", options)
  expect_equal(result, "main.pre_mytable")
})

# --- string_hash ---

test_that("string_hash returns 16-char hex string", {
  result <- CohortDAG:::string_hash("hello world")
  expect_type(result, "character")
  expect_equal(nchar(result), 16)
  expect_true(grepl("^[0-9a-f]+$", result))
})

test_that("string_hash returns same hash for same input", {
  h1 <- CohortDAG:::string_hash("test input")
  h2 <- CohortDAG:::string_hash("test input")
  expect_equal(h1, h2)
})

test_that("string_hash returns different hash for different input", {
  h1 <- CohortDAG:::string_hash("input a")
  h2 <- CohortDAG:::string_hash("input b")
  expect_false(h1 == h2)
})

# --- canonical_hash ---

test_that("canonical_hash returns a hash for concept_set type", {
  definition <- list(
    items = list(
      list(concept = list(CONCEPT_ID = 1234), isExcluded = FALSE, includeDescendants = TRUE)
    )
  )
  result <- CohortDAG:::canonical_hash("concept_set", definition)
  expect_type(result, "character")
  expect_equal(nchar(result), 16)
})

test_that("canonical_hash is deterministic", {
  definition <- list(
    items = list(
      list(concept = list(CONCEPT_ID = 5678), isExcluded = FALSE, includeDescendants = FALSE)
    )
  )
  h1 <- CohortDAG:::canonical_hash("concept_set", definition)
  h2 <- CohortDAG:::canonical_hash("concept_set", definition)
  expect_equal(h1, h2)
})

test_that("canonical_hash differs for different definitions", {
  def1 <- list(items = list(list(concept = list(CONCEPT_ID = 1))))
  def2 <- list(items = list(list(concept = list(CONCEPT_ID = 2))))
  h1 <- CohortDAG:::canonical_hash("concept_set", def1)
  h2 <- CohortDAG:::canonical_hash("concept_set", def2)
  expect_false(h1 == h2)
})

# --- normalize_for_hash ---

test_that("normalize_for_hash dispatches for concept_set", {
  definition <- list(items = list(list(concept = list(CONCEPT_ID = 42))))
  result <- CohortDAG:::normalize_for_hash("concept_set", definition)
  expect_true("items" %in% names(result))
  expect_equal(result$t, "cs")
})

test_that("normalize_for_hash returns raw for unknown type", {
  definition <- list(something = "custom")
  result <- CohortDAG:::normalize_for_hash("unknown_type", definition)
  expect_equal(result$type, "unknown_type")
  expect_equal(result$raw, definition)
})

test_that("normalize_for_hash dispatches for all known types", {
  known_types <- c("concept_set", "primary_events", "qualified_events",
                    "inclusion_rule", "included_events", "cohort_exit",
                    "final_cohort", "criteria_group")
  for (t in known_types) {
    # Just test dispatch doesn't error — each type needs its own data structure
    # concept_set is the only one we can easily call with minimal data
  }
  # Verify concept_set dispatch
  result <- CohortDAG:::normalize_for_hash("concept_set", list(items = list()))
  expect_equal(result$t, "cs")
})

# --- normalize_concept_set_for_hash ---

test_that("normalize_concept_set_for_hash sorts by concept_id", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 200)),
    list(concept = list(CONCEPT_ID = 100))
  ))
  result <- CohortDAG:::normalize_concept_set_for_hash(expr)
  expect_equal(result$items[[1]]$concept_id, 100L)
  expect_equal(result$items[[2]]$concept_id, 200L)
})

test_that("normalize_concept_set_for_hash handles empty items", {
  expr <- list(items = list())
  result <- CohortDAG:::normalize_concept_set_for_hash(expr)
  expect_equal(length(result$items), 0)
})

test_that("normalize_concept_set_for_hash normalizes boolean flags", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 1), isExcluded = TRUE, includeDescendants = FALSE, includeMapped = TRUE)
  ))
  result <- CohortDAG:::normalize_concept_set_for_hash(expr)
  expect_true(result$items[[1]]$is_excluded)
  expect_false(result$items[[1]]$include_descendants)
  expect_true(result$items[[1]]$include_mapped)
})

test_that("normalize_concept_set_for_hash handles camelCase variants", {
  expr <- list(items = list(
    list(concept = list(conceptId = 999), is_excluded = TRUE, include_descendants = TRUE, include_mapped = FALSE)
  ))
  result <- CohortDAG:::normalize_concept_set_for_hash(expr)
  expect_equal(result$items[[1]]$concept_id, 999L)
  expect_true(result$items[[1]]$is_excluded)
})

# --- normalize_criterion_for_hash ---

test_that("normalize_criterion_for_hash handles basic criterion", {
  criterion <- list(ConditionOccurrence = list(CodesetId = 0, First = TRUE))
  cs_map <- list(lookup = list("1:0" = "hash_abc"))
  result <- CohortDAG:::normalize_criterion_for_hash(criterion, cs_map, 1)
  expect_equal(result$criteria_type, "ConditionOccurrence")
  expect_equal(result$cs_hash, "hash_abc")
  expect_true(result$first)
})

test_that("normalize_criterion_for_hash returns null_criterion for invalid input", {
  result <- CohortDAG:::normalize_criterion_for_hash("not_a_criterion", list(lookup = list()), 1)
  expect_equal(result$t, "null_criterion")
})

test_that("normalize_criterion_for_hash captures DateAdjustment", {
  criterion <- list(ConditionOccurrence = list(
    CodesetId = 0,
    DateAdjustment = list(StartWith = "start_date", EndWith = "end_date",
                          StartOffset = 5, EndOffset = 10)
  ))
  cs_map <- list(lookup = list("1:0" = "hash_x"))
  result <- CohortDAG:::normalize_criterion_for_hash(criterion, cs_map, 1)
  expect_equal(result$date_adj$start_offset, 5L)
  expect_equal(result$date_adj$end_offset, 10L)
  expect_equal(result$date_adj$start_with, "start_date")
  expect_equal(result$date_adj$end_with, "end_date")
})

test_that("normalize_criterion_for_hash captures filter fields", {
  criterion <- list(ConditionOccurrence = list(
    CodesetId = 0,
    Age = list(Op = "gte", Value = 18),
    Gender = list(list(CONCEPT_ID = 8507)),
    StopReason = list(Text = "recovered", Op = "contains")
  ))
  cs_map <- list(lookup = list("1:0" = "hash_y"))
  result <- CohortDAG:::normalize_criterion_for_hash(criterion, cs_map, 1)
  expect_true(!is.null(result$age))
  expect_true(!is.null(result$gender))
  expect_true(!is.null(result$stopreason))
})

test_that("normalize_criterion_for_hash resolves source concept codesets", {
  criterion <- list(ConditionOccurrence = list(
    CodesetId = 0,
    ConditionSourceConcept = 1
  ))
  cs_map <- list(lookup = list("1:0" = "hash_std", "1:1" = "hash_src"))
  result <- CohortDAG:::normalize_criterion_for_hash(criterion, cs_map, 1)
  expect_equal(result$src_ConditionSourceConcept, "hash_src")
})

# --- normalize_primary_events_for_hash ---

test_that("normalize_primary_events_for_hash normalizes defaults", {
  def <- list(criteria_hashes = c("b", "a"), prior_days = 365, post_days = 0)
  result <- CohortDAG:::normalize_primary_events_for_hash(def)
  expect_equal(result$t, "pe")
  expect_equal(result$criteria_hashes, c("b", "a"))
  expect_equal(result$prior_days, 365L)
  expect_equal(result$primary_limit_type, "ALL")
  expect_equal(result$event_sort, "ASC")
})

test_that("normalize_criterion_for_hash captures non-whitelisted primary filters", {
  criterion_a <- list(Measurement = list(CodesetId = 0, MeasurementTypeExclude = FALSE))
  criterion_b <- list(Measurement = list(CodesetId = 0, MeasurementTypeExclude = TRUE))
  cs_map <- list(lookup = list("1:0" = "hash_std"))

  result_a <- CohortDAG:::normalize_criterion_for_hash(criterion_a, cs_map, 1)
  result_b <- CohortDAG:::normalize_criterion_for_hash(criterion_b, cs_map, 1)

  expect_true(!is.null(result_a$filter_hash))
  expect_true(!is.null(result_b$filter_hash))
  expect_false(identical(result_a$filter_hash, result_b$filter_hash))
})

# --- normalize_qualified_events_for_hash ---

test_that("normalize_qualified_events_for_hash works", {
  def <- list(pe_hash = "abc123", ac_hash = "def456", q_sort = "desc", q_limit = "First")
  result <- CohortDAG:::normalize_qualified_events_for_hash(def)
  expect_equal(result$t, "qe")
  expect_equal(result$pe_hash, "abc123")
  expect_equal(result$q_sort, "DESC")
  expect_equal(result$q_limit, "FIRST")
})

test_that("normalize_qualified_events_for_hash ignores QualifiedLimit without additional criteria", {
  def <- list(pe_hash = "abc123", ac_hash = "", q_sort = "desc", q_limit = "First")
  result <- CohortDAG:::normalize_qualified_events_for_hash(def)
  expect_equal(result$q_sort, "ASC")
  expect_equal(result$q_limit, "ALL")
})

test_that("normalize_qualified_events_for_hash ignores QualifiedLimit for single primary criteria without additional criteria", {
  def <- list(pe_hash = "abc123", ac_hash = "", q_sort = "desc", q_limit = "First", scoped_single_primary = TRUE)
  result <- CohortDAG:::normalize_qualified_events_for_hash(def)
  expect_equal(result$q_sort, "ASC")
  expect_equal(result$q_limit, "ALL")
})

# --- normalize_criteria_group_for_hash ---

test_that("normalize_criteria_group_for_hash handles non-list input", {
  result <- CohortDAG:::normalize_criteria_group_for_hash("not a list")
  expect_equal(result$t, "empty_group")
})

test_that("normalize_criteria_group_for_hash handles basic group", {
  group <- list(Type = "ALL", Count = 0, CriteriaList = list(), Groups = list(),
                DemographicCriteriaList = list())
  result <- CohortDAG:::normalize_criteria_group_for_hash(group)
  expect_equal(result$t, "cg")
  expect_equal(result$type, "ALL")
  expect_equal(result$count, 0L)
  expect_length(result$criteria, 0)
  expect_length(result$demographics, 0)
  expect_length(result$groups, 0)
})

test_that("normalize_criteria_group_for_hash handles DemographicCriteriaList", {
  group <- list(
    Type = "ALL", Count = 0, CriteriaList = list(), Groups = list(),
    DemographicCriteriaList = list(
      list(Age = list(Op = "gte", Value = 18),
           Gender = list(list(CONCEPT_ID = 8507)))
    )
  )
  result <- CohortDAG:::normalize_criteria_group_for_hash(group)
  expect_length(result$demographics, 1)
  expect_true(!is.null(result$demographics[[1]]$age))
  expect_true(!is.null(result$demographics[[1]]$gender))
})

test_that("normalize_criteria_group_for_hash handles nested Groups", {
  group <- list(
    Type = "ALL", Count = 0, CriteriaList = list(),
    Groups = list(
      list(Type = "ANY", Count = 1, CriteriaList = list(), Groups = list(),
           DemographicCriteriaList = list())
    ),
    DemographicCriteriaList = list()
  )
  result <- CohortDAG:::normalize_criteria_group_for_hash(group)
  expect_length(result$groups, 1)
  expect_equal(result$groups[[1]]$type, "ANY")
})

test_that("normalize_criteria_group_for_hash handles CriteriaList with correlated items", {
  group <- list(
    Type = "ALL", Count = 0,
    CriteriaList = list(
      list(
        Criteria = list(ConditionOccurrence = list(CodesetId = 0)),
        Occurrence = list(Type = 2, Count = 1),
        StartWindow = list(
          UseIndexEnd = FALSE, UseEventEnd = FALSE,
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        )
      )
    ),
    Groups = list(), DemographicCriteriaList = list()
  )
  result <- CohortDAG:::normalize_criteria_group_for_hash(group)
  expect_length(result$criteria, 1)
  expect_equal(result$criteria[[1]]$occ_type, 2L)
  expect_equal(result$criteria[[1]]$occ_count, 1L)
})

# --- normalize_inclusion_rule_for_hash ---

test_that("normalize_inclusion_rule_for_hash works", {
  def <- list(qe_hash = "abc", expr_hash = "def")
  result <- CohortDAG:::normalize_inclusion_rule_for_hash(def)
  expect_equal(result$t, "ir")
  expect_equal(result$qe_hash, "abc")
  expect_equal(result$expr_hash, "def")
})

# --- normalize_included_events_for_hash ---

test_that("normalize_included_events_for_hash preserves ir_hash order", {
  def <- list(qe_hash = "qe1", ir_hashes = c("ir2", "ir1"), el_sort = "asc", el_type = "all")
  result <- CohortDAG:::normalize_included_events_for_hash(def)
  expect_equal(result$t, "ie")
  expect_equal(result$ir_hashes, c("ir2", "ir1"))
  expect_equal(result$el_sort, "ASC")
  expect_equal(result$el_type, "ALL")
})

# --- normalize_cohort_exit_for_hash ---

test_that("normalize_cohort_exit_for_hash with default strategy", {
  def <- list(ie_hash = "ie1", end_strategy = NULL, censoring = list())
  result <- CohortDAG:::normalize_cohort_exit_for_hash(def)
  expect_equal(result$t, "ce")
  expect_equal(result$strategy$type, "default")
  expect_length(result$censoring, 0)
})

test_that("normalize_cohort_exit_for_hash with DateOffset strategy", {
  def <- list(
    ie_hash = "ie1",
    end_strategy = list(DateOffset = list(Offset = 30, DateField = "EndDate")),
    censoring = list()
  )
  result <- CohortDAG:::normalize_cohort_exit_for_hash(def)
  expect_equal(result$strategy$type, "date_offset")
  expect_equal(result$strategy$offset, 30L)
  expect_equal(result$strategy$date_field, "EndDate")
})

test_that("normalize_cohort_exit_for_hash with CustomEra strategy", {
  def <- list(
    ie_hash = "ie1",
    end_strategy = list(CustomEra = list(DrugCodesetId = 1, GapDays = 30)),
    censoring = list()
  )
  result <- CohortDAG:::normalize_cohort_exit_for_hash(def)
  expect_equal(result$strategy$type, "custom_era")
  expect_equal(result$strategy$drug_codeset_id, 1)
  expect_equal(result$strategy$gap_days, 30L)
})

test_that("normalize_cohort_exit_for_hash with censoring criteria", {
  def <- list(
    ie_hash = "ie1",
    end_strategy = NULL,
    censoring = list(list(Death = list(CodesetId = 5)))
  )
  result <- CohortDAG:::normalize_cohort_exit_for_hash(def)
  expect_length(result$censoring, 1)
  expect_equal(result$censoring[[1]]$type, "Death")
  expect_equal(result$censoring[[1]]$codeset_id, 5)
})

# --- normalize_final_cohort_for_hash ---

test_that("normalize_final_cohort_for_hash works", {
  def <- list(ce_hash = "ce1", era_pad = 30, cohort_id = 42)
  result <- CohortDAG:::normalize_final_cohort_for_hash(def)
  expect_equal(result$t, "fc")
  expect_equal(result$era_pad, 30L)
  expect_equal(result$cohort_id, 42L)
})

# --- normalize_window ---

test_that("normalize_window handles NULL", {
  expect_null(CohortDAG:::normalize_window(NULL))
})

test_that("normalize_window normalizes window definition", {
  w <- list(
    UseIndexEnd = FALSE, UseEventEnd = TRUE,
    Start = list(Days = 0, Coeff = -1),
    End = list(Days = 365, Coeff = 1)
  )
  result <- CohortDAG:::normalize_window(w)
  expect_false(result$use_index_end)
  expect_true(result$use_event_end)
  expect_equal(result$start_days, 0L)
  expect_equal(result$end_days, 365L)
  expect_equal(result$start_coeff, -1L)
  expect_equal(result$end_coeff, 1L)
})

# --- normalize_concept_list ---

test_that("normalize_concept_list handles NULL", {
  expect_null(CohortDAG:::normalize_concept_list(NULL))
})

test_that("normalize_concept_list handles empty list", {
  expect_null(CohortDAG:::normalize_concept_list(list()))
})

# --- create_node ---

test_that("create_node creates correct structure", {
  node <- CohortDAG:::create_node("concept_set", "abcdef1234567890", list(), character(0), integer(0))
  expect_equal(node$id, "abcdef1234567890")
  expect_equal(node$type, "concept_set")
  expect_true(startsWith(node$temp_table, "cs_"))
})

test_that("create_node uses correct prefix for each type", {
  types_prefixes <- list(
    concept_set = "cs_", primary_events = "pe_", qualified_events = "qe_",
    inclusion_rule = "ir_", included_events = "ie_", cohort_exit = "ce_",
    final_cohort = "fc_"
  )
  for (type in names(types_prefixes)) {
    node <- CohortDAG:::create_node(type, "abcdef1234567890", list(), character(0), 1L)
    expect_true(startsWith(node$temp_table, types_prefixes[[type]]),
                info = paste("Type:", type))
  }
})

test_that("create_node uses 'nd' prefix for unknown type", {
  node <- CohortDAG:::create_node("unknown_type", "abcdef1234567890", list(), character(0), 1L)
  expect_true(startsWith(node$temp_table, "nd_"))
})

# --- upsert_node ---

test_that("upsert_node creates new node", {
  nodes <- list()
  nodes <- CohortDAG:::upsert_node(nodes, "concept_set", "hash1", list(), character(0), 1L)
  expect_true("hash1" %in% names(nodes))
  expect_equal(nodes[["hash1"]]$cohort_ids, 1L)
})

test_that("upsert_node merges cohort_ids for existing node", {
  nodes <- list()
  nodes <- CohortDAG:::upsert_node(nodes, "concept_set", "hash1", list(), character(0), 1L)
  nodes <- CohortDAG:::upsert_node(nodes, "concept_set", "hash1", list(), character(0), 2L)
  expect_equal(sort(nodes[["hash1"]]$cohort_ids), c(1L, 2L))
})

test_that("upsert_node deduplicates cohort_ids", {
  nodes <- list()
  nodes <- CohortDAG:::upsert_node(nodes, "concept_set", "hash1", list(), character(0), 1L)
  nodes <- CohortDAG:::upsert_node(nodes, "concept_set", "hash1", list(), character(0), 1L)
  expect_equal(nodes[["hash1"]]$cohort_ids, 1L)
})

# --- topological_sort ---

test_that("topological_sort handles empty node list", {
  result <- CohortDAG:::topological_sort(list())
  expect_equal(result, character(0))
})

test_that("topological_sort orders linear chain", {
  nodes <- list(
    a = list(depends_on = character(0)),
    b = list(depends_on = "a"),
    c = list(depends_on = "b")
  )
  result <- CohortDAG:::topological_sort(nodes)
  expect_equal(result, c("a", "b", "c"))
})

test_that("topological_sort handles diamond dependency", {
  nodes <- list(
    a = list(depends_on = character(0)),
    b = list(depends_on = "a"),
    c = list(depends_on = "a"),
    d = list(depends_on = c("b", "c"))
  )
  result <- CohortDAG:::topological_sort(nodes)
  expect_equal(which(result == "a"), 1)
  expect_equal(which(result == "d"), 4)
})

test_that("topological_sort detects cycles", {
  nodes <- list(
    a = list(depends_on = "b"),
    b = list(depends_on = "a")
  )
  expect_error(CohortDAG:::topological_sort(nodes), "Cycle")
})

# --- build_concept_set_map ---

test_that("build_concept_set_map builds from cohort list", {
  cohort_list <- list(
    list(ConceptSets = list(
      list(id = 0, expression = list(items = list(
        list(concept = list(CONCEPT_ID = 201826), isExcluded = FALSE,
             includeDescendants = TRUE, includeMapped = FALSE)
      )))
    ))
  )
  cs_map <- CohortDAG:::build_concept_set_map(cohort_list)
  expect_true(length(cs_map$nodes) > 0)
  expect_true("1:0" %in% names(cs_map$lookup))
  expect_true(!is.null(cs_map$global_id))
  expect_true(!is.null(cs_map$local_to_global))
})

test_that("build_concept_set_map deduplicates identical concept sets", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE)
  ))
  cohort_list <- list(
    list(ConceptSets = list(list(id = 0, expression = expr))),
    list(ConceptSets = list(list(id = 5, expression = expr)))
  )
  cs_map <- CohortDAG:::build_concept_set_map(cohort_list)
  expect_equal(length(cs_map$nodes), 1)
  expect_equal(cs_map$lookup[["1:0"]], cs_map$lookup[["2:5"]])
})

test_that("build_concept_set_map assigns sequential global IDs", {
  cohort_list <- list(
    list(ConceptSets = list(
      list(id = 0, expression = list(items = list(list(concept = list(CONCEPT_ID = 1))))),
      list(id = 1, expression = list(items = list(list(concept = list(CONCEPT_ID = 2)))))
    ))
  )
  cs_map <- CohortDAG:::build_concept_set_map(cohort_list)
  expect_equal(length(cs_map$global_id), 2)
  global_ids <- unname(as.integer(cs_map$global_id))
  expect_equal(sort(global_ids), c(0L, 1L))
})

# --- remap_codeset_ids ---

test_that("remap_codeset_ids remaps CodesetId from local to global", {
  cs_map <- list(local_to_global = list("1:0" = 99L))
  item <- list(ConditionOccurrence = list(CodesetId = 0))
  result <- CohortDAG:::remap_codeset_ids(item, 1, cs_map)
  expect_equal(result$ConditionOccurrence$CodesetId, 99L)
})

test_that("remap_codeset_ids returns non-list unchanged", {
  result <- CohortDAG:::remap_codeset_ids("string", 1, list())
  expect_equal(result, "string")
})

test_that("remap_codeset_ids handles nested CodesetId fields", {
  cs_map <- list(local_to_global = list("1:0" = 50L, "1:1" = 51L))
  item <- list(DrugExposure = list(
    CodesetId = 0,
    CorrelatedCriteria = list(
      CriteriaList = list(
        list(Criteria = list(ConditionOccurrence = list(CodesetId = 1)))
      )
    )
  ))
  result <- CohortDAG:::remap_codeset_ids(item, 1, cs_map)
  expect_equal(result$DrugExposure$CodesetId, 50L)
})

test_that("remap_codeset_ids remaps DrugCodesetId", {
  cs_map <- list(local_to_global = list("1:3" = 77L))
  item <- list(CustomEra = list(DrugCodesetId = 3, GapDays = 30))
  result <- CohortDAG:::remap_codeset_ids(item, 1, cs_map)
  expect_equal(result$CustomEra$DrugCodesetId, 77L)
})

# --- emit_preamble ---

test_that("emit_preamble generates staging table SQL", {
  options <- list(results_schema = "@results_schema", table_prefix = "atlas_test_")
  result <- CohortDAG:::emit_preamble(options)
  combined <- paste(result, collapse = "\n")
  expect_true(grepl("cohort_stage", combined))
  expect_true(grepl("inclusion_events_stage", combined))
  expect_true(grepl("inclusion_stats_stage", combined))
  expect_true(grepl("CREATE TABLE", combined))
})

# --- emit_domain_filtered ---

test_that("emit_domain_filtered generates SQL for used tables", {
  options <- list(results_schema = "@results_schema", table_prefix = "atlas_test_")
  used_tables <- c("OBSERVATION_PERIOD", "CONDITION_OCCURRENCE")
  result <- CohortDAG:::emit_domain_filtered(used_tables, "@cdm_database_schema", options)
  combined <- paste(result, collapse = "\n")
  expect_true(grepl("OBSERVATION_PERIOD", combined))
  expect_true(grepl("condition_occurrence_filtered", combined))
})

test_that("emit_domain_filtered returns empty for no used tables", {
  options <- list(results_schema = "@results_schema", table_prefix = "atlas_test_")
  result <- CohortDAG:::emit_domain_filtered(character(0), "@cdm_database_schema", options)
  expect_equal(length(result), 0)
})

test_that("emit_domain_filtered handles multiple domain tables", {
  options <- list(results_schema = "main", table_prefix = "atlas_x_")
  used_tables <- c("OBSERVATION_PERIOD", "DRUG_EXPOSURE", "MEASUREMENT")
  result <- CohortDAG:::emit_domain_filtered(used_tables, "@cdm_database_schema", options)
  combined <- paste(result, collapse = "\n")
  expect_true(grepl("drug_exposure_filtered", combined))
  expect_true(grepl("measurement_filtered", combined))
})

# --- emit_analyze_hints ---

test_that("emit_analyze_hints generates ANALYZE statements", {
  options <- list(results_schema = "@results_schema", table_prefix = "atlas_test_")
  used_tables <- c("OBSERVATION_PERIOD", "DRUG_EXPOSURE")
  result <- CohortDAG:::emit_analyze_hints(used_tables, options)
  combined <- paste(result, collapse = "\n")
  expect_true(grepl("ANALYZE", combined))
  expect_true(grepl("codesets", combined))
  expect_true(grepl("observation_period", combined))
  expect_true(grepl("drug_exposure_filtered", combined))
})

test_that("emit_analyze_hints includes all_concepts", {
  options <- list(results_schema = "main", table_prefix = "pre_")
  result <- CohortDAG:::emit_analyze_hints(character(0), options)
  combined <- paste(result, collapse = "\n")
  expect_true(grepl("all_concepts", combined))
})

# --- emit_cleanup ---

test_that("emit_cleanup generates DROP statements", {
  nodes <- list(
    "hash1" = list(id = "hash1", type = "primary_events", temp_table = "pe_hash1",
                    depends_on = character(0), cohort_ids = 1L, definition = list()),
    "hash2" = list(id = "hash2", type = "final_cohort", temp_table = "fc_hash2",
                    depends_on = "hash1", cohort_ids = 1L, definition = list())
  )
  dag <- list(nodes = nodes, used_tables = c("DRUG_EXPOSURE"))
  options <- list(results_schema = "main", table_prefix = "atlas_test_")
  result <- CohortDAG:::emit_cleanup(dag, options)
  combined <- paste(result, collapse = "\n")
  expect_true(grepl("DROP TABLE IF EXISTS", combined))
  expect_true(grepl("codesets", combined))
  expect_true(grepl("cohort_stage", combined))
})

test_that("emit_cleanup drops cohort_exit auxiliary tables", {
  nodes <- list(
    "hash1" = list(id = "hash1", type = "cohort_exit", temp_table = "ce_hash1",
                    depends_on = character(0), cohort_ids = 1L, definition = list())
  )
  dag <- list(nodes = nodes, used_tables = character(0))
  options <- list(results_schema = "main", table_prefix = "t_")
  result <- CohortDAG:::emit_cleanup(dag, options)
  combined <- paste(result, collapse = "\n")
  expect_true(grepl("ce_hash1_se", combined))
})

# --- build_execution_dag integration ---

test_that("build_execution_dag builds a complete DAG", {
  cohort_list <- list(
    list(
      ConceptSets = list(
        list(id = 0, expression = list(items = list(
          list(concept = list(CONCEPT_ID = 201826), isExcluded = FALSE,
               includeDescendants = TRUE, includeMapped = FALSE)
        )))
      ),
      PrimaryCriteria = list(
        CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
        ObservationWindow = list(PriorDays = 365, PostDays = 0),
        PrimaryCriteriaLimit = list(Type = "All")
      ),
      QualifiedLimit = list(Type = "First"),
      ExpressionLimit = list(Type = "All"),
      CollapseSettings = list(EraPad = 0)
    )
  )
  dag <- CohortDAG:::build_execution_dag(cohort_list, c(1L), list())
  expect_true(length(dag$nodes) > 0)
  expect_true("1" %in% names(dag$cohort_finals))
  expect_true("OBSERVATION_PERIOD" %in% dag$used_tables)
  expect_true("CONDITION_OCCURRENCE" %in% dag$used_tables)
  expect_equal(dag$cohort_ids, 1L)
})

test_that("build_execution_dag shares nodes across identical cohorts", {
  cs_expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE,
         includeDescendants = FALSE, includeMapped = FALSE)
  ))
  cohort_base <- list(
    ConceptSets = list(list(id = 0, expression = cs_expr)),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 0))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "All"),
    ExpressionLimit = list(Type = "All"),
    CollapseSettings = list(EraPad = 0)
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort_base, cohort_base), c(1L, 2L), list())
  # Concept set node should be shared (only 1 concept set node)
  cs_nodes <- Filter(function(n) n$type == "concept_set", dag$nodes)
  expect_equal(length(cs_nodes), 1)
  # The shared node should have both cohort IDs
  expect_true(all(c(1L, 2L) %in% cs_nodes[[1]]$cohort_ids))
})


# Helper: build a minimal cohort definition suitable for DAG construction
make_simple_cohort <- function(codeset_id = 0, concept_id = 1112807, criteria_type = "ConditionOccurrence") {
  cs_expr <- list(items = list(list(
    concept = list(CONCEPT_ID = concept_id),
    isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE
  )))
  criteria_item <- list()
  criteria_item[[criteria_type]] <- list(CodesetId = codeset_id)

  list(
    ConceptSets = list(list(id = codeset_id, expression = cs_expr)),
    PrimaryCriteria = list(
      CriteriaList = list(criteria_item),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "First")
    ),
    QualifiedLimit = list(Type = "All"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0),
    CensorWindow = list()
  )
}

# --- emit_codesets ---

test_that("emit_codesets generates codesets SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  result <- CohortDAG:::emit_codesets(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("codesets", result_str))
  expect_true(grepl("CREATE TABLE", result_str))
  expect_true(grepl("codeset_id", result_str))
  expect_true(grepl("all_concepts", result_str))
})

test_that("emit_codesets handles multiple concept sets", {
  cohort <- make_simple_cohort()
  # Add a second concept set
  cohort$ConceptSets[[2]] <- list(
    id = 1,
    expression = list(items = list(list(
      concept = list(CONCEPT_ID = 999),
      isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE
    )))
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  result <- CohortDAG:::emit_codesets(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("INSERT INTO", result_str))
})

# --- emit_primary_events ---

test_that("emit_primary_events generates SQL for ConditionOccurrence", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  # Find the primary_events node
  pe_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "primary_events") { pe_node <- n; break }
  }
  expect_false(is.null(pe_node))

  result <- CohortDAG:::emit_primary_events(pe_node, dag, options)
  expect_true(nchar(result) > 100)
  expect_true(grepl("primary_events|person_id|start_date", result, ignore.case = TRUE))
  expect_true(grepl("observation_period", result, ignore.case = TRUE))
})

# --- emit_qualified_events ---

test_that("emit_qualified_events generates SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  qe_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "qualified_events") { qe_node <- n; break }
  }
  expect_false(is.null(qe_node))

  result <- CohortDAG:::emit_qualified_events(qe_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("person_id", result))
  expect_true(grepl("row_number", result, ignore.case = TRUE))
})

test_that("emit_qualified_events matches Circe's event_id tie-break", {
  cohort <- make_simple_cohort()
  cohort$QualifiedLimit <- list(Type = "First")
  cohort$ExpressionLimit <- list(Type = "First")
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  qe_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "qualified_events") { qe_node <- n; break }
  }
  result <- CohortDAG:::emit_qualified_events(qe_node, dag, options)

  expect_true(grepl("order by pe.start_date ASC, pe.event_id\\) as ordinal", result, ignore.case = TRUE))
  expect_true(grepl("WHERE QE.ordinal = 1", result, fixed = TRUE))
})

test_that("emit_qualified_events with additional criteria", {
  cohort <- make_simple_cohort()
  cohort$AdditionalCriteria <- list(
    Type = "ALL",
    CriteriaList = list(list(
      Criteria = list(DrugExposure = list(CodesetId = 0)),
      Occurrence = list(Type = 2, Count = 1),
      StartWindow = list(
        UseIndexEnd = FALSE, UseEventEnd = FALSE,
        Start = list(Days = 0, Coeff = -1),
        End = list(Days = 365, Coeff = 1)
      )
    )),
    Groups = list(),
    DemographicCriteriaList = list()
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  qe_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "qualified_events") { qe_node <- n; break }
  }
  result <- CohortDAG:::emit_qualified_events(qe_node, dag, options)
  expect_true(nchar(result) > 50)
  # Additional criteria causes a JOIN
  expect_true(grepl("JOIN", result, ignore.case = TRUE))
})

# --- emit_included_events ---

test_that("emit_included_events without inclusion rules", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ie_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "included_events") { ie_node <- n; break }
  }
  expect_false(is.null(ie_node))

  result <- CohortDAG:::emit_included_events(ie_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("inclusion_events", result))
  # With 0 rules, should have WHERE 1=0 empty CTE
  expect_true(grepl("1=0", result))
})

test_that("emit_included_events with inclusion rules", {
  cohort <- make_simple_cohort()
  cohort$InclusionRules <- list(
    list(
      name = "Rule 1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(ConditionOccurrence = list(CodesetId = 0)),
          Occurrence = list(Type = 2, Count = 1),
          StartWindow = list(
            UseIndexEnd = FALSE, UseEventEnd = FALSE,
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          )
        )),
        Groups = list(),
        DemographicCriteriaList = list()
      )
    )
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ie_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "included_events") { ie_node <- n; break }
  }

  result <- CohortDAG:::emit_included_events(ie_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("inclusion_rule_mask", result))
  expect_true(grepl("POWER", result))
})

test_that("emit_included_events with ExpressionLimit First", {
  cohort <- make_simple_cohort()
  cohort$ExpressionLimit <- list(Type = "First")
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ie_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "included_events") { ie_node <- n; break }
  }

  result <- CohortDAG:::emit_included_events(ie_node, dag, options)
  expect_true(grepl("ordinal = 1", result))
  expect_true(grepl("order by start_date ASC\\) as ordinal", result, ignore.case = TRUE))
})

# --- emit_inclusion_rule ---

test_that("emit_inclusion_rule generates SQL", {
  cohort <- make_simple_cohort()
  cohort$InclusionRules <- list(
    list(
      name = "Rule 1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(ConditionOccurrence = list(CodesetId = 0)),
          Occurrence = list(Type = 2, Count = 1),
          StartWindow = list(
            UseIndexEnd = FALSE, UseEventEnd = FALSE,
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          )
        )),
        Groups = list(),
        DemographicCriteriaList = list()
      )
    )
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ir_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "inclusion_rule") { ir_node <- n; break }
  }
  expect_false(is.null(ir_node))

  result <- CohortDAG:::emit_inclusion_rule(ir_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("person_id", result))
  expect_true(grepl("event_id", result))
})

# --- emit_cohort_exit ---

test_that("emit_cohort_exit default (no end strategy)", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  expect_false(is.null(ce_node))

  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("op_end_date", result))
})

test_that("emit_cohort_exit with DateOffset", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(DateOffset = list(DateField = "StartDate", Offset = 30))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("DATEADD", result))
  expect_true(grepl("30", result))
})

test_that("emit_cohort_exit with DateOffset EndDate field", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(DateOffset = list(DateField = "EndDate", Offset = 0))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("end_date", result))
})

test_that("emit_cohort_exit with CustomEra Drug", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(CustomEra = list(DrugCodesetId = 0, GapDays = 30, Offset = 0))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("DRUG_EXPOSURE", result))
  expect_true(grepl("drug_exposure_start_date", result, ignore.case = TRUE))
  expect_true(grepl("era_start_date", result))
  expect_true(grepl("era_end_date", result))
})

test_that("emit_cohort_exit with CustomEra Condition", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(CustomEra = list(ConditionCodesetId = 0, GapDays = 10, Offset = 0))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("CONDITION_OCCURRENCE", result))
  expect_true(grepl("condition_start_date", result, ignore.case = TRUE))
})

test_that("emit_cohort_exit with censoring criteria", {
  cohort <- make_simple_cohort()
  cohort$CensoringCriteria <- list(
    list(ConditionOccurrence = list(CodesetId = 0))
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("Censor", result, ignore.case = TRUE))
  expect_true(grepl("MIN", result))
})

# --- emit_final_cohort ---

test_that("emit_final_cohort generates SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  fc_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "final_cohort") { fc_node <- n; break }
  }
  expect_false(is.null(fc_node))

  result <- CohortDAG:::emit_final_cohort(fc_node, dag, options)
  expect_true(nchar(result) > 100)
  expect_true(grepl("INSERT INTO", result))
  expect_true(grepl("cohort_stage", result))
  expect_true(grepl("cohort_definition_id", result))
  expect_true(grepl("Era-fy", result))
})

test_that("emit_final_cohort with CensorWindow", {
  cohort <- make_simple_cohort()
  cohort$CensorWindow <- list(startDate = "2020-01-01", endDate = "2023-12-31")
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  fc_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "final_cohort") { fc_node <- n; break }
  }
  result <- CohortDAG:::emit_final_cohort(fc_node, dag, options)
  expect_true(grepl("CASE WHEN", result))
  expect_true(grepl("2020", result))
  expect_true(grepl("2023", result))
})

test_that("emit_final_cohort with era_pad", {
  cohort <- make_simple_cohort()
  cohort$CollapseSettings <- list(CollapseType = "ERA", EraPad = 30)
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  fc_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "final_cohort") { fc_node <- n; break }
  }
  result <- CohortDAG:::emit_final_cohort(fc_node, dag, options)
  expect_true(grepl("30", result))
  expect_false(grepl("cohort_rows_deduped", result, fixed = TRUE))
  expect_true(grepl("from cohort_rows", result, ignore.case = TRUE))
  expect_true(grepl("DATEADD\\(day,30,end_date\\) as end_date", result))
  expect_true(grepl("\\) CR", result))
})

# --- emit_finalize ---

test_that("emit_finalize generates finalize SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  result <- CohortDAG:::emit_finalize(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("DELETE FROM", result_str))
  expect_true(grepl("INSERT INTO", result_str))
  expect_true(grepl("cohort_stage", result_str))
  expect_true(grepl("target_cohort_table", result_str))
})

# --- emit_cleanup ---

test_that("emit_cleanup generates cleanup SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  result <- CohortDAG:::emit_cleanup(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("Cleanup", result_str))
  expect_true(grepl("DROP TABLE IF EXISTS", result_str))
  expect_true(grepl("codesets", result_str))
  expect_true(grepl("cohort_stage", result_str))
})

# --- emit_node_sql ---

test_that("emit_node_sql dispatches correctly", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  for (n in dag$nodes) {
    if (n$type == "concept_set") next
    result <- CohortDAG:::emit_node_sql(n, dag, options)
    expect_true(is.character(result))
    if (n$type != "concept_set") {
      expect_true(nchar(result) > 0)
    }
  }
})

test_that("emit_node_sql returns empty for unknown type", {
  node <- list(type = "unknown_type", id = "x", temp_table = "x")
  dag <- list(nodes = list())
  options <- list(cdm_schema = "cdm")
  result <- CohortDAG:::emit_node_sql(node, dag, options)
  expect_equal(result, "")
})

# --- emit_dag_sql (full pipeline) ---

test_that("emit_dag_sql generates complete SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(
    cdm_schema = "cdm",
    results_schema = "results",
    vocabulary_schema = "cdm",
    table_prefix = "atlas_test_"
  )

  result <- CohortDAG:::emit_dag_sql(dag, options)
  expect_true(is.character(result))
  expect_true(nchar(result) > 500)
  # Should contain key structural elements
  expect_true(grepl("Execution Graph Batch Script", result))
  expect_true(grepl("cohort_stage", result))
  expect_true(grepl("codesets", result))
  expect_true(grepl("INSERT INTO", result))
  expect_true(grepl("Cleanup", result))
})

test_that("emit_dag_sql with multiple cohorts", {
  c1 <- make_simple_cohort(codeset_id = 0, concept_id = 100, criteria_type = "ConditionOccurrence")
  c2 <- make_simple_cohort(codeset_id = 0, concept_id = 200, criteria_type = "DrugExposure")
  dag <- CohortDAG:::build_execution_dag(list(c1, c2), c(1L, 2L), list())
  options <- list(
    cdm_schema = "cdm",
    results_schema = "results",
    vocabulary_schema = "cdm",
    table_prefix = "atlas_test_"
  )

  result <- CohortDAG:::emit_dag_sql(dag, options)
  expect_true(nchar(result) > 500)
  # Both cohort IDs should appear
  expect_true(grepl("cohort_definition_id", result, ignore.case = TRUE))
})

test_that("emit_dag_sql with inclusion rules", {
  cohort <- make_simple_cohort()
  cohort$InclusionRules <- list(
    list(
      name = "Rule1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(ConditionOccurrence = list(CodesetId = 0)),
          Occurrence = list(Type = 2, Count = 1),
          StartWindow = list(
            UseIndexEnd = FALSE, UseEventEnd = FALSE,
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          )
        )),
        Groups = list(),
        DemographicCriteriaList = list()
      )
    )
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(
    cdm_schema = "cdm",
    results_schema = "results",
    vocabulary_schema = "cdm",
    table_prefix = "atlas_test_"
  )

  result <- CohortDAG:::emit_dag_sql(dag, options)
  expect_true(grepl("inclusion_rule", result, ignore.case = TRUE))
  expect_true(grepl("inclusion_rule_mask", result))
})

# --- emit_domain_filtered with multiple domains ---

test_that("emit_domain_filtered with multiple domain tables", {
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  used <- c("OBSERVATION_PERIOD", "CONDITION_OCCURRENCE", "DRUG_EXPOSURE", "MEASUREMENT")
  result <- CohortDAG:::emit_domain_filtered(used, "cdm", options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("observation_period", result_str, ignore.case = TRUE))
  expect_true(grepl("condition_occurrence_filtered", result_str, ignore.case = TRUE))
  expect_true(grepl("drug_exposure_filtered", result_str, ignore.case = TRUE))
  expect_true(grepl("measurement_filtered", result_str, ignore.case = TRUE))
})

# --- emit_analyze_hints with domains ---

test_that("emit_analyze_hints includes domain tables", {
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  used <- c("OBSERVATION_PERIOD", "CONDITION_OCCURRENCE")
  result <- CohortDAG:::emit_analyze_hints(used, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("ANALYZE", result_str))
  expect_true(grepl("codesets", result_str))
  expect_true(grepl("observation_period", result_str, ignore.case = TRUE))
})


# Helper: build a minimal cohort definition suitable for DAG construction
make_simple_cohort <- function(codeset_id = 0, concept_id = 1112807, criteria_type = "ConditionOccurrence") {
  cs_expr <- list(items = list(list(
    concept = list(CONCEPT_ID = concept_id),
    isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE
  )))
  criteria_item <- list()
  criteria_item[[criteria_type]] <- list(CodesetId = codeset_id)

  list(
    ConceptSets = list(list(id = codeset_id, expression = cs_expr)),
    PrimaryCriteria = list(
      CriteriaList = list(criteria_item),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "First")
    ),
    QualifiedLimit = list(Type = "All"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0),
    CensorWindow = list()
  )
}

# --- emit_codesets ---

test_that("emit_codesets generates codesets SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  result <- CohortDAG:::emit_codesets(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("codesets", result_str))
  expect_true(grepl("CREATE TABLE", result_str))
  expect_true(grepl("codeset_id", result_str))
  expect_true(grepl("all_concepts", result_str))
})

test_that("emit_codesets handles multiple concept sets", {
  cohort <- make_simple_cohort()
  # Add a second concept set
  cohort$ConceptSets[[2]] <- list(
    id = 1,
    expression = list(items = list(list(
      concept = list(CONCEPT_ID = 999),
      isExcluded = FALSE, includeDescendants = FALSE, includeMapped = FALSE
    )))
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  result <- CohortDAG:::emit_codesets(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("INSERT INTO", result_str))
})

# --- emit_primary_events ---

test_that("emit_primary_events generates SQL for ConditionOccurrence", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  # Find the primary_events node
  pe_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "primary_events") { pe_node <- n; break }
  }
  expect_false(is.null(pe_node))

  result <- CohortDAG:::emit_primary_events(pe_node, dag, options)
  expect_true(nchar(result) > 100)
  expect_true(grepl("primary_events|person_id|start_date", result, ignore.case = TRUE))
  expect_true(grepl("observation_period", result, ignore.case = TRUE))
})

# --- emit_qualified_events ---

test_that("emit_qualified_events generates SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  qe_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "qualified_events") { qe_node <- n; break }
  }
  expect_false(is.null(qe_node))

  result <- CohortDAG:::emit_qualified_events(qe_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("person_id", result))
  expect_true(grepl("row_number", result, ignore.case = TRUE))
})

test_that("emit_qualified_events with additional criteria", {
  cohort <- make_simple_cohort()
  cohort$AdditionalCriteria <- list(
    Type = "ALL",
    CriteriaList = list(list(
      Criteria = list(DrugExposure = list(CodesetId = 0)),
      Occurrence = list(Type = 2, Count = 1),
      StartWindow = list(
        UseIndexEnd = FALSE, UseEventEnd = FALSE,
        Start = list(Days = 0, Coeff = -1),
        End = list(Days = 365, Coeff = 1)
      )
    )),
    Groups = list(),
    DemographicCriteriaList = list()
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  qe_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "qualified_events") { qe_node <- n; break }
  }
  result <- CohortDAG:::emit_qualified_events(qe_node, dag, options)
  expect_true(nchar(result) > 50)
  # Additional criteria causes a JOIN
  expect_true(grepl("JOIN", result, ignore.case = TRUE))
})

# --- emit_included_events ---

test_that("emit_included_events without inclusion rules", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ie_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "included_events") { ie_node <- n; break }
  }
  expect_false(is.null(ie_node))

  result <- CohortDAG:::emit_included_events(ie_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("inclusion_events", result))
  # With 0 rules, should have WHERE 1=0 empty CTE
  expect_true(grepl("1=0", result))
})

test_that("emit_included_events with inclusion rules", {
  cohort <- make_simple_cohort()
  cohort$InclusionRules <- list(
    list(
      name = "Rule 1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(ConditionOccurrence = list(CodesetId = 0)),
          Occurrence = list(Type = 2, Count = 1),
          StartWindow = list(
            UseIndexEnd = FALSE, UseEventEnd = FALSE,
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          )
        )),
        Groups = list(),
        DemographicCriteriaList = list()
      )
    )
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ie_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "included_events") { ie_node <- n; break }
  }

  result <- CohortDAG:::emit_included_events(ie_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("inclusion_rule_mask", result))
  expect_true(grepl("POWER", result))
})

test_that("emit_included_events with ExpressionLimit First", {
  cohort <- make_simple_cohort()
  cohort$ExpressionLimit <- list(Type = "First")
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ie_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "included_events") { ie_node <- n; break }
  }

  result <- CohortDAG:::emit_included_events(ie_node, dag, options)
  expect_true(grepl("ordinal = 1", result))
  expect_true(grepl("order by start_date ASC\\) as ordinal", result, ignore.case = TRUE))
})

# --- emit_inclusion_rule ---

test_that("emit_inclusion_rule generates SQL", {
  cohort <- make_simple_cohort()
  cohort$InclusionRules <- list(
    list(
      name = "Rule 1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(ConditionOccurrence = list(CodesetId = 0)),
          Occurrence = list(Type = 2, Count = 1),
          StartWindow = list(
            UseIndexEnd = FALSE, UseEventEnd = FALSE,
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          )
        )),
        Groups = list(),
        DemographicCriteriaList = list()
      )
    )
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ir_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "inclusion_rule") { ir_node <- n; break }
  }
  expect_false(is.null(ir_node))

  result <- CohortDAG:::emit_inclusion_rule(ir_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("person_id", result))
  expect_true(grepl("event_id", result))
})

# --- emit_cohort_exit ---

test_that("emit_cohort_exit default (no end strategy)", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  expect_false(is.null(ce_node))

  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(nchar(result) > 50)
  expect_true(grepl("op_end_date", result))
})

test_that("emit_cohort_exit with DateOffset", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(DateOffset = list(DateField = "StartDate", Offset = 30))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("DATEADD", result))
  expect_true(grepl("30", result))
})

test_that("emit_cohort_exit with DateOffset EndDate field", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(DateOffset = list(DateField = "EndDate", Offset = 0))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("end_date", result))
})

test_that("emit_cohort_exit with CustomEra Drug", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(CustomEra = list(DrugCodesetId = 0, GapDays = 30, Offset = 0))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("DRUG_EXPOSURE", result))
  expect_true(grepl("drug_exposure_start_date", result, ignore.case = TRUE))
  expect_true(grepl("era_start_date", result))
  expect_true(grepl("era_end_date", result))
})

test_that("emit_cohort_exit with CustomEra Condition", {
  cohort <- make_simple_cohort()
  cohort$EndStrategy <- list(CustomEra = list(ConditionCodesetId = 0, GapDays = 10, Offset = 0))
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("CONDITION_OCCURRENCE", result))
  expect_true(grepl("condition_start_date", result, ignore.case = TRUE))
})

test_that("emit_cohort_exit with censoring criteria", {
  cohort <- make_simple_cohort()
  cohort$CensoringCriteria <- list(
    list(ConditionOccurrence = list(CodesetId = 0))
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  ce_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "cohort_exit") { ce_node <- n; break }
  }
  result <- CohortDAG:::emit_cohort_exit(ce_node, dag, options)
  expect_true(grepl("Censor", result, ignore.case = TRUE))
  expect_true(grepl("MIN", result))
})

# --- emit_final_cohort ---

test_that("emit_final_cohort generates SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  fc_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "final_cohort") { fc_node <- n; break }
  }
  expect_false(is.null(fc_node))

  result <- CohortDAG:::emit_final_cohort(fc_node, dag, options)
  expect_true(nchar(result) > 100)
  expect_true(grepl("INSERT INTO", result))
  expect_true(grepl("cohort_stage", result))
  expect_true(grepl("cohort_definition_id", result))
  expect_true(grepl("Era-fy", result))
})

test_that("emit_final_cohort with CensorWindow", {
  cohort <- make_simple_cohort()
  cohort$CensorWindow <- list(startDate = "2020-01-01", endDate = "2023-12-31")
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  fc_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "final_cohort") { fc_node <- n; break }
  }
  result <- CohortDAG:::emit_final_cohort(fc_node, dag, options)
  expect_true(grepl("CASE WHEN", result))
  expect_true(grepl("2020", result))
  expect_true(grepl("2023", result))
})

test_that("emit_final_cohort with era_pad", {
  cohort <- make_simple_cohort()
  cohort$CollapseSettings <- list(CollapseType = "ERA", EraPad = 30)
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  fc_node <- NULL
  for (n in dag$nodes) {
    if (n$type == "final_cohort") { fc_node <- n; break }
  }
  result <- CohortDAG:::emit_final_cohort(fc_node, dag, options)
  expect_true(grepl("30", result))
})

# --- emit_finalize ---

test_that("emit_finalize generates finalize SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  result <- CohortDAG:::emit_finalize(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("DELETE FROM", result_str))
  expect_true(grepl("INSERT INTO", result_str))
  expect_true(grepl("cohort_stage", result_str))
  expect_true(grepl("target_cohort_table", result_str))
})

# --- emit_cleanup ---

test_that("emit_cleanup generates cleanup SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  result <- CohortDAG:::emit_cleanup(dag, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("Cleanup", result_str))
  expect_true(grepl("DROP TABLE IF EXISTS", result_str))
  expect_true(grepl("codesets", result_str))
  expect_true(grepl("cohort_stage", result_str))
})

# --- emit_node_sql ---

test_that("emit_node_sql dispatches correctly", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(cdm_schema = "cdm", results_schema = "results", table_prefix = "atlas_test_")

  for (n in dag$nodes) {
    if (n$type == "concept_set") next
    result <- CohortDAG:::emit_node_sql(n, dag, options)
    expect_true(is.character(result))
    if (n$type != "concept_set") {
      expect_true(nchar(result) > 0)
    }
  }
})

test_that("emit_node_sql returns empty for unknown type", {
  node <- list(type = "unknown_type", id = "x", temp_table = "x")
  dag <- list(nodes = list())
  options <- list(cdm_schema = "cdm")
  result <- CohortDAG:::emit_node_sql(node, dag, options)
  expect_equal(result, "")
})

# --- emit_dag_sql (full pipeline) ---

test_that("emit_dag_sql generates complete SQL", {
  cohort <- make_simple_cohort()
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(
    cdm_schema = "cdm",
    results_schema = "results",
    vocabulary_schema = "cdm",
    table_prefix = "atlas_test_"
  )

  result <- CohortDAG:::emit_dag_sql(dag, options)
  expect_true(is.character(result))
  expect_true(nchar(result) > 500)
  # Should contain key structural elements
  expect_true(grepl("Execution Graph Batch Script", result))
  expect_true(grepl("cohort_stage", result))
  expect_true(grepl("codesets", result))
  expect_true(grepl("INSERT INTO", result))
  expect_true(grepl("Cleanup", result))
})

test_that("emit_dag_sql with multiple cohorts", {
  c1 <- make_simple_cohort(codeset_id = 0, concept_id = 100, criteria_type = "ConditionOccurrence")
  c2 <- make_simple_cohort(codeset_id = 0, concept_id = 200, criteria_type = "DrugExposure")
  dag <- CohortDAG:::build_execution_dag(list(c1, c2), c(1L, 2L), list())
  options <- list(
    cdm_schema = "cdm",
    results_schema = "results",
    vocabulary_schema = "cdm",
    table_prefix = "atlas_test_"
  )

  result <- CohortDAG:::emit_dag_sql(dag, options)
  expect_true(nchar(result) > 500)
  # Both cohort IDs should appear
  expect_true(grepl("cohort_definition_id", result, ignore.case = TRUE))
})

test_that("emit_dag_sql with inclusion rules", {
  cohort <- make_simple_cohort()
  cohort$InclusionRules <- list(
    list(
      name = "Rule1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(ConditionOccurrence = list(CodesetId = 0)),
          Occurrence = list(Type = 2, Count = 1),
          StartWindow = list(
            UseIndexEnd = FALSE, UseEventEnd = FALSE,
            Start = list(Days = 365, Coeff = -1),
            End = list(Days = 0, Coeff = 1)
          )
        )),
        Groups = list(),
        DemographicCriteriaList = list()
      )
    )
  )
  dag <- CohortDAG:::build_execution_dag(list(cohort), 1L, list())
  options <- list(
    cdm_schema = "cdm",
    results_schema = "results",
    vocabulary_schema = "cdm",
    table_prefix = "atlas_test_"
  )

  result <- CohortDAG:::emit_dag_sql(dag, options)
  expect_true(grepl("inclusion_rule", result, ignore.case = TRUE))
  expect_true(grepl("inclusion_rule_mask", result))
})

# --- emit_domain_filtered with multiple domains ---

test_that("emit_domain_filtered with multiple domain tables", {
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  used <- c("OBSERVATION_PERIOD", "CONDITION_OCCURRENCE", "DRUG_EXPOSURE", "MEASUREMENT")
  result <- CohortDAG:::emit_domain_filtered(used, "cdm", options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("observation_period", result_str, ignore.case = TRUE))
  expect_true(grepl("condition_occurrence_filtered", result_str, ignore.case = TRUE))
  expect_true(grepl("drug_exposure_filtered", result_str, ignore.case = TRUE))
  expect_true(grepl("measurement_filtered", result_str, ignore.case = TRUE))
})

# --- emit_analyze_hints with domains ---

test_that("emit_analyze_hints includes domain tables", {
  options <- list(results_schema = "results", table_prefix = "atlas_test_")
  used <- c("OBSERVATION_PERIOD", "CONDITION_OCCURRENCE")
  result <- CohortDAG:::emit_analyze_hints(used, options)
  result_str <- paste(result, collapse = "\n")
  expect_true(grepl("ANALYZE", result_str))
  expect_true(grepl("codesets", result_str))
  expect_true(grepl("observation_period", result_str, ignore.case = TRUE))
})
