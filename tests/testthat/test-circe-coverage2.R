# Additional coverage tests for R/circe.R
# Targets uncovered code paths in domain builders, query internals,
# correlated criteria, demographic criteria, group queries, and end strategies.

skip_on_cran()
skip_if(nzchar(Sys.getenv("CI_TEST_DB")), "Skipping coverage tests on container CI")

# ============================================================================
# Section 1: build_window_criteria - unbounded window paths
# ============================================================================

test_that("build_window_criteria with check_observation_period adds OP bounds", {
  result <- CohortDAG:::build_window_criteria(NULL, NULL, TRUE)
  expect_true(grepl("OP_START_DATE", result))
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria without observation period check", {
  sw <- list(
    Start = list(Days = 30, Coeff = -1),
    End = list(Days = 0, Coeff = 1)
  )
  result <- CohortDAG:::build_window_criteria(sw, NULL, FALSE)
  expect_true(grepl("DATEADD", result))
  expect_true(grepl("-30", result))
  expect_true(grepl(",0,", result))
})

test_that("build_window_criteria with UseIndexEnd and UseEventEnd", {
  sw <- list(
    Start = list(Days = 10, Coeff = -1),
    End = list(Days = 10, Coeff = 1),
    UseIndexEnd = TRUE,
    UseEventEnd = TRUE
  )
  result <- CohortDAG:::build_window_criteria(sw, NULL, FALSE)
  expect_true(grepl("P\\.END_DATE", result))
  expect_true(grepl("A\\.END_DATE", result))
})

test_that("build_window_criteria with endWindow", {
  ew <- list(
    Start = list(Days = 0, Coeff = -1),
    End = list(Days = 365, Coeff = 1),
    UseIndexEnd = FALSE,
    UseEventEnd = FALSE
  )
  result <- CohortDAG:::build_window_criteria(NULL, ew, FALSE)
  expect_true(grepl("DATEADD", result))
})

test_that("build_window_criteria unbounded start (no Days, coeff -1) uses OP_START_DATE", {
  sw <- list(
    Start = list(Coeff = -1),
    End = list(Days = 0, Coeff = 1)
  )
  result <- CohortDAG:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("OP_START_DATE", result))
})

test_that("build_window_criteria unbounded start (no Days, coeff 1) uses OP_END_DATE", {
  sw <- list(
    Start = list(Coeff = 1),
    End = list(Days = 0, Coeff = 1)
  )
  result <- CohortDAG:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria unbounded end (no Days, coeff -1) uses OP_START_DATE", {
  sw <- list(
    Start = list(Days = 0, Coeff = -1),
    End = list(Coeff = -1)
  )
  result <- CohortDAG:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("OP_START_DATE", result))
})

test_that("build_window_criteria unbounded end (no Days, coeff 1) uses OP_END_DATE", {
  sw <- list(
    Start = list(Days = 0, Coeff = -1),
    End = list(Coeff = 1)
  )
  result <- CohortDAG:::build_window_criteria(sw, NULL, TRUE)
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria endWindow unbounded paths", {
  ew <- list(
    Start = list(Coeff = -1),
    End = list(Coeff = 1),
    UseIndexEnd = TRUE,
    UseEventEnd = FALSE
  )
  result <- CohortDAG:::build_window_criteria(NULL, ew, TRUE)
  expect_true(grepl("OP_START_DATE", result))
  expect_true(grepl("OP_END_DATE", result))
})

test_that("build_window_criteria returns empty string with no windows", {
  result <- CohortDAG:::build_window_criteria(NULL, NULL, FALSE)
  expect_equal(result, "")
})

# ============================================================================
# Section 2: get_corelated_criteria_query - various occurrence types
# ============================================================================

test_that("get_corelated_criteria_query with occurrence type 2 (at least)", {
  item <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
    IgnoreObservationPeriod = FALSE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(grepl("CONDITION_OCCURRENCE", result))
  expect_true(grepl("HAVING COUNT", result))
  expect_true(grepl(">= 1", result))
})

test_that("get_corelated_criteria_query with occurrence type 1 (at most) uses LEFT template", {
  item <- list(
    Criteria = list(DrugExposure = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 1, Count = 0, IsDistinct = FALSE),
    IgnoreObservationPeriod = FALSE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("<= 0", result))
})

test_that("get_corelated_criteria_query with occurrence type 0 (exactly) and count 0 uses LEFT", {
  item <- list(
    Criteria = list(Measurement = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 0, Count = 0, IsDistinct = FALSE),
    IgnoreObservationPeriod = FALSE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(grepl("MEASUREMENT", result))
  expect_true(grepl("= 0", result))
})

test_that("get_corelated_criteria_query with IsDistinct=TRUE", {
  item <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 2, Count = 2, IsDistinct = TRUE),
    IgnoreObservationPeriod = FALSE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(grepl("DISTINCT", result))
  expect_true(grepl("domain_concept_id", result))
})

test_that("get_corelated_criteria_query with IsDistinct and CountColumn START_DATE", {
  item <- list(
    Criteria = list(DrugExposure = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = TRUE, CountColumn = "START_DATE"),
    IgnoreObservationPeriod = FALSE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(grepl("DISTINCT", result))
})

test_that("get_corelated_criteria_query with IsDistinct and CountColumn DOMAIN_CONCEPT", {
  item <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = TRUE, CountColumn = "DOMAIN_CONCEPT"),
    IgnoreObservationPeriod = FALSE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(grepl("DISTINCT", result))
  expect_true(grepl("domain_concept_id", result))
})

test_that("get_corelated_criteria_query with RestrictVisit", {
  item <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 0, Coeff = 1)
    ),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
    RestrictVisit = TRUE,
    IgnoreObservationPeriod = FALSE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(grepl("visit_occurrence_id", result))
})

test_that("get_corelated_criteria_query with IgnoreObservationPeriod", {
  item <- list(
    Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
    IgnoreObservationPeriod = TRUE
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_true(nchar(result) > 0)
  # Should NOT have OP_START_DATE check from observation period
  expect_false(grepl("A\\.START_DATE >= P\\.OP_START_DATE AND A\\.START_DATE <= P\\.OP_END_DATE", result))
})

test_that("get_corelated_criteria_query returns empty string for NULL inner criteria", {
  item <- list(
    StartWindow = list(
      Start = list(Days = 0, Coeff = -1),
      End = list(Days = 365, Coeff = 1)
    ),
    Occurrence = list(Type = 2, Count = 1)
  )
  result <- CohortDAG:::get_corelated_criteria_query(item, "#qualified_events", 0, "@cdm_database_schema")
  expect_equal(result, "")
})

# ============================================================================
# Section 3: get_demographic_criteria_query
# ============================================================================

test_that("get_demographic_criteria_query with Age generates age filter", {
  dc <- list(Age = list(Op = "gte", Value = 18))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("year_of_birth", result))
  expect_true(grepl(">= 18", result))
  expect_true(grepl("PERSON", result))
})

test_that("get_demographic_criteria_query with Gender generates gender filter", {
  dc <- list(Gender = list(list(CONCEPT_ID = 8507)))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("8507", result))
})

test_that("get_demographic_criteria_query with Race generates race filter", {
  dc <- list(Race = list(list(CONCEPT_ID = 8516)))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("race_concept_id", result))
  expect_true(grepl("8516", result))
})

test_that("get_demographic_criteria_query with Ethnicity generates ethnicity filter", {
  dc <- list(Ethnicity = list(list(CONCEPT_ID = 38003563)))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("ethnicity_concept_id", result))
  expect_true(grepl("38003563", result))
})

test_that("get_demographic_criteria_query with OccurrenceStartDate", {
  dc <- list(OccurrenceStartDate = list(Op = "gte", Value = "2020-01-01"))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("start_date", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("get_demographic_criteria_query with OccurrenceEndDate", {
  dc <- list(OccurrenceEndDate = list(Op = "lte", Value = "2025-12-31"))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("end_date", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("get_demographic_criteria_query with GenderCS", {
  dc <- list(GenderCS = list(CodesetId = 5, IsExclusion = FALSE))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("codeset_id = 5", result))
})

test_that("get_demographic_criteria_query with RaceCS", {
  dc <- list(RaceCS = list(CodesetId = 6, IsExclusion = FALSE))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("race_concept_id", result))
  expect_true(grepl("codeset_id = 6", result))
})

test_that("get_demographic_criteria_query with EthnicityCS", {
  dc <- list(EthnicityCS = list(CodesetId = 7, IsExclusion = FALSE))
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("ethnicity_concept_id", result))
  expect_true(grepl("codeset_id = 7", result))
})

test_that("get_demographic_criteria_query with empty criteria returns no WHERE", {
  dc <- list()
  result <- CohortDAG:::get_demographic_criteria_query(dc, "#qualified_events", 0)
  expect_true(grepl("Demographic Criteria", result))
  expect_false(grepl("WHERE", result))
})

# ============================================================================
# Section 4: get_criteria_group_query - group types
# ============================================================================

test_that("get_criteria_group_query with empty group returns passthrough", {
  group <- list(Type = "ALL", CriteriaList = list(), Groups = list(), DemographicCriteriaList = list())
  result <- CohortDAG:::get_criteria_group_query(group, "#qualified_events")
  expect_true(grepl("person_id", result))
  expect_true(grepl("event_id", result))
})

test_that("get_criteria_group_query with ALL type", {
  group <- list(
    Type = "ALL",
    CriteriaList = list(
      list(
        Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
        StartWindow = list(
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        ),
        Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
        IgnoreObservationPeriod = FALSE
      )
    )
  )
  result <- CohortDAG:::get_criteria_group_query(group, "#qualified_events")
  expect_true(grepl("HAVING COUNT", result))
  expect_true(grepl("= 1", result))
})

test_that("get_criteria_group_query with ANY type", {
  group <- list(
    Type = "ANY",
    CriteriaList = list(
      list(
        Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
        StartWindow = list(
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        ),
        Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
        IgnoreObservationPeriod = FALSE
      ),
      list(
        Criteria = list(DrugExposure = list(CodesetId = 1)),
        StartWindow = list(
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        ),
        Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
        IgnoreObservationPeriod = FALSE
      )
    )
  )
  result <- CohortDAG:::get_criteria_group_query(group, "#qualified_events")
  expect_true(grepl("HAVING COUNT.*> 0", result))
})

test_that("get_criteria_group_query with AT_LEAST type", {
  group <- list(
    Type = "AT_LEAST",
    Count = 1,
    CriteriaList = list(
      list(
        Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
        StartWindow = list(
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        ),
        Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
        IgnoreObservationPeriod = FALSE
      ),
      list(
        Criteria = list(DrugExposure = list(CodesetId = 2)),
        StartWindow = list(
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        ),
        Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
        IgnoreObservationPeriod = FALSE
      )
    )
  )
  result <- CohortDAG:::get_criteria_group_query(group, "#qualified_events")
  expect_true(grepl("HAVING COUNT.*>= 1", result))
  expect_true(grepl("INNER JOIN", result))
})

test_that("get_criteria_group_query with AT_MOST type uses LEFT JOIN", {
  group <- list(
    Type = "AT_MOST",
    Count = 0,
    CriteriaList = list(
      list(
        Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
        StartWindow = list(
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        ),
        Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
        IgnoreObservationPeriod = FALSE
      )
    )
  )
  result <- CohortDAG:::get_criteria_group_query(group, "#qualified_events")
  expect_true(grepl("LEFT JOIN", result))
  expect_true(grepl("HAVING COUNT.*<= 0", result))
})

test_that("get_criteria_group_query with DemographicCriteriaList", {
  group <- list(
    Type = "ALL",
    CriteriaList = list(),
    DemographicCriteriaList = list(
      list(Age = list(Op = "gte", Value = 18))
    )
  )
  result <- CohortDAG:::get_criteria_group_query(group, "#qualified_events")
  expect_true(grepl("year_of_birth", result))
})

test_that("get_criteria_group_query with nested Groups", {
  group <- list(
    Type = "ALL",
    CriteriaList = list(),
    Groups = list(
      list(
        Type = "ANY",
        CriteriaList = list(
          list(
            Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
            StartWindow = list(
              Start = list(Days = 0, Coeff = -1),
              End = list(Days = 365, Coeff = 1)
            ),
            Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
            IgnoreObservationPeriod = FALSE
          )
        )
      )
    )
  )
  result <- CohortDAG:::get_criteria_group_query(group, "#qualified_events")
  expect_true(grepl("Criteria Group", result))
  expect_true(grepl("CONDITION_OCCURRENCE", result))
})

# ============================================================================
# Section 5: build_cohort_query_internal - end strategies and options
# ============================================================================

# Helper to make a minimal cohort expression for build_cohort_query_internal
make_minimal_cohort_expr <- function(extras = list()) {
  base <- list(
    ConceptSets = list(list(
      id = 1,
      expression = list(items = list(list(
        concept = list(CONCEPT_ID = 123),
        isExcluded = FALSE,
        includeDescendants = FALSE
      )))
    )),
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "All")
    ),
    QualifiedLimit = list(Type = "First"),
    ExpressionLimit = list(Type = "All"),
    InclusionRules = list(),
    CollapseSettings = list(CollapseType = "ERA", EraPad = 0)
  )
  modifyList(base, extras)
}

test_that("build_cohort_query_internal with DateOffset EndStrategy (StartDate)", {
  cohort <- make_minimal_cohort_expr(list(
    EndStrategy = list(DateOffset = list(DateField = "StartDate", Offset = 30))
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("strategy_ends", result$full_sql))
  expect_true(grepl("start_date", result$strategy_ends_sql))
  expect_true(nchar(result$strategy_ends_cleanup) > 0)
})

test_that("build_cohort_query_internal with DateOffset EndStrategy (EndDate)", {
  cohort <- make_minimal_cohort_expr(list(
    EndStrategy = list(DateOffset = list(DateField = "EndDate", Offset = 0))
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("end_date", result$strategy_ends_sql))
})

test_that("build_cohort_query_internal with CustomEra Drug EndStrategy", {
  cohort <- make_minimal_cohort_expr(list(
    EndStrategy = list(CustomEra = list(DrugCodesetId = 1))
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("drug_era", result$strategy_ends_sql))
  expect_true(grepl("strategy_ends", result$full_sql))
})

test_that("build_cohort_query_internal with CustomEra Condition EndStrategy", {
  cohort <- make_minimal_cohort_expr(list(
    EndStrategy = list(CustomEra = list(ConditionCodesetId = 1))
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("condition_era", result$strategy_ends_sql))
})

test_that("build_cohort_query_internal with no EndStrategy uses op_end_date", {
  cohort <- make_minimal_cohort_expr()
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("op_end_date", result$cohort_end_unions))
})

test_that("build_cohort_query_internal with CensoringCriteria", {
  cohort <- make_minimal_cohort_expr(list(
    CensoringCriteria = list(
      list(DrugExposure = list(CodesetId = 1))
    )
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("Censor", result$cohort_end_unions))
})

test_that("build_cohort_query_internal with CensorWindow startDate and endDate", {
  cohort <- make_minimal_cohort_expr(list(
    CensorWindow = list(startDate = "2020-01-01", endDate = "2025-12-31")
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("CASE WHEN", result$final_cohort_sql))
  expect_true(grepl("2020", result$final_cohort_sql))
  expect_true(grepl("2025", result$final_cohort_sql))
})

test_that("build_cohort_query_internal with CensorWindow startDate only", {
  cohort <- make_minimal_cohort_expr(list(
    CensorWindow = list(startDate = "2020-01-01")
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("CASE WHEN start_date", result$final_cohort_sql))
})

test_that("build_cohort_query_internal with non-zero EraPad", {
  cohort <- make_minimal_cohort_expr(list(
    CollapseSettings = list(CollapseType = "ERA", EraPad = 30)
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_equal(result$era_pad, "30")
  expect_true(grepl("30", result$full_sql))
})

test_that("build_cohort_query_internal with multiple inclusion rules", {
  cohort <- make_minimal_cohort_expr()
  cohort$InclusionRules <- list(
    list(
      name = "rule1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(DrugExposure = list(CodesetId = 1)),
          StartWindow = list(
            Start = list(Days = 0, Coeff = -1),
            End = list(Days = 365, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
          IgnoreObservationPeriod = FALSE
        ))
      )
    ),
    list(
      name = "rule2",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(Measurement = list(CodesetId = 1)),
          StartWindow = list(
            Start = list(Days = 0, Coeff = -1),
            End = list(Days = 365, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
          IgnoreObservationPeriod = FALSE
        ))
      )
    )
  )
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("#Inclusion_0", result$inclusion_sql, fixed = TRUE))
  expect_true(grepl("#Inclusion_1", result$inclusion_sql, fixed = TRUE))
  expect_true(grepl("POWER", result$full_sql))
})

test_that("build_cohort_query_internal with generate_stats and no inclusion rules", {
  cohort <- make_minimal_cohort_expr()
  result <- CohortDAG:::build_cohort_query_internal(cohort, list(generate_stats = TRUE))
  expect_true(grepl("Censored Stats", result$inclusion_analysis_sql))
  # No inclusion rules, so no Inclusion Impact Analysis
  expect_false(grepl("Inclusion Impact Analysis", result$inclusion_analysis_sql))
})

test_that("build_cohort_query_internal with generate_stats and inclusion rules", {
  cohort <- make_minimal_cohort_expr()
  cohort$InclusionRules <- list(
    list(
      name = "rule1",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(ConditionOccurrence = list(CodesetId = 1)),
          StartWindow = list(
            Start = list(Days = 0, Coeff = -1),
            End = list(Days = 365, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
          IgnoreObservationPeriod = FALSE
        ))
      )
    )
  )
  result <- CohortDAG:::build_cohort_query_internal(cohort, list(generate_stats = TRUE))
  expect_true(grepl("Censored Stats", result$inclusion_analysis_sql))
  expect_true(grepl("Inclusion Impact Analysis", result$inclusion_analysis_sql))
  expect_true(grepl("best_events", result$inclusion_analysis_sql))
  expect_true(grepl("inclusion_rules", result$inclusion_analysis_sql))
})

test_that("build_cohort_query_internal with ExpressionLimit Last", {
  cohort <- make_minimal_cohort_expr(list(
    ExpressionLimit = list(Type = "Last")
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("DESC", result$included_events_sql))
  expect_true(grepl("ordinal = 1", result$included_events_sql))
})

test_that("build_cohort_query_internal with QualifiedLimit type First", {
  cohort <- make_minimal_cohort_expr(list(
    QualifiedLimit = list(Type = "First")
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(result$has_qualified_limit)
  expect_true(grepl("QE\\.ordinal = 1", result$full_sql))
})

test_that("build_cohort_query_internal with PrimaryCriteriaLimit Last", {
  cohort <- make_minimal_cohort_expr(list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1))),
      ObservationWindow = list(PriorDays = 0, PostDays = 0),
      PrimaryCriteriaLimit = list(Type = "Last")
    )
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("DESC", result$primary_events_sql))
  expect_true(grepl("ordinal = 1", result$primary_events_sql))
})

test_that("build_cohort_query_internal with no concept sets", {
  cohort <- make_minimal_cohort_expr()
  cohort$ConceptSets <- list()
  cohort$PrimaryCriteria$CriteriaList <- list(list(ObservationPeriod = list()))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(nchar(result$codeset_sql) > 0)
  expect_equal(length(result$codeset_union_parts), 0)
})

test_that("build_cohort_query_internal with AdditionalCriteria", {
  cohort <- make_minimal_cohort_expr(list(
    AdditionalCriteria = list(
      Type = "ALL",
      CriteriaList = list(list(
        Criteria = list(DrugExposure = list(CodesetId = 1)),
        StartWindow = list(
          Start = list(Days = 0, Coeff = -1),
          End = list(Days = 365, Coeff = 1)
        ),
        Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
        IgnoreObservationPeriod = FALSE
      ))
    )
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(result$has_additional_criteria)
  expect_true(grepl("AC on AC\\.person_id", result$primary_events_sql))
})

test_that("build_cohort_query_internal with ObservationWindow prior and post days", {
  cohort <- make_minimal_cohort_expr(list(
    PrimaryCriteria = list(
      CriteriaList = list(list(ConditionOccurrence = list(CodesetId = 1))),
      ObservationWindow = list(PriorDays = 365, PostDays = 30),
      PrimaryCriteriaLimit = list(Type = "All")
    )
  ))
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_true(grepl("365", result$primary_events_sql))
  expect_true(grepl("30", result$primary_events_sql))
})

test_that("build_cohort_query_internal returns structured list", {
  cohort <- make_minimal_cohort_expr()
  result <- CohortDAG:::build_cohort_query_internal(cohort)
  expect_type(result, "list")
  expect_true("full_sql" %in% names(result))
  expect_true("codeset_sql" %in% names(result))
  expect_true("primary_events_sql" %in% names(result))
  expect_true("tail_sql" %in% names(result))
  expect_true("era_pad" %in% names(result))
  expect_true("cohort_id" %in% names(result))
})

# ============================================================================
# Section 6: buildCohortQuery backward-compat wrapper
# ============================================================================

test_that("buildCohortQuery from JSON string returns SQL", {
  json_str <- jsonlite::toJSON(make_minimal_cohort_expr(), auto_unbox = TRUE, null = "null")
  result <- CohortDAG:::buildCohortQuery(as.character(json_str))
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
  expect_true(grepl("CONDITION_OCCURRENCE", result))
})

test_that("buildCohortQuery from parsed expression returns SQL", {
  cohort <- make_minimal_cohort_expr()
  result <- CohortDAG:::buildCohortQuery(cohort)
  expect_type(result, "character")
  expect_true(nchar(result) > 100)
})

test_that("buildCohortQuery with options passes through", {
  cohort <- make_minimal_cohort_expr()
  result <- CohortDAG:::buildCohortQuery(cohort, list(cohort_id = 42))
  expect_true(grepl("42", result))
})

# ============================================================================
# Section 7: Domain builders with complex filter combinations
# ============================================================================

# --- ConditionOccurrence with type exclude, stop reason, visit type, gender ---

test_that("build_condition_occurrence_sql with ConditionType exclude", {
  criteria <- list(
    CodesetId = 1,
    ConditionType = list(list(CONCEPT_ID = 38000215)),
    ConditionTypeExclude = TRUE
  )
  result <- CohortDAG:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("not in", result))
  expect_true(grepl("38000215", result))
})

test_that("build_condition_occurrence_sql with ConditionTypeCS", {
  criteria <- list(
    CodesetId = 1,
    ConditionTypeCS = list(CodesetId = 5, IsExclusion = FALSE)
  )
  result <- CohortDAG:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("condition_type_concept_id", result))
  expect_true(grepl("codeset_id = 5", result))
})

test_that("build_condition_occurrence_sql with StopReason filter", {
  criteria <- list(
    CodesetId = 1,
    StopReason = list(Text = "recovered", Op = "contains")
  )
  result <- CohortDAG:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("stop_reason", result))
  expect_true(grepl("LIKE", result))
})

test_that("build_condition_occurrence_sql with Gender filter", {
  criteria <- list(
    CodesetId = 1,
    Gender = list(list(CONCEPT_ID = 8507))
  )
  result <- CohortDAG:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("8507", result))
})

test_that("build_condition_occurrence_sql with VisitType filter", {
  criteria <- list(
    CodesetId = 1,
    VisitType = list(list(CONCEPT_ID = 9201))
  )
  result <- CohortDAG:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
  expect_true(grepl("visit_concept_id", result))
})

test_that("build_condition_occurrence_sql with OccurrenceEndDate", {
  criteria <- list(
    CodesetId = 1,
    OccurrenceEndDate = list(Op = "lte", Value = "2025-12-31")
  )
  result <- CohortDAG:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("end_date", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_condition_occurrence_sql with GenderCS", {
  criteria <- list(
    CodesetId = 1,
    GenderCS = list(CodesetId = 10, IsExclusion = FALSE)
  )
  result <- CohortDAG:::build_condition_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("codeset_id = 10", result))
})

# --- DrugExposure with type exclude, route, age, gender, visit type ---

test_that("build_drug_exposure_sql with DrugType exclude", {
  criteria <- list(
    CodesetId = 1,
    DrugType = list(list(CONCEPT_ID = 38000175)),
    DrugTypeExclude = TRUE
  )
  result <- CohortDAG:::build_drug_exposure_sql(criteria)
  expect_true(grepl("not in", result))
  expect_true(grepl("38000175", result))
})

test_that("build_drug_exposure_sql with DrugTypeCS", {
  criteria <- list(
    CodesetId = 1,
    DrugTypeCS = list(CodesetId = 8)
  )
  result <- CohortDAG:::build_drug_exposure_sql(criteria)
  expect_true(grepl("drug_type_concept_id", result))
  expect_true(grepl("codeset_id = 8", result))
})

test_that("build_drug_exposure_sql with RouteConceptCS", {
  criteria <- list(
    CodesetId = 1,
    RouteConceptCS = list(CodesetId = 9, IsExclusion = FALSE)
  )
  result <- CohortDAG:::build_drug_exposure_sql(criteria)
  expect_true(grepl("route_concept_id", result))
  expect_true(grepl("codeset_id = 9", result))
})

test_that("build_drug_exposure_sql with Refills", {
  criteria <- list(
    CodesetId = 1,
    Refills = list(Op = "gte", Value = 3)
  )
  result <- CohortDAG:::build_drug_exposure_sql(criteria)
  expect_true(grepl("refills", result))
  expect_true(grepl(">= 3", result))
})

test_that("build_drug_exposure_sql with Age and Gender", {
  criteria <- list(
    CodesetId = 1,
    Age = list(Op = "bt", Value = 18, Extent = 65),
    Gender = list(list(CONCEPT_ID = 8507))
  )
  result <- CohortDAG:::build_drug_exposure_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("year_of_birth", result))
  expect_true(grepl("gender_concept_id", result))
})

test_that("build_drug_exposure_sql with VisitType", {
  criteria <- list(
    CodesetId = 1,
    VisitType = list(list(CONCEPT_ID = 9201))
  )
  result <- CohortDAG:::build_drug_exposure_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
})

test_that("build_drug_exposure_sql with OccurrenceStartDate and OccurrenceEndDate", {
  criteria <- list(
    CodesetId = 1,
    OccurrenceStartDate = list(Op = "gte", Value = "2020-01-01"),
    OccurrenceEndDate = list(Op = "lte", Value = "2025-12-31")
  )
  result <- CohortDAG:::build_drug_exposure_sql(criteria)
  expect_true(grepl("start_date", result))
  expect_true(grepl("end_date", result))
})

# --- Visit Occurrence with type exclude, visit length, date adjustment ---

test_that("build_visit_occurrence_sql with VisitType exclude", {
  criteria <- list(
    CodesetId = 1,
    VisitType = list(list(CONCEPT_ID = 9201)),
    VisitTypeExclude = TRUE
  )
  result <- CohortDAG:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("not in", result))
})

test_that("build_visit_occurrence_sql with DateAdjustment", {
  criteria <- list(
    CodesetId = 1,
    DateAdjustment = list(
      StartWith = "start_date",
      EndWith = "end_date",
      StartOffset = 1,
      EndOffset = -1
    )
  )
  result <- CohortDAG:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("DATEADD", result))
})

test_that("build_visit_occurrence_sql with VisitTypeCS", {
  criteria <- list(
    CodesetId = 1,
    VisitTypeCS = list(CodesetId = 5)
  )
  result <- CohortDAG:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("visit_type_concept_id", result))
  expect_true(grepl("codeset_id = 5", result))
})

test_that("build_visit_occurrence_sql with Age and Gender", {
  criteria <- list(
    CodesetId = 1,
    Age = list(Op = "gte", Value = 18),
    Gender = list(list(CONCEPT_ID = 8532))
  )
  result <- CohortDAG:::build_visit_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("8532", result))
})

# --- Procedure with type exclude, modifier CS ---

test_that("build_procedure_occurrence_sql with ProcedureType exclude", {
  criteria <- list(
    CodesetId = 1,
    ProcedureType = list(list(CONCEPT_ID = 38000275)),
    ProcedureTypeExclude = TRUE
  )
  result <- CohortDAG:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("not in", result))
})

test_that("build_procedure_occurrence_sql with ModifierCS", {
  criteria <- list(
    CodesetId = 1,
    ModifierCS = list(CodesetId = 7, IsExclusion = FALSE)
  )
  result <- CohortDAG:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("modifier_concept_id", result))
  expect_true(grepl("codeset_id = 7", result))
})

test_that("build_procedure_occurrence_sql with ProcedureTypeCS", {
  criteria <- list(
    CodesetId = 1,
    ProcedureTypeCS = list(CodesetId = 6)
  )
  result <- CohortDAG:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("procedure_type_concept_id", result))
  expect_true(grepl("codeset_id = 6", result))
})

test_that("build_procedure_occurrence_sql with DateAdjustment", {
  criteria <- list(
    CodesetId = 1,
    DateAdjustment = list(
      StartWith = "start_date",
      EndWith = "end_date",
      StartOffset = 0,
      EndOffset = 1
    )
  )
  result <- CohortDAG:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("DATEADD", result))
})

test_that("build_procedure_occurrence_sql with Age, Gender, VisitType", {
  criteria <- list(
    CodesetId = 1,
    Age = list(Op = "gte", Value = 40),
    Gender = list(list(CONCEPT_ID = 8532)),
    VisitType = list(list(CONCEPT_ID = 9201))
  )
  result <- CohortDAG:::build_procedure_occurrence_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("VISIT_OCCURRENCE", result))
})

# --- Measurement with complex filters ---

test_that("build_measurement_sql with Abnormal flag", {
  criteria <- list(CodesetId = 1, Abnormal = TRUE)
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("4155142", result))
  expect_true(grepl("range_low", result))
  expect_true(grepl("range_high", result))
})

test_that("build_measurement_sql with MeasurementType exclude", {
  criteria <- list(
    CodesetId = 1,
    MeasurementType = list(list(CONCEPT_ID = 44818702)),
    MeasurementTypeExclude = TRUE
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("not in", result))
})

test_that("build_measurement_sql with MeasurementTypeCS", {
  criteria <- list(
    CodesetId = 1,
    MeasurementTypeCS = list(CodesetId = 11)
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("measurement_type_concept_id", result))
  expect_true(grepl("codeset_id = 11", result))
})

test_that("build_measurement_sql with OperatorCS", {
  criteria <- list(
    CodesetId = 1,
    OperatorCS = list(CodesetId = 12)
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("operator_concept_id", result))
  expect_true(grepl("codeset_id = 12", result))
})

test_that("build_measurement_sql with UnitCS", {
  criteria <- list(
    CodesetId = 1,
    UnitCS = list(CodesetId = 13)
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("unit_concept_id", result))
  expect_true(grepl("codeset_id = 13", result))
})

test_that("build_measurement_sql with ValueAsConceptCS", {
  criteria <- list(
    CodesetId = 1,
    ValueAsConceptCS = list(CodesetId = 14)
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("value_as_concept_id", result))
  expect_true(grepl("codeset_id = 14", result))
})

test_that("build_measurement_sql with RangeLowRatio and RangeHighRatio", {
  criteria <- list(
    CodesetId = 1,
    RangeLowRatio = list(Op = "gt", Value = 0.5),
    RangeHighRatio = list(Op = "lt", Value = 1.5)
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("NULLIF.*range_low", result))
  expect_true(grepl("NULLIF.*range_high", result))
})

test_that("build_measurement_sql with VisitType", {
  criteria <- list(
    CodesetId = 1,
    VisitType = list(list(CONCEPT_ID = 9201))
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
  expect_true(grepl("visit_concept_id", result))
})

test_that("build_measurement_sql with Age and Gender", {
  criteria <- list(
    CodesetId = 1,
    Age = list(Op = "bt", Value = 18, Extent = 80),
    Gender = list(list(CONCEPT_ID = 8507))
  )
  result <- CohortDAG:::build_measurement_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("year_of_birth", result))
  expect_true(grepl("gender_concept_id", result))
})

# --- Observation with complex filters ---

test_that("build_observation_sql with ValueAsConcept", {
  criteria <- list(
    CodesetId = 1,
    ValueAsConcept = list(list(CONCEPT_ID = 45877994))
  )
  result <- CohortDAG:::build_observation_sql(criteria)
  expect_true(grepl("value_as_concept_id", result))
  expect_true(grepl("45877994", result))
})

test_that("build_observation_sql with ValueAsConceptCS", {
  criteria <- list(
    CodesetId = 1,
    ValueAsConceptCS = list(CodesetId = 15)
  )
  result <- CohortDAG:::build_observation_sql(criteria)
  expect_true(grepl("value_as_concept_id", result))
  expect_true(grepl("codeset_id = 15", result))
})

test_that("build_observation_sql with Unit filter", {
  criteria <- list(
    CodesetId = 1,
    Unit = list(list(CONCEPT_ID = 8876))
  )
  result <- CohortDAG:::build_observation_sql(criteria)
  expect_true(grepl("unit_concept_id", result))
  expect_true(grepl("8876", result))
})

test_that("build_observation_sql with UnitCS", {
  criteria <- list(
    CodesetId = 1,
    UnitCS = list(CodesetId = 16)
  )
  result <- CohortDAG:::build_observation_sql(criteria)
  expect_true(grepl("unit_concept_id", result))
  expect_true(grepl("codeset_id = 16", result))
})

test_that("build_observation_sql with ValueAsNumber", {
  criteria <- list(
    CodesetId = 1,
    ValueAsNumber = list(Op = "gt", Value = 50)
  )
  result <- CohortDAG:::build_observation_sql(criteria)
  expect_true(grepl("value_as_number", result))
})

test_that("build_observation_sql with Age and Gender", {
  criteria <- list(
    CodesetId = 1,
    Age = list(Op = "gte", Value = 21),
    Gender = list(list(CONCEPT_ID = 8532))
  )
  result <- CohortDAG:::build_observation_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("year_of_birth", result))
})

test_that("build_observation_sql with OccurrenceStartDate and OccurrenceEndDate", {
  criteria <- list(
    CodesetId = 1,
    OccurrenceStartDate = list(Op = "gte", Value = "2020-01-01"),
    OccurrenceEndDate = list(Op = "lte", Value = "2025-12-31")
  )
  result <- CohortDAG:::build_observation_sql(criteria)
  expect_true(grepl("start_date", result))
  expect_true(grepl("end_date", result))
})

# --- DeviceExposure with complex filters ---

test_that("build_device_exposure_sql with DeviceType exclude", {
  criteria <- list(
    CodesetId = 1,
    DeviceType = list(list(CONCEPT_ID = 44818707)),
    DeviceTypeExclude = TRUE
  )
  result <- CohortDAG:::build_device_exposure_sql(criteria)
  expect_true(grepl("not in", result))
})

test_that("build_device_exposure_sql with DeviceTypeCS", {
  criteria <- list(
    CodesetId = 1,
    DeviceTypeCS = list(CodesetId = 17)
  )
  result <- CohortDAG:::build_device_exposure_sql(criteria)
  expect_true(grepl("device_type_concept_id", result))
  expect_true(grepl("codeset_id = 17", result))
})

test_that("build_device_exposure_sql with Age and Gender", {
  criteria <- list(
    CodesetId = 1,
    Age = list(Op = "gte", Value = 18),
    Gender = list(list(CONCEPT_ID = 8507))
  )
  result <- CohortDAG:::build_device_exposure_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
})

test_that("build_device_exposure_sql with VisitType", {
  criteria <- list(
    CodesetId = 1,
    VisitType = list(list(CONCEPT_ID = 9201))
  )
  result <- CohortDAG:::build_device_exposure_sql(criteria)
  expect_true(grepl("VISIT_OCCURRENCE", result))
})

test_that("build_device_exposure_sql with OccurrenceStartDate and OccurrenceEndDate", {
  criteria <- list(
    CodesetId = 1,
    OccurrenceStartDate = list(Op = "gte", Value = "2020-01-01"),
    OccurrenceEndDate = list(Op = "lte", Value = "2025-12-31")
  )
  result <- CohortDAG:::build_device_exposure_sql(criteria)
  expect_true(grepl("start_date", result))
  expect_true(grepl("end_date", result))
})

# --- Death with complex filters ---

test_that("build_death_sql with DeathType exclude", {
  criteria <- list(
    CodesetId = 1,
    DeathType = list(list(CONCEPT_ID = 38003569)),
    DeathTypeExclude = TRUE
  )
  result <- CohortDAG:::build_death_sql(criteria)
  expect_true(grepl("not in", result))
})

test_that("build_death_sql with DeathTypeCS", {
  criteria <- list(
    CodesetId = 1,
    DeathTypeCS = list(CodesetId = 20)
  )
  result <- CohortDAG:::build_death_sql(criteria)
  expect_true(grepl("death_type_concept_id", result))
  expect_true(grepl("codeset_id = 20", result))
})

test_that("build_death_sql with Gender filter", {
  criteria <- list(
    CodesetId = 1,
    Gender = list(list(CONCEPT_ID = 8507))
  )
  result <- CohortDAG:::build_death_sql(criteria)
  expect_true(grepl("PERSON", result))
  expect_true(grepl("gender_concept_id", result))
})

test_that("build_death_sql with GenderCS", {
  criteria <- list(
    CodesetId = 1,
    GenderCS = list(CodesetId = 21)
  )
  result <- CohortDAG:::build_death_sql(criteria)
  expect_true(grepl("gender_concept_id", result))
  expect_true(grepl("codeset_id = 21", result))
})

test_that("build_death_sql with OccurrenceStartDate", {
  criteria <- list(
    CodesetId = 1,
    OccurrenceStartDate = list(Op = "bt", Value = "2020-01-01", Extent = "2025-12-31")
  )
  result <- CohortDAG:::build_death_sql(criteria)
  expect_true(grepl("start_date", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_death_sql with DeathSourceConcept", {
  criteria <- list(DeathSourceConcept = 2)
  result <- CohortDAG:::build_death_sql(criteria)
  expect_true(grepl("cause_source_concept_id", result))
})

# --- Specimen with First flag ---

test_that("build_specimen_sql with First flag", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CohortDAG:::build_specimen_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
  expect_true(grepl("row_number", result))
})

# --- Visit Detail with First flag ---

test_that("build_visit_detail_sql with First flag generates ordinal", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CohortDAG:::build_visit_detail_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

# --- Payer Plan Period with First flag ---

test_that("build_payer_plan_period_sql with First flag generates ordinal", {
  criteria <- list(First = TRUE)
  result <- CohortDAG:::build_payer_plan_period_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

# --- Condition Era with First flag ---

test_that("build_condition_era_sql with First flag generates ordinal", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CohortDAG:::build_condition_era_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

# --- Drug Era with First flag ---

test_that("build_drug_era_sql with First flag generates ordinal", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CohortDAG:::build_drug_era_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

# --- Dose Era with First flag ---

test_that("build_dose_era_sql with First flag generates ordinal", {
  criteria <- list(CodesetId = 1, First = TRUE)
  result <- CohortDAG:::build_dose_era_sql(criteria)
  expect_true(grepl("ordinal = 1", result))
})

# --- Observation Period with UserDefinedPeriod ---

test_that("build_observation_period_sql with UserDefinedPeriod startDate and endDate", {
  criteria <- list(
    UserDefinedPeriod = list(startDate = "2020-01-01", endDate = "2025-12-31"),
    First = TRUE
  )
  result <- CohortDAG:::build_observation_period_sql(criteria)
  expect_true(grepl("DATEFROMPARTS\\(2020", result))
  expect_true(grepl("DATEFROMPARTS\\(2025", result))
  expect_true(grepl("ordinal = 1", result))
})

# --- Location Region without CodesetId ---

test_that("build_location_region_sql without CodesetId generates SQL without codeset filter", {
  criteria <- list()
  result <- CohortDAG:::build_location_region_sql(criteria)
  expect_true(grepl("LOCATION_HISTORY", result))
  expect_false(grepl("codeset_id", result))
})

# ============================================================================
# Section 8: get_codeset_where_clause
# ============================================================================

test_that("get_codeset_where_clause with valid codeset_id", {
  result <- CohortDAG:::get_codeset_where_clause(5, "ce.condition_concept_id")
  expect_true(grepl("WHERE", result))
  expect_true(grepl("codeset_id = 5", result))
})

test_that("get_codeset_where_clause with NULL returns empty string", {
  result <- CohortDAG:::get_codeset_where_clause(NULL, "ce.condition_concept_id")
  expect_equal(result, "")
})

# ============================================================================
# Section 9: Concept set expression query edge cases
# ============================================================================

test_that("build_concept_set_expression_query with only excluded items returns 0=1", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = TRUE, includeDescendants = FALSE)
  ))
  result <- CohortDAG:::build_concept_set_expression_query(expr)
  expect_true(grepl("0=1", result))
})

test_that("build_concept_set_expression_query with multiple include items", {
  expr <- list(items = list(
    list(concept = list(CONCEPT_ID = 100), isExcluded = FALSE, includeDescendants = FALSE),
    list(concept = list(CONCEPT_ID = 200), isExcluded = FALSE, includeDescendants = FALSE),
    list(concept = list(CONCEPT_ID = 300), isExcluded = FALSE, includeDescendants = TRUE)
  ))
  result <- CohortDAG:::build_concept_set_expression_query(expr)
  expect_true(grepl("100", result))
  expect_true(grepl("200", result))
  expect_true(grepl("300", result))
  expect_true(grepl("CONCEPT_ANCESTOR", result))
})

test_that("build_concept_set_expression_query with include mapped descendants", {
  expr <- list(items = list(
    list(
      concept = list(CONCEPT_ID = 100),
      isExcluded = FALSE,
      includeDescendants = TRUE,
      includeMapped = TRUE
    )
  ))
  result <- CohortDAG:::build_concept_set_expression_query(expr)
  expect_true(grepl("Maps to", result))
  expect_true(grepl("CONCEPT_ANCESTOR", result))
  expect_true(grepl("concept_relationship", result))
})

test_that("build_concept_set_expression_query with NULL items list", {
  expr <- list()
  result <- CohortDAG:::build_concept_set_expression_query(expr)
  expect_true(grepl("0=1", result))
})

# ============================================================================
# Section 10: cohort_expression_from_json edge cases
# ============================================================================

test_that("cohort_expression_from_json strips cdmVersionRange", {
  json <- '{"cdmVersionRange":">=5.0","PrimaryCriteria":{"CriteriaList":[{"ConditionOccurrence":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"ConceptSets":[]}'
  result <- CohortDAG:::cohort_expression_from_json(json)
  expect_null(result$cdmVersionRange)
})

test_that("cohort_expression_from_json handles empty censorWindow", {
  # Empty JSON object {} is parsed by jsonlite; the code checks identical(data$censorWindow, list())
  # to strip it. Verify the parse succeeds and censorWindow is either NULL or empty list.
  json <- '{"censorWindow":{},"PrimaryCriteria":{"CriteriaList":[{"ConditionOccurrence":{"CodesetId":1}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"ConceptSets":[]}'
  result <- CohortDAG:::cohort_expression_from_json(json)
  # Either stripped to NULL or remains as empty list - both are acceptable

  expect_true(is.null(result$censorWindow) || length(result$censorWindow) == 0)
})

test_that("cohort_expression_from_json normalizes concept set expression defaults", {
  json <- '{"PrimaryCriteria":{"CriteriaList":[{"ConditionOccurrence":{"CodesetId":0}}],"ObservationWindow":{"PriorDays":0,"PostDays":0},"PrimaryCriteriaLimit":{"Type":"All"}},"ConceptSets":[{"id":0,"expression":{"items":[{"concept":{"CONCEPT_ID":123}}]}}]}'
  result <- CohortDAG:::cohort_expression_from_json(json)
  cs_expr <- result$ConceptSets[[1]]$expression
  expect_false(is.null(cs_expr$isExcluded))
  expect_false(is.null(cs_expr$includeMapped))
  expect_false(is.null(cs_expr$includeDescendants))
})

# ============================================================================
# Section 11: cohortExpressionFromJson error handling
# ============================================================================

test_that("cohortExpressionFromJson errors on non-character input", {
  expect_error(CohortDAG:::cohortExpressionFromJson(42), "character")
  expect_error(CohortDAG:::cohortExpressionFromJson(list()), "character")
})

test_that("cohortExpressionFromJson errors on vector input", {
  expect_error(CohortDAG:::cohortExpressionFromJson(c("a", "b")), "character")
})

# ============================================================================
# Section 12: wrap_criteria_query
# ============================================================================

test_that("wrap_criteria_query wraps base query with criteria group", {
  base_query <- "SELECT person_id, 1 as event_id, start_date, end_date, NULL as visit_occurrence_id, start_date as sort_date FROM test_table"
  group <- list(
    Type = "ALL",
    CriteriaList = list(list(
      Criteria = list(DrugExposure = list(CodesetId = 1)),
      StartWindow = list(
        Start = list(Days = 0, Coeff = -1),
        End = list(Days = 365, Coeff = 1)
      ),
      Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
      IgnoreObservationPeriod = FALSE
    ))
  )
  result <- CohortDAG:::wrap_criteria_query(base_query, group)
  expect_true(grepl("OBSERVATION_PERIOD", result))
  expect_true(grepl("AC on AC\\.person_id", result))
  expect_true(grepl("DRUG_EXPOSURE", result))
})

# ============================================================================
# Section 13: ProviderSpecialty with codeset-based filter
# ============================================================================

test_that("add_provider_specialty_filter with ProviderSpecialtyCS", {
  criteria <- list(
    ProviderSpecialtyCS = list(CodesetId = 25, IsExclusion = FALSE)
  )
  result <- CohortDAG:::add_provider_specialty_filter(criteria, "@cdm_database_schema", "co")
  expect_true(!is.null(result$join_sql))
  expect_true(grepl("PROVIDER", result$join_sql))
  expect_true(length(result$where_parts) > 0)
  expect_true(any(grepl("specialty_concept_id", result$where_parts)))
  expect_true(any(grepl("codeset_id = 25", result$where_parts)))
})

# ============================================================================
# Section 14: date_string_to_sql edge cases
# ============================================================================

test_that("date_string_to_sql handles December 31st", {
  result <- CohortDAG:::date_string_to_sql("2025-12-31")
  expect_equal(result, "DATEFROMPARTS(2025, 12, 31)")
})

test_that("date_string_to_sql strips leading zeros", {
  result <- CohortDAG:::date_string_to_sql("2020-01-01")
  expect_equal(result, "DATEFROMPARTS(2020, 1, 1)")
})

# ============================================================================
# Section 15: build_date_range_clause with all operator types
# ============================================================================

test_that("build_date_range_clause with lt operator", {
  dr <- list(Op = "lt", Value = "2020-06-15")
  result <- CohortDAG:::build_date_range_clause("start_date", dr)
  expect_true(grepl("< ", result))
  expect_true(grepl("DATEFROMPARTS", result))
})

test_that("build_date_range_clause with lte operator", {
  dr <- list(Op = "lte", Value = "2020-06-15")
  result <- CohortDAG:::build_date_range_clause("start_date", dr)
  expect_true(grepl("<= ", result))
})

test_that("build_date_range_clause with eq operator", {
  dr <- list(Op = "eq", Value = "2020-06-15")
  result <- CohortDAG:::build_date_range_clause("start_date", dr)
  expect_true(grepl("= ", result))
})

test_that("build_date_range_clause with neq operator", {
  dr <- list(Op = "!eq", Value = "2020-06-15")
  result <- CohortDAG:::build_date_range_clause("start_date", dr)
  expect_true(grepl("<> ", result))
})

test_that("build_date_range_clause with gte operator", {
  dr <- list(Op = "gte", Value = "2020-01-01")
  result <- CohortDAG:::build_date_range_clause("start_date", dr)
  expect_true(grepl(">= ", result))
})

test_that("build_date_range_clause returns NULL for bt without Extent", {
  dr <- list(Op = "bt", Value = "2020-01-01")
  result <- CohortDAG:::build_date_range_clause("start_date", dr)
  expect_null(result)
})

test_that("build_date_range_clause returns NULL for missing Op", {
  dr <- list(Value = "2020-01-01")
  result <- CohortDAG:::build_date_range_clause("start_date", dr)
  expect_null(result)
})

# ============================================================================
# Section 16: build_numeric_range_clause edge cases
# ============================================================================

test_that("build_numeric_range_clause with not-between no format", {
  nr <- list(Op = "!bt", Value = 10, Extent = 50)
  result <- CohortDAG:::build_numeric_range_clause("age", nr)
  expect_true(grepl("^not ", result))
  expect_true(grepl(">= 10", result))
  expect_true(grepl("<= 50", result))
})

test_that("build_numeric_range_clause returns NULL for bt without Extent", {
  nr <- list(Op = "bt", Value = 10)
  result <- CohortDAG:::build_numeric_range_clause("age", nr)
  expect_null(result)
})

test_that("build_numeric_range_clause returns NULL for missing Op", {
  nr <- list(Value = 10)
  result <- CohortDAG:::build_numeric_range_clause("age", nr)
  expect_null(result)
})

# ============================================================================
# Section 17: get_criteria_sql with CorrelatedCriteria
# ============================================================================

test_that("get_criteria_sql handles CorrelatedCriteria on domain", {
  criteria <- list(
    ConditionOccurrence = list(
      CodesetId = 1,
      CorrelatedCriteria = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(DrugExposure = list(CodesetId = 1)),
          StartWindow = list(
            Start = list(Days = 0, Coeff = -1),
            End = list(Days = 365, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
          IgnoreObservationPeriod = FALSE
        ))
      )
    )
  )
  result <- CohortDAG:::get_criteria_sql(criteria)
  expect_true(grepl("CONDITION_OCCURRENCE", result))
  expect_true(grepl("DRUG_EXPOSURE", result))
  expect_true(grepl("AC on AC\\.person_id", result))
})

# ============================================================================
# Section 18: Full integration - buildCohortQuery with complex cohort
# ============================================================================

test_that("buildCohortQuery with inclusion rules and DateOffset end strategy", {
  json_obj <- make_minimal_cohort_expr(list(
    InclusionRules = list(list(
      name = "has_drug",
      expression = list(
        Type = "ALL",
        CriteriaList = list(list(
          Criteria = list(DrugExposure = list(CodesetId = 1)),
          StartWindow = list(
            Start = list(Days = 0, Coeff = -1),
            End = list(Days = 365, Coeff = 1)
          ),
          Occurrence = list(Type = 2, Count = 1, IsDistinct = FALSE),
          IgnoreObservationPeriod = FALSE
        ))
      )
    )),
    EndStrategy = list(DateOffset = list(DateField = "StartDate", Offset = 365))
  ))
  result <- CohortDAG:::buildCohortQuery(json_obj)
  expect_true(grepl("inclusion", result, ignore.case = TRUE))
  expect_true(grepl("strategy_ends", result))
  expect_true(grepl("365", result))
})

test_that("buildCohortQuery errors when PrimaryCriteria is missing", {
  cohort <- list(ConceptSets = list())
  expect_error(CohortDAG:::buildCohortQuery(cohort), "PrimaryCriteria")
})

# ============================================================================
# Section 19: build_text_filter_clause with list that has unknown op (default)
# ============================================================================

test_that("build_text_filter_clause with unknown op uses default eq", {
  tf <- list(Text = "value", Op = "unknownOp")
  result <- CohortDAG:::build_text_filter_clause(tf, "col")
  expect_true(grepl("= 'value'", result))
})

# ============================================================================
# Section 20: get_codeset_in_expression
# ============================================================================

test_that("get_codeset_in_expression with is_exclusion TRUE", {
  result <- CohortDAG:::get_codeset_in_expression(5, "C.concept_id", TRUE)
  expect_true(grepl("not", result))
  expect_true(grepl("codeset_id = 5", result))
})

test_that("get_codeset_in_expression with is_exclusion FALSE", {
  result <- CohortDAG:::get_codeset_in_expression(5, "C.concept_id", FALSE)
  expect_false(grepl("not", result))
  expect_true(grepl("codeset_id = 5", result))
})

# ============================================================================
# Section 21: get_date_adjustment_expression with various inputs
# ============================================================================

test_that("get_date_adjustment_expression with camelCase keys", {
  da <- list(startOffset = 3, endOffset = -2)
  result <- CohortDAG:::get_date_adjustment_expression(da, "s", "e")
  expect_true(grepl("3", result))
  expect_true(grepl("-2", result))
})

test_that("get_date_adjustment_expression with PascalCase keys", {
  da <- list(StartOffset = 10, EndOffset = 5)
  result <- CohortDAG:::get_date_adjustment_expression(da, "start_col", "end_col")
  expect_true(grepl("10", result))
  expect_true(grepl("5", result))
})

test_that("get_date_adjustment_expression with no offsets defaults to 0", {
  da <- list()
  result <- CohortDAG:::get_date_adjustment_expression(da, "s", "e")
  expect_true(grepl("DATEADD\\(day,0", result))
})
