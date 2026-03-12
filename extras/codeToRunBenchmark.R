cdmDisconnect(cdm)

library(CohortDAG)
source("extras/benchmark_cohort_generation.R")
# With cohort set from a folder:
pathToCohorts <- system.file("cohorts", package = "PhenotypeLibrary")
cohort_set <- CDMConnector::readCohortSet(pathToCohorts)

library(CDMConnector)
library(dplyr)
con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir("synpuf-110k"))
cdm <- cdmFromCon(con, "main", "main")


# result <- benchmark_cohort_generation(cdm, cohort_set, cohort_path = pathToCohorts)

# debugonce(benchmark_cohort_generation)
cdm <- benchmark_cohort_generation(cdm, cohort_set[1:100,])


tally(cdm$cohort_bench_new)
tally(cdm$cohort_bench_old)

cohortCount(cdm$cohort_bench_old) %>%
  left_join(
    cohortCount(cdm$cohort_bench_new) %>% rename_with(~paste0(., "_new"), matches("number"))
  ) %>%
  mutate(
    n_records_match = number_records == number_records_new,
    n_subjects_match = number_subjects == number_subjects_new,
  )

profvis::profvis({
  generateCohortSet2(cdm, cohort_set[1:10,], "cohort")
})

