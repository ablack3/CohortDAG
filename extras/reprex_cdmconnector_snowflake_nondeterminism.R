#!/usr/bin/env Rscript

# Minimal repro for Snowflake nondeterminism in CDMConnector::generateCohortSet().
#
# Expected:
#   Re-running generateCohortSet() for the same cohort on the same Snowflake CDM
#   should return identical cohort rows.
#
# Observed:
#   Row counts can match while the returned row sets differ between runs.
#
# Required env vars:
#   SNOWFLAKE_SERVER
#   SNOWFLAKE_USER
#   SNOWFLAKE_PASSWORD
#   SNOWFLAKE_DATABASE
#   SNOWFLAKE_WAREHOUSE
#   SNOWFLAKE_DRIVER
#   SNOWFLAKE_CDM_SCHEMA
#   SNOWFLAKE_SCRATCH_SCHEMA
#
# Optional env vars:
#   REPRO_COHORT_NAME   default: acute_respiratory_failure_in_inpatient_or_emergency_room
#   REPRO_COHORT_ID     default: 27
#   REPRO_RUNS          default: 2

suppressPackageStartupMessages({
  library(CDMConnector)
  library(DBI)
  library(dplyr)
  library(odbc)
})

stop_if_missing <- function(x, name) {
  if (!nzchar(x)) stop("Missing env var: ", name, call. = FALSE)
  x
}

split_schema <- function(schema, database = "") {
  parts <- strsplit(schema, "\\.")[[1L]]
  if (length(parts) == 1L) {
    if (nzchar(database)) {
      return(c(catalog = database, schema = parts[[1L]]))
    }
    return(parts[[1L]])
  }
  c(catalog = parts[[1L]], schema = parts[[2L]])
}

row_key <- function(df) {
  paste(
    as.integer(df$cohort_definition_id),
    as.integer(df$subject_id),
    as.character(as.Date(df$cohort_start_date)),
    as.character(as.Date(df$cohort_end_date)),
    sep = "|"
  )
}

read_single_cohort_set <- function(cohort_name, cohort_id, cohort_dir = file.path("inst", "cohorts")) {
  cohort_file <- file.path(cohort_dir, paste0(cohort_name, ".json"))
  if (!file.exists(cohort_file)) {
    stop("Cohort JSON not found: ", cohort_file, call. = FALSE)
  }

  temp_dir <- tempfile("repro_cohort_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
  file.copy(cohort_file, file.path(temp_dir, basename(cohort_file)))

  cohort_set <- CDMConnector::readCohortSet(temp_dir)
  cohort_set$cohort_definition_id <- as.integer(cohort_id)
  cohort_set
}

cohort_name <- Sys.getenv(
  "REPRO_COHORT_NAME",
  unset = "acute_respiratory_failure_in_inpatient_or_emergency_room"
)
cohort_id <- as.integer(Sys.getenv("REPRO_COHORT_ID", unset = "27"))
n_runs <- as.integer(Sys.getenv("REPRO_RUNS", unset = "2"))

if (is.na(n_runs) || n_runs < 2L) {
  stop("REPRO_RUNS must be >= 2", call. = FALSE)
}

server <- stop_if_missing(Sys.getenv("SNOWFLAKE_SERVER", unset = ""), "SNOWFLAKE_SERVER")
user <- stop_if_missing(Sys.getenv("SNOWFLAKE_USER", unset = ""), "SNOWFLAKE_USER")
password <- stop_if_missing(Sys.getenv("SNOWFLAKE_PASSWORD", unset = ""), "SNOWFLAKE_PASSWORD")
database <- stop_if_missing(Sys.getenv("SNOWFLAKE_DATABASE", unset = ""), "SNOWFLAKE_DATABASE")
warehouse <- stop_if_missing(Sys.getenv("SNOWFLAKE_WAREHOUSE", unset = ""), "SNOWFLAKE_WAREHOUSE")
driver <- stop_if_missing(Sys.getenv("SNOWFLAKE_DRIVER", unset = ""), "SNOWFLAKE_DRIVER")
cdm_schema_raw <- stop_if_missing(Sys.getenv("SNOWFLAKE_CDM_SCHEMA", unset = ""), "SNOWFLAKE_CDM_SCHEMA")
scratch_schema_raw <- stop_if_missing(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA", unset = ""), "SNOWFLAKE_SCRATCH_SCHEMA")

con <- DBI::dbConnect(
  odbc::odbc(),
  SERVER = server,
  UID = user,
  PWD = password,
  DATABASE = database,
  WAREHOUSE = warehouse,
  DRIVER = driver
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

cdm <- CDMConnector::cdmFromCon(
  con = con,
  cdmSchema = split_schema(cdm_schema_raw, database),
  cdmName = "snowflake_repro",
  writeSchema = split_schema(scratch_schema_raw, database)
)

cohort_set <- read_single_cohort_set(cohort_name, cohort_id)
table_prefix <- paste0("repro_", format(Sys.time(), "%H%M%S"), "_", Sys.getpid())

cat("Cohort:", cohort_name, "\n")
cat("Cohort ID:", cohort_id, "\n")
cat("Runs:", n_runs, "\n\n")

results <- vector("list", n_runs)
times <- numeric(n_runs)

for (i in seq_len(n_runs)) {
  table_name <- sprintf("%s_%02d", table_prefix, i)
  tryCatch(CDMConnector::dropSourceTable(cdm, name = table_name), error = function(e) NULL)

  cat("Run ", i, "/", n_runs, " ... ", sep = "")
  tm <- system.time({
    cdm <- CDMConnector::generateCohortSet(
      cdm = cdm,
      cohortSet = cohort_set,
      name = table_name,
      computeAttrition = FALSE,
      overwrite = TRUE
    )
  })

  times[[i]] <- unname(tm[["elapsed"]])
  results[[i]] <- as.data.frame(dplyr::collect(cdm[[table_name]]))
  cat("rows=", nrow(results[[i]]), ", time=", sprintf("%.3f", times[[i]]), "s\n", sep = "")

  tryCatch(CDMConnector::dropSourceTable(cdm, name = table_name), error = function(e) NULL)
}

for (i in seq_len(n_runs - 1L)) {
  key_a <- row_key(results[[i]])
  key_b <- row_key(results[[i + 1L]])
  only_a <- setdiff(key_a, key_b)
  only_b <- setdiff(key_b, key_a)

  cat("\nCompare run ", i, " vs ", i + 1L, "\n", sep = "")
  cat("Rows run ", i, ": ", nrow(results[[i]]), "\n", sep = "")
  cat("Rows run ", i + 1L, ": ", nrow(results[[i + 1L]]), "\n", sep = "")
  cat("Only in run ", i, ": ", length(only_a), "\n", sep = "")
  cat("Only in run ", i + 1L, ": ", length(only_b), "\n", sep = "")

  if (length(only_a) > 0L || length(only_b) > 0L) {
    cat("\nSample rows only in run ", i, ":\n", sep = "")
    if (length(only_a) > 0L) {
      cat(paste(utils::head(sort(only_a), 10), collapse = "\n"), "\n", sep = "")
    } else {
      cat("<none>\n")
    }

    cat("\nSample rows only in run ", i + 1L, ":\n", sep = "")
    if (length(only_b) > 0L) {
      cat(paste(utils::head(sort(only_b), 10), collapse = "\n"), "\n", sep = "")
    } else {
      cat("<none>\n")
    }
  }
}
