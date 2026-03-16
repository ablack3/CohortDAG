#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(CDMConnector)
  library(dplyr)
  library(DBI)
  library(odbc)
})

qualify_schema <- function(schema, database = "") {
  parts <- strsplit(schema, "\\.", fixed = FALSE)[[1L]]
  if (length(parts) == 1L) {
    if (nzchar(database)) {
      return(paste(database, parts[[1L]], sep = "."))
    }
    return(parts[[1L]])
  }
  paste(parts[seq_len(min(2L, length(parts)))], collapse = ".")
}

split_schema <- function(schema, database = "") {
  qualified <- qualify_schema(schema, database)
  parts <- strsplit(qualified, "\\.", fixed = FALSE)[[1L]]
  if (length(parts) == 1L) {
    return(parts[[1L]])
  }
  c(catalog = parts[[1L]], schema = parts[[2L]])
}

get_connection <- function() {
  user <- Sys.getenv("SNOWFLAKE_USER", unset = "")
  password <- Sys.getenv("SNOWFLAKE_PASSWORD", unset = "")
  server <- Sys.getenv("SNOWFLAKE_SERVER", unset = "")
  database <- Sys.getenv("SNOWFLAKE_DATABASE", unset = "")
  warehouse <- Sys.getenv("SNOWFLAKE_WAREHOUSE", unset = "")
  driver <- Sys.getenv("SNOWFLAKE_DRIVER", unset = "")

  if (!all(nzchar(c(user, password, server, database, warehouse, driver)))) {
    stop("Set SNOWFLAKE_SERVER, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_DATABASE, SNOWFLAKE_WAREHOUSE, and SNOWFLAKE_DRIVER.")
  }

  DBI::dbConnect(
    odbc::odbc(),
    SERVER = server,
    UID = user,
    PWD = password,
    DATABASE = database,
    WAREHOUSE = warehouse,
    DRIVER = driver
  )
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

pairwise_compare <- function(run_a, run_b, cohort_name, cohort_id, run_id_a, run_id_b) {
  key_a <- row_key(run_a)
  key_b <- row_key(run_b)
  only_a <- setdiff(key_a, key_b)
  only_b <- setdiff(key_b, key_a)

  data.frame(
    cohort_definition_id = cohort_id,
    cohort_name = cohort_name,
    run_a = run_id_a,
    run_b = run_id_b,
    rows_a = nrow(run_a),
    rows_b = nrow(run_b),
    only_in_a = length(only_a),
    only_in_b = length(only_b),
    identical = length(only_a) == 0L && length(only_b) == 0L,
    stringsAsFactors = FALSE
  )
}

pairwise_diff_rows <- function(run_a, run_b, cohort_name, cohort_id, run_id_a, run_id_b) {
  key_a <- row_key(run_a)
  key_b <- row_key(run_b)
  only_a <- setdiff(key_a, key_b)
  only_b <- setdiff(key_b, key_a)

  rows <- list()
  if (length(only_a) > 0L) {
    rows[[length(rows) + 1L]] <- data.frame(
      cohort_definition_id = cohort_id,
      cohort_name = cohort_name,
      run_a = run_id_a,
      run_b = run_id_b,
      context = "only_in_a",
      row_key = only_a,
      stringsAsFactors = FALSE
    )
  }
  if (length(only_b) > 0L) {
    rows[[length(rows) + 1L]] <- data.frame(
      cohort_definition_id = cohort_id,
      cohort_name = cohort_name,
      run_a = run_id_a,
      run_b = run_id_b,
      context = "only_in_b",
      row_key = only_b,
      stringsAsFactors = FALSE
    )
  }
  if (length(rows) == 0L) {
    return(data.frame(
      cohort_definition_id = integer(0),
      cohort_name = character(0),
      run_a = integer(0),
      run_b = integer(0),
      context = character(0),
      row_key = character(0),
      stringsAsFactors = FALSE
    ))
  }
  do.call(rbind, rows)
}

read_single_cohort_set <- function(cohort_name, cohort_dir = file.path("inst", "cohorts"), cohort_id = NULL) {
  cohort_file <- file.path(cohort_dir, paste0(cohort_name, ".json"))
  if (!file.exists(cohort_file)) {
    stop("Missing cohort JSON: ", cohort_file)
  }

  temp_dir <- tempfile("cdmconnector_repro_")
  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
  file.copy(cohort_file, file.path(temp_dir, basename(cohort_file)))
  cohort_set <- CDMConnector::readCohortSet(temp_dir)
  if (!is.null(cohort_id)) {
    cohort_set$cohort_definition_id <- cohort_id
  }
  cohort_set
}

default_cohorts <- c(
  "acute_respiratory_failure_in_inpatient_or_emergency_room",
  "acute_urinary_tract_infections_uti"
)

cohort_names <- Sys.getenv("CDMCONNECTOR_REPRO_COHORTS", unset = paste(default_cohorts, collapse = ","))
cohort_names <- trimws(strsplit(cohort_names, ",", fixed = TRUE)[[1L]])
cohort_names <- cohort_names[nzchar(cohort_names)]

repeats <- as.integer(Sys.getenv("CDMCONNECTOR_REPRO_REPEATS", unset = "2"))
sample_n <- as.integer(Sys.getenv("CDMCONNECTOR_REPRO_SAMPLE_N", unset = "20"))
cohort_dir <- Sys.getenv("CDMCONNECTOR_REPRO_COHORT_DIR", unset = file.path("inst", "cohorts"))

if (repeats < 2L) {
  stop("CDMCONNECTOR_REPRO_REPEATS must be >= 2")
}

snowflake_database <- Sys.getenv("SNOWFLAKE_DATABASE", unset = "")
cdm_schema <- split_schema(Sys.getenv("SNOWFLAKE_CDM_SCHEMA", unset = "CDM53"), snowflake_database)
write_schema <- split_schema(Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA", unset = "SCRATCH"), snowflake_database)

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_dir <- file.path("inst", "benchmark-results", paste0("cdmconnector_snowflake_reprex_", timestamp))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
name_prefix <- paste0("cdmrepro_", format(Sys.time(), "%H%M%S"), "_", Sys.getpid())

cohort_ids <- seq_along(cohort_names)
names(cohort_ids) <- cohort_names
if ("acute_respiratory_failure_in_inpatient_or_emergency_room" %in% cohort_names) {
  cohort_ids[["acute_respiratory_failure_in_inpatient_or_emergency_room"]] <- 27L
}
if ("acute_urinary_tract_infections_uti" %in% cohort_names) {
  cohort_ids[["acute_urinary_tract_infections_uti"]] <- 35L
}

con <- get_connection()
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

cdm <- CDMConnector::cdmFromCon(
  con,
  cdmSchema = cdm_schema,
  cdmName = "cdmconnector_repro",
  writeSchema = write_schema
)

summary_rows <- list()
diff_rows <- list()

cat("Output directory:", normalizePath(out_dir, winslash = "/", mustWork = FALSE), "\n")
cat("CDM schema:", paste(cdm_schema, collapse = "."), "\n")
cat("Write schema:", paste(write_schema, collapse = "."), "\n")

for (cohort_name in cohort_names) {
  cohort_id <- cohort_ids[[cohort_name]]
  cohort_set <- read_single_cohort_set(cohort_name, cohort_dir = cohort_dir, cohort_id = cohort_id)
  run_results <- vector("list", repeats)
  run_times <- numeric(repeats)

  cat("\n=== Cohort:", cohort_name, "(id=", cohort_id, ") ===\n", sep = "")

  for (run_idx in seq_len(repeats)) {
    table_name <- sprintf("%s_%03d_%02d", name_prefix, cohort_id, run_idx)
    tryCatch(CDMConnector::dropSourceTable(cdm, name = table_name), error = function(e) NULL)
    cat("Run ", run_idx, "/", repeats, " ... ", sep = "")
    tm <- system.time({
      cdm <- CDMConnector::generateCohortSet(
        cdm,
        cohortSet = cohort_set,
        name = table_name,
        computeAttrition = FALSE,
        overwrite = TRUE
      )
    })
    run_times[[run_idx]] <- unname(tm[["elapsed"]])
    run_results[[run_idx]] <- as.data.frame(dplyr::collect(cdm[[table_name]]))
    cat("rows=", nrow(run_results[[run_idx]]), ", time=", sprintf("%.3f", run_times[[run_idx]]), "s\n", sep = "")
    tryCatch(CDMConnector::dropSourceTable(cdm, name = table_name), error = function(e) NULL)
  }

  for (run_idx in seq_len(repeats - 1L)) {
    cmp <- pairwise_compare(
      run_results[[run_idx]],
      run_results[[run_idx + 1L]],
      cohort_name = cohort_name,
      cohort_id = cohort_id,
      run_id_a = run_idx,
      run_id_b = run_idx + 1L
    )
    cmp$time_a_sec <- round(run_times[[run_idx]], 3)
    cmp$time_b_sec <- round(run_times[[run_idx + 1L]], 3)
    summary_rows[[length(summary_rows) + 1L]] <- cmp

    diffs <- pairwise_diff_rows(
      run_results[[run_idx]],
      run_results[[run_idx + 1L]],
      cohort_name = cohort_name,
      cohort_id = cohort_id,
      run_id_a = run_idx,
      run_id_b = run_idx + 1L
    )

    if (nrow(diffs) > 0L) {
      diff_rows[[length(diff_rows) + 1L]] <- utils::head(diffs, sample_n)
      cat(
        "  Non-identical runs detected: only_in_run_", run_idx, "=",
        cmp$only_in_a, ", only_in_run_", run_idx + 1L, "=",
        cmp$only_in_b, "\n",
        sep = ""
      )
    } else {
      cat("  Runs ", run_idx, " and ", run_idx + 1L, " are identical.\n", sep = "")
    }
  }
}

summary_df <- do.call(rbind, summary_rows)
diff_df <- if (length(diff_rows) > 0L) do.call(rbind, diff_rows) else {
  data.frame(
    cohort_definition_id = integer(0),
    cohort_name = character(0),
    run_a = integer(0),
    run_b = integer(0),
    context = character(0),
    row_key = character(0),
    stringsAsFactors = FALSE
  )
}

utils::write.csv(summary_df, file.path(out_dir, "cdmconnector_reprex_summary.csv"), row.names = FALSE)
utils::write.csv(diff_df, file.path(out_dir, "cdmconnector_reprex_rowdiff_sample.csv"), row.names = FALSE)

cat("\nSummary written to:", file.path(out_dir, "cdmconnector_reprex_summary.csv"), "\n")
cat("Row diff sample written to:", file.path(out_dir, "cdmconnector_reprex_rowdiff_sample.csv"), "\n")

if (any(!summary_df$identical)) {
  cat("\nNon-determinism reproduced.\n")
} else {
  cat("\nNo non-determinism observed in this run.\n")
}
