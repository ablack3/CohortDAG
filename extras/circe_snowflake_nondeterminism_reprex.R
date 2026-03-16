#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(CirceR)
  library(SqlRender)
  library(DatabaseConnector)
})

truthy <- function(x) {
  tolower(if (is.null(x) || length(x) == 0L) "" else x) %in% c("true", "1", "yes")
}

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
    return(list(database = "", schema = parts[[1L]]))
  }
  list(database = parts[[1L]], schema = parts[[2L]])
}

get_connection <- function() {
  connection_string <- Sys.getenv("SNOWFLAKE_CONNECTION_STRING", unset = "")
  user <- Sys.getenv("SNOWFLAKE_USER", unset = "")
  password <- Sys.getenv("SNOWFLAKE_PASSWORD", unset = "")

  if (!nzchar(connection_string) || !nzchar(user) || !nzchar(password)) {
    stop("Set SNOWFLAKE_CONNECTION_STRING, SNOWFLAKE_USER, and SNOWFLAKE_PASSWORD.")
  }

  connection_details <- DatabaseConnector::createConnectionDetails(
    dbms = "snowflake",
    connectionString = connection_string,
    user = user,
    password = password
  )

  DatabaseConnector::connect(connection_details)
}

use_current_schema <- function(con, scratch_schema) {
  parts <- split_schema(scratch_schema)
  if (nzchar(parts$database)) {
    DatabaseConnector::executeSql(
      con,
      paste0("USE DATABASE ", parts$database, ";"),
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
  if (nzchar(parts$schema)) {
    DatabaseConnector::executeSql(
      con,
      paste0("USE SCHEMA ", parts$schema, ";"),
      progressBar = FALSE,
      reportOverallTime = FALSE
    )
  }
}

create_target_table <- function(con, target_schema, target_table) {
  DatabaseConnector::executeSql(
    con,
    paste0(
      "DROP TABLE IF EXISTS ", target_schema, ".", target_table, ";\n",
      "CREATE TABLE ", target_schema, ".", target_table, " (",
      "cohort_definition_id INT NOT NULL, ",
      "subject_id BIGINT NOT NULL, ",
      "cohort_start_date DATE NOT NULL, ",
      "cohort_end_date DATE NOT NULL",
      ");"
    ),
    progressBar = FALSE,
    reportOverallTime = FALSE
  )
}

drop_target_table <- function(con, target_schema, target_table) {
  try(
    DatabaseConnector::executeSql(
      con,
      paste0("DROP TABLE IF EXISTS ", target_schema, ".", target_table, ";"),
      progressBar = FALSE,
      reportOverallTime = FALSE
    ),
    silent = TRUE
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

query_target <- function(con, target_schema, target_table, cohort_id) {
  sql <- paste0(
    "SELECT cohort_definition_id, subject_id, cohort_start_date, cohort_end_date ",
    "FROM ", target_schema, ".", target_table, " ",
    "WHERE cohort_definition_id = ", cohort_id, " ",
    "ORDER BY subject_id, cohort_start_date, cohort_end_date"
  )
  DatabaseConnector::querySql(con, sql)
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

read_cohort_json <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

build_circe_sql <- function(json_path, cohort_id, cdm_schema, vocab_schema, target_schema, target_table) {
  expression <- CirceR::cohortExpressionFromJson(read_cohort_json(json_path))
  options <- CirceR::createGenerateOptions(
    cohortIdFieldName = "cohort_definition_id",
    cohortId = cohort_id,
    cdmSchema = "@cdm_database_schema",
    targetTable = "@target_database_schema.@target_cohort_table",
    resultSchema = "@results_schema",
    vocabularySchema = "@vocabulary_database_schema",
    generateStats = FALSE
  )

  raw_sql <- CirceR::buildCohortQuery(expression, options)
  rendered_sql <- SqlRender::render(
    raw_sql,
    cdm_database_schema = cdm_schema,
    vocabulary_database_schema = vocab_schema,
    results_schema = target_schema,
    target_database_schema = target_schema,
    target_cohort_table = target_table
  )
  SqlRender::translate(rendered_sql, targetDialect = "snowflake")
}

extract_order_snippets <- function(sql_text) {
  lines <- unlist(strsplit(sql_text, "\n", fixed = TRUE))
  hits <- grep("row_number\\(\\) over", lines, value = TRUE)
  paste(hits, collapse = "\n")
}

default_cohorts <- c(
  "acute_respiratory_failure_in_inpatient_or_emergency_room",
  "acute_urinary_tract_infections_uti"
)

cohort_names <- Sys.getenv("CIRCE_REPRO_COHORTS", unset = paste(default_cohorts, collapse = ","))
cohort_names <- trimws(strsplit(cohort_names, ",", fixed = TRUE)[[1L]])
cohort_names <- cohort_names[nzchar(cohort_names)]

cohort_dir <- Sys.getenv("CIRCE_REPRO_COHORT_DIR", unset = file.path("inst", "cohorts"))
repeats <- as.integer(Sys.getenv("CIRCE_REPRO_REPEATS", unset = "2"))
sample_n <- as.integer(Sys.getenv("CIRCE_REPRO_SAMPLE_N", unset = "20"))

snowflake_database <- Sys.getenv("SNOWFLAKE_DATABASE", unset = "")
cdm_schema <- qualify_schema(
  Sys.getenv("SNOWFLAKE_CDM_SCHEMA", unset = "CDM53"),
  snowflake_database
)
scratch_schema <- qualify_schema(
  Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA", unset = "SCRATCH"),
  snowflake_database
)
vocab_schema <- qualify_schema(
  Sys.getenv("SNOWFLAKE_VOCAB_SCHEMA", unset = Sys.getenv("SNOWFLAKE_CDM_SCHEMA", unset = "CDM53")),
  snowflake_database
)

if (repeats < 2L) {
  stop("CIRCE_REPRO_REPEATS must be >= 2")
}

timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
out_dir <- file.path("inst", "benchmark-results", paste0("circe_snowflake_reprex_", timestamp))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

cohort_files <- file.path(cohort_dir, paste0(cohort_names, ".json"))
missing_files <- cohort_files[!file.exists(cohort_files)]
if (length(missing_files) > 0L) {
  stop("Missing cohort JSON files: ", paste(missing_files, collapse = ", "))
}

cohort_ids <- seq_along(cohort_files)
names(cohort_ids) <- cohort_names
if ("acute_respiratory_failure_in_inpatient_or_emergency_room" %in% cohort_names) {
  cohort_ids[["acute_respiratory_failure_in_inpatient_or_emergency_room"]] <- 27L
}
if ("acute_urinary_tract_infections_uti" %in% cohort_names) {
  cohort_ids[["acute_urinary_tract_infections_uti"]] <- 35L
}

con <- get_connection()
on.exit(try(DatabaseConnector::disconnect(con), silent = TRUE), add = TRUE)
use_current_schema(con, scratch_schema)

summary_rows <- list()
diff_rows <- list()

cat("Output directory:", normalizePath(out_dir, winslash = "/", mustWork = FALSE), "\n")
cat("CDM schema:", cdm_schema, "\n")
cat("Scratch schema:", scratch_schema, "\n")
cat("Vocabulary schema:", vocab_schema, "\n")

for (cohort_name in cohort_names) {
  cohort_id <- cohort_ids[[cohort_name]]
  json_path <- file.path(cohort_dir, paste0(cohort_name, ".json"))
  target_table <- paste0("circe_repro_", cohort_id)

  cat("\n=== Cohort:", cohort_name, "(id=", cohort_id, ") ===\n", sep = "")
  create_target_table(con, scratch_schema, target_table)
  on.exit(drop_target_table(con, scratch_schema, target_table), add = TRUE)

  sql_text <- build_circe_sql(
    json_path = json_path,
    cohort_id = cohort_id,
    cdm_schema = cdm_schema,
    vocab_schema = vocab_schema,
    target_schema = scratch_schema,
    target_table = target_table
  )
  writeLines(sql_text, file.path(out_dir, sprintf("cohort_%03d_circe_snowflake.sql", cohort_id)))
  writeLines(extract_order_snippets(sql_text), file.path(out_dir, sprintf("cohort_%03d_order_snippets.txt", cohort_id)))

  run_results <- vector("list", repeats)
  run_times <- numeric(repeats)
  for (run_idx in seq_len(repeats)) {
    cat("Run ", run_idx, "/", repeats, " ... ", sep = "")
    tm <- system.time({
      DatabaseConnector::executeSql(
        con,
        sql_text,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    })
    run_times[[run_idx]] <- unname(tm[["elapsed"]])
    run_results[[run_idx]] <- query_target(con, scratch_schema, target_table, cohort_id)
    cat("rows=", nrow(run_results[[run_idx]]), ", time=", sprintf("%.3f", run_times[[run_idx]]), "s\n", sep = "")
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

  drop_target_table(con, scratch_schema, target_table)
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

utils::write.csv(summary_df, file.path(out_dir, "circe_reprex_summary.csv"), row.names = FALSE)
utils::write.csv(diff_df, file.path(out_dir, "circe_reprex_rowdiff_sample.csv"), row.names = FALSE)

cat("\nSummary written to:", file.path(out_dir, "circe_reprex_summary.csv"), "\n")
cat("Row diff sample written to:", file.path(out_dir, "circe_reprex_rowdiff_sample.csv"), "\n")

if (any(!summary_df$identical)) {
  cat("\nNon-determinism reproduced.\n")
} else {
  cat("\nNo non-determinism observed in this run.\n")
}
