#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(pkgload)
  library(CDMConnector)
  library(dplyr)
  library(withr)
})

pkgload::load_all(".", quiet = TRUE)
source("tests/testthat/helper-compare.R")

assignInNamespace("cohdSimilarConcepts", function(...) NULL, ns = "CDMConnector")

split_schema <- function(s, database) {
  parts <- strsplit(s, "\\.")[[1L]]
  if (length(parts) == 1L) {
    return(c(catalog = database, schema = parts[1L]))
  }
  if (length(parts) == 2L) {
    return(c(catalog = parts[1L], schema = parts[2L]))
  }
  c(catalog = parts[1L], schema = parts[2L], prefix = parts[3L])
}

normalize_schema_str_local <- function(x) {
  CohortDAG:::normalize_schema_str(x)
}

row_key <- function(df) {
  if (nrow(df) == 0L) {
    return(character(0))
  }
  paste(
    as.integer(df$cohort_definition_id),
    as.integer(df$subject_id),
    as.character(as.Date(df$cohort_start_date)),
    as.character(as.Date(df$cohort_end_date)),
    sep = "|"
  )
}

collect_table_df <- function(tbl) {
  df <- as.data.frame(dplyr::collect(tbl))
  keep <- intersect(
    c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date"),
    names(df)
  )
  df <- df[, keep, drop = FALSE]
  if ("cohort_definition_id" %in% names(df)) df$cohort_definition_id <- as.integer(df$cohort_definition_id)
  if ("subject_id" %in% names(df)) df$subject_id <- as.integer(df$subject_id)
  if ("cohort_start_date" %in% names(df)) df$cohort_start_date <- as.character(as.Date(df$cohort_start_date))
  if ("cohort_end_date" %in% names(df)) df$cohort_end_date <- as.character(as.Date(df$cohort_end_date))
  df
}

run_generation_pair <- function(cdm, cohort_set, old_name, new_name) {
  cleanup <- function(name) {
    tryCatch(CDMConnector::dropSourceTable(cdm, name = name), error = function(e) NULL)
    tryCatch(CDMConnector::dropSourceTable(cdm, name = paste0(name, "_set")), error = function(e) NULL)
    tryCatch(CDMConnector::dropSourceTable(cdm, name = paste0(name, "_attrition")), error = function(e) NULL)
    tryCatch(CDMConnector::dropSourceTable(cdm, name = paste0(name, "_codelist")), error = function(e) NULL)
  }
  cleanup(old_name)
  cleanup(new_name)

  old_err <- NULL
  new_err <- NULL
  old_time <- system.time({
    old_err <<- tryCatch({
      cdm <- CDMConnector::generateCohortSet(
        cdm,
        cohortSet = cohort_set,
        name = old_name,
        computeAttrition = FALSE,
        overwrite = TRUE
      )
      NULL
    }, error = function(e) conditionMessage(e))
  })
  new_time <- system.time({
    new_err <<- tryCatch({
      cdm <- CohortDAG::generateCohortSet2(
        cdm,
        cohortSet = cohort_set,
        name = new_name,
        computeAttrition = FALSE,
        overwrite = TRUE
      )
      NULL
    }, error = function(e) conditionMessage(e))
  })

  result <- list(
    cdm = cdm,
    old_err = old_err,
    new_err = new_err,
    old_time = old_time[["elapsed"]],
    new_time = new_time[["elapsed"]]
  )

  if (!is.null(old_err) || !is.null(new_err)) {
    cleanup(old_name)
    cleanup(new_name)
    return(result)
  }

  result$df_old <- collect_table_df(cdm[[old_name]])
  result$df_new <- collect_table_df(cdm[[new_name]])
  cleanup(old_name)
  cleanup(new_name)
  result
}

results_dir <- file.path("inst", "benchmark-results", "live-db")
debug_dir <- file.path(
  results_dir,
  paste0("snowflake_targeted_debug_", format(Sys.time(), "%Y%m%d_%H%M%S"))
)
dir.create(debug_dir, recursive = TRUE, showWarnings = FALSE)

mismatch_file <- Sys.getenv(
  "SNOWFLAKE_MISMATCH_FILE",
  unset = file.path(results_dir, "snowflake_20260315_155042", "tier3_live_mismatch_details.csv")
)
batch_size <- as.integer(Sys.getenv("SNOWFLAKE_DEBUG_BATCH_SIZE", unset = "50"))
run_batch_reruns <- tolower(Sys.getenv("SNOWFLAKE_DEBUG_RUN_BATCH", unset = "false")) %in% c("true", "1", "yes")

server <- Sys.getenv("SNOWFLAKE_SERVER", unset = "")
user <- Sys.getenv("SNOWFLAKE_USER", unset = "")
password <- Sys.getenv("SNOWFLAKE_PASSWORD", unset = "")
database <- Sys.getenv("SNOWFLAKE_DATABASE", unset = "")
warehouse <- Sys.getenv("SNOWFLAKE_WAREHOUSE", unset = "")
driver <- Sys.getenv("SNOWFLAKE_DRIVER", unset = "")
scratch <- Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA", unset = "")
cdm_schema_raw <- Sys.getenv("SNOWFLAKE_CDM_SCHEMA", unset = "CDM")

required <- c(server, user, password, database, warehouse, driver, scratch, cdm_schema_raw)
if (any(required == "")) {
  stop("Snowflake environment variables are missing")
}
if (!file.exists(mismatch_file)) {
  stop("Mismatch file not found: ", mismatch_file)
}

mismatches <- read.csv(mismatch_file, stringsAsFactors = FALSE)
if (nrow(mismatches) == 0L) {
  stop("Mismatch file is empty: ", mismatch_file)
}

cohorts_dir <- unzip_cohorts()
on.exit(unlink(cohorts_dir, recursive = TRUE), add = TRUE)
all_files <- sort(list.files(cohorts_dir, pattern = "\\.json$", full.names = TRUE))
all_names <- tools::file_path_sans_ext(basename(all_files))

con <- DBI::dbConnect(
  odbc::odbc(),
  server = server,
  uid = user,
  pwd = password,
  database = database,
  warehouse = warehouse,
  driver = driver
)
on.exit(try(DBI::dbDisconnect(con), silent = TRUE), add = TRUE)

cdm_schema_spec <- split_schema(cdm_schema_raw, database)
write_schema_spec <- split_schema(scratch, database)
cdm_schema_str <- normalize_schema_str_local(cdm_schema_spec)
write_schema_str <- normalize_schema_str_local(write_schema_spec)

cdm <- CDMConnector::cdmFromCon(
  con,
  cdmSchema = cdm_schema_spec,
  cdmName = "snowflake_debug",
  writeSchema = write_schema_spec
)

local_options(list(CohortDAG.force_target_dialect = "snowflake"))

summary_rows <- list()
rowdiff_rows <- list()
batch_result_cache <- new.env(parent = emptyenv())

for (i in seq_len(nrow(mismatches))) {
  mm <- mismatches[i, , drop = FALSE]
  cohort_name <- mm$cohort_name[[1L]]
  cohort_id <- mm$cohort_definition_id[[1L]]
  batch_idx <- mm$batch[[1L]]
  match_idx <- which(all_names == cohort_name)

  if (length(match_idx) != 1L) {
    summary_rows[[length(summary_rows) + 1L]] <- data.frame(
      batch = batch_idx,
      cohort_definition_id = cohort_id,
      cohort_name = cohort_name,
      single_status = "json_not_found",
      batch_status = NA_character_,
      single_accuracy = NA_real_,
      batch_accuracy = NA_real_,
      stringsAsFactors = FALSE
    )
    next
  }

  single_dir <- tempfile("snowflake_single_")
  dir.create(single_dir, recursive = TRUE)
  on.exit(unlink(single_dir, recursive = TRUE), add = TRUE)
  file.copy(all_files[match_idx], file.path(single_dir, basename(all_files[match_idx])))
  single_set <- make_unique_cohort_names(CDMConnector::readCohortSet(single_dir))
  single_set$cohort_definition_id <- cohort_id
  if ("original_cohort_definition_id" %in% names(single_set)) {
    single_set$original_cohort_definition_id <- cohort_id
  }

  if (isTRUE(run_batch_reruns)) {
    batch_start <- (batch_idx - 1L) * batch_size + 1L
    batch_end <- min(batch_idx * batch_size, length(all_files))
    batch_files <- all_files[batch_start:batch_end]
    batch_dir <- tempfile("snowflake_batch_")
    dir.create(batch_dir, recursive = TRUE)
    on.exit(unlink(batch_dir, recursive = TRUE), add = TRUE)
    file.copy(batch_files, file.path(batch_dir, basename(batch_files)))
    batch_set <- make_unique_cohort_names(CDMConnector::readCohortSet(batch_dir))
  } else {
    batch_set <- NULL
  }

  single_res <- run_generation_pair(
    cdm, single_set,
    old_name = paste0("sf_old_single_", cohort_id),
    new_name = paste0("sf_new_single_", cohort_id)
  )
  cdm <- single_res$cdm

  if (isTRUE(run_batch_reruns)) {
    cache_key <- as.character(batch_idx)
    if (exists(cache_key, envir = batch_result_cache, inherits = FALSE)) {
      batch_res <- get(cache_key, envir = batch_result_cache, inherits = FALSE)
    } else {
      batch_res <- run_generation_pair(
        cdm, batch_set,
        old_name = paste0("sf_old_batch_", batch_idx),
        new_name = paste0("sf_new_batch_", batch_idx)
      )
      assign(cache_key, batch_res, envir = batch_result_cache)
    }
    cdm <- batch_res$cdm
  } else {
    batch_res <- list(
      cdm = cdm,
      old_err = NULL,
      new_err = NULL,
      old_time = NA_real_,
      new_time = NA_real_,
      df_old = NULL,
      df_new = NULL
    )
  }

  single_cmp <- NULL
  batch_cmp <- NULL
  single_only_old <- character(0)
  single_only_new <- character(0)
  batch_only_old <- character(0)
  batch_only_new <- character(0)

  if (is.null(single_res$old_err) && is.null(single_res$new_err)) {
    single_cmp <- compare_cohort_dfs_detailed(single_res$df_old, single_res$df_new)
    single_only_old <- setdiff(row_key(single_res$df_old), row_key(single_res$df_new))
    single_only_new <- setdiff(row_key(single_res$df_new), row_key(single_res$df_old))
  }

  if (isTRUE(run_batch_reruns) && is.null(batch_res$old_err) && is.null(batch_res$new_err)) {
    batch_old_one <- batch_res$df_old[batch_res$df_old$cohort_definition_id == cohort_id, , drop = FALSE]
    batch_new_one <- batch_res$df_new[batch_res$df_new$cohort_definition_id == cohort_id, , drop = FALSE]
    batch_cmp <- compare_cohort_dfs_detailed(batch_old_one, batch_new_one)
    batch_only_old <- setdiff(row_key(batch_old_one), row_key(batch_new_one))
    batch_only_new <- setdiff(row_key(batch_new_one), row_key(batch_old_one))
  } else {
    batch_old_one <- data.frame()
    batch_new_one <- data.frame()
  }

  circe_sql <- CohortDAG:::atlas_json_to_sql(
    all_files[match_idx],
    cohort_id = cohort_id,
    cdm_schema = cdm_schema_str,
    vocabulary_schema = cdm_schema_str,
    target_schema = write_schema_str,
    target_table = paste0("debug_target_", cohort_id),
    target_dialect = "snowflake",
    render = TRUE,
    generate_stats = FALSE
  )
  dag_sql <- CohortDAG:::atlas_json_to_sql_batch(
    single_set,
    cdm_schema = cdm_schema_str,
    results_schema = write_schema_str,
    target_dialect = "snowflake",
    optimize = TRUE
  )

  writeLines(circe_sql, file.path(debug_dir, sprintf("cohort_%03d_circe.sql", cohort_id)))
  writeLines(dag_sql, file.path(debug_dir, sprintf("cohort_%03d_dag.sql", cohort_id)))

  summary_rows[[length(summary_rows) + 1L]] <- data.frame(
    batch = batch_idx,
    cohort_definition_id = cohort_id,
    cohort_name = cohort_name,
    single_status = if (!is.null(single_res$old_err) || !is.null(single_res$new_err)) {
      "method_error"
    } else if (isTRUE(single_cmp$identical)) {
      "single_match"
    } else {
      "single_mismatch"
    },
    batch_status = if (!isTRUE(run_batch_reruns)) {
      "not_run"
    } else if (!is.null(batch_res$old_err) || !is.null(batch_res$new_err)) {
      "method_error"
    } else if (isTRUE(batch_cmp$identical)) {
      "batch_match"
    } else {
      "batch_mismatch"
    },
    single_accuracy = if (is.null(single_cmp)) NA_real_ else round(100 * mean(single_cmp$per_cohort$identical), 3),
    batch_accuracy = if (is.null(batch_cmp)) NA_real_ else round(100 * mean(batch_cmp$per_cohort$identical), 3),
    single_rows_old = if (is.null(single_cmp)) NA_integer_ else single_cmp$n_rows_old,
    single_rows_new = if (is.null(single_cmp)) NA_integer_ else single_cmp$n_rows_new,
    batch_rows_old = if (is.null(batch_cmp)) NA_integer_ else batch_cmp$n_rows_old,
    batch_rows_new = if (is.null(batch_cmp)) NA_integer_ else batch_cmp$n_rows_new,
    single_only_in_old = length(single_only_old),
    single_only_in_new = length(single_only_new),
    batch_only_in_old = length(batch_only_old),
    batch_only_in_new = length(batch_only_new),
    single_old_sec = round(single_res$old_time, 3),
    single_new_sec = round(single_res$new_time, 3),
    batch_old_sec = round(batch_res$old_time, 3),
    batch_new_sec = round(batch_res$new_time, 3),
    single_error = paste(na.omit(c(single_res$old_err, single_res$new_err)), collapse = " | "),
    batch_error = if (isTRUE(run_batch_reruns)) paste(na.omit(c(batch_res$old_err, batch_res$new_err)), collapse = " | ") else "",
    stringsAsFactors = FALSE
  )

  if (length(single_only_old) > 0L) {
    rowdiff_rows[[length(rowdiff_rows) + 1L]] <- data.frame(
      batch = batch_idx,
      cohort_definition_id = cohort_id,
      cohort_name = cohort_name,
      context = "single_old_only",
      row_key = single_only_old,
      stringsAsFactors = FALSE
    )
  }
  if (length(single_only_new) > 0L) {
    rowdiff_rows[[length(rowdiff_rows) + 1L]] <- data.frame(
      batch = batch_idx,
      cohort_definition_id = cohort_id,
      cohort_name = cohort_name,
      context = "single_new_only",
      row_key = single_only_new,
      stringsAsFactors = FALSE
    )
  }
  if (length(batch_only_old) > 0L) {
    rowdiff_rows[[length(rowdiff_rows) + 1L]] <- data.frame(
      batch = batch_idx,
      cohort_definition_id = cohort_id,
      cohort_name = cohort_name,
      context = "batch_old_only",
      row_key = batch_only_old,
      stringsAsFactors = FALSE
    )
  }
  if (length(batch_only_new) > 0L) {
    rowdiff_rows[[length(rowdiff_rows) + 1L]] <- data.frame(
      batch = batch_idx,
      cohort_definition_id = cohort_id,
      cohort_name = cohort_name,
      context = "batch_new_only",
      row_key = batch_only_new,
      stringsAsFactors = FALSE
    )
  }
}

summary_df <- do.call(rbind, summary_rows)
rowdiff_df <- if (length(rowdiff_rows) > 0L) do.call(rbind, rowdiff_rows) else {
  data.frame(
    batch = integer(0),
    cohort_definition_id = integer(0),
    cohort_name = character(0),
    context = character(0),
    row_key = character(0),
    stringsAsFactors = FALSE
  )
}

utils::write.csv(summary_df, file.path(debug_dir, "snowflake_targeted_summary.csv"), row.names = FALSE)
utils::write.csv(rowdiff_df, file.path(debug_dir, "snowflake_targeted_rowdiffs.csv"), row.names = FALSE)

message("Wrote debug artifacts to: ", debug_dir)
