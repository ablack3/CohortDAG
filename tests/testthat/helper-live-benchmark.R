# helper-live-benchmark.R
# Shared helpers for tier-3 live database batch benchmarks.

live_benchmark_truthy <- function(x) {
  tolower(if (is.null(x) || length(x) == 0L) "" else x) %in% c("true", "1", "yes")
}

live_benchmark_force_dialect <- function(dbms) {
  switch(
    tolower(dbms),
    postgres = "postgresql",
    redshift = "redshift",
    sqlserver = "sql server",
    snowflake = "snowflake",
    spark = "spark",
    bigquery = "bigquery",
    duckdb = "duckdb",
    dbms
  )
}

live_benchmark_results_dir <- function() {
  results_dir <- Sys.getenv("TIER3_RESULTS_DIR", unset = "")
  if (!nzchar(results_dir)) {
    results_dir <- normalizePath(
      file.path(testthat::test_path(), "..", "..", "inst", "benchmark-results", "live-db"),
      winslash = "/",
      mustWork = FALSE
    )
  }
  dir.create(results_dir, recursive = TRUE, showWarnings = FALSE)
  results_dir
}

live_benchmark_requested_dbms <- function() {
  requested <- Sys.getenv("TIER3_DBMS", unset = "")
  if (nzchar(requested)) {
    return(unique(trimws(strsplit(requested, ",", fixed = TRUE)[[1L]])))
  }
  if (exists("dbToTest", inherits = TRUE)) {
    return(unique(get("dbToTest", inherits = TRUE)))
  }
  c("duckdb")
}

live_benchmark_db_available <- function(dbms) {
  dbms <- tolower(dbms)
  using_database_connector <- isTRUE(get0("testUsingDatabaseConnector", inherits = TRUE, ifnotfound = FALSE))

  if (dbms == "duckdb") {
    return(rlang::is_installed("duckdb"))
  }

  if (using_database_connector) {
    return(switch(
      dbms,
      postgres = nzchar(Sys.getenv("CDM5_POSTGRESQL_SERVER", unset = "")),
      redshift = nzchar(Sys.getenv("CDM5_REDSHIFT_SERVER", unset = "")),
      sqlserver = nzchar(Sys.getenv("CDM5_SQL_SERVER_SERVER", unset = "")),
      snowflake = nzchar(Sys.getenv("SNOWFLAKE_CONNECTION_STRING", unset = "")) &&
        nzchar(Sys.getenv("SNOWFLAKE_USER", unset = "")),
      bigquery = nzchar(Sys.getenv("BIGQUERY_CONNECTION_STRING", unset = "")),
      spark = nzchar(Sys.getenv("DATABRICKS_CONNECTION_STRING", unset = "")) &&
        nzchar(Sys.getenv("DATABRICKS_USER", unset = "")),
      FALSE
    ))
  }

  switch(
    dbms,
    postgres = nzchar(Sys.getenv("CDM5_POSTGRESQL_DBNAME", unset = "")),
    redshift = nzchar(Sys.getenv("CDM5_REDSHIFT_DBNAME", unset = "")),
    sqlserver = nzchar(Sys.getenv("CDM5_SQL_SERVER_USER", unset = "")),
    snowflake = nzchar(Sys.getenv("SNOWFLAKE_USER", unset = "")),
    bigquery = nzchar(Sys.getenv("BIGQUERY_SERVICE_ACCOUNT_JSON_PATH", unset = "")),
    spark = nzchar(Sys.getenv("DATABRICKS_HTTPPATH", unset = "")),
    FALSE
  )
}

live_benchmark_dbms <- function() {
  requested <- live_benchmark_requested_dbms()
  requested[vapply(requested, live_benchmark_db_available, logical(1))]
}

live_benchmark_worker_count <- function(dbms) {
  n_dbms <- length(dbms)
  requested <- as.integer(Sys.getenv("TIER3_N_CORES", unset = "0"))
  if (is.na(requested) || requested < 0L) {
    requested <- 0L
  }
  if (requested == 0L) {
    detected <- tryCatch(parallel::detectCores(logical = FALSE), error = function(e) 1L)
    requested <- min(n_dbms, max(1L, detected))
  }
  max(1L, min(as.integer(n_dbms), requested))
}

live_benchmark_cleanup_prefix <- function(cdm, con, write_schema) {
  existing <- tryCatch(CDMConnector::listTables(con, schema = write_schema), error = function(e) character())
  prefix <- CohortDAG:::extract_write_prefix(write_schema)
  if (!nzchar(prefix) || length(existing) == 0L) {
    return(invisible(NULL))
  }
  to_drop <- existing[startsWith(existing, prefix)]
  if (length(to_drop) == 0L) {
    return(invisible(NULL))
  }
  dbms <- dbplyr::dbms(con)
  for (tbl_name in to_drop) {
    stripped <- sub(paste0("^", prefix), "", tbl_name)
    tryCatch(
      DBI::dbRemoveTable(con, CDMConnector:::.inSchema(write_schema, stripped, dbms = dbms)),
      error = function(e) NULL
    )
  }
}

cleanup_live_benchmark_tables <- function(cdm, names) {
  for (tbl_name in unique(names)) {
    if (!nzchar(tbl_name)) next
    tryCatch(CDMConnector::dropSourceTable(cdm, name = tbl_name), error = function(e) NULL)
  }
}

cleanup_live_benchmark_outputs <- function(cdm, base_name) {
  cleanup_live_benchmark_tables(cdm, c(
    base_name,
    paste0(base_name, "_set"),
    paste0(base_name, "_attrition"),
    paste0(base_name, "_codelist")
  ))
}

with_live_benchmark_log <- function(log_file, expr) {
  log_con <- file(log_file, open = "at")
  output_depth <- sink.number(type = "output")
  message_depth <- sink.number(type = "message")
  sink(log_con, type = "output", split = TRUE)
  sink(log_con, type = "message")
  on.exit({
    while (sink.number(type = "message") > message_depth) {
      sink(type = "message")
    }
    while (sink.number(type = "output") > output_depth) {
      sink(type = "output")
    }
    close(log_con)
  }, add = TRUE)
  force(expr)
}

read_live_benchmark_batch <- function(batch_files) {
  temp_dir <- tempfile("tier3_live_batch_")
  dir.create(temp_dir, recursive = TRUE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
  for (f in batch_files) {
    file.copy(f, file.path(temp_dir, basename(f)))
  }
  make_unique_cohort_names(CDMConnector::readCohortSet(temp_dir))
}

resolve_live_benchmark_batches <- function(n_batches) {
  batch_start <- as.integer(Sys.getenv("TIER3_BATCH_START", unset = "1"))
  batch_count <- as.integer(Sys.getenv("TIER3_BATCH_COUNT", unset = "2"))
  if (is.na(batch_start) || batch_start < 1L) batch_start <- 1L
  if (is.na(batch_count) || batch_count < 1L) batch_count <- 2L
  batch_end <- min(n_batches, batch_start + batch_count - 1L)
  seq.int(batch_start, batch_end)
}

build_live_benchmark_summary <- function(dbms, results, selected_batches, batch_size, timestamp_str) {
  ok <- results[results$status == "ok", , drop = FALSE]
  mismatches <- results[results$status == "mismatch", , drop = FALSE]
  unstable <- results[results$status == "reference_unstable", , drop = FALSE]
  new_unstable <- results[results$status == "new_unstable", , drop = FALSE]
  both_unstable <- results[results$status == "both_unstable", , drop = FALSE]
  errors <- results[results$status %in% c("error", "compare_error", "read_error", "method_error"), , drop = FALSE]

  total_cohorts_evaluated <- sum(results$n_cohorts[results$status %in% c("ok", "mismatch", "reference_unstable", "new_unstable", "both_unstable")], na.rm = TRUE)
  total_identical <- sum(results$n_identical, na.rm = TRUE)
  total_old <- sum(results$time_old_sec[results$status %in% c("ok", "mismatch", "reference_unstable", "new_unstable", "both_unstable")], na.rm = TRUE)
  total_new <- sum(results$time_new_sec[results$status %in% c("ok", "mismatch", "reference_unstable", "new_unstable", "both_unstable")], na.rm = TRUE)
  overall_accuracy <- if (total_cohorts_evaluated > 0) {
    round(100 * total_identical / total_cohorts_evaluated, 3)
  } else {
    NA_real_
  }

  data.frame(
    dbms = dbms,
    metric = c(
      "timestamp", "total_batches_run", "batch_start", "batch_end", "batch_size",
      "batches_ok", "batches_reference_unstable", "batches_new_unstable", "batches_both_unstable", "batches_mismatch", "batches_error",
      "overall_accuracy_pct", "total_time_old_sec", "total_time_new_sec",
      "overall_ratio_new_over_old", "overall_speedup_old_over_new",
      "total_rows_old", "total_rows_new",
      "total_reference_unstable_cohorts", "total_new_unstable_cohorts", "total_confirmed_mismatch_cohorts"
    ),
    value = c(
      timestamp_str, length(selected_batches), min(selected_batches), max(selected_batches), batch_size,
      nrow(ok), nrow(unstable), nrow(new_unstable), nrow(both_unstable), nrow(mismatches), nrow(errors),
      overall_accuracy, round(total_old, 3), round(total_new, 3),
      if (total_old > 0) round(total_new / total_old, 3) else NA_real_,
      if (total_new > 0) round(total_old / total_new, 3) else NA_real_,
      sum(results$rows_old, na.rm = TRUE), sum(results$rows_new, na.rm = TRUE),
      sum(results$n_reference_unstable, na.rm = TRUE),
      sum(results$n_new_unstable, na.rm = TRUE),
      sum(results$n_confirmed_mismatch, na.rm = TRUE)
    ),
    stringsAsFactors = FALSE
  )
}

live_benchmark_collect_df <- function(cdm, name) {
  as.data.frame(dplyr::collect(cdm[[name]]))
}

live_benchmark_profile_sql_enabled <- function() {
  live_benchmark_truthy(Sys.getenv("TIER3_PROFILE_SQL", unset = "false"))
}

live_benchmark_run_method <- function(cdm, cohort_set, name, method = c("old", "new"), log_file = NULL) {
  method <- match.arg(method)
  cleanup_live_benchmark_outputs(cdm, name)

  runner <- switch(
    method,
    old = function(cdm, cohort_set, name) {
      CDMConnector::generateCohortSet(
        cdm,
        cohortSet = cohort_set,
        name = name,
        computeAttrition = FALSE,
        overwrite = TRUE
      )
    },
    new = function(cdm, cohort_set, name) {
      CohortDAG::generateCohortSet2(
        cdm,
        cohortSet = cohort_set,
        name = name,
        computeAttrition = FALSE,
        overwrite = TRUE
      )
    }
  )

  err <- NULL
  df <- NULL
  profile_sql_file <- if (identical(method, "new") && live_benchmark_profile_sql_enabled() && !is.null(log_file)) {
    file.path(dirname(log_file), paste0(name, "_sql_profile.csv"))
  } else {
    ""
  }
  timing_expr <- function() {
    system.time({
      result <- tryCatch({
        withr::local_options(list(CohortDAG.profile_sql_file = profile_sql_file))
        cdm <<- runner(cdm, cohort_set, name)
        NULL
      }, error = function(e) conditionMessage(e))
      err <<- result
    })
  }

  tm <- if (!is.null(log_file)) {
    with_live_benchmark_log(log_file, timing_expr())
  } else {
    timing_expr()
  }

  if (is.null(err)) {
    df <- tryCatch(live_benchmark_collect_df(cdm, name), error = function(e) e)
    if (inherits(df, "error")) {
      err <- paste("collect ", name, " failed: ", conditionMessage(df), sep = "")
      df <- NULL
    }
  }

  list(
    cdm = cdm,
    error = err,
    time_sec = unname(tm[["elapsed"]]),
    df = df
  )
}

live_benchmark_reference_stability_enabled <- function(dbms) {
  default_value <- if (tolower(dbms) == "snowflake") "true" else "false"
  live_benchmark_truthy(Sys.getenv("TIER3_REFERENCE_STABILITY_CHECK", unset = default_value))
}

live_benchmark_new_stability_enabled <- function(dbms) {
  default_value <- if (tolower(dbms) == "snowflake") "true" else "false"
  live_benchmark_truthy(Sys.getenv("TIER3_NEW_STABILITY_CHECK", unset = default_value))
}

live_benchmark_force_method_stability <- function(method, dbms) {
  method <- match.arg(method, c("old", "new"))
  env_name <- if (method == "old") {
    "TIER3_FORCE_REFERENCE_STABILITY"
  } else {
    "TIER3_FORCE_NEW_STABILITY"
  }
  default_value <- if (method == "new" && tolower(dbms) %in% c("snowflake", "redshift", "sqlserver")) {
    "true"
  } else {
    "false"
  }
  live_benchmark_truthy(Sys.getenv(env_name, unset = default_value))
}

live_benchmark_stability_cohort_ids <- function(cohort_set, mismatch_details, method, dbms) {
  force_all <- live_benchmark_force_method_stability(method, dbms)
  ids <- if (force_all || nrow(mismatch_details) == 0L) {
    cohort_set$cohort_definition_id
  } else {
    mismatch_details$cohort_definition_id
  }
  ids <- unique(ids)
  max_ids <- as.integer(Sys.getenv("TIER3_REFERENCE_STABILITY_MAX_COHORTS", unset = "20"))
  if (!is.na(max_ids) && max_ids > 0L && length(ids) > max_ids) {
    ids <- utils::head(ids, max_ids)
  }
  ids
}

live_benchmark_method_stability <- function(cdm, cohort_set, mismatch_details, batch_idx, dbms, method = c("old", "new"), log_fn = NULL, log_file = NULL) {
  method <- match.arg(method)
  empty <- data.frame(
    cohort_definition_id = integer(0),
    stability_status = character(0),
    unstable = logical(0),
    rows_first = integer(0),
    rows_second = integer(0),
    only_in_first = integer(0),
    only_in_second = integer(0),
    time_first_sec = numeric(0),
    time_second_sec = numeric(0),
    error = character(0),
    stringsAsFactors = FALSE
  )
  enabled <- if (method == "old") {
    live_benchmark_reference_stability_enabled(dbms)
  } else {
    live_benchmark_new_stability_enabled(dbms)
  }
  if (!enabled) {
    return(list(cdm = cdm, details = empty))
  }

  ids <- live_benchmark_stability_cohort_ids(
    cohort_set = cohort_set,
    mismatch_details = mismatch_details,
    method = method,
    dbms = dbms
  )
  if (length(ids) == 0L) {
    return(list(cdm = cdm, details = empty))
  }

  out <- vector("list", length(ids))
  for (i in seq_along(ids)) {
    cohort_id <- ids[[i]]
    if (!is.null(log_fn)) {
      log_fn(method, " stability check start cohort_definition_id=", cohort_id)
    }
    single_set <- cohort_set[cohort_set$cohort_definition_id == cohort_id, , drop = FALSE]
    run1 <- live_benchmark_run_method(
      cdm,
      single_set,
      sprintf("live_%scheck1_b%03d_c%04d", method, batch_idx, cohort_id),
      method = method,
      log_file = log_file
    )
    cdm <- run1$cdm
    cleanup_live_benchmark_outputs(cdm, sprintf("live_%scheck1_b%03d_c%04d", method, batch_idx, cohort_id))

    if (!is.null(run1$error)) {
      out[[i]] <- data.frame(
        cohort_definition_id = cohort_id,
        stability_status = "error",
        unstable = NA,
        rows_first = NA_integer_,
        rows_second = NA_integer_,
        only_in_first = NA_integer_,
        only_in_second = NA_integer_,
        time_first_sec = round(run1$time_sec, 3),
        time_second_sec = NA_real_,
        error = paste0("run1: ", run1$error),
        stringsAsFactors = FALSE
      )
      next
    }

    run2 <- live_benchmark_run_method(
      cdm,
      single_set,
      sprintf("live_%scheck2_b%03d_c%04d", method, batch_idx, cohort_id),
      method = method,
      log_file = log_file
    )
    cdm <- run2$cdm
    cleanup_live_benchmark_outputs(cdm, sprintf("live_%scheck2_b%03d_c%04d", method, batch_idx, cohort_id))

    if (!is.null(run2$error)) {
      out[[i]] <- data.frame(
        cohort_definition_id = cohort_id,
        stability_status = "error",
        unstable = NA,
        rows_first = if (!is.null(run1$df)) nrow(run1$df) else NA_integer_,
        rows_second = NA_integer_,
        only_in_first = NA_integer_,
        only_in_second = NA_integer_,
        time_first_sec = round(run1$time_sec, 3),
        time_second_sec = round(run2$time_sec, 3),
        error = paste0("run2: ", run2$error),
        stringsAsFactors = FALSE
      )
      next
    }

    cmp_ref <- compare_cohort_dfs_detailed(run1$df, run2$df)
    if (!is.null(log_fn)) {
      log_fn(
        method, " stability cohort_definition_id=", cohort_id,
        " unstable=", if (cmp_ref$identical) "false" else "true",
        " only_in_first=", nrow(cmp_ref$only_in_old),
        " only_in_second=", nrow(cmp_ref$only_in_new)
      )
    }
    out[[i]] <- data.frame(
      cohort_definition_id = cohort_id,
      stability_status = "checked",
      unstable = !cmp_ref$identical,
      rows_first = cmp_ref$n_rows_old,
      rows_second = cmp_ref$n_rows_new,
      only_in_first = nrow(cmp_ref$only_in_old),
      only_in_second = nrow(cmp_ref$only_in_new),
      time_first_sec = round(run1$time_sec, 3),
      time_second_sec = round(run2$time_sec, 3),
      error = "",
      stringsAsFactors = FALSE
    )
  }

  list(cdm = cdm, details = do.call(rbind, out))
}

live_benchmark_reference_stability <- function(cdm, cohort_set, mismatch_details, batch_idx, dbms, log_fn = NULL, log_file = NULL) {
  live_benchmark_method_stability(
    cdm = cdm,
    cohort_set = cohort_set,
    mismatch_details = mismatch_details,
    batch_idx = batch_idx,
    dbms = dbms,
    method = "old",
    log_fn = log_fn,
    log_file = log_file
  )
}

live_benchmark_new_stability <- function(cdm, cohort_set, mismatch_details, batch_idx, dbms, log_fn = NULL, log_file = NULL) {
  live_benchmark_method_stability(
    cdm = cdm,
    cohort_set = cohort_set,
    mismatch_details = mismatch_details,
    batch_idx = batch_idx,
    dbms = dbms,
    method = "new",
    log_fn = log_fn,
    log_file = log_file
  )
}

run_live_db_batch_benchmark <- function(dbms) {
  stopifnot(rlang::is_installed("CDMConnector"))

  if (live_benchmark_truthy(Sys.getenv("TIER3_DISABLE_COHD", unset = "true"))) {
    assignInNamespace("cohdSimilarConcepts", function(...) NULL, ns = "CDMConnector")
  }

  batch_size <- as.integer(Sys.getenv("TIER3_BATCH_SIZE", unset = "50"))
  max_cohorts <- as.integer(Sys.getenv("TIER3_MAX_COHORTS", unset = "0"))
  results_dir <- live_benchmark_results_dir()
  timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
  db_dir <- file.path(results_dir, paste0(dbms, "_", timestamp_str))
  dir.create(db_dir, recursive = TRUE, showWarnings = FALSE)
  log_dir <- file.path(db_dir, "logs")
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)

  con <- get_connection(dbms, DatabaseConnector = testUsingDatabaseConnector)
  on.exit(disconnect(con), add = TRUE)

  cdm_schema <- get_cdm_schema(dbms)
  write_schema <- get_write_schema(dbms, prefix = write_prefix())
  if (any(cdm_schema == "") || any(write_schema == "")) {
    stop("Missing CDM or write schema for ", dbms)
  }

  cdm <- CDMConnector::cdmFromCon(
    con,
    cdmSchema = cdm_schema,
    cdmName = paste0("synpuf_", dbms),
    writeSchema = write_schema
  )
  on.exit(live_benchmark_cleanup_prefix(cdm, con, write_schema), add = TRUE)

  withr::local_options(list(CohortDAG.force_target_dialect = live_benchmark_force_dialect(dbms)))

  cohorts_dir <- unzip_cohorts()
  on.exit(unlink(cohorts_dir, recursive = TRUE), add = TRUE)
  all_files <- sort(list.files(cohorts_dir, pattern = "\\.json$", full.names = TRUE))
  if (!is.na(max_cohorts) && max_cohorts > 0L) {
    all_files <- utils::head(all_files, max_cohorts)
  }
  n_total <- length(all_files)
  n_batches <- ceiling(n_total / batch_size)
  batch_list <- lapply(seq_len(n_batches), function(i) {
    start_idx <- (i - 1L) * batch_size + 1L
    end_idx <- min(i * batch_size, n_total)
    all_files[start_idx:end_idx]
  })
  selected_batches <- resolve_live_benchmark_batches(n_batches)

  results <- list()
  mismatch_rows <- list()

  for (batch_idx in selected_batches) {
    batch_log <- file.path(log_dir, sprintf("batch_%03d.log", batch_idx))
    append_log <- function(...) {
      msg <- sprintf(
        "[%s] dbms=%s batch=%03d %s",
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        dbms,
        batch_idx,
        paste(..., collapse = "")
      )
      cat(msg, "\n", file = batch_log, append = TRUE)
      message(msg)
    }

    cohort_set <- tryCatch(read_live_benchmark_batch(batch_list[[batch_idx]]), error = function(e) e)
    if (inherits(cohort_set, "error")) {
      msg <- conditionMessage(cohort_set)
      append_log("read failed: ", msg)
      results[[length(results) + 1L]] <- data.frame(
        dbms = dbms,
        batch = batch_idx,
        n_cohorts = length(batch_list[[batch_idx]]),
        time_old_sec = NA_real_,
        time_new_sec = NA_real_,
        ratio_new_over_old = NA_real_,
        speedup_old_over_new = NA_real_,
        rows_old = NA_integer_,
        rows_new = NA_integer_,
        n_identical = NA_integer_,
        n_mismatch = NA_integer_,
        n_reference_unstable = NA_integer_,
        n_new_unstable = NA_integer_,
        n_confirmed_mismatch = NA_integer_,
        accuracy_pct = NA_real_,
        status = "read_error",
        error_msg = msg,
        stringsAsFactors = FALSE
      )
      next
    }

    old_name <- sprintf("live_old_b%03d", batch_idx)
    new_name <- sprintf("live_new_b%03d", batch_idx)
    on.exit(cleanup_live_benchmark_outputs(cdm, old_name), add = TRUE)
    on.exit(cleanup_live_benchmark_outputs(cdm, new_name), add = TRUE)
    cleanup_live_benchmark_outputs(cdm, old_name)
    cleanup_live_benchmark_outputs(cdm, new_name)

    append_log("loaded ", nrow(cohort_set), " cohorts")

    run_old <- live_benchmark_run_method(cdm, cohort_set, old_name, method = "old", log_file = batch_log)
    cdm <- run_old$cdm
    err_old <- run_old$error
    df_old <- run_old$df
    t_old <- c(elapsed = run_old$time_sec)
    append_log("old finished in ", sprintf("%.3f", t_old[["elapsed"]]), "s")

    run_new <- live_benchmark_run_method(cdm, cohort_set, new_name, method = "new", log_file = batch_log)
    cdm <- run_new$cdm
    err_new <- run_new$error
    df_new <- run_new$df
    t_new <- c(elapsed = run_new$time_sec)
    append_log("new finished in ", sprintf("%.3f", t_new[["elapsed"]]), "s")

    if (!is.null(err_old) || !is.null(err_new)) {
      err_msg <- paste(
        stats::na.omit(c(
          if (!is.null(err_old)) paste0("OLD: ", err_old) else NA_character_,
          if (!is.null(err_new)) paste0("NEW: ", err_new) else NA_character_
        )),
        collapse = " | "
      )
      append_log("batch failed: ", err_msg)
      results[[length(results) + 1L]] <- data.frame(
        dbms = dbms,
        batch = batch_idx,
        n_cohorts = nrow(cohort_set),
        time_old_sec = round(t_old[["elapsed"]], 3),
        time_new_sec = round(t_new[["elapsed"]], 3),
        ratio_new_over_old = if (t_old[["elapsed"]] > 0) round(t_new[["elapsed"]] / t_old[["elapsed"]], 3) else NA_real_,
        speedup_old_over_new = if (t_new[["elapsed"]] > 0) round(t_old[["elapsed"]] / t_new[["elapsed"]], 3) else NA_real_,
        rows_old = if (!is.null(df_old)) nrow(df_old) else NA_integer_,
        rows_new = if (!is.null(df_new)) nrow(df_new) else NA_integer_,
        n_identical = NA_integer_,
        n_mismatch = nrow(cohort_set),
        n_reference_unstable = 0L,
        n_new_unstable = 0L,
        n_confirmed_mismatch = nrow(cohort_set),
        accuracy_pct = 0,
        status = "method_error",
        error_msg = err_msg,
        stringsAsFactors = FALSE
      )
      cleanup_live_benchmark_outputs(cdm, old_name)
      cleanup_live_benchmark_outputs(cdm, new_name)
      next
    }

    cmp <- compare_cohort_dfs_detailed(df_old, df_new)
    cohort_index <- cohort_set[, c("cohort_definition_id", "original_cohort_definition_id", "cohort_name")]
    cmp$per_cohort <- merge(
      cohort_index,
      cmp$per_cohort,
      by = "cohort_definition_id",
      all.x = TRUE,
      sort = FALSE
    )
    cmp$per_cohort$rows_old[is.na(cmp$per_cohort$rows_old)] <- 0L
    cmp$per_cohort$rows_new[is.na(cmp$per_cohort$rows_new)] <- 0L
    cmp$per_cohort$identical[is.na(cmp$per_cohort$identical)] <- TRUE

    n_identical <- sum(cmp$per_cohort$identical)
    n_mismatch <- sum(!cmp$per_cohort$identical)
    accuracy_pct <- round(100 * n_identical / nrow(cmp$per_cohort), 3)
    stability <- live_benchmark_reference_stability(
      cdm,
      cohort_set,
      cmp$mismatch_details,
      batch_idx,
      dbms,
      log_fn = append_log,
      log_file = batch_log
    )
    cdm <- stability$cdm
    stability_df <- stability$details
    new_stability <- live_benchmark_new_stability(
      cdm,
      cohort_set,
      cmp$mismatch_details,
      batch_idx,
      dbms,
      log_fn = append_log,
      log_file = batch_log
    )
    cdm <- new_stability$cdm
    new_stability_df <- new_stability$details
    n_reference_unstable <- if (nrow(stability_df) > 0L) {
      sum(!is.na(stability_df$unstable) & stability_df$unstable)
    } else {
      0L
    }
    n_new_unstable <- if (nrow(new_stability_df) > 0L) {
      sum(!is.na(new_stability_df$unstable) & new_stability_df$unstable)
    } else {
      0L
    }
    if (nrow(cmp$mismatch_details) > 0L) {
      checked_ids <- unique(c(
        stability_df$cohort_definition_id[stability_df$stability_status == "checked" & !stability_df$unstable],
        new_stability_df$cohort_definition_id[new_stability_df$stability_status == "checked" & !new_stability_df$unstable]
      ))
      unstable_ids <- unique(c(
        stability_df$cohort_definition_id[!is.na(stability_df$unstable) & stability_df$unstable],
        new_stability_df$cohort_definition_id[!is.na(new_stability_df$unstable) & new_stability_df$unstable]
      ))
      mismatch_ids <- unique(cmp$mismatch_details$cohort_definition_id)
      n_confirmed_mismatch <- sum(mismatch_ids %in% setdiff(checked_ids, unstable_ids))
    } else {
      n_confirmed_mismatch <- 0L
    }

    append_log(
      "accuracy=", sprintf("%.3f", accuracy_pct),
      "% mismatches=", n_mismatch,
      " reference_unstable=", n_reference_unstable,
      " new_unstable=", n_new_unstable,
      " ratio=", if (t_old[["elapsed"]] > 0) sprintf("%.3f", t_new[["elapsed"]] / t_old[["elapsed"]]) else "NA"
    )

    if (cmp$identical) {
      forced_ids <- unique(c(
        stability_df$cohort_definition_id[!is.na(stability_df$unstable) & stability_df$unstable],
        new_stability_df$cohort_definition_id[!is.na(new_stability_df$unstable) & new_stability_df$unstable]
      ))
      if (length(forced_ids) > 0L) {
        issue <- ifelse(
          forced_ids %in% stability_df$cohort_definition_id[!is.na(stability_df$unstable) & stability_df$unstable] &
            forced_ids %in% new_stability_df$cohort_definition_id[!is.na(new_stability_df$unstable) & new_stability_df$unstable],
          "both_unstable",
          ifelse(
            forced_ids %in% new_stability_df$cohort_definition_id[!is.na(new_stability_df$unstable) & new_stability_df$unstable],
            "new_unstable",
            "reference_unstable"
          )
        )
          mismatch_rows[[length(mismatch_rows) + 1L]] <- data.frame(
            dbms = dbms,
            batch = batch_idx,
            cohort_definition_id = forced_ids,
            original_cohort_definition_id = cohort_set$original_cohort_definition_id[match(forced_ids, cohort_set$cohort_definition_id)],
            cohort_name = cohort_set$cohort_name[match(forced_ids, cohort_set$cohort_definition_id)],
            rows_old = NA_integer_,
            rows_new = NA_integer_,
            only_in_old = NA_integer_,
            only_in_new = NA_integer_,
            issue = issue,
            old_stability_status = stability_df$stability_status[match(forced_ids, stability_df$cohort_definition_id)],
            old_unstable = stability_df$unstable[match(forced_ids, stability_df$cohort_definition_id)],
            old_rows_first = stability_df$rows_first[match(forced_ids, stability_df$cohort_definition_id)],
            old_rows_second = stability_df$rows_second[match(forced_ids, stability_df$cohort_definition_id)],
            old_only_in_first = stability_df$only_in_first[match(forced_ids, stability_df$cohort_definition_id)],
            old_only_in_second = stability_df$only_in_second[match(forced_ids, stability_df$cohort_definition_id)],
            old_time_first_sec = stability_df$time_first_sec[match(forced_ids, stability_df$cohort_definition_id)],
            old_time_second_sec = stability_df$time_second_sec[match(forced_ids, stability_df$cohort_definition_id)],
            old_error = stability_df$error[match(forced_ids, stability_df$cohort_definition_id)],
            new_stability_status = new_stability_df$stability_status[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_unstable = new_stability_df$unstable[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_rows_first = new_stability_df$rows_first[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_rows_second = new_stability_df$rows_second[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_only_in_first = new_stability_df$only_in_first[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_only_in_second = new_stability_df$only_in_second[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_time_first_sec = new_stability_df$time_first_sec[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_time_second_sec = new_stability_df$time_second_sec[match(forced_ids, new_stability_df$cohort_definition_id)],
            new_error = new_stability_df$error[match(forced_ids, new_stability_df$cohort_definition_id)],
            stringsAsFactors = FALSE
          )
      }
    }

    if (nrow(cmp$mismatch_details) > 0L) {
      mm <- merge(
        transform(cmp$mismatch_details, dbms = dbms, batch = batch_idx, issue = "row_mismatch"),
        cohort_set[, c("cohort_definition_id", "original_cohort_definition_id", "cohort_name")],
        by = "cohort_definition_id",
        all.x = TRUE,
        sort = FALSE
      )
      if (nrow(stability_df) > 0L) {
        names(stability_df)[names(stability_df) != "cohort_definition_id"] <- paste0("old_", names(stability_df)[names(stability_df) != "cohort_definition_id"])
        mm <- merge(mm, stability_df, by = "cohort_definition_id", all.x = TRUE, sort = FALSE)
      } else {
        mm$old_stability_status <- NA_character_
        mm$old_unstable <- NA
        mm$old_rows_first <- NA_integer_
        mm$old_rows_second <- NA_integer_
        mm$old_only_in_first <- NA_integer_
        mm$old_only_in_second <- NA_integer_
        mm$old_time_first_sec <- NA_real_
        mm$old_time_second_sec <- NA_real_
        mm$old_error <- NA_character_
      }
      if (nrow(new_stability_df) > 0L) {
        names(new_stability_df)[names(new_stability_df) != "cohort_definition_id"] <- paste0("new_", names(new_stability_df)[names(new_stability_df) != "cohort_definition_id"])
        mm <- merge(mm, new_stability_df, by = "cohort_definition_id", all.x = TRUE, sort = FALSE)
      } else {
        mm$new_stability_status <- NA_character_
        mm$new_unstable <- NA
        mm$new_rows_first <- NA_integer_
        mm$new_rows_second <- NA_integer_
        mm$new_only_in_first <- NA_integer_
        mm$new_only_in_second <- NA_integer_
        mm$new_time_first_sec <- NA_real_
        mm$new_time_second_sec <- NA_real_
        mm$new_error <- NA_character_
      }
      mm$issue <- ifelse(
        !is.na(mm$old_unstable) & mm$old_unstable & !(!is.na(mm$new_unstable) & mm$new_unstable),
        "reference_unstable",
        ifelse(
          !is.na(mm$new_unstable) & mm$new_unstable & !(!is.na(mm$old_unstable) & mm$old_unstable),
          "new_unstable",
          ifelse(
            !is.na(mm$old_unstable) & mm$old_unstable & !is.na(mm$new_unstable) & mm$new_unstable,
            "both_unstable",
            ifelse(
              mm$old_stability_status == "error" | mm$new_stability_status == "error",
              "stability_check_error",
              mm$issue
            )
          )
        )
      )
      mismatch_rows[[length(mismatch_rows) + 1L]] <- mm[, c(
        "dbms", "batch", "cohort_definition_id", "original_cohort_definition_id", "cohort_name",
        "rows_old", "rows_new", "only_in_old", "only_in_new", "issue",
        "old_stability_status", "old_unstable",
        "old_rows_first", "old_rows_second",
        "old_only_in_first", "old_only_in_second",
        "old_time_first_sec", "old_time_second_sec", "old_error",
        "new_stability_status", "new_unstable",
        "new_rows_first", "new_rows_second",
        "new_only_in_first", "new_only_in_second",
        "new_time_first_sec", "new_time_second_sec", "new_error"
      )]
    }

    results[[length(results) + 1L]] <- data.frame(
      dbms = dbms,
      batch = batch_idx,
      n_cohorts = nrow(cohort_set),
      time_old_sec = round(t_old[["elapsed"]], 3),
      time_new_sec = round(t_new[["elapsed"]], 3),
      ratio_new_over_old = if (t_old[["elapsed"]] > 0) round(t_new[["elapsed"]] / t_old[["elapsed"]], 3) else NA_real_,
      speedup_old_over_new = if (t_new[["elapsed"]] > 0) round(t_old[["elapsed"]] / t_new[["elapsed"]], 3) else NA_real_,
      rows_old = cmp$n_rows_old,
      rows_new = cmp$n_rows_new,
      n_identical = n_identical,
      n_mismatch = n_mismatch,
      n_reference_unstable = n_reference_unstable,
      n_new_unstable = n_new_unstable,
      n_confirmed_mismatch = n_confirmed_mismatch,
      accuracy_pct = accuracy_pct,
      status = if (cmp$identical) {
        if (n_new_unstable > 0L && n_reference_unstable > 0L) {
          "both_unstable"
        } else if (n_new_unstable > 0L) {
          "new_unstable"
        } else if (n_reference_unstable > 0L) {
          "reference_unstable"
        } else {
          "ok"
        }
      } else if (n_reference_unstable > 0L && n_confirmed_mismatch == 0L && n_reference_unstable == n_mismatch && n_new_unstable == 0L) {
        "reference_unstable"
      } else if (n_new_unstable > 0L && n_confirmed_mismatch == 0L && n_new_unstable == n_mismatch && n_reference_unstable == 0L) {
        "new_unstable"
      } else if (n_new_unstable > 0L && n_reference_unstable > 0L && n_confirmed_mismatch == 0L && pmax(n_new_unstable, n_reference_unstable) <= n_mismatch) {
        "both_unstable"
      } else {
        "mismatch"
      },
      error_msg = if (cmp$identical) "" else cmp$details,
      stringsAsFactors = FALSE
    )

    cleanup_live_benchmark_outputs(cdm, old_name)
    cleanup_live_benchmark_outputs(cdm, new_name)
  }

  results_df <- do.call(rbind, results)
  mismatch_df <- if (length(mismatch_rows) > 0L) {
    do.call(rbind, mismatch_rows)
  } else {
    data.frame(
      dbms = character(0),
      batch = integer(0),
      cohort_definition_id = integer(0),
      original_cohort_definition_id = integer(0),
      cohort_name = character(0),
      rows_old = integer(0),
      rows_new = integer(0),
      only_in_old = integer(0),
      only_in_new = integer(0),
      issue = character(0),
      old_stability_status = character(0),
      old_unstable = logical(0),
      old_rows_first = integer(0),
      old_rows_second = integer(0),
      old_only_in_first = integer(0),
      old_only_in_second = integer(0),
      old_time_first_sec = numeric(0),
      old_time_second_sec = numeric(0),
      old_error = character(0),
      new_stability_status = character(0),
      new_unstable = logical(0),
      new_rows_first = integer(0),
      new_rows_second = integer(0),
      new_only_in_first = integer(0),
      new_only_in_second = integer(0),
      new_time_first_sec = numeric(0),
      new_time_second_sec = numeric(0),
      new_error = character(0),
      stringsAsFactors = FALSE
    )
  }
  summary_df <- build_live_benchmark_summary(dbms, results_df, selected_batches, batch_size, timestamp_str)

  utils::write.csv(results_df, file.path(db_dir, "tier3_live_batch_results.csv"), row.names = FALSE)
  utils::write.csv(mismatch_df, file.path(db_dir, "tier3_live_mismatch_details.csv"), row.names = FALSE)
  utils::write.csv(summary_df, file.path(db_dir, "tier3_live_summary.csv"), row.names = FALSE)

  list(
    dbms = dbms,
    results = results_df,
    mismatches = mismatch_df,
    summary = summary_df,
    result_dir = db_dir
  )
}
