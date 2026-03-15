skip_if_not_tier(2L)

# ---------------------------------------------------------------------------
# Tier 2: Full library benchmark — generateCohortSet vs generateCohortSet2
# Runs all bundled cohorts in batches, records timing, and writes mismatch logs.
# ---------------------------------------------------------------------------

test_that("full cohort library: generateCohortSet2 matches generateCohortSet on all cohorts", {
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("duckdb")
  library(CDMConnector)

  disable_cohd <- tolower(Sys.getenv("TIER2_DISABLE_COHD", unset = "true")) %in%
    c("true", "1", "yes")
  if (disable_cohd) {
    assignInNamespace("cohdSimilarConcepts", function(...) NULL, ns = "CDMConnector")
  }

  cohorts_dir <- unzip_cohorts()
  on.exit(unlink(cohorts_dir, recursive = TRUE), add = TRUE)

  all_files <- sort(list.files(cohorts_dir, pattern = "\\.json$", full.names = TRUE))
  max_cohorts <- as.integer(Sys.getenv("TIER2_MAX_COHORTS", unset = "0"))
  if (!is.na(max_cohorts) && max_cohorts > 0L) {
    all_files <- utils::head(all_files, max_cohorts)
  }
  n_total <- length(all_files)
  batch_size <- as.integer(Sys.getenv("TIER2_BATCH_SIZE", unset = "50"))
  n_subjects <- as.integer(Sys.getenv("TIER2_N_SUBJECTS",
                                      unset = as.character(batch_size * 10L)))
  seed_base <- as.integer(Sys.getenv("TIER2_SEED_BASE", unset = "1"))
  detected_cores <- tryCatch(parallel::detectCores(logical = FALSE), error = function(e) 1L)
  default_cores <- if (is.na(detected_cores) || detected_cores < 1L) 1L else min(4L, detected_cores)
  n_cores <- as.integer(Sys.getenv("TIER2_N_CORES", unset = as.character(default_cores)))

  results_dir <- Sys.getenv("TIER2_RESULTS_DIR", unset = "")
  if (!nzchar(results_dir)) {
    results_dir <- normalizePath(
      file.path(testthat::test_path(), "..", "..", "inst", "benchmark-results"),
      winslash = "/",
      mustWork = FALSE
    )
  }
  dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)

  timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
  log_dir <- file.path(results_dir, paste0("tier2_logs_", timestamp_str))
  dir.create(log_dir, showWarnings = FALSE, recursive = TRUE)

  n_batches <- ceiling(n_total / batch_size)
  batch_list <- lapply(seq_len(n_batches), function(i) {
    start_idx <- (i - 1L) * batch_size + 1L
    end_idx <- min(i * batch_size, n_total)
    all_files[start_idx:end_idx]
  })
  batch_start <- as.integer(Sys.getenv("TIER2_BATCH_START", unset = "1"))
  batch_end <- as.integer(Sys.getenv("TIER2_BATCH_END", unset = as.character(n_batches)))
  if (is.na(batch_start) || batch_start < 1L) batch_start <- 1L
  if (is.na(batch_end) || batch_end > n_batches) batch_end <- n_batches
  selected_batches <- seq.int(batch_start, batch_end)
  selected_cohorts <- sum(vapply(batch_list[selected_batches], length, integer(1)))

  message(sprintf(
    "\n=== Tier 2: Full library benchmark (%d cohorts selected, batches %d-%d of %d, %d cores) ===",
    selected_cohorts, batch_start, batch_end, n_batches, n_cores
  ))

  run_one_batch <- function(batch_idx) {
    batch_files <- batch_list[[batch_idx]]
    batch_log <- file.path(log_dir, sprintf("batch_%03d.log", batch_idx))
    temp_dir <- tempfile(sprintf("tier2_batch_%03d_", batch_idx))
    dir.create(temp_dir, recursive = TRUE)

    append_log <- function(...) {
      msg <- sprintf(
        "[%s] batch=%03d %s",
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        batch_idx,
        paste(..., collapse = "")
      )
      cat(msg, "\n", file = batch_log, append = TRUE)
      message(msg)
    }

    on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

    append_log("starting")
    for (f in batch_files) {
      file.copy(f, file.path(temp_dir, basename(f)))
    }

    cohort_set <- tryCatch(
      make_unique_cohort_names(CDMConnector::readCohortSet(temp_dir)),
      error = function(e) e
    )

    if (inherits(cohort_set, "error") || nrow(cohort_set) == 0L) {
      msg <- if (inherits(cohort_set, "error")) conditionMessage(cohort_set) else "empty cohort set"
      append_log("read failed: ", msg)
      return(list(
        summary = data.frame(
          batch = batch_idx,
          n_cohorts = length(batch_files),
          time_old_sec = NA_real_,
          time_new_sec = NA_real_,
          ratio_new_over_old = NA_real_,
          speedup_old_over_new = NA_real_,
          rows_old = NA_integer_,
          rows_new = NA_integer_,
          n_identical = NA_integer_,
          n_mismatch = NA_integer_,
          accuracy_pct = NA_real_,
          status = "read_error",
          error_msg = msg,
          stringsAsFactors = FALSE
        ),
        mismatches = NULL
      ))
    }

    append_log("loaded ", nrow(cohort_set), " cohorts; n=", n_subjects)
    batch_seed <- if (is.na(seed_base)) batch_idx else seed_base + batch_idx - 1L
    append_log("using seed=", batch_seed, " for old/new mock CDMs")

    build_mock_cdm <- function() {
      tryCatch(
        CDMConnector::cdmFromCohortSet(cohort_set, n = n_subjects, seed = batch_seed),
        error = function(e) e
      )
    }

    cdm_old <- build_mock_cdm()
    cdm_new <- build_mock_cdm()
    if (inherits(cdm_old, "error") || inherits(cdm_new, "error")) {
      msg <- paste(
        stats::na.omit(c(
          if (inherits(cdm_old, "error")) paste0("OLD_CDM: ", conditionMessage(cdm_old)) else NA_character_,
          if (inherits(cdm_new, "error")) paste0("NEW_CDM: ", conditionMessage(cdm_new)) else NA_character_
        )),
        collapse = " | "
      )
      append_log("cdmFromCohortSet failed: ", msg)
      return(list(
        summary = data.frame(
          batch = batch_idx,
          n_cohorts = nrow(cohort_set),
          time_old_sec = NA_real_,
          time_new_sec = NA_real_,
          ratio_new_over_old = NA_real_,
          speedup_old_over_new = NA_real_,
          rows_old = NA_integer_,
          rows_new = NA_integer_,
          n_identical = NA_integer_,
          n_mismatch = NA_integer_,
          accuracy_pct = NA_real_,
          status = "cdm_error",
          error_msg = msg,
          stringsAsFactors = FALSE
        ),
        mismatches = NULL
      ))
    }
    on.exit(tryCatch(CDMConnector::cdmDisconnect(cdm_old), error = function(e) NULL), add = TRUE)
    on.exit(tryCatch(CDMConnector::cdmDisconnect(cdm_new), error = function(e) NULL), add = TRUE)

    append_log("running CDMConnector::generateCohortSet")
    t0_old <- Sys.time()
    err_old <- tryCatch({
      cdm_old <- CDMConnector::generateCohortSet(
        cdm_old,
        cohort_set,
        name = "bench_old",
        computeAttrition = FALSE
      )
      NULL
    }, error = function(e) conditionMessage(e))
    time_old <- as.numeric(difftime(Sys.time(), t0_old, units = "secs"))
    append_log("old finished in ", sprintf("%.3f", time_old), "s")

    df_old <- NULL
    if (is.null(err_old)) {
      df_old <- tryCatch(as.data.frame(dplyr::collect(cdm_old$bench_old)), error = function(e) e)
      if (inherits(df_old, "error")) {
        err_old <- paste("collect bench_old failed:", conditionMessage(df_old))
        df_old <- NULL
      }
    }

    append_log("running CohortDAG::generateCohortSet2")
    t0_new <- Sys.time()
    err_new <- tryCatch({
      cdm_new <- CohortDAG::generateCohortSet2(
        cdm_new,
        cohort_set,
        name = "bench_new",
        computeAttrition = FALSE
      )
      NULL
    }, error = function(e) conditionMessage(e))
    time_new <- as.numeric(difftime(Sys.time(), t0_new, units = "secs"))
    append_log("new finished in ", sprintf("%.3f", time_new), "s")

    df_new <- NULL
    if (is.null(err_new)) {
      df_new <- tryCatch(as.data.frame(dplyr::collect(cdm_new$bench_new)), error = function(e) e)
      if (inherits(df_new, "error")) {
        err_new <- paste("collect bench_new failed:", conditionMessage(df_new))
        df_new <- NULL
      }
    }

    if (!is.null(err_old) || !is.null(err_new)) {
      err_msg <- paste(
        stats::na.omit(c(
          if (!is.null(err_old)) paste0("OLD: ", err_old) else NA_character_,
          if (!is.null(err_new)) paste0("NEW: ", err_new) else NA_character_
        )),
        collapse = " | "
      )
      append_log("batch failed: ", err_msg)
      return(list(
        summary = data.frame(
          batch = batch_idx,
          n_cohorts = nrow(cohort_set),
          time_old_sec = round(time_old, 3),
          time_new_sec = round(time_new, 3),
          ratio_new_over_old = if (time_old > 0) round(time_new / time_old, 3) else NA_real_,
          speedup_old_over_new = if (time_new > 0) round(time_old / time_new, 3) else NA_real_,
          rows_old = if (!is.null(df_old)) nrow(df_old) else NA_integer_,
          rows_new = if (!is.null(df_new)) nrow(df_new) else NA_integer_,
          n_identical = NA_integer_,
          n_mismatch = nrow(cohort_set),
          accuracy_pct = 0,
          status = "error",
          error_msg = err_msg,
          stringsAsFactors = FALSE
        ),
        mismatches = transform(
          cohort_set[, c("cohort_definition_id", "original_cohort_definition_id", "cohort_name")],
          batch = batch_idx,
          rows_old = NA_integer_,
          rows_new = NA_integer_,
          only_in_old = NA_integer_,
          only_in_new = NA_integer_,
          issue = "batch_error"
        )
      ))
    }

    cmp <- tryCatch(compare_cohort_dfs_detailed(df_old, df_new), error = function(e) e)
    if (inherits(cmp, "error")) {
      msg <- conditionMessage(cmp)
      append_log("comparison failed: ", msg)
      return(list(
        summary = data.frame(
          batch = batch_idx,
          n_cohorts = nrow(cohort_set),
          time_old_sec = round(time_old, 3),
          time_new_sec = round(time_new, 3),
          ratio_new_over_old = if (time_old > 0) round(time_new / time_old, 3) else NA_real_,
          speedup_old_over_new = if (time_new > 0) round(time_old / time_new, 3) else NA_real_,
          rows_old = nrow(df_old),
          rows_new = nrow(df_new),
          n_identical = NA_integer_,
          n_mismatch = nrow(cohort_set),
          accuracy_pct = NA_real_,
          status = "compare_error",
          error_msg = msg,
          stringsAsFactors = FALSE
        ),
        mismatches = transform(
          cohort_set[, c("cohort_definition_id", "original_cohort_definition_id", "cohort_name")],
          batch = batch_idx,
          rows_old = NA_integer_,
          rows_new = NA_integer_,
          only_in_old = NA_integer_,
          only_in_new = NA_integer_,
          issue = "compare_error"
        )
      ))
    }

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
    cmp$identical <- all(cmp$per_cohort$identical)
    cmp$details <- if (cmp$identical) {
      sprintf("All %d cohorts match (%d rows)", nrow(cmp$per_cohort), cmp$n_rows_old)
    } else {
      mismatch_ids <- cmp$per_cohort$cohort_definition_id[!cmp$per_cohort$identical]
      sprintf("%d/%d cohorts mismatched (ids: %s); old=%d rows, new=%d rows",
              sum(!cmp$per_cohort$identical), nrow(cmp$per_cohort),
              paste(utils::head(mismatch_ids, 10), collapse = ","),
              cmp$n_rows_old, cmp$n_rows_new)
    }

    n_identical <- sum(cmp$per_cohort$identical)
    n_mismatch <- sum(!cmp$per_cohort$identical)
    accuracy_pct <- round(100 * n_identical / nrow(cmp$per_cohort), 3)
    append_log(
      "accuracy=", sprintf("%.3f", accuracy_pct),
      "% mismatches=", n_mismatch,
      " ratio=", if (time_old > 0) sprintf("%.3f", time_new / time_old) else "NA"
    )

    mismatch_rows <- NULL
    if (nrow(cmp$mismatch_details) > 0L) {
        mismatch_rows <- merge(
          transform(cmp$mismatch_details, batch = batch_idx, issue = "row_mismatch"),
        cohort_set[, c("cohort_definition_id", "original_cohort_definition_id", "cohort_name")],
        by = "cohort_definition_id",
        all.x = TRUE,
        sort = FALSE
      )
      mismatch_rows <- mismatch_rows[, c(
        "batch", "cohort_definition_id", "original_cohort_definition_id", "cohort_name",
        "rows_old", "rows_new", "only_in_old", "only_in_new", "issue"
      )]
      append_log(
        "mismatched cohort_definition_id(s): ",
        paste(mismatch_rows$cohort_definition_id, collapse = ",")
      )
    }

    list(
      summary = data.frame(
        batch = batch_idx,
        n_cohorts = nrow(cohort_set),
        time_old_sec = round(time_old, 3),
        time_new_sec = round(time_new, 3),
        ratio_new_over_old = if (time_old > 0) round(time_new / time_old, 3) else NA_real_,
        speedup_old_over_new = if (time_new > 0) round(time_old / time_new, 3) else NA_real_,
        rows_old = cmp$n_rows_old,
        rows_new = cmp$n_rows_new,
        n_identical = n_identical,
        n_mismatch = n_mismatch,
        accuracy_pct = accuracy_pct,
        status = if (cmp$identical) "ok" else "mismatch",
        error_msg = if (cmp$identical) "" else cmp$details,
        stringsAsFactors = FALSE
      ),
      mismatches = mismatch_rows
    )
  }

  message(sprintf("Processing %d batches of %d cohorts", length(selected_batches), batch_size))
  batch_results <- if (n_cores > 1L) {
    cl <- parallel::makeCluster(n_cores)
    on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)
    parallel::clusterExport(
      cl,
      varlist = c(
        "batch_list", "log_dir", "n_subjects", "run_one_batch",
        "make_unique_cohort_names", "compare_cohort_dfs", "compare_cohort_dfs_detailed"
      ),
      envir = environment()
    )
    parallel::clusterEvalQ(cl, {
      library(CDMConnector)
      NULL
    })
    parallel::parLapplyLB(cl, selected_batches, run_one_batch)
  } else {
    lapply(selected_batches, run_one_batch)
  }

  results <- do.call(rbind, lapply(batch_results, `[[`, "summary"))
  rownames(results) <- NULL

  mismatch_list <- Filter(function(x) !is.null(x) && nrow(x) > 0L, lapply(batch_results, `[[`, "mismatches"))
  mismatch_df <- if (length(mismatch_list) > 0L) {
    do.call(rbind, mismatch_list)
  } else {
    data.frame(
      batch = integer(0),
      cohort_definition_id = integer(0),
      original_cohort_definition_id = integer(0),
      cohort_name = character(0),
      rows_old = integer(0),
      rows_new = integer(0),
      only_in_old = integer(0),
      only_in_new = integer(0),
      issue = character(0),
      stringsAsFactors = FALSE
    )
  }

  batch_file <- file.path(results_dir, paste0("tier2_batch_results_", timestamp_str, ".csv"))
  utils::write.csv(results, batch_file, row.names = FALSE)
  utils::write.csv(results, file.path(results_dir, "tier2_batch_results_latest.csv"), row.names = FALSE)

  mismatch_file <- file.path(results_dir, paste0("tier2_mismatch_details_", timestamp_str, ".csv"))
  utils::write.csv(mismatch_df, mismatch_file, row.names = FALSE)
  utils::write.csv(mismatch_df, file.path(results_dir, "tier2_mismatch_details_latest.csv"), row.names = FALSE)

  ok <- results[results$status == "ok", , drop = FALSE]
  mismatches <- results[results$status == "mismatch", , drop = FALSE]
  errors <- results[results$status %in% c("error", "compare_error", "read_error", "cdm_error"), , drop = FALSE]

  total_cohorts_evaluated <- sum(results$n_cohorts[results$status %in% c("ok", "mismatch")], na.rm = TRUE)
  total_identical <- sum(results$n_identical, na.rm = TRUE)
  total_old <- sum(results$time_old_sec[results$status %in% c("ok", "mismatch")], na.rm = TRUE)
  total_new <- sum(results$time_new_sec[results$status %in% c("ok", "mismatch")], na.rm = TRUE)
  overall_accuracy <- if (total_cohorts_evaluated > 0) {
    round(100 * total_identical / total_cohorts_evaluated, 3)
  } else {
    NA_real_
  }

  summary_df <- data.frame(
    metric = c(
      "total_cohorts", "total_batches_available", "total_batches_run", "batch_start", "batch_end",
      "batch_size", "subjects_per_batch", "cores_used", "max_cohorts_override",
      "batches_ok", "batches_mismatch", "batches_error",
      "overall_accuracy_pct", "total_time_old_sec", "total_time_new_sec",
      "overall_ratio_new_over_old", "overall_speedup_old_over_new",
      "median_batch_accuracy_pct", "median_batch_ratio_new_over_old",
      "total_rows_old", "total_rows_new", "mismatch_rows_logged"
    ),
    value = c(
      selected_cohorts, n_batches, length(selected_batches), batch_start, batch_end,
      batch_size, n_subjects,
      n_cores, if (max_cohorts > 0L) max_cohorts else NA_real_,
      nrow(ok), nrow(mismatches), nrow(errors),
      overall_accuracy, round(total_old, 3), round(total_new, 3),
      if (total_old > 0) round(total_new / total_old, 3) else NA_real_,
      if (total_new > 0) round(total_old / total_new, 3) else NA_real_,
      round(stats::median(results$accuracy_pct, na.rm = TRUE), 3),
      round(stats::median(results$ratio_new_over_old, na.rm = TRUE), 3),
      sum(results$rows_old, na.rm = TRUE), sum(results$rows_new, na.rm = TRUE),
      nrow(mismatch_df)
    ),
    stringsAsFactors = FALSE
  )

  summary_file <- file.path(results_dir, paste0("tier2_summary_", timestamp_str, ".csv"))
  utils::write.csv(summary_df, summary_file, row.names = FALSE)
  utils::write.csv(summary_df, file.path(results_dir, "tier2_summary_latest.csv"), row.names = FALSE)

  message("\n========== TIER 2 RESULTS ==========")
  message(sprintf("Cohorts tested              : %d", selected_cohorts))
  message(sprintf("Batches run                 : %d-%d of %d", batch_start, batch_end, n_batches))
  message(sprintf("Batches OK                  : %d / %d", nrow(ok), length(selected_batches)))
  message(sprintf("Batches mismatch            : %d", nrow(mismatches)))
  message(sprintf("Batches error               : %d", nrow(errors)))
  message(sprintf("Overall accuracy            : %.3f%%", overall_accuracy))
  message(sprintf("Total time old              : %.3fs", total_old))
  message(sprintf("Total time new              : %.3fs", total_new))
  if (total_old > 0) {
    message(sprintf("Overall ratio new/old       : %.3f", total_new / total_old))
  }
  if (total_new > 0) {
    message(sprintf("Overall speedup old/new     : %.3f", total_old / total_new))
  }
  message("Results CSV                 : ", batch_file)
  message("Mismatch CSV                : ", mismatch_file)
  message("Summary CSV                 : ", summary_file)
  message("Batch logs                  : ", log_dir)
  message("====================================\n")

  if (nrow(mismatches) > 0) {
    message("MISMATCHED BATCHES:")
    for (i in seq_len(nrow(mismatches))) {
      message(sprintf(
        "  batch=%d mismatch=%d accuracy=%.3f%% %s",
        mismatches$batch[i],
        mismatches$n_mismatch[i],
        mismatches$accuracy_pct[i],
        mismatches$error_msg[i]
      ))
    }
  }

  if (nrow(errors) > 0) {
    message("ERRORED BATCHES:")
    for (i in seq_len(nrow(errors))) {
      message(sprintf("  batch=%d %s", errors$batch[i], errors$error_msg[i]))
    }
  }

  expect_equal(nrow(errors), 0L,
               info = paste("Errored batches:", paste(errors$batch, collapse = ",")))
  expect_equal(nrow(mismatches), 0L,
               info = paste("Mismatched batches:", paste(mismatches$batch, collapse = ",")))
})
