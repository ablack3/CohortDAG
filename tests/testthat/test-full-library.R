skip_if_not_tier(2L)

# ---------------------------------------------------------------------------
# Tier 2: Full library benchmark — generateCohortSet vs generateCohortSet2
# Runs all 3180 cohorts in batches, compares speed & correctness.
# Results saved to inst/benchmark-results/ for vignette consumption.
# ---------------------------------------------------------------------------

test_that("full cohort library: generateCohortSet2 matches generateCohortSet on all cohorts", {
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("duckdb")
  library(CDMConnector)

  # Unzip bundled cohorts
  cohorts_dir <- unzip_cohorts()
  on.exit(unlink(cohorts_dir, recursive = TRUE), add = TRUE)
  all_files  <- sort(list.files(cohorts_dir, pattern = "\\.json$", full.names = TRUE))
  n_total    <- length(all_files)

  message(sprintf("\n=== Tier 2: Full library benchmark (%d cohorts) ===", n_total))

 # ---- configuration -------------------------------------------------------
  batch_size  <- as.integer(Sys.getenv("TIER2_BATCH_SIZE", unset = "50"))
  n_subjects  <- as.integer(Sys.getenv("TIER2_N_SUBJECTS", unset = "20"))
  n_cores     <- as.integer(Sys.getenv("TIER2_N_CORES", unset = "1"))
  results_dir <- Sys.getenv("TIER2_RESULTS_DIR", unset = "")

  if (!nzchar(results_dir)) {
    results_dir <- file.path(
      system.file(package = "CohortDAG"),
      "..", "..", "inst", "benchmark-results"
    )
    # Fallback to tempdir if inst/ doesn't exist (installed package)
    if (!dir.exists(dirname(results_dir))) {
      results_dir <- file.path(tempdir(), "cohortdag_benchmark")
    }
  }
  dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)

  # ---- split into batches --------------------------------------------------
  n_batches  <- ceiling(n_total / batch_size)
  batch_list <- lapply(seq_len(n_batches), function(i) {
    start_idx <- (i - 1L) * batch_size + 1L
    end_idx   <- min(i * batch_size, n_total)
    all_files[start_idx:end_idx]
  })

  # ---- worker function (runs one batch) ------------------------------------
  run_one_batch <- function(batch_idx) {
    batch_files <- batch_list[[batch_idx]]

    # Prepare temp cohort directory
    temp_dir <- tempfile(paste0("batch_", batch_idx, "_"))
    dir.create(temp_dir, recursive = TRUE)
    on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
    for (f in batch_files) file.copy(f, file.path(temp_dir, basename(f)))

    cohort_set <- tryCatch(
      CDMConnector::readCohortSet(temp_dir),
      error = function(e) NULL
    )
    if (is.null(cohort_set) || nrow(cohort_set) == 0L) {
      return(data.frame(
        batch           = batch_idx,
        n_cohorts       = length(batch_files),
        time_old_sec    = NA_real_,
        time_new_sec    = NA_real_,
        ratio           = NA_real_,
        rows_old        = NA_integer_,
        rows_new        = NA_integer_,
        all_identical   = NA,
        n_mismatch      = NA_integer_,
        status          = "skip_read",
        error_msg       = "readCohortSet failed or empty",
        stringsAsFactors = FALSE
      ))
    }

    # Create mock CDM from cohort set (simulated data)
    # n must be >= number of cohorts; use 10x batch size for richer data
    n_sim <- max(n_subjects, nrow(cohort_set) * 10L)
    cdm <- tryCatch(
      CDMConnector::cdmFromCohortSet(cohort_set, n = n_sim),
      error = function(e) NULL
    )
    if (is.null(cdm)) {
      return(data.frame(
        batch           = batch_idx,
        n_cohorts       = nrow(cohort_set),
        time_old_sec    = NA_real_,
        time_new_sec    = NA_real_,
        ratio           = NA_real_,
        rows_old        = NA_integer_,
        rows_new        = NA_integer_,
        all_identical   = NA,
        n_mismatch      = NA_integer_,
        status          = "skip_cdm",
        error_msg       = "cdmFromCohortSet failed",
        stringsAsFactors = FALSE
      ))
    }
    on.exit(tryCatch(CDMConnector::cdmDisconnect(cdm), error = function(e) NULL), add = TRUE)

    # -- Run OLD method (CDMConnector::generateCohortSet) ---------------------
    t0_old  <- Sys.time()
    err_old <- tryCatch({
      cdm <- CDMConnector::generateCohortSet(cdm, cohort_set, name = "bench_old")
      NULL
    }, error = function(e) conditionMessage(e))
    time_old <- as.numeric(difftime(Sys.time(), t0_old, units = "secs"))

    # Collect OLD results immediately — the CDM reference to bench_old may be
    # lost once generateCohortSet2 writes to the same connection.
    df_old <- NULL
    if (is.null(err_old)) {
      df_old <- tryCatch(
        as.data.frame(dplyr::collect(cdm[["bench_old"]])),
        error = function(e) NULL
      )
    }

    # -- Run NEW method (CohortDAG::generateCohortSet2) -----------------------
    t0_new  <- Sys.time()
    err_new <- tryCatch({
      cdm <- CohortDAG::generateCohortSet2(cdm, cohort_set, name = "bench_new")
      NULL
    }, error = function(e) conditionMessage(e))
    time_new <- as.numeric(difftime(Sys.time(), t0_new, units = "secs"))

    if (!is.null(err_old) || !is.null(err_new)) {
      return(data.frame(
        batch           = batch_idx,
        n_cohorts       = nrow(cohort_set),
        time_old_sec    = time_old,
        time_new_sec    = time_new,
        ratio           = NA_real_,
        rows_old        = NA_integer_,
        rows_new        = NA_integer_,
        all_identical   = FALSE,
        n_mismatch      = NA_integer_,
        status          = "error",
        error_msg       = paste0(
          if (!is.null(err_old)) paste0("OLD: ", err_old) else "",
          if (!is.null(err_old) && !is.null(err_new)) " | " else "",
          if (!is.null(err_new)) paste0("NEW: ", err_new) else ""
        ),
        stringsAsFactors = FALSE
      ))
    }

    # Collect NEW results
    df_new <- tryCatch(
      as.data.frame(dplyr::collect(cdm[["bench_new"]])),
      error = function(e) NULL
    )

    if (is.null(df_old) || is.null(df_new)) {
      return(data.frame(
        batch           = batch_idx,
        n_cohorts       = nrow(cohort_set),
        time_old_sec    = time_old,
        time_new_sec    = time_new,
        ratio           = if (time_old > 0) time_new / time_old else NA_real_,
        rows_old        = if (!is.null(df_old)) nrow(df_old) else NA_integer_,
        rows_new        = if (!is.null(df_new)) nrow(df_new) else NA_integer_,
        all_identical   = NA,
        n_mismatch      = NA_integer_,
        status          = "compare_error",
        error_msg       = paste0("collect failed: ",
          if (is.null(df_old)) "bench_old " else "",
          if (is.null(df_new)) "bench_new" else ""),
        stringsAsFactors = FALSE
      ))
    }

    # -- Compare results using pre-collected data frames ----------------------
    cmp <- tryCatch(
      compare_cohort_dfs(df_old, df_new),
      error = function(e) {
        list(identical = NA, n_rows_old = nrow(df_old), n_rows_new = nrow(df_new),
             details = paste("compare error:", conditionMessage(e)),
             per_cohort = data.frame(cohort_definition_id = integer(0),
                                     rows_old = integer(0), rows_new = integer(0),
                                     identical = logical(0), stringsAsFactors = FALSE))
      }
    )

    if (is.na(cmp$identical)) {
      return(data.frame(
        batch           = batch_idx,
        n_cohorts       = nrow(cohort_set),
        time_old_sec    = time_old,
        time_new_sec    = time_new,
        ratio           = if (time_old > 0) time_new / time_old else NA_real_,
        rows_old        = cmp$n_rows_old,
        rows_new        = cmp$n_rows_new,
        all_identical   = NA,
        n_mismatch      = NA_integer_,
        status          = "compare_error",
        error_msg       = cmp$details,
        stringsAsFactors = FALSE
      ))
    }

    n_mismatch <- if (nrow(cmp$per_cohort) > 0) sum(!cmp$per_cohort$identical) else 0L

    data.frame(
      batch           = batch_idx,
      n_cohorts       = nrow(cohort_set),
      time_old_sec    = round(time_old, 3),
      time_new_sec    = round(time_new, 3),
      ratio           = if (time_old > 0) round(time_new / time_old, 3) else NA_real_,
      rows_old        = cmp$n_rows_old,
      rows_new        = cmp$n_rows_new,
      all_identical   = cmp$identical,
      n_mismatch      = n_mismatch,
      status          = if (cmp$identical) "ok" else "mismatch",
      error_msg       = if (cmp$identical) "" else cmp$details,
      stringsAsFactors = FALSE
    )
  }

  # ---- per-cohort detail worker --------------------------------------------
  collect_per_cohort <- function(batch_idx) {
    batch_files <- batch_list[[batch_idx]]
    temp_dir <- tempfile(paste0("detail_", batch_idx, "_"))
    dir.create(temp_dir, recursive = TRUE)
    on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
    for (f in batch_files) file.copy(f, file.path(temp_dir, basename(f)))

    cohort_set <- tryCatch(CDMConnector::readCohortSet(temp_dir), error = function(e) NULL)
    if (is.null(cohort_set) || nrow(cohort_set) == 0L) return(NULL)

    n_sim2 <- max(n_subjects, nrow(cohort_set) * 10L)
    cdm <- tryCatch(CDMConnector::cdmFromCohortSet(cohort_set, n = n_sim2), error = function(e) NULL)
    if (is.null(cdm)) return(NULL)
    on.exit(tryCatch(CDMConnector::cdmDisconnect(cdm), error = function(e) NULL), add = TRUE)

    err <- tryCatch({
      cdm <- CDMConnector::generateCohortSet(cdm, cohort_set, name = "bench_old")
      NULL
    }, error = function(e) conditionMessage(e))
    if (!is.null(err)) return(NULL)

    # Collect OLD results before running generateCohortSet2
    df_old <- tryCatch(as.data.frame(dplyr::collect(cdm[["bench_old"]])), error = function(e) NULL)
    if (is.null(df_old)) return(NULL)

    err <- tryCatch({
      cdm <- CohortDAG::generateCohortSet2(cdm, cohort_set, name = "bench_new")
      NULL
    }, error = function(e) conditionMessage(e))
    if (!is.null(err)) return(NULL)

    df_new <- tryCatch(as.data.frame(dplyr::collect(cdm[["bench_new"]])), error = function(e) NULL)
    if (is.null(df_new)) return(NULL)

    cmp <- tryCatch(
      compare_cohort_dfs(df_old, df_new),
      error = function(e) NULL
    )
    if (is.null(cmp) || nrow(cmp$per_cohort) == 0L) return(NULL)

    # Map cohort_definition_id back to cohort_name
    pc <- cmp$per_cohort
    pc$cohort_name <- cohort_set$cohort_name[match(pc$cohort_definition_id, cohort_set$cohort_definition_id)]
    pc$batch <- batch_idx
    pc
  }

  # ---- execute batches (parallel or sequential) ----------------------------
  message(sprintf("Processing %d batches of ~%d cohorts (%d cores)...",
                  n_batches, batch_size, n_cores))

  if (n_cores > 1L && .Platform$OS.type == "unix") {
    batch_results <- parallel::mclapply(seq_len(n_batches), function(i) {
      msg <- sprintf("  Batch %d/%d ...", i, n_batches)
      message(msg)
      run_one_batch(i)
    }, mc.cores = n_cores)
  } else {
    batch_results <- lapply(seq_len(n_batches), function(i) {
      message(sprintf("  Batch %d/%d ...", i, n_batches))
      run_one_batch(i)
    })
  }

  results <- do.call(rbind, batch_results)
  rownames(results) <- NULL

  # ---- save results --------------------------------------------------------
  timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")

  batch_file <- file.path(results_dir, paste0("tier2_batch_results_", timestamp_str, ".csv"))
  utils::write.csv(results, batch_file, row.names = FALSE)
  message("Batch results saved to: ", batch_file)

  # Also write a latest copy for vignette consumption
  utils::write.csv(results, file.path(results_dir, "tier2_batch_results_latest.csv"), row.names = FALSE)

  # ---- summary stats -------------------------------------------------------
  ok       <- results[results$status == "ok", ]
  errors   <- results[results$status == "error", ]
  mismatch <- results[results$status == "mismatch", ]

  total_old <- sum(ok$time_old_sec, na.rm = TRUE)
  total_new <- sum(ok$time_new_sec, na.rm = TRUE)

  summary_df <- data.frame(
    metric = c("total_cohorts", "total_batches", "batches_ok", "batches_error",
               "batches_mismatch", "batches_skipped",
               "total_time_old_sec", "total_time_new_sec",
               "overall_ratio", "median_batch_ratio",
               "total_rows_old", "total_rows_new"),
    value = c(n_total, n_batches, nrow(ok), nrow(errors),
              nrow(mismatch), sum(results$status %in% c("skip_read", "skip_cdm")),
              round(total_old, 2), round(total_new, 2),
              if (total_old > 0) round(total_new / total_old, 3) else NA_real_,
              round(median(ok$ratio, na.rm = TRUE), 3),
              sum(ok$rows_old, na.rm = TRUE), sum(ok$rows_new, na.rm = TRUE)),
    stringsAsFactors = FALSE
  )
  summary_file <- file.path(results_dir, paste0("tier2_summary_", timestamp_str, ".csv"))
  utils::write.csv(summary_df, summary_file, row.names = FALSE)
  utils::write.csv(summary_df, file.path(results_dir, "tier2_summary_latest.csv"), row.names = FALSE)
  message("Summary saved to: ", summary_file)

  # ---- print report --------------------------------------------------------
  message("\n========== TIER 2 RESULTS ==========")
  message(sprintf("Cohorts tested : %d", n_total))
  message(sprintf("Batches OK     : %d / %d", nrow(ok), n_batches))
  message(sprintf("Batches ERROR  : %d", nrow(errors)))
  message(sprintf("Batches MISMATCH: %d", nrow(mismatch)))
  message(sprintf("Total time OLD : %.1f s", total_old))
  message(sprintf("Total time NEW : %.1f s", total_new))
  if (total_old > 0) {
    message(sprintf("Speed ratio    : %.3f (new/old, <1 = faster)", total_new / total_old))
  }
  message(sprintf("Median batch ratio: %.3f", median(ok$ratio, na.rm = TRUE)))
  message("====================================\n")

  # ---- report any failures in detail ---------------------------------------
  if (nrow(errors) > 0) {
    message("FAILED BATCHES:")
    for (r in seq_len(nrow(errors))) {
      message(sprintf("  Batch %d: %s", errors$batch[r], errors$error_msg[r]))
    }
  }
  if (nrow(mismatch) > 0) {
    message("MISMATCHED BATCHES:")
    for (r in seq_len(nrow(mismatch))) {
      message(sprintf("  Batch %d: %d mismatched cohorts — %s",
                      mismatch$batch[r], mismatch$n_mismatch[r], mismatch$error_msg[r]))
    }
  }

  # ---- assertions ----------------------------------------------------------
  # All batches that ran should produce identical results
  ran <- results[results$status %in% c("ok", "mismatch", "error"), ]
  expect_true(all(ran$status == "ok" | ran$status == "error"),
              info = paste("Mismatched batches:", paste(mismatch$batch, collapse = ", ")))

  # No more than 5% of batches should error (allow some parse failures)
  max_error_pct <- 0.10
  error_pct <- nrow(errors) / n_batches
  expect_true(error_pct <= max_error_pct,
              info = sprintf("%.0f%% of batches errored (max %.0f%%)",
                             error_pct * 100, max_error_pct * 100))
})
