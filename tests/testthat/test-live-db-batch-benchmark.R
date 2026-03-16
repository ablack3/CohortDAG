skip_if_not_tier(3L)
skip_if_not_run_tier3_benchmark()

test_that("live databases: batched generateCohortSet vs generateCohortSet2 benchmark", {
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("dplyr")

  available_dbms <- live_benchmark_dbms()
  skip_if(length(available_dbms) == 0L, "No configured databases available for live benchmark")

  message(sprintf(
    "\n=== Tier 3 live batch benchmark (%s) ===",
    paste(available_dbms, collapse = ", ")
  ))

  results_dir <- live_benchmark_results_dir()
  timestamp_str <- format(Sys.time(), "%Y%m%d_%H%M%S")
  aggregate_results <- list()
  aggregate_mismatches <- list()
  aggregate_summary <- list()
  failure_rows <- list()

  run_one_db <- function(dbms) {
    message(sprintf("\n--- DBMS: %s ---", dbms))
    tryCatch(run_live_db_batch_benchmark(dbms), error = function(e) e)
  }

  n_workers <- live_benchmark_worker_count(available_dbms)
  db_results <- if (n_workers > 1L && length(available_dbms) > 1L) {
    cl <- parallel::makeCluster(n_workers)
    on.exit(try(parallel::stopCluster(cl), silent = TRUE), add = TRUE)
    parallel::clusterExport(
      cl,
      varlist = c(
        "run_one_db", "run_live_db_batch_benchmark",
        "live_benchmark_truthy", "live_benchmark_force_dialect", "live_benchmark_results_dir",
        "live_benchmark_requested_dbms", "live_benchmark_db_available", "live_benchmark_dbms",
        "live_benchmark_worker_count", "live_benchmark_cleanup_prefix",
        "cleanup_live_benchmark_tables", "cleanup_live_benchmark_outputs",
        "with_live_benchmark_log",
        "read_live_benchmark_batch", "resolve_live_benchmark_batches",
        "build_live_benchmark_summary", "live_benchmark_collect_df",
        "live_benchmark_profile_sql_enabled", "live_benchmark_run_method",
        "live_benchmark_reference_stability_enabled",
        "live_benchmark_reference_stability", "live_benchmark_new_stability_enabled",
        "live_benchmark_force_method_stability", "live_benchmark_stability_cohort_ids",
        "live_benchmark_method_stability", "live_benchmark_new_stability",
        "get_connection", "get_cdm_schema", "get_write_schema", "disconnect",
        "write_prefix", "testUsingDatabaseConnector",
        "make_unique_cohort_names", "compare_cohort_dfs_detailed", "unzip_cohorts"
      ),
      envir = environment()
    )
    parallel::clusterEvalQ(cl, {
      library(CDMConnector)
      library(CohortDAG)
      library(dplyr)
      NULL
    })
    parallel::parLapplyLB(cl, available_dbms, run_one_db)
  } else {
    lapply(available_dbms, run_one_db)
  }

  for (idx in seq_along(db_results)) {
    dbms <- available_dbms[[idx]]
    res <- db_results[[idx]]

    if (inherits(res, "error")) {
      failure_rows[[length(failure_rows) + 1L]] <- data.frame(
        dbms = dbms,
        status = "error",
        message = conditionMessage(res),
        stringsAsFactors = FALSE
      )
      message(sprintf("[%-10s] benchmark failed: %s", dbms, conditionMessage(res)))
      next
    }

    aggregate_results[[length(aggregate_results) + 1L]] <- res$results
    aggregate_mismatches[[length(aggregate_mismatches) + 1L]] <- res$mismatches
    aggregate_summary[[length(aggregate_summary) + 1L]] <- res$summary

    vals <- setNames(res$summary$value, res$summary$metric)
    message(sprintf(
      "[%-10s] accuracy=%s%% speedup=%sx ref_unstable=%s new_unstable=%s both_unstable=%s mismatches=%s errors=%s dir=%s",
      dbms,
      vals[["overall_accuracy_pct"]],
      vals[["overall_speedup_old_over_new"]],
      vals[["batches_reference_unstable"]],
      vals[["batches_new_unstable"]],
      vals[["batches_both_unstable"]],
      vals[["batches_mismatch"]],
      vals[["batches_error"]],
      res$result_dir
    ))
  }

  aggregate_results_df <- if (length(aggregate_results) > 0L) {
    do.call(rbind, aggregate_results)
  } else {
    data.frame()
  }
  aggregate_mismatches_df <- if (length(aggregate_mismatches) > 0L) {
    do.call(rbind, aggregate_mismatches)
  } else {
    data.frame()
  }
  aggregate_summary_df <- if (length(aggregate_summary) > 0L) {
    do.call(rbind, aggregate_summary)
  } else {
    data.frame()
  }
  failure_df <- if (length(failure_rows) > 0L) {
    do.call(rbind, failure_rows)
  } else {
    data.frame(
      dbms = character(0),
      status = character(0),
      message = character(0),
      stringsAsFactors = FALSE
    )
  }

  utils::write.csv(
    aggregate_results_df,
    file.path(results_dir, paste0("tier3_live_batch_results_", timestamp_str, ".csv")),
    row.names = FALSE
  )
  utils::write.csv(
    aggregate_mismatches_df,
    file.path(results_dir, paste0("tier3_live_mismatch_details_", timestamp_str, ".csv")),
    row.names = FALSE
  )
  utils::write.csv(
    aggregate_summary_df,
    file.path(results_dir, paste0("tier3_live_summary_", timestamp_str, ".csv")),
    row.names = FALSE
  )
  utils::write.csv(
    failure_df,
    file.path(results_dir, paste0("tier3_live_failures_", timestamp_str, ".csv")),
    row.names = FALSE
  )

  expect_equal(nrow(failure_df), 0L, info = paste(failure_df$dbms, failure_df$message, collapse = " | "))
  if (nrow(aggregate_results_df) > 0L) {
    bad_rows <- aggregate_results_df[!aggregate_results_df$status %in% c("ok", "reference_unstable", "both_unstable"), , drop = FALSE]
    expect_equal(
      nrow(bad_rows),
      0L,
      info = paste(
        sprintf("%s batch=%s status=%s", bad_rows$dbms, bad_rows$batch, bad_rows$status),
        collapse = ", "
      )
    )
  }
})
