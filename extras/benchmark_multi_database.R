# extras/benchmark_multi_database.R
# Run cohort generation benchmarking and equivalence checks across multiple
# databases (Postgres, Redshift, Snowflake, Spark, SQL Server). Writes timing
# and equivalence results to CSV.
#
# Usage:
#   source("extras/benchmark_cohort_generation.R")
#   source("extras/benchmark_multi_database.R")
#
#   cohort_set <- CDMConnector::readCohortSet("path/to/cohorts")
#   cdms <- list(
#     postgres   = cdm_pg,
#     redshift   = cdm_rs,
#     snowflake  = cdm_snow,
#     spark      = cdm_spark,
#     sql_server = cdm_sqlserver
#   )
#   run_benchmark_multi_database(
#     cdms = cdms,
#     cohort_set = cohort_set,
#     cohort_path = "path/to/cohorts",
#     results_csv = "benchmark_results.csv",
#     equivalence_csv = "benchmark_equivalence.csv"
#   )
#
# Requires: CDMConnector, dplyr, CohortDAG

#' Run cohort generation benchmark and equivalence check on multiple databases
#'
#' For each CDM in \code{cdms}, runs the old (CDMConnector::generateCohortSet) and
#' new (generateCohortSet2) cohort generation, records timing, and compares the
#' two cohort tables for identical rows (order ignored). Writes two CSVs:
#' benchmark results (timing per database) and equivalence results (per-database,
#' per-cohort match status).
#'
#' @param cdms Named list of CDM reference objects. Names are used as the
#'   \code{database} identifier in the output CSVs (e.g. "postgres", "redshift",
#'   "snowflake", "spark", "sql_server").
#' @param cohort_set Cohort set data frame (e.g. from CDMConnector::readCohortSet).
#' @param cohort_path Optional. Path to folder of cohort JSON files (for logging).
#' @param name_old Table name for the old method's cohort table.
#' @param name_new Table name for the new method's cohort table.
#' @param results_csv Path to write benchmark timing CSV. Columns: database,
#'   time_old_sec, time_new_sec, ratio_new_over_old, n_cohorts, files_included.
#' @param equivalence_csv Path to write equivalence CSV. Columns: database,
#'   cohort_definition_id, n_old, n_new, rows_identical; plus one row per database
#'   with cohort_definition_id NA for overall table match.
#' @param verbose Logical. If TRUE, print progress and summaries.
#' @return Invisible list with \code{results} (timing data frame), \code{equivalence}
#'   (equivalence data frame), and \code{benchmark_list} (per-database benchmark
#'   results from \code{benchmark_cohort_generation}).
#' @export
run_benchmark_multi_database <- function(cdms,
                                        cohort_set,
                                        cohort_path = NULL,
                                        name_old = "cohort_bench_old",
                                        name_new = "cohort_bench_new",
                                        results_csv = "benchmark_results.csv",
                                        equivalence_csv = "benchmark_equivalence.csv",
                                        verbose = TRUE) {

  stopifnot(
    is.list(cdms),
    length(cdms) >= 1L,
    !is.null(names(cdms)),
    all(nzchar(names(cdms))),
    is.data.frame(cohort_set),
    "cohort_definition_id" %in% names(cohort_set),
    "cohort" %in% names(cohort_set)
  )

  # Resolve files_included once for all DBs
  if (is.character(cohort_path) && length(cohort_path) == 1L && dir.exists(cohort_path)) {
    files_included <- sort(list.files(cohort_path, pattern = "\\.json$", ignore.case = TRUE))
  } else {
    files_included <- if ("cohort_name" %in% names(cohort_set)) {
      as.character(cohort_set$cohort_name)
    } else {
      paste0("cohort_", cohort_set$cohort_definition_id)
    }
  }
  files_included_str <- paste(files_included, collapse = "; ")
  n_cohorts <- nrow(cohort_set)

  results_rows <- list()
  equiv_rows <- list()
  benchmark_list <- list()

  for (db_name in names(cdms)) {
    cdm <- cdms[[db_name]]
    if (verbose) message("=== Database: ", db_name, " ===")

    # Run benchmark (old + new cohort generation)
    res <- tryCatch({
      benchmark_cohort_generation(
        cdm = cdm,
        cohortSet = cohort_set,
        name_old = name_old,
        name_new = name_new,
        cohort_path = cohort_path,
        verbose = verbose
      )
    }, error = function(e) {
      if (verbose) message("Benchmark failed for ", db_name, ": ", conditionMessage(e))
      NULL
    })

    if (is.null(res)) {
      results_rows[[db_name]] <- data.frame(
        database = db_name,
        time_old_sec = NA_real_,
        time_new_sec = NA_real_,
        ratio_new_over_old = NA_real_,
        n_cohorts = n_cohorts,
        files_included = files_included_str,
        status = "error",
        stringsAsFactors = FALSE
      )
      equiv_rows[[db_name]] <- data.frame(
        database = db_name,
        cohort_definition_id = NA_integer_,
        n_old = NA_integer_,
        n_new = NA_integer_,
        rows_identical = NA,
        status = "benchmark_failed",
        stringsAsFactors = FALSE
      )
      next
    }

    benchmark_list[[db_name]] <- res

    # Timing row
    ratio <- if (res$time_old_sec > 0) res$time_new_sec / res$time_old_sec else NA_real_
    results_rows[[db_name]] <- data.frame(
      database = db_name,
      time_old_sec = res$time_old_sec,
      time_new_sec = res$time_new_sec,
      ratio_new_over_old = ratio,
      n_cohorts = n_cohorts,
      files_included = files_included_str,
      status = "ok",
      stringsAsFactors = FALSE
    )

    # Equivalence: compare the two cohort tables
    cmp <- tryCatch({
      compare_cohort_tables(res$cdm, name_old = name_old, name_new = name_new)
    }, error = function(e) {
      if (verbose) message("Compare failed for ", db_name, ": ", conditionMessage(e))
      NULL
    })

    if (is.null(cmp)) {
      equiv_rows[[db_name]] <- data.frame(
        database = db_name,
        cohort_definition_id = NA_integer_,
        n_old = NA_integer_,
        n_new = NA_integer_,
        rows_identical = NA,
        status = "compare_failed",
        stringsAsFactors = FALSE
      )
    } else {
      # Overall summary row
      equiv_rows[[db_name]] <- data.frame(
        database = db_name,
        cohort_definition_id = NA_integer_,
        n_old = cmp$n_rows_old,
        n_new = cmp$n_rows_new,
        rows_identical = cmp$identical,
        status = if (cmp$identical) "ok" else "mismatch",
        stringsAsFactors = FALSE
      )
      # Per-cohort rows
      if (nrow(cmp$per_cohort) > 0) {
        per <- data.frame(
          database = db_name,
          cohort_definition_id = cmp$per_cohort$cohort_definition_id,
          n_old = cmp$per_cohort$n_old,
          n_new = cmp$per_cohort$n_new,
          rows_identical = cmp$per_cohort$identical,
          status = ifelse(cmp$per_cohort$identical, "ok", "mismatch"),
          stringsAsFactors = FALSE
        )
        equiv_rows[[db_name]] <- rbind(equiv_rows[[db_name]], per)
      }
    }
  }

  results_df <- do.call(rbind, results_rows)
  rownames(results_df) <- NULL

  equivalence_df <- do.call(rbind, equiv_rows)
  rownames(equivalence_df) <- NULL

  if (is.character(results_csv) && length(results_csv) == 1L && nzchar(results_csv)) {
    utils::write.csv(results_df, results_csv, row.names = FALSE)
    if (verbose) message("Wrote benchmark results to: ", results_csv)
  }
  if (is.character(equivalence_csv) && length(equivalence_csv) == 1L && nzchar(equivalence_csv)) {
    utils::write.csv(equivalence_df, equivalence_csv, row.names = FALSE)
    if (verbose) message("Wrote equivalence results to: ", equivalence_csv)
  }

  invisible(list(
    results = results_df,
    equivalence = equivalence_df,
    benchmark_list = benchmark_list
  ))
}
