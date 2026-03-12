# extras/benchmark_cohort_generation.R
# Benchmark CDMConnector::generateCohortSet (old) vs CohortDAG::generateCohortSet2 (new)
# on a live database. Logs overall time per method and the files included in the cohort set.
#
# Usage:
#   source("extras/benchmark_cohort_generation.R")
#   # With cohort set from a folder:
#   cohort_set <- CDMConnector::readCohortSet("path/to/cohorts")
#   result <- benchmark_cohort_generation(cdm, cohort_set, cohort_path = "path/to/cohorts")
#
#   # Or without path (files_included will use cohort_name from cohort set):
#   result <- benchmark_cohort_generation(cdm, cohort_set)
#
# Requires: CDMConnector, CohortDAG (load with devtools::load_all() or install)

#' Benchmark cohort generation: old (CDMConnector::generateCohortSet) vs new (generateCohortSet2)
#'
#' Runs both methods on a live CDM with the same cohort set, records overall time for each,
#' and logs the files (or cohort names) included in the cohort set.
#'
#' @param cdm A cdm reference object (e.g. from CDMConnector::cdm_connect or cdm_from_con).
#' @param cohortSet Cohort set data frame (e.g. from CDMConnector::readCohortSet). Must have
#'   \code{cohort_definition_id} and \code{cohort}; \code{cohort_name} is used for logging if present.
#' @param name_old Character. Table name for the cohort table written by the old method.
#' @param name_new Character. Table name for the cohort table written by the new method.
#' @param cohort_path Optional. Path to the folder containing cohort JSON files. If provided,
#'   \code{files_included} in the result will list the JSON file names; otherwise cohort
#'   names from \code{cohortSet} are used.
#' @param log_file Optional. If provided, log lines are appended to this file.
#' @param verbose Logical. If TRUE, print timing and file summary to the console.
#' @return A list with:
#'   \item{time_old_sec}{Elapsed seconds for CDMConnector::generateCohortSet.}
#'   \item{time_new_sec}{Elapsed seconds for generateCohortSet2.}
#'   \item{files_included}{Character vector of file names or cohort names in the cohort set.}
#'   \item{n_cohorts}{Number of cohorts in the cohort set.}
#'   \item{cdm}{The cdm object after both runs (contains both \code{name_old} and \code{name_new} tables).}
#'   \item{log}{Character vector of log lines (timing and files) for saving to a file.}
#' @export
benchmark_cohort_generation <- function(cdm,
                                       cohortSet,
                                       name_old = "cohort_bench_old",
                                       name_new = "cohort_bench_new",
                                       cohort_path = NULL,
                                       log_file = NULL,
                                       verbose = TRUE) {

  stopifnot(
    is.data.frame(cohortSet),
    "cohort_definition_id" %in% names(cohortSet),
    "cohort" %in% names(cohortSet)
  )

  n_cohorts <- nrow(cohortSet)

  # Resolve "files included" for logging
  if (is.character(cohort_path) && length(cohort_path) == 1L && dir.exists(cohort_path)) {
    files_included <- sort(list.files(cohort_path, pattern = "\\.json$", ignore.case = TRUE))
  } else {
    files_included <- if ("cohort_name" %in% names(cohortSet)) {
      as.character(cohortSet$cohort_name)
    } else {
      paste0("cohort_", cohortSet$cohort_definition_id)
    }
  }

  log_lines <- character(0)
  append_log <- function(...) {
    msg <- paste0("[benchmark ", format(Sys.time(), "%H:%M:%S"), "] ", paste(..., collapse = ""))
    log_lines <<- c(log_lines, msg)
    if (verbose) message(msg)
  }

  append_log("=== Cohort generation benchmark (live DB) ===")
  append_log("Cohort set size: ", n_cohorts)
  append_log("Files / cohorts included: ", paste(files_included, collapse = ", "))

  # --- Old method: CDMConnector::generateCohortSet ---
  append_log("Running CDMConnector::generateCohortSet (old) -> ", name_old, " ...")
  t0_old <- Sys.time()
  cdm <- CDMConnector::generateCohortSet(cdm, cohortSet, name = name_old)
  time_old_sec <- as.numeric(difftime(Sys.time(), t0_old, units = "secs"))
  append_log("Old method finished in ", round(time_old_sec, 2), " s")

  # --- New method: generateCohortSet2 ---
  append_log("Running generateCohortSet2 (new) -> ", name_new, " ...")
  t0_new <- Sys.time()
  cdm <- generateCohortSet2(cdm, cohortSet, name = name_new)
  time_new_sec <- as.numeric(difftime(Sys.time(), t0_new, units = "secs"))
  append_log("New method finished in ", round(time_new_sec, 2), " s")

  append_log("=== Summary ===")
  append_log("Old (generateCohortSet): ", round(time_old_sec, 2), " s")
  append_log("New (generateCohortSet2): ", round(time_new_sec, 2), " s")
  if (time_old_sec > 0) {
    ratio <- time_new_sec / time_old_sec
    append_log("Ratio (new/old): ", round(ratio, 2))
  }
  append_log("Files in cohort set: ", paste(files_included, collapse = ", "))

  result <- list(
    cdm = cdm,
    time_old_sec = time_old_sec,
    time_new_sec = time_new_sec,
    files_included = files_included,
    n_cohorts = n_cohorts,
    cdm = cdm,
    log = log_lines
  )

  if (is.character(log_file) && length(log_file) == 1L && nzchar(log_file)) {
    cat("\n", result$log, sep = "\n", file = log_file, append = TRUE)
    message("Log appended to: ", log_file)
  }
  cdm
}


#' Compare two cohort tables for identical rows (order ignored)
#'
#' Collects both tables from the cdm, normalizes key columns, and checks that
#' the set of rows is identical. Used to verify that the old and new cohort
#' generation methods produce the same results.
#'
#' @param cdm A cdm reference containing the two cohort tables.
#' @param name_old Table name for the cohort table from the old method.
#' @param name_new Table name for the cohort table from the new method.
#' @return A list with:
#'   \item{identical}{Logical. TRUE if both tables have the same set of rows.}
#'   \item{n_rows_old}{Total rows in the old table.}
#'   \item{n_rows_new}{Total rows in the new table.}
#'   \item{per_cohort}{Data frame with cohort_definition_id, n_old, n_new, identical.}
#'   \item{details}{Character. Brief message for logging.}
#' @export
compare_cohort_tables <- function(cdm, name_old = "cohort_bench_old", name_new = "cohort_bench_new") {
  key_cols <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")

  df_old <- dplyr::collect(cdm[[name_old]])
  df_new <- dplyr::collect(cdm[[name_new]])

  # Normalize for comparison (dates may be Date or character across backends)
  norm <- function(df) {
    df <- df[do.call(order, df[key_cols]), key_cols, drop = FALSE]
    for (col in c("cohort_start_date", "cohort_end_date")) {
      if (col %in% names(df)) df[[col]] <- as.character(df[[col]])
    }
    rownames(df) <- NULL
    df
  }
  old_norm <- norm(df_old)
  new_norm <- norm(df_new)

  identical_rows <- nrow(old_norm) == nrow(new_norm) && identical(old_norm, new_norm)

  # Per-cohort row comparison for reporting
  if (nrow(df_old) > 0 || nrow(df_new) > 0) {
    cids <- sort(unique(c(df_old$cohort_definition_id, df_new$cohort_definition_id)))
    per_list <- lapply(cids, function(cid) {
      o <- df_old[df_old$cohort_definition_id == cid, key_cols, drop = FALSE]
      n <- df_new[df_new$cohort_definition_id == cid, key_cols, drop = FALSE]
      o <- norm(o)
      n <- norm(n)
      data.frame(
        cohort_definition_id = cid,
        n_old = nrow(o),
        n_new = nrow(n),
        identical = identical(o, n)
      )
    })
    per <- do.call(rbind, per_list)
  } else {
    per <- data.frame(cohort_definition_id = integer(), n_old = integer(), n_new = integer(), identical = logical())
  }

  details <- if (identical_rows) {
    paste0("Tables identical: ", nrow(df_old), " rows")
  } else {
    paste0("MISMATCH: old=", nrow(df_old), " rows, new=", nrow(df_new), " rows")
  }

  list(
    identical = identical_rows,
    n_rows_old = nrow(df_old),
    n_rows_new = nrow(df_new),
    per_cohort = per,
    details = details
  )
}
