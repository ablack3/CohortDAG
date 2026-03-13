#' Get the bundled cohort set
#'
#' Extracts the 3000+ cohort definitions shipped with CohortDAG from the
#' bundled zip file and returns a cohort set data frame via
#' \code{CDMConnector::readCohortSet}.
#'
#' @param n_max Maximum number of cohort definitions to return. Defaults to
#'   \code{Inf} (all cohorts). Use a smaller value for quick testing.
#' @return A cohort set data frame suitable for \code{generateCohortSet2}.
#' @export
getCohortSet <- function(n_max = Inf) {
  zip_path <- system.file("cohorts.zip", package = "CohortDAG", mustWork = TRUE)
  temp_dir <- tempfile("cohortdag_cohorts_")
  dir.create(temp_dir, recursive = TRUE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)

  utils::unzip(zip_path, exdir = temp_dir)

  if (is.finite(n_max)) {
    all_files <- sort(list.files(temp_dir, pattern = "\\.json$", full.names = TRUE))
    keep <- utils::head(all_files, n_max)
    remove <- setdiff(all_files, keep)
    if (length(remove) > 0L) file.remove(remove)
  }

  CDMConnector::readCohortSet(temp_dir)
}

#' Benchmark cohort generation: old vs new
#'
#' Runs both CDMConnector::generateCohortSet (old) and generateCohortSet2 (new)
#' on a live CDM with the same cohort set, records overall time for each,
#' and logs the files (or cohort names) included in the cohort set.
#'
#' @param cdm A cdm reference object.
#' @param cohortSet Cohort set data frame (e.g. from CDMConnector::readCohortSet).
#' @param name_old Character. Table name for the old method's cohort table.
#' @param name_new Character. Table name for the new method's cohort table.
#' @param cohort_path Optional. Path to the folder containing cohort JSON files.
#' @param log_file Optional. If provided, log lines are appended to this file.
#' @param verbose Logical. If TRUE, print timing to the console.
#' @return The cdm object after both runs (contains both tables).
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

  append_log("Running CDMConnector::generateCohortSet (old) -> ", name_old, " ...")
  t0_old <- Sys.time()
  cdm <- CDMConnector::generateCohortSet(cdm, cohortSet, name = name_old)
  time_old_sec <- as.numeric(difftime(Sys.time(), t0_old, units = "secs"))
  append_log("Old method finished in ", round(time_old_sec, 2), " s")

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

  if (is.character(log_file) && length(log_file) == 1L && nzchar(log_file)) {
    cat("\n", log_lines, sep = "\n", file = log_file, append = TRUE)
    if (verbose) message("Log appended to: ", log_file)
  }

  list(
    cdm = cdm,
    time_old_sec = time_old_sec,
    time_new_sec = time_new_sec,
    ratio = if (time_old_sec > 0) time_new_sec / time_old_sec else NA_real_,
    n_cohorts = n_cohorts,
    files_included = files_included,
    log = log_lines
  )
}


#' Compare two cohort tables for identical rows (order ignored)
#'
#' Collects both tables from the cdm, normalizes key columns, and checks that
#' the set of rows is identical.
#'
#' @param cdm A cdm reference containing the two cohort tables.
#' @param name_old Table name for the cohort table from the old method.
#' @param name_new Table name for the cohort table from the new method.
#' @return A list with `identical`, `n_rows_old`, `n_rows_new`, `per_cohort`, `details`.
#' @export
compare_cohort_tables <- function(cdm, name_old = "cohort_bench_old", name_new = "cohort_bench_new") {
  key_cols <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")

  df_old <- dplyr::collect(cdm[[name_old]])
  df_new <- dplyr::collect(cdm[[name_new]])

  norm <- function(df) {
    df <- df[, key_cols[key_cols %in% names(df)], drop = FALSE]
    for (col in c("cohort_start_date", "cohort_end_date")) {
      if (col %in% names(df)) df[[col]] <- as.character(df[[col]])
    }
    for (col in c("cohort_definition_id", "subject_id")) {
      if (col %in% names(df)) df[[col]] <- as.integer(df[[col]])
    }
    if (nrow(df) > 0L) {
      present <- key_cols[key_cols %in% names(df)]
      df <- df[do.call(order, as.list(df[present])), present, drop = FALSE]
    }
    rownames(df) <- NULL
    df
  }
  old_norm <- norm(df_old)
  new_norm <- norm(df_new)

  identical_rows <- nrow(old_norm) == nrow(new_norm) && identical(old_norm, new_norm)

  if (nrow(df_old) > 0 || nrow(df_new) > 0) {
    cids <- sort(unique(c(df_old$cohort_definition_id, df_new$cohort_definition_id)))
    per_list <- lapply(cids, function(cid) {
      o <- norm(df_old[df_old$cohort_definition_id == cid, , drop = FALSE])
      n <- norm(df_new[df_new$cohort_definition_id == cid, , drop = FALSE])
      data.frame(
        cohort_definition_id = cid,
        n_old = nrow(o),
        n_new = nrow(n),
        identical = identical(o, n)
      )
    })
    per <- do.call(rbind, per_list)
  } else {
    per <- data.frame(cohort_definition_id = integer(), n_old = integer(),
                      n_new = integer(), identical = logical())
  }

  details <- if (identical_rows) {
    paste0("Tables identical: ", nrow(df_old), " rows")
  } else {
    paste0("MISMATCH: old=", nrow(df_old), " rows, new=", nrow(df_new), " rows")
  }

  list(identical = identical_rows, n_rows_old = nrow(df_old), n_rows_new = nrow(df_new),
       per_cohort = per, details = details)
}


#' Run cohort generation benchmark across multiple databases
#'
#' For each CDM in `cdms`, runs old and new cohort generation, records timing,
#' and compares the two cohort tables for identical rows.
#'
#' @param cdms Named list of CDM reference objects.
#' @param cohort_set Cohort set data frame.
#' @param cohort_path Optional path to folder of cohort JSON files.
#' @param name_old Table name for the old method's cohort table.
#' @param name_new Table name for the new method's cohort table.
#' @param results_csv Path to write benchmark timing CSV.
#' @param equivalence_csv Path to write equivalence CSV.
#' @param verbose Logical.
#' @return Invisible list with `results`, `equivalence`, and `benchmark_list`.
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
    is.list(cdms), length(cdms) >= 1L, !is.null(names(cdms)),
    is.data.frame(cohort_set),
    "cohort_definition_id" %in% names(cohort_set)
  )

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

    res <- tryCatch({
      benchmark_cohort_generation(
        cdm = cdm, cohortSet = cohort_set,
        name_old = name_old, name_new = name_new,
        cohort_path = cohort_path, verbose = verbose
      )
    }, error = function(e) {
      if (verbose) message("Benchmark failed for ", db_name, ": ", conditionMessage(e))
      NULL
    })

    if (is.null(res)) {
      results_rows[[db_name]] <- data.frame(
        database = db_name, time_old_sec = NA_real_, time_new_sec = NA_real_,
        ratio_new_over_old = NA_real_, n_cohorts = n_cohorts,
        files_included = files_included_str, status = "error",
        stringsAsFactors = FALSE
      )
      next
    }

    benchmark_list[[db_name]] <- res
    ratio <- if (res$time_old_sec > 0) res$time_new_sec / res$time_old_sec else NA_real_
    results_rows[[db_name]] <- data.frame(
      database = db_name, time_old_sec = res$time_old_sec, time_new_sec = res$time_new_sec,
      ratio_new_over_old = ratio, n_cohorts = n_cohorts,
      files_included = files_included_str, status = "ok",
      stringsAsFactors = FALSE
    )

    cmp <- tryCatch({
      compare_cohort_tables(res$cdm, name_old = name_old, name_new = name_new)
    }, error = function(e) {
      if (verbose) message("Compare failed for ", db_name, ": ", conditionMessage(e))
      NULL
    })

    if (!is.null(cmp)) {
      equiv_rows[[db_name]] <- data.frame(
        database = db_name, cohort_definition_id = NA_integer_,
        n_old = cmp$n_rows_old, n_new = cmp$n_rows_new,
        rows_identical = cmp$identical,
        status = if (cmp$identical) "ok" else "mismatch",
        stringsAsFactors = FALSE
      )
      if (nrow(cmp$per_cohort) > 0) {
        per <- data.frame(
          database = db_name,
          cohort_definition_id = cmp$per_cohort$cohort_definition_id,
          n_old = cmp$per_cohort$n_old, n_new = cmp$per_cohort$n_new,
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
  if (!is.null(equivalence_df)) rownames(equivalence_df) <- NULL

  if (is.character(results_csv) && nzchar(results_csv)) {
    utils::write.csv(results_df, results_csv, row.names = FALSE)
    if (verbose) message("Wrote benchmark results to: ", results_csv)
  }
  if (is.character(equivalence_csv) && nzchar(equivalence_csv)) {
    utils::write.csv(equivalence_df, equivalence_csv, row.names = FALSE)
    if (verbose) message("Wrote equivalence results to: ", equivalence_csv)
  }

  invisible(list(results = results_df, equivalence = equivalence_df, benchmark_list = benchmark_list))
}
