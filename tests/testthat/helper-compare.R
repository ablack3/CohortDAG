# helper-compare.R
# Shared helpers for comparing cohort tables across methods and platforms.

#' Compare two cohort tables for identical rows (order ignored).
#' Sorts by key columns and normalizes dates/integers for cross-backend comparison.
#' @param cdm CDM reference with both tables.
#' @param name_a Table name for first cohort table.
#' @param name_b Table name for second cohort table.
#' @return List with `identical` (logical), `n_a`, `n_b`, `details`.
#' @noRd
cohort_tables_identical_sorted <- function(cdm, name_a, name_b) {
  key_cols <- c("cohort_definition_id", "subject_id", "cohort_start_date", "cohort_end_date")
  df_a <- dplyr::collect(cdm[[name_a]])
  df_b <- dplyr::collect(cdm[[name_b]])
  key_present <- key_cols[key_cols %in% names(df_a) & key_cols %in% names(df_b)]
  norm <- function(df) {
    if (length(key_present) == 0L) return(df)
    df <- df[, key_present, drop = FALSE]
    for (col in c("cohort_start_date", "cohort_end_date")) {
      if (col %in% names(df)) df[[col]] <- as.character(df[[col]])
    }
    for (col in c("cohort_definition_id", "subject_id")) {
      if (col %in% names(df)) df[[col]] <- as.integer(df[[col]])
    }
    if (nrow(df) > 0L)
      df <- df[do.call(order, as.list(df[key_present])), key_present, drop = FALSE]
    rownames(df) <- NULL
    df
  }
  a_norm <- norm(df_a)
  b_norm <- norm(df_b)
  identical_rows <- nrow(a_norm) == nrow(b_norm) &&
    (nrow(a_norm) == 0L || all(mapply(function(a, b) all(a == b, na.rm = TRUE) && sum(is.na(a)) == sum(is.na(b)), a_norm, b_norm)))
  if (!identical_rows && nrow(df_a) == 0L && nrow(df_b) == 0L && length(key_present) >= 2L)
    identical_rows <- TRUE
  details <- if (identical_rows) {
    paste0("Tables identical: ", nrow(df_a), " rows")
  } else {
    paste0("MISMATCH: ", name_a, "=", nrow(df_a), " rows, ", name_b, "=", nrow(df_b), " rows")
  }
  list(identical = identical_rows, n_a = nrow(df_a), n_b = nrow(df_b), details = details)
}

#' Compare two generated cohort tables with per-cohort breakdown.
#' This is the main comparison function used by tier 2 tests.
#' Normalizes types (int64 -> int, numeric -> int for subject_id),
#' ignores row ordering, and reports per-cohort match status.
#' @param cdm CDM reference containing both cohort tables.
#' @param name_old Character name of the old (reference) cohort table.
#' @param name_new Character name of the new cohort table to validate.
#' @return List with `identical` (logical), `n_rows_old`, `n_rows_new`,
#'   `details` (character), and `per_cohort` (data.frame).
#' @noRd
compare_cohort_tables <- function(cdm, name_old, name_new) {
  df_old <- as.data.frame(dplyr::collect(cdm[[name_old]]))
  df_new <- as.data.frame(dplyr::collect(cdm[[name_new]]))
  compare_cohort_dfs(df_old, df_new)
}

#' Compare two cohort data frames (already collected) for identical rows.
#' @param df_old Data frame from the reference cohort table.
#' @param df_new Data frame from the new cohort table to validate.
#' @return List with `identical`, `n_rows_old`, `n_rows_new`, `details`, `per_cohort`.
#' @noRd
compare_cohort_dfs <- function(df_old, df_new) {
  key_cols <- c("cohort_definition_id", "subject_id",
                "cohort_start_date", "cohort_end_date")

  # Keep only the standard cohort columns
  key_present <- key_cols[key_cols %in% names(df_old) & key_cols %in% names(df_new)]
  if (length(key_present) == 0L) {
    stop("No common key columns found between '", name_old, "' and '", name_new, "'")
  }
  df_old <- df_old[, key_present, drop = FALSE]
  df_new <- df_new[, key_present, drop = FALSE]

  # Normalize types: cast subject_id and cohort_definition_id to integer,
  # dates to character (handles Date vs POSIXct differences)
  normalize_df <- function(df) {
    for (col in c("cohort_definition_id", "subject_id")) {
      if (col %in% names(df)) df[[col]] <- as.integer(df[[col]])
    }
    for (col in c("cohort_start_date", "cohort_end_date")) {
      if (col %in% names(df)) df[[col]] <- as.character(as.Date(df[[col]]))
    }
    df
  }

  df_old <- normalize_df(df_old)
  df_new <- normalize_df(df_new)

  # Per-cohort comparison
  all_ids <- sort(unique(c(df_old$cohort_definition_id, df_new$cohort_definition_id)))

  per_cohort <- do.call(rbind, lapply(all_ids, function(cid) {
    old_c <- df_old[df_old$cohort_definition_id == cid, , drop = FALSE]
    new_c <- df_new[df_new$cohort_definition_id == cid, , drop = FALSE]

    # Sort both by all key columns for order-independent comparison
    if (nrow(old_c) > 0L) {
      old_c <- old_c[do.call(order, as.list(old_c)), , drop = FALSE]
      rownames(old_c) <- NULL
    }
    if (nrow(new_c) > 0L) {
      new_c <- new_c[do.call(order, as.list(new_c)), , drop = FALSE]
      rownames(new_c) <- NULL
    }

    is_match <- nrow(old_c) == nrow(new_c) &&
      (nrow(old_c) == 0L || all(mapply(function(a, b) all(a == b, na.rm = TRUE) && sum(is.na(a)) == sum(is.na(b)), old_c, new_c)))

    data.frame(
      cohort_definition_id = cid,
      rows_old             = nrow(old_c),
      rows_new             = nrow(new_c),
      identical            = is_match,
      stringsAsFactors     = FALSE
    )
  }))

  if (is.null(per_cohort) || nrow(per_cohort) == 0L) {
    per_cohort <- data.frame(
      cohort_definition_id = integer(0),
      rows_old             = integer(0),
      rows_new             = integer(0),
      identical            = logical(0),
      stringsAsFactors     = FALSE
    )
  }

  n_mismatch   <- sum(!per_cohort$identical)
  all_match    <- n_mismatch == 0L
  n_rows_old   <- nrow(df_old)
  n_rows_new   <- nrow(df_new)

  details <- if (all_match) {
    sprintf("All %d cohorts match (%d rows)", length(all_ids), n_rows_old)
  } else {
    mismatched_ids <- per_cohort$cohort_definition_id[!per_cohort$identical]
    sprintf("%d/%d cohorts mismatched (ids: %s); old=%d rows, new=%d rows",
            n_mismatch, length(all_ids),
            paste(utils::head(mismatched_ids, 10), collapse = ","),
            n_rows_old, n_rows_new)
  }

  list(
    identical  = all_match,
    n_rows_old = n_rows_old,
    n_rows_new = n_rows_new,
    details    = details,
    per_cohort = per_cohort
  )
}

#' Load a small cohort set from inst/cohorts for platform tests.
#' @param n_max Maximum number of cohorts to load (default 2).
#' @return A cohort set data frame.
#' @noRd
platform_test_cohort_set <- function(n_max = 2L) {
  cohorts_dir <- system.file("cohorts", package = "CohortDAG")
  if (!nzchar(cohorts_dir) || !dir.exists(cohorts_dir)) {
    cohorts_dir <- file.path(testthat::test_path(), "..", "..", "inst", "cohorts")
  }
  if (!dir.exists(cohorts_dir)) stop("Cannot find cohorts directory")
  preferred <- c("smoker.json", "aspirin_users.json", "cohort.json", "antidepressants.json")
  all_files <- list.files(cohorts_dir, pattern = "\\.json$", full.names = TRUE)
  available <- basename(all_files)
  chosen <- preferred[preferred %in% available]
  if (length(chosen) == 0L) chosen <- utils::head(available, n_max)
  chosen <- utils::head(chosen, n_max)
  paths <- file.path(cohorts_dir, chosen)
  temp_dir <- tempfile("platform_cohort_set")
  dir.create(temp_dir, recursive = TRUE)
  for (p in paths) file.copy(p, file.path(temp_dir, basename(p)))
  cohort_set <- CDMConnector::readCohortSet(temp_dir)
  unlink(temp_dir, recursive = TRUE)
  cohort_set
}
