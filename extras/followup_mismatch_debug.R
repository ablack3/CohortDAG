#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(pkgload)
  library(CDMConnector)
  library(dplyr)
})

pkgload::load_all(".", quiet = TRUE)
source("tests/testthat/helper-compare.R")

assignInNamespace("cohdSimilarConcepts", function(...) NULL, ns = "CDMConnector")

results_dir <- file.path("inst", "benchmark-results")
dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)

mismatch_file <- Sys.getenv(
  "FOLLOWUP_MISMATCH_FILE",
  unset = file.path(results_dir, "tier2_mismatch_details_latest.csv")
)
n_subjects <- as.integer(Sys.getenv("FOLLOWUP_N_SUBJECTS", unset = "500"))
seed <- as.integer(Sys.getenv("FOLLOWUP_SEED", unset = "1"))
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

if (!file.exists(mismatch_file)) {
  stop("Mismatch file not found: ", mismatch_file)
}

mismatches <- read.csv(mismatch_file, stringsAsFactors = FALSE)
if (nrow(mismatches) == 0L) {
  stop("Mismatch file has no rows: ", mismatch_file)
}

cohorts_dir <- unzip_cohorts()
on.exit(unlink(cohorts_dir, recursive = TRUE), add = TRUE)
all_files <- sort(list.files(cohorts_dir, pattern = "json$", full.names = TRUE))
all_names <- tools::file_path_sans_ext(basename(all_files))

row_keys <- function(df) {
  if (nrow(df) == 0L) return(character(0))
  paste(
    as.integer(df$cohort_definition_id),
    as.integer(df$subject_id),
    as.character(as.Date(df$cohort_start_date)),
    as.character(as.Date(df$cohort_end_date)),
    sep = "|"
  )
}

followup_rows <- list()
rowdiff_rows <- list()

for (i in seq_len(nrow(mismatches))) {
  cohort_name <- mismatches$cohort_name[[i]]
  match_idx <- which(all_names == cohort_name)

  if (length(match_idx) != 1L) {
    followup_rows[[length(followup_rows) + 1L]] <- data.frame(
      batch = mismatches$batch[[i]],
      cohort_name = cohort_name,
      original_status = "batch_mismatch",
      single_status = "file_not_found",
      single_only_in_old = NA_integer_,
      single_only_in_new = NA_integer_,
      stringsAsFactors = FALSE
    )
    next
  }

  temp_dir <- tempfile("followup_cohort_")
  dir.create(temp_dir, recursive = TRUE)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
  file.copy(all_files[match_idx], file.path(temp_dir, basename(all_files[match_idx])))

  cohort_set <- make_unique_cohort_names(CDMConnector::readCohortSet(temp_dir))
  cdm <- tryCatch(
    CDMConnector::cdmFromCohortSet(cohort_set, n = n_subjects, seed = seed),
    error = function(e) e
  )

  if (inherits(cdm, "error")) {
    followup_rows[[length(followup_rows) + 1L]] <- data.frame(
      batch = mismatches$batch[[i]],
      cohort_name = cohort_name,
      original_status = "batch_mismatch",
      single_status = "cdm_error",
      single_only_in_old = NA_integer_,
      single_only_in_new = NA_integer_,
      message = conditionMessage(cdm),
      stringsAsFactors = FALSE
    )
    unlink(temp_dir, recursive = TRUE)
    next
  }

  on.exit(tryCatch(CDMConnector::cdmDisconnect(cdm), error = function(e) NULL), add = TRUE)

  err_old <- tryCatch({
    cdm <- CDMConnector::generateCohortSet(
      cdm, cohort_set, name = "bench_old", computeAttrition = FALSE
    )
    NULL
  }, error = function(e) conditionMessage(e))

  err_new <- tryCatch({
    cdm <- CohortDAG::generateCohortSet2(
      cdm, cohort_set, name = "bench_new", computeAttrition = FALSE
    )
    NULL
  }, error = function(e) conditionMessage(e))

  if (!is.null(err_old) || !is.null(err_new)) {
    followup_rows[[length(followup_rows) + 1L]] <- data.frame(
      batch = mismatches$batch[[i]],
      cohort_name = cohort_name,
      original_status = "batch_mismatch",
      single_status = "method_error",
      single_only_in_old = NA_integer_,
      single_only_in_new = NA_integer_,
      message = paste(
        na.omit(c(
          if (!is.null(err_old)) paste0("OLD: ", err_old),
          if (!is.null(err_new)) paste0("NEW: ", err_new)
        )),
        collapse = " | "
      ),
      stringsAsFactors = FALSE
    )
    tryCatch(CDMConnector::cdmDisconnect(cdm), error = function(e) NULL)
    unlink(temp_dir, recursive = TRUE)
    next
  }

  df_old <- as.data.frame(dplyr::collect(cdm$bench_old))
  df_new <- as.data.frame(dplyr::collect(cdm$bench_new))
  cmp <- compare_cohort_dfs_detailed(df_old, df_new)

  old_keys <- row_keys(df_old)
  new_keys <- row_keys(df_new)
  only_old <- setdiff(old_keys, new_keys)
  only_new <- setdiff(new_keys, old_keys)

  followup_rows[[length(followup_rows) + 1L]] <- data.frame(
    batch = mismatches$batch[[i]],
    cohort_name = cohort_name,
    original_status = "batch_mismatch",
    single_status = if (cmp$identical) "single_match" else "single_mismatch",
    single_only_in_old = length(only_old),
    single_only_in_new = length(only_new),
    message = if (cmp$identical) "" else cmp$details,
    stringsAsFactors = FALSE
  )

  if (length(only_old) > 0L) {
    rowdiff_rows[[length(rowdiff_rows) + 1L]] <- data.frame(
      batch = mismatches$batch[[i]],
      cohort_name = cohort_name,
      source = "old_only",
      row_key = utils::head(only_old, 10),
      stringsAsFactors = FALSE
    )
  }
  if (length(only_new) > 0L) {
    rowdiff_rows[[length(rowdiff_rows) + 1L]] <- data.frame(
      batch = mismatches$batch[[i]],
      cohort_name = cohort_name,
      source = "new_only",
      row_key = utils::head(only_new, 10),
      stringsAsFactors = FALSE
    )
  }

  tryCatch(CDMConnector::cdmDisconnect(cdm), error = function(e) NULL)
  unlink(temp_dir, recursive = TRUE)
}

followup_df <- do.call(rbind, followup_rows)
rowdiff_df <- if (length(rowdiff_rows) > 0L) {
  do.call(rbind, rowdiff_rows)
} else {
  data.frame(
    batch = integer(0),
    cohort_name = character(0),
    source = character(0),
    row_key = character(0),
    stringsAsFactors = FALSE
  )
}

followup_path <- file.path(results_dir, paste0("targeted_mismatch_followup_", timestamp, ".csv"))
rowdiff_path <- file.path(results_dir, paste0("targeted_mismatch_rowdiffs_", timestamp, ".csv"))
utils::write.csv(followup_df, followup_path, row.names = FALSE)
utils::write.csv(rowdiff_df, rowdiff_path, row.names = FALSE)
utils::write.csv(followup_df, file.path(results_dir, "targeted_mismatch_followup_latest.csv"), row.names = FALSE)
utils::write.csv(rowdiff_df, file.path(results_dir, "targeted_mismatch_rowdiffs_latest.csv"), row.names = FALSE)

message("Wrote: ", followup_path)
message("Wrote: ", rowdiff_path)
