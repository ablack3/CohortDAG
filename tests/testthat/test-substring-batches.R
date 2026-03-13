skip_if_not_tier(2L)

test_that("substring-matched cohort batches generate correctly", {
  skip_if_not_installed("CDMConnector")
  skip_if_not_installed("duckdb")
  library(CDMConnector)

  cohorts_dir <- unzip_cohorts()
  on.exit(unlink(cohorts_dir, recursive = TRUE), add = TRUE)
  all_files <- list.files(cohorts_dir, pattern = "\\.json$")
  all_names <- tools::file_path_sans_ext(all_files)

  # Find common substrings (at least 4 chars, appearing in 3+ cohort names)
  find_common_substrings <- function(names, min_len = 4, min_count = 3, max_substrings = 20) {
    # Extract words/tokens from underscore-separated names
    tokens <- unique(unlist(strsplit(names, "[_\\s]+")))
    tokens <- tokens[nchar(tokens) >= min_len]

    # Count how many names contain each token
    counts <- vapply(tokens, function(tok) {
      sum(grepl(tok, names, fixed = TRUE, ignore.case = TRUE))
    }, integer(1))

    tokens <- tokens[counts >= min_count]
    counts <- counts[tokens]

    # Sort by count descending, take top N
    ord <- order(counts, decreasing = TRUE)
    head(tokens[ord], max_substrings)
  }

  substrings <- find_common_substrings(all_names)
  if (length(substrings) == 0) skip("No common substrings found")

  # Test each substring batch (limit to first 10 for time)
  substrings <- head(substrings, 10)

  results <- data.frame(
    substring = character(),
    n_cohorts = integer(),
    time_sec = numeric(),
    success = logical(),
    stringsAsFactors = FALSE
  )

  for (substr_token in substrings) {
    matching <- all_files[grepl(substr_token, all_names, fixed = TRUE, ignore.case = TRUE)]
    matching <- head(matching, 30)  # cap at 30 per group

    temp_dir <- tempfile("substr_cohorts")
    dir.create(temp_dir, recursive = TRUE)
    for (f in matching) file.copy(file.path(cohorts_dir, f), file.path(temp_dir, f))

    cohort_set <- tryCatch(CDMConnector::readCohortSet(temp_dir), error = function(e) NULL)
    if (is.null(cohort_set) || nrow(cohort_set) == 0) {
      unlink(temp_dir, recursive = TRUE)
      next
    }

    cdm <- tryCatch(CDMConnector::cdmFromCohortSet(cohort_set, n = 20), error = function(e) NULL)
    if (is.null(cdm)) {
      unlink(temp_dir, recursive = TRUE)
      next
    }

    t0 <- Sys.time()
    err <- tryCatch({
      cdm <- CohortDAG::generateCohortSet2(cdm, cohort_set, name = "cohort")
      NULL
    }, error = function(e) conditionMessage(e))
    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs"))

    results <- rbind(results, data.frame(
      substring = substr_token,
      n_cohorts = nrow(cohort_set),
      time_sec = elapsed,
      success = is.null(err),
      stringsAsFactors = FALSE
    ))

    tryCatch(CDMConnector::cdmDisconnect(cdm), error = function(e) NULL)
    unlink(temp_dir, recursive = TRUE)

    message(sprintf("Substring '%s': %d cohorts, %.1fs, %s",
                    substr_token, nrow(cohort_set), elapsed,
                    if (is.null(err)) "OK" else paste("FAIL:", err)))
  }

  # Save results
  results_dir <- file.path(tempdir(), "cohortdag_benchmark")
  dir.create(results_dir, showWarnings = FALSE, recursive = TRUE)
  utils::write.csv(results, file.path(results_dir, "substring_batch_results.csv"), row.names = FALSE)

  failed <- results[!results$success, ]
  expect_equal(nrow(failed), 0L,
               info = paste("Failed substrings:", paste(failed$substring, collapse = ", ")))
})
