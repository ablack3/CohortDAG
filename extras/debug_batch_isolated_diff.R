#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(dplyr)
  library(pkgload)
  library(CDMConnector)
})

pkgload::load_all(".", quiet = TRUE)
source("tests/testthat/helper-compare.R")

args <- commandArgs(trailingOnly = TRUE)
batch_id <- as.integer(args[[1]] %||% Sys.getenv("DBG_BATCH", unset = "55"))
cohort_id <- as.integer(args[[2]] %||% Sys.getenv("DBG_COHORT_ID", unset = "8"))
subject_ids_arg <- args[[3]] %||% Sys.getenv("DBG_SUBJECT_IDS", unset = "")
out_dir <- args[[4]] %||% Sys.getenv("DBG_OUT_DIR", unset = tempfile("cohortdag_dbg_"))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

subject_ids <- if (nzchar(subject_ids_arg)) {
  as.integer(strsplit(subject_ids_arg, ",", fixed = TRUE)[[1]])
} else {
  integer(0)
}

assignInNamespace("cohdSimilarConcepts", function(...) NULL, ns = "CDMConnector")

normalize_df <- function(df) {
  if (is.null(df) || !nrow(df)) return(df)
  for (nm in names(df)) {
    if (inherits(df[[nm]], "Date")) df[[nm]] <- as.character(df[[nm]])
  }
  df
}

row_keys <- function(df) {
  if (is.null(df) || !nrow(df)) return(character(0))
  do.call(paste, c(normalize_df(df), sep = "|"))
}

read_batch_cohort_set <- function(batch_id) {
  cohorts_dir <- unzip_cohorts()
  all_files <- sort(list.files(cohorts_dir, pattern = "json$", full.names = TRUE))
  start_idx <- (batch_id - 1L) * 50L + 1L
  end_idx <- min(batch_id * 50L, length(all_files))
  batch_files <- all_files[start_idx:end_idx]
  tmpd <- tempfile(sprintf("dbg_batch_%03d_", batch_id))
  dir.create(tmpd)
  file.copy(batch_files, file.path(tmpd, basename(batch_files)))
  cohort_set <- make_unique_cohort_names(CDMConnector::readCohortSet(tmpd))
  list(
    cohorts_dir = cohorts_dir,
    temp_dir = tmpd,
    cohort_set = cohort_set
  )
}

make_db_cdm <- function(local_cdm) {
  con <- DBI::dbConnect(duckdb::duckdb())
  for (nm in names(local_cdm)) {
    if (inherits(local_cdm[[nm]], "cohort_table")) next
    DBI::dbWriteTable(
      con,
      name = DBI::Id(schema = "main", table = nm),
      value = dplyr::as_tibble(local_cdm[[nm]]),
      overwrite = TRUE
    )
  }
  cdm_db <- CDMConnector::cdmFromCon(
    con = con,
    cdmSchema = "main",
    writeSchema = "main",
    cdmName = omopgenerics::cdmName(local_cdm)
  )
  list(con = con, cdm = cdm_db)
}

build_probe_sql <- function(cohort_set, prefix, cdm_table_sql = NULL) {
  cohort_list <- lapply(cohort_set$cohort, function(x) {
    if (is.character(x) && length(x) == 1L) {
      jsonlite::fromJSON(x, simplifyVector = FALSE)
    } else {
      x
    }
  })

  options <- list(
    cdm_schema = "main",
    results_schema = "main",
    vocabulary_schema = "main",
    table_prefix = prefix,
    cdm_table_sql = cdm_table_sql
  )

  dag <- CohortDAG:::build_execution_dag(
    cohort_list = cohort_list,
    cohort_ids = cohort_set$cohort_definition_id,
    options = options
  )

  sorted <- CohortDAG:::topological_sort(dag$nodes)
  ns <- asNamespace("CohortDAG")
  sql_context <- get(".sql_context", envir = ns)
  old_cs <- sql_context$codesets_table
  sql_context$codesets_table <- CohortDAG:::qualify_table("codesets", options)
  on.exit(sql_context$codesets_table <- old_cs, add = TRUE)

  parts <- list(
    CohortDAG:::emit_preamble(options),
    CohortDAG:::emit_codesets(dag, options),
    CohortDAG:::emit_domain_filtered(dag$used_tables, "main", options, dag$unfiltered_tables),
    CohortDAG:::emit_analyze_hints(dag$used_tables, options)
  )

  for (node_id in sorted) {
    node <- dag$nodes[[node_id]]
    if (node$type == "concept_set") next
    sql <- CohortDAG:::emit_node_sql(node, dag, options)
    if (nzchar(sql)) {
      parts[[length(parts) + 1L]] <- c(
        sprintf("-- [%s] %s", node$type, substr(node$id, 1, 8)),
        sql,
        ""
      )
    }
  }

  sql <- paste(unlist(parts), collapse = "\n")
  sql <- stringi::stri_replace_all_fixed(sql, "@vocabulary_database_schema", "main")
  sql <- stringi::stri_replace_all_fixed(sql, "@target_database_schema", "main")
  sql <- stringi::stri_replace_all_fixed(sql, "@target_cohort_table", paste0(prefix, "output"))
  sql <- stringi::stri_replace_all_fixed(sql, "@cdm_database_schema", "main")
  sql <- stringi::stri_replace_all_fixed(sql, "@results_schema", "main")

  list(sql = sql, dag = dag, options = options)
}

execute_probe <- function(cdm_db, probe) {
  sql_split <- stringi::stri_split_regex(probe$sql, ";[ \t]*\r?\n")[[1]]
  sql_split <- trimws(sql_split)
  sql_split <- sql_split[nzchar(sql_split)]
  sql_split <- sql_split[grepl("(CREATE|DROP|INSERT|SELECT|DELETE|UPDATE|WITH|TRUNCATE|ANALYZE)",
                               sql_split, ignore.case = TRUE)]
  for (i in seq_along(sql_split)) {
    sql_split[[i]] <- CohortDAG:::resolve_literal_conditionals(sql_split[[i]])
  }
  translated <- CohortDAG:::translate_cohort_stmts(sql_split, "duckdb")
  translated <- translated[nzchar(translated)]
  for (stmt in translated) {
    DBI::dbExecute(cdmCon(cdm_db), stmt)
  }
}

table_data_for_cohort <- function(cdm_db, probe, cohort_id, subject_ids = integer(0)) {
  node_ids <- names(probe$dag$nodes)[vapply(
    probe$dag$nodes,
    function(n) cohort_id %in% n$cohort_ids,
    logical(1)
  )]
  out <- list()
  for (nid in node_ids) {
    node <- probe$dag$nodes[[nid]]
    if (node$type %in% c("concept_set", "final_cohort")) next
    qtbl <- CohortDAG:::qualify_table(node$temp_table, probe$options)
    df <- DBI::dbGetQuery(cdmCon(cdm_db), paste0("SELECT * FROM ", qtbl))
    if (length(subject_ids) && "person_id" %in% names(df)) {
      df <- df[df$person_id %in% subject_ids, , drop = FALSE]
    }
    if (nrow(df)) {
      df <- df[do.call(order, c(as.list(df), na.last = TRUE)), , drop = FALSE]
    }
    out[[node$type]] <- normalize_df(df)
  }

  cohort_stage_tbl <- CohortDAG:::qualify_table("cohort_stage", probe$options)
  stage_df <- DBI::dbGetQuery(cdmCon(cdm_db), paste0("SELECT * FROM ", cohort_stage_tbl))
  stage_df <- stage_df[stage_df$cohort_definition_id == cohort_id, , drop = FALSE]
  if (length(subject_ids)) {
    stage_df <- stage_df[stage_df$subject_id %in% subject_ids, , drop = FALSE]
  }
  if (nrow(stage_df)) {
    stage_df <- stage_df[do.call(order, c(as.list(stage_df), na.last = TRUE)), , drop = FALSE]
  }
  out$cohort_stage <- normalize_df(stage_df)
  out
}

diff_tables <- function(full_data, iso_data) {
  table_names <- union(names(full_data), names(iso_data))
  do.call(rbind, lapply(table_names, function(tbl) {
    full_df <- full_data[[tbl]] %||% data.frame()
    iso_df <- iso_data[[tbl]] %||% data.frame()
    full_keys <- row_keys(full_df)
    iso_keys <- row_keys(iso_df)
    data.frame(
      table_name = tbl,
      rows_full = nrow(full_df),
      rows_isolated = nrow(iso_df),
      only_in_full = sum(!full_keys %in% iso_keys),
      only_in_isolated = sum(!iso_keys %in% full_keys),
      identical = identical(full_keys, iso_keys),
      stringsAsFactors = FALSE
    )
  }))
}

batch_info <- read_batch_cohort_set(batch_id)
on.exit(unlink(batch_info$cohorts_dir, recursive = TRUE), add = TRUE)
on.exit(unlink(batch_info$temp_dir, recursive = TRUE), add = TRUE)
cohort_set <- batch_info$cohort_set
subset_set <- cohort_set[cohort_set$cohort_definition_id == cohort_id, , drop = FALSE]
if (!nrow(subset_set)) stop("cohort_id not present in batch: ", cohort_id)

mock_cdm <- CDMConnector::cdmFromCohortSet(cohort_set, n = 500)
on.exit(tryCatch(CDMConnector::cdmDisconnect(mock_cdm), error = function(e) NULL), add = TRUE)

full_db <- make_db_cdm(mock_cdm)
on.exit(tryCatch(DBI::dbDisconnect(full_db$con, shutdown = TRUE), error = function(e) NULL), add = TRUE)
iso_db <- make_db_cdm(mock_cdm)
on.exit(tryCatch(DBI::dbDisconnect(iso_db$con, shutdown = TRUE), error = function(e) NULL), add = TRUE)

full_probe <- build_probe_sql(cohort_set, prefix = "dbgfull_")
iso_probe <- build_probe_sql(subset_set, prefix = "dbgiso_")

execute_probe(full_db$cdm, full_probe)
execute_probe(iso_db$cdm, iso_probe)

full_data <- table_data_for_cohort(full_db$cdm, full_probe, cohort_id, subject_ids)
iso_data <- table_data_for_cohort(iso_db$cdm, iso_probe, cohort_id, subject_ids)
summary_df <- diff_tables(full_data, iso_data)

write.csv(summary_df, file.path(out_dir, "summary.csv"), row.names = FALSE)

for (nm in names(full_data)) {
  utils::write.csv(full_data[[nm]], file.path(out_dir, sprintf("full_%s.csv", nm)), row.names = FALSE)
}
for (nm in names(iso_data)) {
  utils::write.csv(iso_data[[nm]], file.path(out_dir, sprintf("isolated_%s.csv", nm)), row.names = FALSE)
}

print(summary_df)
cat("Wrote debug artifacts to:", out_dir, "\n")
