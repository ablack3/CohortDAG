#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(DBI)
  library(odbc)
  library(pkgload)
  library(SqlRender)
  library(CDMConnector)
})

pkgload::load_all(".", quiet = TRUE)

split_schema <- function(s, database) {
  parts <- strsplit(s, "\\.")[[1L]]
  if (length(parts) == 1L) {
    return(c(catalog = database, schema = parts[1L]))
  }
  if (length(parts) == 2L) {
    return(c(catalog = parts[1L], schema = parts[2L]))
  }
  c(catalog = parts[1L], schema = parts[2L], prefix = parts[3L])
}

normalize_schema_str_local <- function(x) {
  CohortDAG:::normalize_schema_str(x)
}

extract_one <- function(pattern, text) {
  m <- regexec(pattern, text, perl = TRUE)
  hit <- regmatches(text, m)[[1L]]
  if (length(hit) < 2L) return(NA_character_)
  hit[[2L]]
}

server <- Sys.getenv("SNOWFLAKE_SERVER", unset = "")
user <- Sys.getenv("SNOWFLAKE_USER", unset = "")
password <- Sys.getenv("SNOWFLAKE_PASSWORD", unset = "")
database <- Sys.getenv("SNOWFLAKE_DATABASE", unset = "")
warehouse <- Sys.getenv("SNOWFLAKE_WAREHOUSE", unset = "")
driver <- Sys.getenv("SNOWFLAKE_DRIVER", unset = "")
scratch <- Sys.getenv("SNOWFLAKE_SCRATCH_SCHEMA", unset = "")
debug_dir <- Sys.getenv(
  "SNOWFLAKE_DEBUG_DIR",
  unset = file.path("inst", "benchmark-results", "live-db", "snowflake_targeted_debug_20260315_195428")
)
cohort_id <- as.integer(Sys.getenv("SNOWFLAKE_TIE_COHORT_ID", unset = "27"))
person_ids <- as.integer(strsplit(Sys.getenv("SNOWFLAKE_TIE_PERSON_IDS", unset = "100580,54123,11853"), ",", fixed = TRUE)[[1L]])

if (any(c(server, user, password, database, warehouse, driver, scratch) == "")) {
  stop("Snowflake env vars missing")
}

circe_path <- file.path(debug_dir, sprintf("cohort_%03d_circe.sql", cohort_id))
dag_path <- file.path(debug_dir, sprintf("cohort_%03d_dag.sql", cohort_id))
if (!file.exists(circe_path) || !file.exists(dag_path)) {
  stop("SQL files not found for cohort ", cohort_id, " in ", debug_dir)
}

circe_sql <- paste(readLines(circe_path, warn = FALSE), collapse = "\n")
dag_sql <- paste(readLines(dag_path, warn = FALSE), collapse = "\n")

circe_qe <- extract_one("CREATE TEMP TABLE\\s+([^\\s]+qualified_events)", circe_sql)
circe_ie <- extract_one("CREATE TEMP TABLE\\s+([^\\s]+included_events)", circe_sql)
circe_ce <- extract_one("CREATE TEMP TABLE\\s+([^\\s]+strategy_ends)", circe_sql)
circe_cr <- extract_one("CREATE TEMP TABLE\\s+([^\\s]+cohort_rows)", circe_sql)
dag_qe <- extract_one("CREATE TABLE\\s+([^\\s]+_qe_[A-Za-z0-9]+)", dag_sql)
dag_ie <- extract_one("CREATE TABLE\\s+([^\\s]+_ie_[A-Za-z0-9]+)", dag_sql)
dag_ce <- extract_one("CREATE TABLE\\s+([^\\s]+_ce_[A-Za-z0-9]+)", dag_sql)
dag_prefix <- extract_one("(atlas_[a-f0-9]+)_", dag_sql)

con <- dbConnect(
  odbc::odbc(),
  server = server,
  uid = user,
  pwd = password,
  database = database,
  warehouse = warehouse,
  driver = driver
)
on.exit(try(dbDisconnect(con), silent = TRUE), add = TRUE)

scratch_spec <- split_schema(scratch, database)
cdm_schema_spec <- split_schema(Sys.getenv("SNOWFLAKE_CDM_SCHEMA", unset = "CDM"), database)
cdm_schema_str <- normalize_schema_str_local(cdm_schema_spec)
write_schema_str <- normalize_schema_str_local(scratch_spec)
if (!is.null(scratch_spec[["catalog"]]) && nzchar(scratch_spec[["catalog"]])) {
  DBI::dbExecute(con, sprintf("USE DATABASE %s", scratch_spec[["catalog"]]))
}
if (!is.null(scratch_spec[["schema"]]) && nzchar(scratch_spec[["schema"]])) {
  DBI::dbExecute(con, sprintf("USE SCHEMA %s", scratch_spec[["schema"]]))
}

dag_sql <- SqlRender::render(
  dag_sql,
  vocabulary_database_schema = cdm_schema_str,
  target_database_schema = write_schema_str,
  target_cohort_table = paste0("debug_target_", cohort_id)
)
dag_target_delete <- sprintf(
  "DELETE FROM %s.%s WHERE cohort_definition_id IN (%d)",
  write_schema_str,
  paste0("debug_target_", cohort_id),
  cohort_id
)

run_until <- function(sql_text, stop_patterns) {
  stmts <- SqlRender::splitSql(sql_text)
  for (stmt in stmts) {
    stmt_trim <- trimws(stmt)
    if (!nzchar(stmt_trim)) next
    if (grepl("ANALYZE ", stmt_trim, fixed = TRUE)) next
    if (any(vapply(stop_patterns, function(p) nzchar(p) && grepl(p, stmt_trim, fixed = TRUE), logical(1)))) {
      break
    }
    DBI::dbExecute(con, stmt_trim)
  }
}

query_tbl <- function(tbl) {
  if (is.na(tbl) || !nzchar(tbl)) return(data.frame())
  sql <- sprintf(
    "SELECT * FROM %s WHERE person_id IN (%s) ORDER BY person_id, start_date, end_date, event_id",
    tbl,
    paste(person_ids, collapse = ",")
  )
  as.data.frame(DBI::dbGetQuery(con, sql))
}

query_ce <- function(tbl) {
  if (is.na(tbl) || !nzchar(tbl)) return(data.frame())
  sql <- sprintf(
    "SELECT * FROM %s WHERE person_id IN (%s) ORDER BY person_id, end_date, event_id",
    tbl,
    paste(person_ids, collapse = ",")
  )
  as.data.frame(DBI::dbGetQuery(con, sql))
}

out_dir <- file.path(debug_dir, sprintf("intermediates_%03d", cohort_id))
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

run_until(circe_sql, c(circe_cr, "-- generate cohort periods into #final_cohort"))
utils::write.csv(query_tbl(circe_qe), file.path(out_dir, "circe_qualified_events.csv"), row.names = FALSE)
utils::write.csv(query_tbl(circe_ie), file.path(out_dir, "circe_included_events.csv"), row.names = FALSE)
utils::write.csv(query_ce(circe_ce), file.path(out_dir, "circe_strategy_ends.csv"), row.names = FALSE)

run_until(dag_sql, c(dag_target_delete))
utils::write.csv(query_tbl(dag_qe), file.path(out_dir, "dag_qualified_events.csv"), row.names = FALSE)
utils::write.csv(query_tbl(dag_ie), file.path(out_dir, "dag_included_events.csv"), row.names = FALSE)
utils::write.csv(query_ce(dag_ce), file.path(out_dir, "dag_cohort_exit.csv"), row.names = FALSE)

if (!is.na(dag_prefix) && nzchar(dag_prefix)) {
  scratch_schema <- normalize_schema_str_local(scratch_spec)
  tables <- tryCatch(CDMConnector::listTables(con, schema = scratch_schema), error = function(e) character())
  to_drop <- tables[startsWith(tables, dag_prefix)]
  if (length(to_drop) > 0L) {
    dbms <- dbplyr::dbms(con)
    for (tbl_name in to_drop) {
      stripped <- sub(paste0("^", dag_prefix, "_?"), "", tbl_name)
      tryCatch(
        DBI::dbRemoveTable(con, CDMConnector:::.inSchema(scratch_spec, stripped, dbms = dbms)),
        error = function(e) NULL
      )
    }
  }
}

message("Wrote intermediate rows to: ", out_dir)
