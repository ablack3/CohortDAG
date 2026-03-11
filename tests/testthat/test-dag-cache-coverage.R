# Tests for R/dag_cache.R
# Uses in-memory DuckDB for registry operations.

# ---- Constants ----

test_that("CACHE_TABLE_PREFIX is a fixed stable string", {
  expect_true(is.character(CACHE_TABLE_PREFIX))
  expect_equal(CACHE_TABLE_PREFIX, "dagcache_")
})

test_that("CACHE_REGISTRY_TABLE is a fixed string", {
  expect_true(is.character(CACHE_REGISTRY_TABLE))
  expect_equal(CACHE_REGISTRY_TABLE, "dag_cache_registry")
})

# ---- cache_registry_qname ----

test_that("cache_registry_qname builds qualified name", {
  result <- cache_registry_qname("myschema")
  expect_equal(result, "myschema.dag_cache_registry")
})

test_that("cache_registry_qname includes write_prefix", {
  result <- cache_registry_qname("myschema", write_prefix = "pfx_")
  expect_equal(result, "myschema.pfx_dag_cache_registry")
})

test_that("cache_registry_qname with empty prefix", {
  result <- cache_registry_qname("rs", write_prefix = "")
  expect_equal(result, "rs.dag_cache_registry")
})

# ---- cache_registry_ddl ----

test_that("cache_registry_ddl produces valid SQL (default dialect)", {
  ddl <- cache_registry_ddl("myschema")
  full <- paste(ddl, collapse = "\n")
  expect_true(grepl("CREATE TABLE IF NOT EXISTS", full))
  expect_true(grepl("myschema.dag_cache_registry", full, fixed = TRUE))
  expect_true(grepl("node_hash", full))
  expect_true(grepl("node_type", full))
  expect_true(grepl("table_name", full))
  expect_true(grepl("PRIMARY KEY", full))
  expect_true(grepl("created_at", full))
  expect_true(grepl("last_used_at", full))
  expect_true(grepl("cohort_ids", full))
})

test_that("cache_registry_ddl includes write_prefix", {
  ddl <- cache_registry_ddl("myschema", write_prefix = "pfx_")
  full <- paste(ddl, collapse = "\n")
  expect_true(grepl("myschema.pfx_dag_cache_registry", full, fixed = TRUE))
})

# ---- cache_table_name ----

test_that("cache_table_name builds correct name", {
  node <- list(type = "primary_events", id = "aabbccdd11223344", temp_table = "pe_aabbccdd")
  result <- cache_table_name(node, "rs")
  expect_equal(result, "rs.dagcache_pe_aabbccdd")
})

test_that("cache_table_name with write_prefix", {
  node <- list(type = "primary_events", id = "abc", temp_table = "pe_abc")
  result <- cache_table_name(node, "rs", write_prefix = "pfx_")
  expect_equal(result, "rs.pfx_dagcache_pe_abc")
})

# ---- DuckDB integration tests ----

test_that("ensure_cache_registry creates registry table", {
  skip_if_not_installed("duckdb")
  skip_if_not_installed("DBI")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS test_schema")
  ensure_cache_registry(con, "test_schema")

  tables <- DBI::dbListTables(con)
  expect_true("dag_cache_registry" %in% tolower(tables))
})

test_that("ensure_cache_registry is idempotent", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS ts")
  expect_no_error(ensure_cache_registry(con, "ts"))
  expect_no_error(ensure_cache_registry(con, "ts"))
})

test_that("cache_lookup returns FALSE for empty registry", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS ts")
  ensure_cache_registry(con, "ts")

  result <- cache_lookup(con, "ts", c("abc123", "def456"))
  expect_equal(length(result), 2L)
  expect_false(result[["abc123"]])
  expect_false(result[["def456"]])
})

test_that("cache_lookup with empty hashes returns empty", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- cache_lookup(con, "ts", character(0))
  expect_equal(length(result), 0L)
})

test_that("cache_register and cache_lookup round-trip", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS ts")
  ensure_cache_registry(con, "ts")

  cache_register(con, "ts", "aabbccdd11223344", "primary_events",
                 "ts.dagcache_pe_aabbccdd", c(1L, 2L))

  result <- cache_lookup(con, "ts", c("aabbccdd11223344", "xxxxxxxxxxxxxxxx"))
  expect_true(result[["aabbccdd11223344"]])
  expect_false(result[["xxxxxxxxxxxxxxxx"]])
})

test_that("cache_register handles duplicate (upsert)", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS ts")
  ensure_cache_registry(con, "ts")

  cache_register(con, "ts", "aabbccdd11223344", "primary_events", "ts.dagcache_pe_aabb", 1L)
  # Second register should update, not error
  expect_no_error(
    cache_register(con, "ts", "aabbccdd11223344", "primary_events", "ts.dagcache_pe_aabb", c(1L, 2L))
  )
})

test_that("cache_validate checks table existence", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS ts")
  ensure_cache_registry(con, "ts")

  # Register a node with a real backing table
  DBI::dbExecute(con, "CREATE TABLE ts.dagcache_pe_aabbccdd (x INT)")
  cache_register(con, "ts", "aabbccdd11223344", "primary_events",
                 "ts.dagcache_pe_aabbccdd", 1L)

  # Register a node whose table does NOT exist
  cache_register(con, "ts", "1111111122222222", "qualified_events",
                 "ts.dagcache_qe_11111111", 1L)

  valid <- cache_validate(con, "ts", c("aabbccdd11223344", "1111111122222222"))
  expect_true(valid[["aabbccdd11223344"]])
  expect_false(valid[["1111111122222222"]])
})

test_that("cache_validate with empty hashes returns empty", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  result <- cache_validate(con, "ts", character(0))
  expect_equal(length(result), 0L)
})

test_that("cache_touch updates timestamp without error", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS ts")
  ensure_cache_registry(con, "ts")
  cache_register(con, "ts", "hash1234567890ab", "primary_events", "ts.dagcache_pe_hash", 1L)
  expect_no_error(cache_touch(con, "ts", "hash1234567890ab"))
})

test_that("cache_touch with empty hashes does nothing", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  expect_no_error(cache_touch(con, "ts", character(0)))
})

# ---- dag_cache_list ----

test_that("dag_cache_list returns all entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  cache_register(con, "rs", "stat111111111111", "primary_events", "rs.dagcache_pe_stat1", 1L)
  cache_register(con, "rs", "stat222222222222", "qualified_events", "rs.dagcache_qe_stat2", 2L)

  result <- dag_cache_list(con, "rs")
  expect_equal(nrow(result), 2L)
  expect_true("node_hash" %in% names(result))
  expect_true("node_type" %in% names(result))
})

test_that("dag_cache_list returns empty data frame for empty cache", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  result <- dag_cache_list(con, "rs")
  expect_equal(nrow(result), 0L)
})

# ---- dag_cache_gc ----

test_that("dag_cache_gc removes orphaned entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  # Register a node with no backing table (orphaned)
  cache_register(con, "rs", "orphan1234567890", "primary_events",
                 "rs.dagcache_pe_orphan12", 1L)

  # Register a node with a real backing table
  DBI::dbExecute(con, "CREATE TABLE rs.dagcache_pe_real1234 (x INT)")
  cache_register(con, "rs", "real123456789012", "primary_events",
                 "rs.dagcache_pe_real1234", 1L)

  # GC with max_age_days = 0 (collect everything)
  removed <- dag_cache_gc(con, "rs", max_age_days = 0, dry_run = FALSE)
  expect_equal(nrow(removed), 2L)

  # Verify registry is empty
  registry <- dag_cache_list(con, "rs")
  expect_equal(nrow(registry), 0L)
})

test_that("dag_cache_gc dry_run does not delete", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  cache_register(con, "rs", "dry1234567890ab", "primary_events", "rs.dagcache_pe_dry1", 1L)

  removed <- dag_cache_gc(con, "rs", max_age_days = 0, dry_run = TRUE)
  expect_equal(nrow(removed), 1L)

  # Entry should still exist
  registry <- dag_cache_list(con, "rs")
  expect_equal(nrow(registry), 1L)
})

test_that("dag_cache_gc with empty cache returns empty", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  removed <- dag_cache_gc(con, "rs", max_age_days = 0, dry_run = FALSE)
  expect_equal(nrow(removed), 0L)
})

# ---- dag_cache_clear ----

test_that("dag_cache_clear removes all entries", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  DBI::dbExecute(con, "CREATE TABLE rs.dagcache_pe_test1 (x INT)")
  cache_register(con, "rs", "test111111111111", "primary_events", "rs.dagcache_pe_test1", 1L)
  DBI::dbExecute(con, "CREATE TABLE rs.dagcache_qe_test2 (x INT)")
  cache_register(con, "rs", "test222222222222", "qualified_events", "rs.dagcache_qe_test2", 1L)

  dag_cache_clear(con, "rs")
  registry <- dag_cache_list(con, "rs")
  expect_equal(nrow(registry), 0L)
})

test_that("dag_cache_clear on empty cache does not error", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  expect_no_error(dag_cache_clear(con, "rs"))
})

# ---- dag_cache_stats ----

test_that("dag_cache_stats returns correct summary", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  cache_register(con, "rs", "stat111111111111", "primary_events", "rs.dagcache_pe_stat1", 1L)
  cache_register(con, "rs", "stat222222222222", "primary_events", "rs.dagcache_pe_stat2", 2L)
  cache_register(con, "rs", "stat333333333333", "qualified_events", "rs.dagcache_qe_stat3", 1L)

  stats <- dag_cache_stats(con, "rs")
  expect_equal(stats$total_entries, 3L)
  expect_equal(stats$by_type[["primary_events"]], 2L)
  expect_equal(stats$by_type[["qualified_events"]], 1L)
  expect_false(is.na(stats$oldest))
  expect_false(is.na(stats$newest))
})

test_that("dag_cache_stats with empty cache", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  stats <- dag_cache_stats(con, "rs")
  expect_equal(stats$total_entries, 0L)
  expect_equal(length(stats$by_type), 0L)
  expect_true(is.na(stats$oldest))
})

# ---- cache_register_new_nodes ----

test_that("cache_register_new_nodes skips concept_set and final_cohort", {
  skip_if_not_installed("duckdb")
  con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS rs")
  ensure_cache_registry(con, "rs")

  dag <- list(nodes = list(
    "h1" = list(id = "h1", type = "concept_set", temp_table = "cs_h1", cohort_ids = 1L),
    "h2" = list(id = "h2", type = "primary_events", temp_table = "pe_h2", cohort_ids = 1L),
    "h3" = list(id = "h3", type = "final_cohort", temp_table = "fc_h3", cohort_ids = 1L)
  ))
  options <- list(table_prefix = "dagcache_", results_schema = "rs")

  cache_register_new_nodes(dag, options, con, "rs", c("h1", "h2", "h3"))

  registry <- dag_cache_list(con, "rs")
  # Only h2 (primary_events) should be registered; h1 (concept_set) and h3 (final_cohort) skipped
  expect_equal(nrow(registry), 1L)
  expect_equal(registry$node_type[1], "primary_events")
})
