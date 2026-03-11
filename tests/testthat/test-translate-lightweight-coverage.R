# Tests for R/translate_lightweight.R
# Pure R string transformation tests - no database needed.

# ---- .extract_func_args ----

test_that(".extract_func_args extracts simple function arguments", {
  sql <- "DATEADD(day, 5, start_date)"
  result <- .extract_func_args(sql, 8L)  # position of '('
  expect_equal(result$args, c("day", "5", "start_date"))
  expect_equal(result$close_pos, nchar(sql))
})

test_that(".extract_func_args handles nested parentheses", {
  sql <- "DATEADD(day, CAST(x AS INT), start_date)"
  result <- .extract_func_args(sql, 8L)
  expect_equal(length(result$args), 3L)
  expect_equal(result$args[1], "day")
  expect_true(grepl("CAST", result$args[2]))
  expect_equal(result$args[3], "start_date")
})

test_that(".extract_func_args handles quoted strings with commas", {
  sql <- "FUNC('a,b', 'c')"
  result <- .extract_func_args(sql, 5L)
  expect_equal(length(result$args), 2L)
  expect_equal(result$args[1], "'a,b'")
  expect_equal(result$args[2], "'c'")
})

test_that(".extract_func_args handles double-quoted strings", {
  sql <- 'FUNC("col,name", val)'
  result <- .extract_func_args(sql, 5L)
  expect_equal(length(result$args), 2L)
})

test_that(".extract_func_args handles unbalanced parens gracefully", {
  sql <- "FUNC(a, b"
  result <- .extract_func_args(sql, 5L)
  expect_true(length(result$args) >= 1L)
})

# ---- .rewrite_func ----

test_that(".rewrite_func replaces function calls", {
  sql <- "SELECT ISNULL(a, b) FROM t"
  result <- .rewrite_func(sql, "ISNULL", .isnull_to_coalesce)
  expect_true(grepl("COALESCE", result))
  expect_false(grepl("ISNULL", result))
})

test_that(".rewrite_func handles multiple occurrences", {
  sql <- "SELECT ISNULL(a, b), ISNULL(c, d) FROM t"
  result <- .rewrite_func(sql, "ISNULL", .isnull_to_coalesce)
  expect_equal(length(gregexpr("COALESCE", result)[[1]]), 2L)
})

test_that(".rewrite_func is case insensitive", {
  sql <- "SELECT isnull(a, b) FROM t"
  result <- .rewrite_func(sql, "ISNULL", .isnull_to_coalesce)
  expect_true(grepl("COALESCE", result))
})

# ---- DuckDB transformers ----

test_that(".dateadd_duckdb converts day unit", {
  result <- .dateadd_duckdb(c("day", "5", "start_date"))
  expect_true(grepl("TO_DAYS", result))
  expect_true(grepl("CAST\\(5 AS INTEGER\\)", result))
  expect_true(grepl("start_date", result))
})

test_that(".dateadd_duckdb converts month unit", {
  result <- .dateadd_duckdb(c("month", "3", "d"))
  expect_true(grepl("TO_MONTHS", result))
})

test_that(".dateadd_duckdb converts year unit", {
  result <- .dateadd_duckdb(c("year", "1", "d"))
  expect_true(grepl("TO_YEARS", result))
})

test_that(".dateadd_duckdb handles abbreviations dd, mm, yy, yyyy", {
  expect_true(grepl("TO_DAYS", .dateadd_duckdb(c("dd", "1", "d"))))
  expect_true(grepl("TO_DAYS", .dateadd_duckdb(c("d", "1", "d"))))
  expect_true(grepl("TO_MONTHS", .dateadd_duckdb(c("mm", "1", "d"))))
  expect_true(grepl("TO_MONTHS", .dateadd_duckdb(c("m", "1", "d"))))
  expect_true(grepl("TO_YEARS", .dateadd_duckdb(c("yy", "1", "d"))))
  expect_true(grepl("TO_YEARS", .dateadd_duckdb(c("yyyy", "1", "d"))))
})

test_that(".dateadd_duckdb handles time units", {
  expect_true(grepl("TO_SECONDS", .dateadd_duckdb(c("second", "1", "d"))))
  expect_true(grepl("TO_MINUTES", .dateadd_duckdb(c("minute", "1", "d"))))
  expect_true(grepl("TO_HOURS", .dateadd_duckdb(c("hour", "1", "d"))))
})

test_that(".datediff_duckdb converts day unit", {
  result <- .datediff_duckdb(c("day", "s", "e"))
  expect_true(grepl("CAST\\(e AS DATE\\)", result))
  expect_true(grepl("CAST\\(s AS DATE\\)", result))
})

test_that(".datediff_duckdb converts year unit", {
  result <- .datediff_duckdb(c("year", "s", "e"))
  expect_true(grepl("EXTRACT\\(YEAR", result))
})

test_that(".datediff_duckdb converts month unit", {
  result <- .datediff_duckdb(c("month", "s", "e"))
  expect_true(grepl("extract\\(year from age", result))
  expect_true(grepl("extract\\(month from age", result))
})

test_that(".datediff_duckdb abbreviations work", {
  expect_true(grepl("CAST", .datediff_duckdb(c("d", "s", "e"))))
  expect_true(grepl("CAST", .datediff_duckdb(c("dd", "s", "e"))))
  expect_true(grepl("EXTRACT", .datediff_duckdb(c("yy", "s", "e"))))
  expect_true(grepl("EXTRACT", .datediff_duckdb(c("yyyy", "s", "e"))))
  expect_true(grepl("age", .datediff_duckdb(c("m", "s", "e"))))
  expect_true(grepl("age", .datediff_duckdb(c("mm", "s", "e"))))
})

test_that(".datefromparts_duckdb builds string concat", {
  result <- .datefromparts_duckdb(c("2020", "1", "15"))
  expect_true(grepl(":: DATE", result))
  expect_true(grepl("CAST\\(2020 AS VARCHAR\\)", result))
})

test_that(".eomonth_duckdb uses DATE_TRUNC", {
  result <- .eomonth_duckdb(c("start_date"))
  expect_true(grepl("DATE_TRUNC", result))
  expect_true(grepl("INTERVAL '1 MONTH'", result))
  expect_true(grepl("INTERVAL '1 day'", result))
})

# ---- PostgreSQL transformers ----

test_that(".dateadd_postgresql uses interval arithmetic", {
  result <- .dateadd_postgresql(c("day", "5", "start_date"))
  expect_true(grepl("INTERVAL '1 day'", result))
  expect_true(grepl("CAST\\(start_date AS DATE\\)", result))
})

test_that(".dateadd_postgresql handles month and year", {
  expect_true(grepl("INTERVAL '1 month'", .dateadd_postgresql(c("month", "1", "d"))))
  expect_true(grepl("INTERVAL '1 year'", .dateadd_postgresql(c("year", "1", "d"))))
})

test_that(".datediff_postgresql is same as duckdb", {
  expect_identical(.datediff_postgresql, .datediff_duckdb)
})

test_that(".datefromparts_postgresql uses make_date", {
  result <- .datefromparts_postgresql(c("2020", "1", "15"))
  expect_true(grepl("make_date", result))
  expect_true(grepl("CAST\\(2020 AS INTEGER\\)", result))
})

test_that(".eomonth_postgresql is same as duckdb", {
  expect_identical(.eomonth_postgresql, .eomonth_duckdb)
})

# ---- Shared transformers ----

test_that(".isnull_to_coalesce converts correctly", {
  result <- .isnull_to_coalesce(c("a", "b"))
  expect_equal(result, "COALESCE(a, b)")
})

test_that(".iif_to_case converts correctly", {
  result <- .iif_to_case(c("x > 0", "1", "0"))
  expect_equal(result, "CASE WHEN x > 0 THEN 1 ELSE 0 END")
})

# ---- .rewrite_functions_vec ----

test_that(".rewrite_functions_vec handles mixed function statements for DuckDB", {
  stmts <- c(
    "SELECT DATEADD(day, 5, start_date) FROM t",
    "SELECT DATEDIFF(day, a, b) FROM t",
    "SELECT ISNULL(x, 0) FROM t",
    "SELECT col FROM t"
  )
  result <- .rewrite_functions_vec(stmts, "duckdb")
  expect_true(grepl("TO_DAYS", result[1]))
  expect_true(grepl("CAST", result[2]))
  expect_true(grepl("COALESCE", result[3]))
  expect_equal(result[4], stmts[4])
})

test_that(".rewrite_functions_vec handles DATEFROMPARTS", {
  stmts <- c("SELECT DATEFROMPARTS(2020, 1, 15)")
  result <- .rewrite_functions_vec(stmts, "duckdb")
  expect_true(grepl(":: DATE", result[1]))
})

test_that(".rewrite_functions_vec handles EOMONTH", {
  stmts <- c("SELECT EOMONTH(start_date)")
  result <- .rewrite_functions_vec(stmts, "duckdb")
  expect_true(grepl("DATE_TRUNC", result[1]))
})

test_that(".rewrite_functions_vec handles IIF", {
  stmts <- c("SELECT IIF(x > 0, 1, 0)")
  result <- .rewrite_functions_vec(stmts, "duckdb")
  expect_true(grepl("CASE WHEN", result[1]))
})

test_that(".rewrite_functions_vec for postgresql uses correct functions", {
  stmts <- c("SELECT DATEADD(day, 5, d)", "SELECT DATEFROMPARTS(2020, 1, 1)")
  result <- .rewrite_functions_vec(stmts, "postgresql")
  expect_true(grepl("INTERVAL", result[1]))
  expect_true(grepl("make_date", result[2]))
})

# ---- .transform_select_into ----

test_that(".transform_select_into transforms SELECT INTO", {
  stmt <- "SELECT col1, col2 INTO newtable FROM source"
  result <- .transform_select_into(stmt)
  expect_true(grepl("CREATE TABLE newtable AS", result))
  expect_false(grepl("INTO newtable", result))
})

test_that(".transform_select_into skips INSERT INTO", {
  stmt <- "INSERT INTO target SELECT col FROM source"
  result <- .transform_select_into(stmt)
  expect_equal(result, stmt)
})

test_that(".transform_select_into skips statements without INTO", {
  stmt <- "SELECT col FROM source"
  result <- .transform_select_into(stmt)
  expect_equal(result, stmt)
})

test_that(".transform_select_into handles schema.table names", {
  stmt <- "SELECT col INTO myschema.newtable FROM source"
  result <- .transform_select_into(stmt)
  expect_true(grepl("CREATE TABLE myschema.newtable AS", result))
})

# ---- .translate_duckdb (full pipeline) ----

test_that(".translate_duckdb strips ANALYZE statements", {
  stmts <- c("SELECT 1", "ANALYZE my_table", "SELECT 2")
  result <- .translate_duckdb(stmts)
  expect_equal(result[2], "")
  expect_true(nzchar(result[1]))
  expect_true(nzchar(result[3]))
})

test_that(".translate_duckdb converts COUNT_BIG to COUNT", {
  stmts <- c("SELECT COUNT_BIG(*) FROM t")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("COUNT\\(", result[1]))
  expect_false(grepl("COUNT_BIG", result[1]))
})

test_that(".translate_duckdb converts GETDATE() to CURRENT_DATE", {
  stmts <- c("SELECT GETDATE()")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("CURRENT_DATE", result[1]))
})

test_that(".translate_duckdb converts TRY_CAST to CAST", {
  stmts <- c("SELECT TRY_CAST(x AS INT)")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("CAST\\(", result[1]))
  expect_false(grepl("TRY_CAST", result[1]))
})

test_that(".translate_duckdb converts TRUNCATE TABLE to DELETE FROM", {
  stmts <- c("TRUNCATE TABLE my_table")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("DELETE FROM", result[1]))
})

test_that(".translate_duckdb converts VARCHAR(MAX) to TEXT", {
  stmts <- c("CREATE TABLE t (c VARCHAR(MAX))")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("TEXT", result[1]))
})

test_that(".translate_duckdb converts DATETIME to TIMESTAMP", {
  stmts <- c("CREATE TABLE t (c DATETIME)")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("TIMESTAMP", result[1]))
})

test_that(".translate_duckdb converts FLOAT to NUMERIC", {
  stmts <- c("CREATE TABLE t (c FLOAT)")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("NUMERIC", result[1]))
})

test_that(".translate_duckdb converts SELECT INTO to CREATE TABLE AS", {
  stmts <- c("SELECT col INTO newtable FROM source")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("CREATE TABLE newtable AS", result[1]))
})

test_that(".translate_duckdb rewrites YEAR(), MONTH(), DAY() with CAST", {
  stmts <- c("SELECT YEAR(start_date) FROM t")
  result <- .translate_duckdb(stmts)
  expect_true(grepl("YEAR\\(CAST\\(", result[1]))
  expect_true(grepl("AS DATE", result[1]))
})

test_that(".translate_duckdb does not double-wrap YEAR(CAST(x AS DATE))", {
  stmts <- c("SELECT YEAR(CAST(x AS DATE)) FROM t")
  result <- .translate_duckdb(stmts)
  # Should have exactly one CAST, not nested
  casts <- gregexpr("CAST", result[1])[[1]]
  expect_equal(length(casts), 1L)
})

# ---- .translate_postgresql ----

test_that(".translate_postgresql converts functions", {
  stmts <- c("SELECT DATEADD(day, 5, d), COUNT_BIG(*), GETDATE()")
  result <- .translate_postgresql(stmts)
  expect_true(grepl("INTERVAL", result[1]))
  expect_true(grepl("COUNT\\(", result[1]))
  expect_true(grepl("CURRENT_DATE", result[1]))
})

test_that(".translate_postgresql converts types", {
  stmts <- c("CREATE TABLE t (c VARCHAR(MAX), d DATETIME, e FLOAT)")
  result <- .translate_postgresql(stmts)
  expect_true(grepl("TEXT", result[1]))
  expect_true(grepl("TIMESTAMP", result[1]))
  expect_true(grepl("NUMERIC", result[1]))
})

test_that(".translate_postgresql converts SELECT INTO", {
  stmts <- c("SELECT a INTO newtable FROM src")
  result <- .translate_postgresql(stmts)
  expect_true(grepl("CREATE TABLE newtable AS", result[1]))
})

# ---- translate_cohort_stmts (main entry) ----

test_that("translate_cohort_stmts returns unchanged for NULL dialect", {
  stmts <- c("SELECT 1", "SELECT 2")
  result <- translate_cohort_stmts(stmts, NULL)
  expect_identical(result, stmts)
})

test_that("translate_cohort_stmts uses DuckDB path", {
  stmts <- c("SELECT DATEADD(day, 1, d) FROM t")
  result <- translate_cohort_stmts(stmts, "duckdb")
  expect_true(grepl("TO_DAYS", result[1]))
})

test_that("translate_cohort_stmts handles SQL Server ANALYZE conversion", {
  stmts <- c("SELECT 1", "ANALYZE my_table")
  result <- translate_cohort_stmts(stmts, "sql server")
  expect_true(grepl("UPDATE STATISTICS", result[2]))
})

test_that("translate_cohort_stmts passes through for SQL Server without ANALYZE", {
  stmts <- c("SELECT 1")
  result <- translate_cohort_stmts(stmts, "sql server")
  expect_equal(result, stmts)
})

# ---- .translate_stmts_r (fallback path) ----

test_that(".translate_stmts_r handles oracle ANALYZE removal", {
  stmts <- c("SELECT 1", "ANALYZE my_table")
  result <- .translate_stmts_r(stmts, "oracle")
  expect_equal(result[2], "")
})

test_that(".translate_stmts_r handles snowflake ANALYZE removal", {
  stmts <- c("SELECT 1", "ANALYZE my_table")
  result <- .translate_stmts_r(stmts, "snowflake")
  expect_equal(result[2], "")
})

test_that(".translate_stmts_r handles bigquery ANALYZE removal", {
  stmts <- c("SELECT 1", "ANALYZE my_table")
  result <- .translate_stmts_r(stmts, "bigquery")
  expect_equal(result[2], "")
})

test_that(".translate_stmts_r handles spark ANALYZE conversion", {
  stmts <- c("ANALYZE my_table")
  result <- .translate_stmts_r(stmts, "spark")
  expect_true(grepl("ANALYZE TABLE", result[1]))
  expect_true(grepl("COMPUTE STATISTICS", result[1]))
})

test_that(".translate_stmts_r strips NULL constraints for spark", {
  stmts <- c("CREATE TABLE t (a INT NOT NULL, b VARCHAR NULL)")
  result <- .translate_stmts_r(stmts, "spark")
  expect_false(grepl("NOT NULL", result[1]))
  expect_false(grepl("\\bNULL\\b", result[1]))
})

test_that(".translate_stmts_r preserves empty statements", {
  stmts <- c("", "SELECT 1")
  result <- .translate_stmts_r(stmts, "postgresql")
  expect_equal(result[1], "")
})

test_that(".translate_stmts_r handles DROP TABLE (skips sqlrender)", {
  stmts <- c("DROP TABLE IF EXISTS my_table")
  result <- .translate_stmts_r(stmts, "postgresql")
  expect_true(grepl("DROP TABLE", result[1]))
})

test_that(".translate_stmts_r handles plain CREATE TABLE (skips sqlrender)", {
  stmts <- c("CREATE TABLE t (a INT, b VARCHAR)")
  result <- .translate_stmts_r(stmts, "postgresql")
  expect_true(grepl("CREATE TABLE", result[1]))
})

# ---- .rewrite_datepart_funcs_duckdb ----

test_that(".rewrite_datepart_funcs_duckdb wraps YEAR with CAST", {
  stmts <- c("SELECT YEAR(start_date) FROM t")
  result <- .rewrite_datepart_funcs_duckdb(stmts)
  expect_true(grepl("YEAR\\(CAST\\(start_date AS DATE\\)\\)", result[1]))
})

test_that(".rewrite_datepart_funcs_duckdb wraps MONTH with CAST", {
  stmts <- c("SELECT MONTH(start_date)")
  result <- .rewrite_datepart_funcs_duckdb(stmts)
  expect_true(grepl("MONTH\\(CAST\\(start_date AS DATE\\)\\)", result[1]))
})

test_that(".rewrite_datepart_funcs_duckdb wraps DAY with CAST", {
  stmts <- c("SELECT DAY(start_date)")
  result <- .rewrite_datepart_funcs_duckdb(stmts)
  expect_true(grepl("DAY\\(CAST\\(start_date AS DATE\\)\\)", result[1]))
})

test_that(".rewrite_datepart_funcs_duckdb skips already-casted args", {
  stmts <- c("SELECT YEAR(CAST(x AS DATE))")
  result <- .rewrite_datepart_funcs_duckdb(stmts)
  casts <- gregexpr("CAST", result[1])[[1]]
  expect_equal(length(casts), 1L)
})

test_that(".rewrite_datepart_funcs_duckdb leaves non-matching statements alone", {
  stmts <- c("SELECT col FROM t")
  result <- .rewrite_datepart_funcs_duckdb(stmts)
  expect_equal(result, stmts)
})
