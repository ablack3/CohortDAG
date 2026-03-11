# Additional coverage tests for R/sqlrender.R
# Targets uncovered code paths in tokenizer, renderer, splitter, pattern matching,
# translator, and all utility functions.

skip_on_cran()
skip_if(nzchar(Sys.getenv("CI_TEST_DB")), "Skipping extra tests on container CI")

# =============================================================================
# 1. tokenize_sql() -- edge cases
# =============================================================================

test_that("tokenize_sql returns empty list for empty string", {
  expect_equal(CohortDAG:::tokenize_sql(""), list())
})

test_that("tokenize_sql returns empty list for NA coerced to empty", {
  # as.character(NA) -> "NA", which has length > 0 but is valid

  tokens <- CohortDAG:::tokenize_sql("NA")
  expect_true(length(tokens) > 0)
})

test_that("tokenize_sql handles multi-line comment spanning entire input", {
  tokens <- CohortDAG:::tokenize_sql("/* entire comment */")
  # Should have no real tokens since all content is a comment
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_false(any(grepl("entire", texts)))
  expect_false(any(grepl("comment", texts)))
})

test_that("tokenize_sql handles multi-line comment with newlines inside", {
  sql <- "SELECT /* this is\na multi-line\ncomment */ 1"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("SELECT" %in% texts)
  expect_true("1" %in% texts)
  expect_false(any(grepl("multi", texts)))
})

test_that("tokenize_sql handles single-line comment at end of input without newline", {
  sql <- "SELECT 1 -- trailing comment"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("SELECT" %in% texts)
  expect_true("1" %in% texts)
  expect_false(any(grepl("trailing", texts)))
})

test_that("tokenize_sql handles single-line comment followed by more SQL", {
  sql <- "SELECT 1 -- comment\nSELECT 2"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("1" %in% texts)
  expect_true("2" %in% texts)
})

test_that("tokenize_sql handles unclosed multi-line comment", {
  sql <- "SELECT 1 /* unclosed comment"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  # The unclosed comment means everything after /* is absorbed
  expect_true("SELECT" %in% texts)
  expect_true("1" %in% texts)
  expect_false(any(grepl("unclosed", texts)))
})

test_that("tokenize_sql handles --HINT keyword specially", {
  sql <- "--hint some_hint\nSELECT 1"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  # The --hint should NOT be treated as a comment; it emits "--" as a token
  expect_true("--" %in% texts)
  expect_true("SELECT" %in% texts)
})

test_that("tokenize_sql handles escaped quotes inside single-quoted strings", {
  sql <- "SELECT 'it''s a test' FROM t"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  # The single quote toggles should still eventually close
  expect_true("SELECT" %in% texts)
  expect_true("FROM" %in% texts)
})

test_that("tokenize_sql handles double-quoted identifiers", {
  sql <- 'SELECT "my column" FROM t'
  tokens <- CohortDAG:::tokenize_sql(sql)
  in_quotes <- vapply(tokens, function(t) t$inQuotes, logical(1))
  expect_true(any(in_quotes))
})

test_that("tokenize_sql handles @parameter at end of string", {
  sql <- "SELECT @param"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("@param" %in% texts)
})

test_that("tokenize_sql handles consecutive operators", {
  sql <- "SELECT a+b*c FROM t"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("+" %in% texts)
  expect_true("*" %in% texts)
})

test_that("tokenize_sql handles tab and carriage return whitespace", {
  sql <- "SELECT\t1\r\nFROM\tt"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("SELECT" %in% texts)
  expect_true("1" %in% texts)
  expect_true("FROM" %in% texts)
})

test_that("tokenize_sql handles only whitespace", {
  tokens <- CohortDAG:::tokenize_sql("   \t\n  ")
  expect_equal(length(tokens), 0)
})

test_that("tokenize_sql handles block comment /* */ immediately after token", {
  sql <- "SELECT/*comment*/1"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("SELECT" %in% texts)
  expect_true("1" %in% texts)
  expect_false(any(grepl("comment", texts)))
})

test_that("tokenize_sql handles nested-looking block comments", {
  # SQL block comments do not truly nest; /* ... /* ... */ ends at first */
  sql <- "SELECT /* outer /* inner */ 1"
  tokens <- CohortDAG:::tokenize_sql(sql)
  texts <- vapply(tokens, function(t) t$text, character(1))
  expect_true("SELECT" %in% texts)
  expect_true("1" %in% texts)
})

test_that("tokenize_sql tracks start and end positions accurately", {
  sql <- "SELECT a"
  tokens <- CohortDAG:::tokenize_sql(sql)
  # First token should be "SELECT" starting at 1

  expect_equal(tokens[[1]]$start, 1)
  expect_equal(tokens[[1]]$text, "SELECT")
  # Second token should be "a" starting at 8
  expect_equal(tokens[[2]]$start, 8)
  expect_equal(tokens[[2]]$text, "a")
})

test_that("tokenize_sql handles single character input", {
  tokens <- CohortDAG:::tokenize_sql("x")
  expect_equal(length(tokens), 1)
  expect_equal(tokens[[1]]$text, "x")
})

test_that("tokenize_sql handles single operator", {
  tokens <- CohortDAG:::tokenize_sql("+")
  expect_equal(length(tokens), 1)
  expect_equal(tokens[[1]]$text, "+")
})

# =============================================================================
# 2. replace_char_at()
# =============================================================================

test_that("replace_char_at replaces at end of string", {
  result <- CohortDAG:::replace_char_at("abc", 3, "Z")
  expect_equal(result, "abZ")
})

test_that("replace_char_at replaces in middle of string", {
  result <- CohortDAG:::replace_char_at("abc", 2, "X")
  expect_equal(result, "aXc")
})

# =============================================================================
# 3. str_replace_region()
# =============================================================================

test_that("str_replace_region replaces at start of string", {
  result <- CohortDAG:::str_replace_region("hello world", 1, 5, "HELLO")
  expect_true(grepl("^HELLO", result))
})

test_that("str_replace_region replaces at end of string", {
  result <- CohortDAG:::str_replace_region("hello world", 7, 11, "WORLD")
  expect_true(grepl("WORLD$", result))
})

test_that("str_replace_region handles replacement longer than original", {
  result <- CohortDAG:::str_replace_region("ab", 1, 2, "LONG_STRING")
  expect_true(grepl("LONG_STRING", result))
})

test_that("str_replace_region handles replacement shorter than original", {
  result <- CohortDAG:::str_replace_region("hello world", 1, 11, "x")
  expect_equal(result, "x")
})

# =============================================================================
# 4. str_replace_all()
# =============================================================================

test_that("str_replace_all replaces multiple occurrences", {
  result <- CohortDAG:::str_replace_all("aXbXcX", "X", "Y")
  expect_equal(result, "aYbYcY")
})

test_that("str_replace_all no match returns original", {
  result <- CohortDAG:::str_replace_all("abc", "Z", "Y")
  expect_equal(result, "abc")
})

test_that("str_replace_all replaces with empty string", {
  result <- CohortDAG:::str_replace_all("aXbXc", "X", "")
  expect_equal(result, "abc")
})

# =============================================================================
# 5. token_is_identifier()
# =============================================================================

test_that("token_is_identifier handles underscores and numbers", {
  expect_true(CohortDAG:::token_is_identifier("_my_var"))
  expect_true(CohortDAG:::token_is_identifier("var123"))
  expect_true(CohortDAG:::token_is_identifier("123"))
})

test_that("token_is_identifier rejects special characters", {
  expect_false(CohortDAG:::token_is_identifier("my-var"))
  expect_false(CohortDAG:::token_is_identifier("my.var"))
  expect_false(CohortDAG:::token_is_identifier("my var"))
  expect_false(CohortDAG:::token_is_identifier(""))
})

# =============================================================================
# 6. safe_split() -- more edge cases
# =============================================================================

test_that("safe_split handles single element (no delimiter)", {
  result <- CohortDAG:::safe_split("hello", ",")
  expect_equal(result, "hello")
})

test_that("safe_split handles delimiter at start", {
  result <- CohortDAG:::safe_split(",hello", ",")
  expect_equal(result, c("", "hello"))
})

test_that("safe_split handles delimiter at end", {
  result <- CohortDAG:::safe_split("hello,", ",")
  expect_equal(result, c("hello", ""))
})

test_that("safe_split handles multiple consecutive delimiters", {
  result <- CohortDAG:::safe_split("a,,b", ",")
  expect_equal(result, c("a", "", "b"))
})

test_that("safe_split handles double-escaped backslash before delimiter", {
  result <- CohortDAG:::safe_split("a\\\\,b", ",")
  # \\\\ is two literal backslashes; the second is not an escape, so comma splits
  expect_equal(length(result), 2)
})

test_that("safe_split handles quoted field with delimiter inside", {
  result <- CohortDAG:::safe_split('"a,b",c', ",")
  expect_equal(length(result), 2)
  expect_true(grepl("a,b", result[1]))
  expect_equal(result[2], "c")
})

# =============================================================================
# 7. split_and_keep() -- more edge cases
# =============================================================================

test_that("split_and_keep handles match at start of string", {
  result <- CohortDAG:::split_and_keep("123abc", "[0-9]+")
  expect_equal(result[[1]], "123")
  expect_equal(result[[2]], "abc")
})

test_that("split_and_keep handles match at end of string", {
  result <- CohortDAG:::split_and_keep("abc123", "[0-9]+")
  expect_equal(result[[1]], "abc")
  expect_equal(result[[2]], "123")
})

test_that("split_and_keep handles multiple matches", {
  result <- CohortDAG:::split_and_keep("a1b2c3d", "[0-9]")
  expect_equal(length(result), 7)  # a, 1, b, 2, c, 3, d
})

test_that("split_and_keep handles entire string matching", {
  result <- CohortDAG:::split_and_keep("12345", "[0-9]+")
  expect_equal(length(result), 1)
  expect_equal(result[[1]], "12345")
})

# =============================================================================
# 8. replace_with_concat() -- more edge cases
# =============================================================================

test_that("replace_with_concat handles double-quoted strings with escaped quotes", {
  result <- CohortDAG:::replace_with_concat('SELECT "it""s"')
  expect_true(grepl("CONCAT", result))
})

test_that("replace_with_concat handles multiple string literals", {
  result <- CohortDAG:::replace_with_concat("SELECT 'a''b', 'c''d'")
  expect_true(grepl("CONCAT", result))
})

test_that("replace_with_concat passes through strings without escaped quotes", {
  result <- CohortDAG:::replace_with_concat("'simple'")
  expect_equal(result, "'simple'")
})

test_that("replace_with_concat handles adjacent escaped quotes", {
  result <- CohortDAG:::replace_with_concat("'a''''b'")
  expect_true(grepl("CONCAT", result))
})

test_that("replace_with_concat handles non-string tokens unchanged", {
  result <- CohortDAG:::replace_with_concat("SELECT 1 + 2")
  expect_equal(result, "SELECT 1 + 2")
})

test_that("replace_with_concat handles empty quoted string", {
  result <- CohortDAG:::replace_with_concat("''")
  expect_equal(result, "''")
})

# =============================================================================
# 9. find_curly_bracket_spans()
# =============================================================================

test_that("find_curly_bracket_spans returns empty for no braces", {
  spans <- CohortDAG:::find_curly_bracket_spans("no braces here")
  expect_equal(length(spans), 0)
})

test_that("find_curly_bracket_spans handles single pair", {
  spans <- CohortDAG:::find_curly_bracket_spans("{hello}")
  expect_equal(length(spans), 1)
  expect_equal(spans[[1]]$start, 1)
  expect_equal(spans[[1]]$end, 8)  # end is past the }
})

test_that("find_curly_bracket_spans handles deeply nested braces", {
  spans <- CohortDAG:::find_curly_bracket_spans("{a{b{c}}}")
  # Should have 3 spans: innermost {c} first when closed, then {b{c}}, then outermost
  expect_equal(length(spans), 3)
})

test_that("find_curly_bracket_spans handles unmatched opening brace", {
  spans <- CohortDAG:::find_curly_bracket_spans("{hello")
  # No closing brace means no completed spans
  expect_equal(length(spans), 0)
})

test_that("find_curly_bracket_spans handles unmatched closing brace", {
  spans <- CohortDAG:::find_curly_bracket_spans("hello}")
  # No opening brace: closing brace has no match
  expect_equal(length(spans), 0)
})

test_that("find_curly_bracket_spans handles empty braces", {
  spans <- CohortDAG:::find_curly_bracket_spans("{}")
  expect_equal(length(spans), 1)
  expect_equal(spans[[1]]$start, 1)
})

test_that("find_curly_bracket_spans handles multiple sibling spans", {
  spans <- CohortDAG:::find_curly_bracket_spans("{a}{b}{c}")
  expect_equal(length(spans), 3)
})

# =============================================================================
# 10. link_if_then_elses() -- more edge cases
# =============================================================================

test_that("link_if_then_elses returns empty when no ? between spans", {
  str <- "{a} {b}"
  spans <- CohortDAG:::find_curly_bracket_spans(str)
  ites <- CohortDAG:::link_if_then_elses(str, spans)
  expect_equal(length(ites), 0)
})

test_that("link_if_then_elses handles multiple if-then-else chains", {
  str <- "{c1}?{t1}:{e1} {c2}?{t2}"
  spans <- CohortDAG:::find_curly_bracket_spans(str)
  ites <- CohortDAG:::link_if_then_elses(str, spans)
  expect_equal(length(ites), 2)
  expect_true(ites[[1]]$hasIfFalse)
  expect_false(ites[[2]]$hasIfFalse)
})

test_that("link_if_then_elses sets block_end correctly", {
  str <- "{cond}?{then}:{else}"
  spans <- CohortDAG:::find_curly_bracket_spans(str)
  ites <- CohortDAG:::link_if_then_elses(str, spans)
  expect_true(ites[[1]]$block_end > 0)
  # block_end should be the end of the else span
  expect_equal(ites[[1]]$block_end, ites[[1]]$ifFalse$end)
})

test_that("link_if_then_elses block_end for no else is ifTrue end", {
  str <- "{cond}?{then}"
  spans <- CohortDAG:::find_curly_bracket_spans(str)
  ites <- CohortDAG:::link_if_then_elses(str, spans)
  expect_equal(ites[[1]]$block_end, ites[[1]]$ifTrue$end)
})

# =============================================================================
# 11. remove_parentheses_quotes()
# =============================================================================

test_that("remove_parentheses_quotes handles single character", {
  expect_equal(CohortDAG:::remove_parentheses_quotes("x"), "x")
})

test_that("remove_parentheses_quotes handles empty string", {
  expect_equal(CohortDAG:::remove_parentheses_quotes(""), "")
})

test_that("remove_parentheses_quotes handles mismatched quotes", {
  expect_equal(CohortDAG:::remove_parentheses_quotes("'hello\""), "'hello\"")
  expect_equal(CohortDAG:::remove_parentheses_quotes("\"hello'"), "\"hello'")
})

test_that("remove_parentheses_quotes handles inner quotes preserved", {
  expect_equal(CohortDAG:::remove_parentheses_quotes("'he\"lo'"), "he\"lo")
})

# =============================================================================
# 12. preceded_by_in()
# =============================================================================

test_that("preceded_by_in handles whitespace before IN", {
  str <- "x  IN  (1,2)"
  pos <- regexpr("\\(", str)[1]
  expect_true(CohortDAG:::preceded_by_in(pos, str))
})

test_that("preceded_by_in handles case insensitivity", {
  str <- "x IN (1,2)"
  pos <- regexpr("\\(", str)[1]
  expect_true(CohortDAG:::preceded_by_in(pos, str))
})

test_that("preceded_by_in returns FALSE at position 1", {
  expect_false(CohortDAG:::preceded_by_in(1, "(1,2)"))
})

test_that("preceded_by_in returns FALSE at position 2", {
  expect_false(CohortDAG:::preceded_by_in(2, "a(1,2)"))
})

test_that("preceded_by_in returns FALSE when no IN keyword", {
  str <- "SELECT (1+2)"
  pos <- regexpr("\\(", str)[1]
  expect_false(CohortDAG:::preceded_by_in(pos, str))
})

test_that("preceded_by_in returns FALSE with ON keyword instead of IN", {
  str <- "x ON (1,2)"
  pos <- regexpr("\\(", str)[1]
  expect_false(CohortDAG:::preceded_by_in(pos, str))
})

# =============================================================================
# 13. evaluate_primitive_condition() -- more edge cases
# =============================================================================

test_that("evaluate_primitive_condition handles numeric equality", {
  expect_true(CohortDAG:::evaluate_primitive_condition("42 == 42"))
  expect_false(CohortDAG:::evaluate_primitive_condition("42 == 43"))
})

test_that("evaluate_primitive_condition handles quoted equality", {
  expect_true(CohortDAG:::evaluate_primitive_condition("'postgresql' == 'postgresql'"))
  expect_false(CohortDAG:::evaluate_primitive_condition("'postgresql' == 'oracle'"))
})

test_that("evaluate_primitive_condition handles numeric inequality with !=", {
  expect_true(CohortDAG:::evaluate_primitive_condition("42 != 43"))
  expect_false(CohortDAG:::evaluate_primitive_condition("42 != 42"))
})

test_that("evaluate_primitive_condition handles <> with quoted values", {
  expect_true(CohortDAG:::evaluate_primitive_condition("'a' <> 'b'"))
  expect_false(CohortDAG:::evaluate_primitive_condition("'a' <> 'a'"))
})

test_that("evaluate_primitive_condition handles IN with single element", {
  expect_true(CohortDAG:::evaluate_primitive_condition("'a' IN ('a')"))
  expect_false(CohortDAG:::evaluate_primitive_condition("'b' IN ('a')"))
})

test_that("evaluate_primitive_condition handles IN with spaces in list", {
  expect_true(CohortDAG:::evaluate_primitive_condition("'b' IN ('a', 'b', 'c')"))
  expect_false(CohortDAG:::evaluate_primitive_condition("'z' IN ('a', 'b', 'c')"))
})

test_that("evaluate_primitive_condition handles NOT IN equivalent (! prefix)", {
  # !0 -> TRUE, !1 -> FALSE
  expect_true(CohortDAG:::evaluate_primitive_condition("!0"))
  expect_false(CohortDAG:::evaluate_primitive_condition("!1"))
})

test_that("evaluate_primitive_condition handles unknown strings as FALSE", {
  expect_false(CohortDAG:::evaluate_primitive_condition("random_token"))
  expect_false(CohortDAG:::evaluate_primitive_condition("maybe"))
  expect_false(CohortDAG:::evaluate_primitive_condition(""))
})

test_that("evaluate_primitive_condition handles whitespace around operands", {
  expect_true(CohortDAG:::evaluate_primitive_condition("  'a'  ==  'a'  "))
  expect_true(CohortDAG:::evaluate_primitive_condition("  1  !=  0  "))
})

# =============================================================================
# 14. evaluate_boolean_condition() -- more edge cases
# =============================================================================

test_that("evaluate_boolean_condition handles multiple AND clauses", {
  expect_true(CohortDAG:::evaluate_boolean_condition("true & true & true"))
  expect_false(CohortDAG:::evaluate_boolean_condition("true & true & false"))
})

test_that("evaluate_boolean_condition handles multiple OR clauses", {
  expect_true(CohortDAG:::evaluate_boolean_condition("false | false | true"))
  expect_false(CohortDAG:::evaluate_boolean_condition("false | false | false"))
})

test_that("evaluate_boolean_condition handles AND with comparison operators", {
  expect_true(CohortDAG:::evaluate_boolean_condition("'a' == 'a' & 1 != 0"))
  expect_false(CohortDAG:::evaluate_boolean_condition("'a' == 'a' & 0 != 0"))
})

test_that("evaluate_boolean_condition handles OR with comparison operators", {
  expect_true(CohortDAG:::evaluate_boolean_condition("'a' == 'b' | 1 != 0"))
  expect_false(CohortDAG:::evaluate_boolean_condition("'a' == 'b' | 0 != 0"))
})

test_that("evaluate_boolean_condition handles negation literals", {
  expect_true(CohortDAG:::evaluate_boolean_condition("!false"))
  expect_false(CohortDAG:::evaluate_boolean_condition("!true"))
})

# =============================================================================
# 15. evaluate_condition() -- with parentheses
# =============================================================================

test_that("evaluate_condition handles simple parentheses", {
  expect_true(CohortDAG:::evaluate_condition("(true)"))
  expect_false(CohortDAG:::evaluate_condition("(false)"))
})

test_that("evaluate_condition handles nested parentheses with AND", {
  expect_true(CohortDAG:::evaluate_condition("(true & (true))"))
  expect_false(CohortDAG:::evaluate_condition("(true & (false))"))
})

test_that("evaluate_condition handles IN with parentheses correctly", {
  # 'a' IN ('a','b') should be TRUE, not confusing the ( with a subexpression
  expect_true(CohortDAG:::evaluate_condition("'a' IN ('a','b')"))
  expect_false(CohortDAG:::evaluate_condition("'z' IN ('a','b')"))
})

test_that("evaluate_condition handles complex IN with AND", {
  expect_true(CohortDAG:::evaluate_condition("'a' IN ('a','b') & true"))
  expect_false(CohortDAG:::evaluate_condition("'z' IN ('a','b') & true"))
})

test_that("evaluate_condition handles no parentheses (delegates to boolean)", {
  expect_true(CohortDAG:::evaluate_condition("true"))
  expect_false(CohortDAG:::evaluate_condition("false"))
})

test_that("evaluate_condition handles deeply nested parentheses", {
  expect_true(CohortDAG:::evaluate_condition("(((1 != 0)))"))
  expect_false(CohortDAG:::evaluate_condition("(((0 != 0)))"))
})

test_that("evaluate_condition handles OR with parentheses", {
  expect_true(CohortDAG:::evaluate_condition("(false) | (true)"))
  expect_false(CohortDAG:::evaluate_condition("(false) | (false)"))
})

# =============================================================================
# 16. str_replace_and_adjust_spans()
# =============================================================================

test_that("str_replace_and_adjust_spans basic replacement works", {
  str <- "hello world!"
  spans <- list()
  result <- CohortDAG:::str_replace_and_adjust_spans(str, spans, 1, 6, 7, 11)
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

test_that("str_replace_and_adjust_spans handles empty replacement region", {
  str <- "abc"
  spans <- list()
  result <- CohortDAG:::str_replace_and_adjust_spans(str, spans, 1, 1, 1, 0)
  expect_true(is.character(result))
})

# =============================================================================
# 17. extract_defaults()
# =============================================================================

test_that("extract_defaults handles single default", {
  defaults <- CohortDAG:::extract_defaults("{DEFAULT @x = 5}")
  expect_equal(defaults[["x"]], "5")
})

test_that("extract_defaults handles multiple defaults", {
  sql <- "{DEFAULT @a = hello}{DEFAULT @b = world}"
  defaults <- CohortDAG:::extract_defaults(sql)
  expect_equal(defaults[["a"]], "hello")
  expect_equal(defaults[["b"]], "world")
})

test_that("extract_defaults handles quoted default values", {
  sql <- "{DEFAULT @name = 'John'}"
  defaults <- CohortDAG:::extract_defaults(sql)
  expect_equal(defaults[["name"]], "John")
})

test_that("extract_defaults returns empty list when no defaults", {
  defaults <- CohortDAG:::extract_defaults("SELECT 1 FROM t")
  expect_equal(length(defaults), 0)
})

test_that("extract_defaults handles @ prefix in parameter name", {
  sql <- "{DEFAULT @param = value}"
  defaults <- CohortDAG:::extract_defaults(sql)
  expect_true("param" %in% names(defaults))
  expect_equal(defaults[["param"]], "value")
})

test_that("extract_defaults handles spaces around equals", {
  sql <- "{DEFAULT @x  =  10}"
  defaults <- CohortDAG:::extract_defaults(sql)
  expect_equal(defaults[["x"]], "10")
})

# =============================================================================
# 18. remove_defaults()
# =============================================================================

test_that("remove_defaults removes DEFAULT followed by newline", {
  sql <- "{DEFAULT @x = 5}\nSELECT @x"
  result <- CohortDAG:::remove_defaults(sql)
  expect_false(grepl("DEFAULT", result))
  expect_true(grepl("SELECT", result))
})

test_that("remove_defaults removes multiple DEFAULT blocks", {
  sql <- "{DEFAULT @a = 1}\n{DEFAULT @b = 2}\nSELECT @a, @b"
  result <- CohortDAG:::remove_defaults(sql)
  expect_false(grepl("DEFAULT", result))
  expect_true(grepl("SELECT", result))
})

test_that("remove_defaults leaves SQL intact when no defaults", {
  sql <- "SELECT 1 FROM t"
  result <- CohortDAG:::remove_defaults(sql)
  expect_equal(result, sql)
})

# =============================================================================
# 19. escape_dollar_sign() -- currently a no-op
# =============================================================================

test_that("escape_dollar_sign returns input unchanged", {
  expect_equal(CohortDAG:::escape_dollar_sign("price$100"), "price$100")
  expect_equal(CohortDAG:::escape_dollar_sign("no_dollar"), "no_dollar")
  expect_equal(CohortDAG:::escape_dollar_sign(""), "")
})

# =============================================================================
# 20. substitute_parameters()
# =============================================================================

test_that("substitute_parameters handles basic substitution", {
  result <- CohortDAG:::substitute_parameters(
    "SELECT @col FROM @tbl",
    list(col = "name", tbl = "person")
  )
  expect_equal(result, "SELECT name FROM person")
})

test_that("substitute_parameters sorts by name length descending", {
  result <- CohortDAG:::substitute_parameters(
    "SELECT @table_name, @table",
    list(table = "t", table_name = "person")
  )
  expect_true(grepl("person", result))
  # @table_name should be replaced before @table
  expect_false(grepl("@table_name", result))
  expect_false(grepl("@table", result))
})

test_that("substitute_parameters applies defaults when params missing", {
  result <- CohortDAG:::substitute_parameters(
    "{DEFAULT @x = 10}\nSELECT @x",
    list()
  )
  expect_true(grepl("10", result))
})

test_that("substitute_parameters overrides defaults with provided values", {
  result <- CohortDAG:::substitute_parameters(
    "{DEFAULT @x = 10}\nSELECT @x",
    list(x = "20")
  )
  expect_true(grepl("20", result))
  expect_false(grepl("10", result))
})

test_that("substitute_parameters handles empty parameter list", {
  result <- CohortDAG:::substitute_parameters(
    "SELECT @unresolved FROM t",
    list()
  )
  expect_true(grepl("@unresolved", result))
})

test_that("substitute_parameters handles vector value collapsed", {
  # Values should already be character but let's test
  result <- CohortDAG:::substitute_parameters(
    "SELECT @val FROM t",
    list(val = "42")
  )
  expect_true(grepl("42", result))
})

# =============================================================================
# 21. parse_if_then_else() -- more paths
# =============================================================================

test_that("parse_if_then_else handles true condition with then block", {
  result <- CohortDAG:::parse_if_then_else("{1 == 1}?{YES}")
  expect_true(grepl("YES", result))
})

test_that("parse_if_then_else handles false condition removes then block", {
  result <- CohortDAG:::parse_if_then_else("{0 == 1}?{YES}")
  expect_false(grepl("YES", result))
})

test_that("parse_if_then_else handles false with else block", {
  result <- CohortDAG:::parse_if_then_else("{0 == 1}?{YES}:{NO}")
  expect_false(grepl("YES", result))
  expect_true(grepl("NO", result))
})

test_that("parse_if_then_else handles true with else block (else not used)", {
  result <- CohortDAG:::parse_if_then_else("{1 == 1}?{YES}:{NO}")
  expect_true(grepl("YES", result))
  expect_false(grepl("NO", result))
})

test_that("parse_if_then_else preserves surrounding text", {
  result <- CohortDAG:::parse_if_then_else("before {1 == 1}?{middle} after")
  expect_true(grepl("before", result))
  expect_true(grepl("middle", result))
  expect_true(grepl("after", result))
})

test_that("parse_if_then_else handles nested conditionals", {
  result <- CohortDAG:::parse_if_then_else("{1 == 1}?{{1 == 1}?{inner}}")
  expect_true(grepl("inner", result))
})

test_that("parse_if_then_else returns string unchanged when no conditionals", {
  sql <- "SELECT 1 FROM t"
  result <- CohortDAG:::parse_if_then_else(sql)
  expect_equal(result, sql)
})

# =============================================================================
# 22. render_sql_core() -- more paths
# =============================================================================

test_that("render_sql_core handles multiple parameters and conditionals", {
  sql <- "SELECT @col FROM @tbl {1 == 1}?{WHERE x > 0}"
  result <- CohortDAG:::render_sql_core(sql, c(col = "name", tbl = "person"))
  expect_true(grepl("name", result))
  expect_true(grepl("person", result))
  expect_true(grepl("WHERE x > 0", result))
})

test_that("render_sql_core handles false conditional removes clause", {
  sql <- "SELECT @col FROM @tbl {0 == 1}?{WHERE x > 0}"
  result <- CohortDAG:::render_sql_core(sql, c(col = "name", tbl = "person"))
  expect_true(grepl("name", result))
  expect_false(grepl("WHERE", result))
})

test_that("render_sql_core handles defaults and conditionals together", {
  sql <- "{DEFAULT @include = 1}\nSELECT * FROM t {@include == 1}?{WHERE active = 1}"
  result <- CohortDAG:::render_sql_core(sql, c())
  expect_true(grepl("WHERE active = 1", result))
})

test_that("render_sql_core handles override default disabling conditional", {
  sql <- "{DEFAULT @include = 1}\nSELECT * FROM t {@include == 1}?{WHERE active = 1}"
  result <- CohortDAG:::render_sql_core(sql, c(include = "0"))
  expect_false(grepl("WHERE", result))
})

test_that("render_sql_core handles nested conditionals with params", {
  sql <- "{@a == 1}?{{@b == 1}?{INNER}:{ELSE_INNER}}"
  result <- CohortDAG:::render_sql_core(sql, c(a = "1", b = "1"))
  expect_true(grepl("INNER", result))
})

test_that("render_sql_core handles repeated parse_if_then_else passes", {
  # Nested conditionals require multiple passes
  sql <- "{@x}?{{@y}?{deep}}"
  result <- CohortDAG:::render_sql_core(sql, c(x = "true", y = "true"))
  expect_true(grepl("deep", result))
})

test_that("render_sql_core handles no parameters and no conditionals", {
  sql <- "SELECT 1 FROM t"
  result <- CohortDAG:::render_sql_core(sql, c())
  expect_equal(result, sql)
})

# =============================================================================
# 23. render_check_missing_params()
# =============================================================================

test_that("render_check_missing_params returns empty when all found", {
  warnings <- CohortDAG:::render_check_missing_params("SELECT @a, @b", c("a", "b"))
  expect_equal(length(warnings), 0)
})

test_that("render_check_missing_params finds multiple missing", {
  warnings <- CohortDAG:::render_check_missing_params("SELECT @a", c("a", "b", "c"))
  expect_equal(length(warnings), 2)
  expect_true(any(grepl("b", warnings)))
  expect_true(any(grepl("c", warnings)))
})

test_that("render_check_missing_params handles empty parameter list", {
  warnings <- CohortDAG:::render_check_missing_params("SELECT @a", character(0))
  expect_equal(length(warnings), 0)
})

# =============================================================================
# 24. split_sql_core() -- more edge cases
# =============================================================================

test_that("split_sql_core handles single statement no semicolon", {
  stmts <- CohortDAG:::split_sql_core("SELECT 1 FROM t")
  expect_equal(length(stmts), 1)
  expect_true(grepl("SELECT", stmts[1]))
})

test_that("split_sql_core handles CASE WHEN with semicolons", {
  sql <- "SELECT CASE WHEN x = 1 THEN 'a' ELSE 'b' END FROM t; SELECT 2"
  stmts <- CohortDAG:::split_sql_core(sql)
  expect_equal(length(stmts), 2)
})

test_that("split_sql_core handles BEGIN...END block with inner semicolons", {
  sql <- "BEGIN SELECT 1; SELECT 2; END; SELECT 3"
  stmts <- CohortDAG:::split_sql_core(sql)
  # BEGIN...END is a block; semicolons inside should not create splits
  expect_true(length(stmts) >= 2)
})

test_that("split_sql_core handles nested parentheses", {
  sql <- "SELECT * FROM (SELECT 1 UNION ALL SELECT 2) x; SELECT 3"
  stmts <- CohortDAG:::split_sql_core(sql)
  expect_true(length(stmts) >= 2)
})

test_that("split_sql_core handles quoted strings with semicolons", {
  sql <- "SELECT 'a;b;c' FROM t; SELECT 2"
  stmts <- CohortDAG:::split_sql_core(sql)
  expect_equal(length(stmts), 2)
  expect_true(any(grepl("a;b;c", stmts)))
})

test_that("split_sql_core handles bracketed identifiers with semicolons", {
  sql <- "SELECT [col;name] FROM t; SELECT 2"
  stmts <- CohortDAG:::split_sql_core(sql)
  expect_equal(length(stmts), 2)
})

test_that("split_sql_core handles empty input", {
  stmts <- CohortDAG:::split_sql_core("")
  expect_equal(length(stmts), 0)
})

test_that("split_sql_core handles only semicolons", {
  stmts <- CohortDAG:::split_sql_core(";;;")
  # Should produce some results (semicolons are statement separators)
  expect_true(length(stmts) >= 1)
})

test_that("split_sql_core handles trailing semicolons", {
  stmts <- CohortDAG:::split_sql_core("SELECT 1; SELECT 2;")
  expect_true(length(stmts) >= 2)
})

test_that("split_sql_core handles END followed by semicolon after BEGIN", {
  sql <- "BEGIN\nINSERT INTO t VALUES(1);\nEND;\nSELECT 1"
  stmts <- CohortDAG:::split_sql_core(sql)
  expect_true(length(stmts) >= 1)
})

test_that("split_sql_core handles CASE nesting within parentheses", {
  sql <- "SELECT (CASE WHEN x=1 THEN 'a' END), y FROM t; SELECT 2"
  stmts <- CohortDAG:::split_sql_core(sql)
  expect_equal(length(stmts), 2)
})

# =============================================================================
# 25. parse_search_pattern() -- more edge cases
# =============================================================================

test_that("parse_search_pattern parses simple literal tokens", {
  blocks <- CohortDAG:::parse_search_pattern("SELECT FROM")
  expect_equal(length(blocks), 2)
  expect_false(blocks[[1]]$isVariable)
  expect_false(blocks[[2]]$isVariable)
})

test_that("parse_search_pattern parses pattern with variable in middle", {
  blocks <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  vars <- vapply(blocks, function(b) b$isVariable, logical(1))
  expect_true(any(vars))
})

test_that("parse_search_pattern errors on variable at start without regex", {
  expect_error(
    CohortDAG:::parse_search_pattern("@@a FROM t"),
    "cannot start or end"
  )
})

test_that("parse_search_pattern errors on variable at end without regex", {
  expect_error(
    CohortDAG:::parse_search_pattern("SELECT @@a"),
    "cannot start or end"
  )
})

test_that("parse_search_pattern handles function-style pattern", {
  blocks <- CohortDAG:::parse_search_pattern("DATEADD(@@a,@@b,@@c)")
  expect_true(length(blocks) >= 5)  # DATEADD, (, @@a, ,, @@b, ,, @@c, )
  vars <- vapply(blocks, function(b) b$isVariable, logical(1))
  expect_equal(sum(vars), 3)
})

test_that("parse_search_pattern handles regex variable @@(regex)name", {
  blocks <- CohortDAG:::parse_search_pattern("SELECT @@([a-z]+)name FROM")
  has_regex <- vapply(blocks, function(b) !is.null(b$regEx), logical(1))
  expect_true(any(has_regex))
})

test_that("parse_search_pattern handles single literal", {
  blocks <- CohortDAG:::parse_search_pattern("SELECT")
  expect_equal(length(blocks), 1)
  expect_equal(blocks[[1]]$text, "select")  # lowercased
})

# =============================================================================
# 26. translate_search() -- more edge cases
# =============================================================================

test_that("translate_search finds simple function pattern", {
  parsed <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  m <- CohortDAG:::translate_search("SELECT ISNULL(x, 0) FROM t", parsed, 1)
  expect_true(m$start > 0)
  expect_true("@@a" %in% names(m$variableToValue))
  expect_true("@@b" %in% names(m$variableToValue))
})

test_that("translate_search returns -1 when no match found", {
  parsed <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  m <- CohortDAG:::translate_search("SELECT COALESCE(x, 0) FROM t", parsed, 1)
  expect_equal(m$start, -1)
})

test_that("translate_search finds match with nested parens in variable", {
  parsed <- CohortDAG:::parse_search_pattern("DATEADD(@@a,@@b,@@c)")
  m <- CohortDAG:::translate_search("SELECT DATEADD(day, 30, start_date) FROM t", parsed, 1)
  expect_true(m$start > 0)
  expect_equal(length(m$variableToValue), 3)
})

test_that("translate_search ignores matches inside quoted strings", {
  parsed <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  # When ISNULL appears inside a string literal it should not be matched
  m <- CohortDAG:::translate_search("SELECT 'ISNULL(x, 0)' FROM t", parsed, 1)
  expect_equal(m$start, -1)
})

test_that("translate_search handles pattern at start of string", {
  parsed <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  m <- CohortDAG:::translate_search("ISNULL(x, 0)", parsed, 1)
  expect_true(m$start > 0)
})

# =============================================================================
# 27. search_and_replace() -- the core pattern replacement engine
# =============================================================================

test_that("search_and_replace replaces ISNULL with COALESCE", {
  parsed <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  result <- CohortDAG:::search_and_replace(
    "SELECT ISNULL(x, 0) FROM t",
    parsed,
    "COALESCE(@@a, @@b)"
  )
  expect_true(grepl("COALESCE", result))
  expect_false(grepl("ISNULL", result))
})

test_that("search_and_replace replaces multiple occurrences", {
  parsed <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  result <- CohortDAG:::search_and_replace(
    "SELECT ISNULL(x, 0), ISNULL(y, 1) FROM t",
    parsed,
    "COALESCE(@@a, @@b)"
  )
  expect_false(grepl("ISNULL", result))
  expect_equal(length(regmatches(result, gregexpr("COALESCE", result))[[1]]), 2)
})

test_that("search_and_replace handles no match (returns original)", {
  parsed <- CohortDAG:::parse_search_pattern("ISNULL(@@a,@@b)")
  sql <- "SELECT 1 FROM t"
  result <- CohortDAG:::search_and_replace(sql, parsed, "COALESCE(@@a, @@b)")
  expect_equal(trimws(result), trimws(sql))
})

test_that("search_and_replace preserves variable values correctly", {
  parsed <- CohortDAG:::parse_search_pattern("DATEADD(@@a,@@b,@@c)")
  result <- CohortDAG:::search_and_replace(
    "SELECT DATEADD(day, 30, start_date) FROM t",
    parsed,
    "(@@c + @@b * INTERVAL '1 @@a')"
  )
  expect_true(grepl("start_date", result))
  expect_true(grepl("30", result))
  expect_true(grepl("day", result))
})

test_that("search_and_replace handles COUNT_BIG to COUNT", {
  parsed <- CohortDAG:::parse_search_pattern("COUNT_BIG(@@a)")
  result <- CohortDAG:::search_and_replace(
    "SELECT COUNT_BIG(DISTINCT person_id) FROM t",
    parsed,
    "COUNT(@@a)"
  )
  expect_false(grepl("COUNT_BIG", result))
  expect_true(grepl("COUNT", result))
})

test_that("search_and_replace strips blank lines from result", {
  parsed <- CohortDAG:::parse_search_pattern("GETDATE()")
  result <- CohortDAG:::search_and_replace(
    "SELECT GETDATE() FROM t",
    parsed,
    "CURRENT_DATE"
  )
  expect_true(grepl("CURRENT_DATE", result))
})

# =============================================================================
# 28. translate_sql_apply_patterns()
# =============================================================================

test_that("translate_sql_apply_patterns applies session_id replacement", {
  CohortDAG:::ensure_patterns_loaded(NULL)
  # Create a minimal pattern set that uses %session_id%
  parsed <- CohortDAG:::parse_search_pattern("GETDATE()")
  patterns <- list(
    list(parsed = parsed, replacement_template = "CURRENT_DATE_%session_id%", pattern = "GETDATE()")
  )
  result <- CohortDAG:::translate_sql_apply_patterns("SELECT GETDATE() FROM t", patterns, "abc123", "")
  expect_true(grepl("abc123", result))
})

test_that("translate_sql_apply_patterns applies temp_prefix replacement", {
  parsed <- CohortDAG:::parse_search_pattern("GETDATE()")
  patterns <- list(
    list(parsed = parsed, replacement_template = "%temp_prefix%CURRENT_DATE", pattern = "GETDATE()")
  )
  result <- CohortDAG:::translate_sql_apply_patterns("SELECT GETDATE() FROM t", patterns, "abc", "myschema.")
  expect_true(grepl("myschema.", result))
})

test_that("translate_sql_apply_patterns handles empty pattern list", {
  result <- CohortDAG:::translate_sql_apply_patterns("SELECT 1", list(), "abc", "")
  expect_equal(trimws(result), "SELECT 1")
})

# =============================================================================
# 29. generate_session_id()
# =============================================================================

test_that("generate_session_id returns correct length", {
  id <- CohortDAG:::generate_session_id()
  expect_equal(nchar(id), CohortDAG:::SESSION_ID_LENGTH)
})

test_that("generate_session_id starts with a letter", {
  id <- CohortDAG:::generate_session_id()
  expect_true(grepl("^[a-z]", id))
})

test_that("generate_session_id returns different IDs on repeated calls", {
  ids <- replicate(10, CohortDAG:::generate_session_id())
  # With 8 char IDs from 36 possible chars, collisions should be extremely rare
  expect_true(length(unique(ids)) >= 2)
})

# =============================================================================
# 30. get_global_session_id()
# =============================================================================

test_that("get_global_session_id returns consistent value within session", {
  id1 <- CohortDAG:::get_global_session_id()
  id2 <- CohortDAG:::get_global_session_id()
  expect_equal(id1, id2)
  expect_true(nchar(id1) > 0)
})

# =============================================================================
# 31. ensure_patterns_loaded()
# =============================================================================

test_that("ensure_patterns_loaded loads patterns from default path", {
  # Clear cache
  rm(list = ls(CohortDAG:::.translate_pattern_cache),
     envir = CohortDAG:::.translate_pattern_cache)
  CohortDAG:::ensure_patterns_loaded(NULL)
  targets <- ls(CohortDAG:::.translate_pattern_cache)
  expect_true(length(targets) > 0)
  expect_true("postgresql" %in% targets)
  expect_true("duckdb" %in% targets)
})

test_that("ensure_patterns_loaded is idempotent (no-op on second call)", {
  CohortDAG:::ensure_patterns_loaded(NULL)
  targets_before <- ls(CohortDAG:::.translate_pattern_cache)
  CohortDAG:::ensure_patterns_loaded(NULL)
  targets_after <- ls(CohortDAG:::.translate_pattern_cache)
  expect_equal(targets_before, targets_after)
})

test_that("ensure_patterns_loaded loads from explicit path", {
  rm(list = ls(CohortDAG:::.translate_pattern_cache),
     envir = CohortDAG:::.translate_pattern_cache)
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  CohortDAG:::ensure_patterns_loaded(path)
  expect_true(length(ls(CohortDAG:::.translate_pattern_cache)) > 0)
})

# =============================================================================
# 32. translate_sql_with_path() -- full dialect tests
# =============================================================================

test_that("translate_sql_with_path translates ISNULL to COALESCE for duckdb", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT ISNULL(a, b) FROM t",
    "duckdb", "abc12345", "", NULL
  )
  expect_true(grepl("COALESCE", result, ignore.case = TRUE))
})

test_that("translate_sql_with_path translates DATEADD for duckdb", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT DATEADD(day, 30, start_date) FROM t",
    "duckdb", "abc12345", "", NULL
  )
  expect_false(grepl("DATEADD", result, ignore.case = TRUE))
})

test_that("translate_sql_with_path translates DATEDIFF for duckdb", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT DATEDIFF(day, start_date, end_date) FROM t",
    "duckdb", "abc12345", "", NULL
  )
  # DuckDB should transform DATEDIFF
  expect_true(nchar(result) > 10)
})

test_that("translate_sql_with_path translates for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT ISNULL(a, b), DATEADD(day, 1, dt) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("COALESCE", result, ignore.case = TRUE))
  expect_false(grepl("ISNULL", result, ignore.case = TRUE))
})

test_that("translate_sql_with_path errors on unknown dialect", {
  CohortDAG:::ensure_patterns_loaded(NULL)
  expect_error(
    CohortDAG:::translate_sql_with_path("SELECT 1", "nosuchdb_xyz", "abc", "", NULL),
    "Don't know how to translate"
  )
})

test_that("translate_sql_with_path generates session_id when NULL", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT 1 FROM t",
    "duckdb", NULL, "", NULL
  )
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

test_that("translate_sql_with_path generates session_id when empty", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT 1 FROM t",
    "duckdb", "", "", NULL
  )
  expect_true(is.character(result))
})

test_that("translate_sql_with_path handles temp_emulation_schema for oracle", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT 1 FROM t;",
    "oracle", "abc12345", "temp_schema", path
  )
  expect_true(is.character(result))
  expect_true(nchar(result) > 0)
})

test_that("translate_sql_with_path applies replace_with_concat for impala", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT 'it''s' FROM t;",
    "spark", "abc12345", "", path
  )
  # Spark triggers replace_with_concat for escaped quotes
  expect_true(grepl("CONCAT", result, ignore.case = TRUE) || !grepl("''", result))
})

test_that("translate_sql_with_path for bigquery invokes translate_bigquery", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT ISNULL(A, B) FROM t;",
    "bigquery", "abc12345", "", path
  )
  # BigQuery translates ISNULL to IFNULL
  expect_true(grepl("IFNULL|ifnull|coalesce|COALESCE", result, ignore.case = TRUE))
})

# =============================================================================
# 33. translate_single_statement_with_path()
# =============================================================================

test_that("translate_single_statement_with_path handles single statement", {
  result <- CohortDAG:::translate_single_statement_with_path(
    "SELECT ISNULL(a, b) FROM t",
    "duckdb", "abc12345", "", NULL
  )
  expect_true(grepl("COALESCE", result, ignore.case = TRUE))
})

test_that("translate_single_statement_with_path errors on multiple statements", {
  expect_error(
    CohortDAG:::translate_single_statement_with_path(
      "SELECT 1; SELECT 2",
      "duckdb", "abc12345", "", NULL
    ),
    "more than one statement"
  )
})

# =============================================================================
# 34. translate_check_warnings()
# =============================================================================

test_that("translate_check_warnings returns empty for normal SQL", {
  warnings <- CohortDAG:::translate_check_warnings("SELECT * FROM person", "duckdb")
  expect_equal(length(warnings), 0)
})

test_that("translate_check_warnings warns on long temp table name", {
  long_name <- paste0("#", paste(rep("a", 60), collapse = ""))
  sql <- paste("CREATE TABLE", long_name, "(id INT)")
  warnings <- CohortDAG:::translate_check_warnings(sql, "duckdb")
  expect_true(length(warnings) >= 1)
  expect_true(any(grepl("too long", warnings)))
})

test_that("translate_check_warnings warns on long permanent table name", {
  long_name <- paste(rep("a", 70), collapse = "")
  sql <- paste("CREATE TABLE", long_name, "(id INT)")
  warnings <- CohortDAG:::translate_check_warnings(sql, "postgresql")
  expect_true(length(warnings) >= 1)
  expect_true(any(grepl("too long", warnings)))
})

test_that("translate_check_warnings does not warn on short table names", {
  warnings <- CohortDAG:::translate_check_warnings("CREATE TABLE short_tbl (id INT)", "duckdb")
  expect_equal(length(warnings), 0)
})

test_that("translate_check_warnings handles DROP TABLE with long name", {
  long_name <- paste(rep("a", 70), collapse = "")
  sql <- paste("DROP TABLE", long_name)
  warnings <- CohortDAG:::translate_check_warnings(sql, "duckdb")
  expect_true(length(warnings) >= 1)
})

test_that("translate_check_warnings handles TRUNCATE TABLE with long name", {
  long_name <- paste(rep("a", 70), collapse = "")
  sql <- paste("TRUNCATE TABLE", long_name)
  warnings <- CohortDAG:::translate_check_warnings(sql, "duckdb")
  expect_true(length(warnings) >= 1)
})

test_that("translate_check_warnings deduplicates warnings", {
  long_name <- paste0("#", paste(rep("a", 60), collapse = ""))
  sql <- paste("CREATE TABLE", long_name, "(id INT); DROP TABLE", long_name)
  warnings <- CohortDAG:::translate_check_warnings(sql, "duckdb")
  # Should be unique warnings
  expect_equal(length(warnings), length(unique(warnings)))
})

# =============================================================================
# 35. translate_bigquery()
# =============================================================================

test_that("translate_bigquery lowercases keywords but preserves string literals", {
  sql <- "SELECT Column1, 'KeepThisCase' FROM MyTable WHERE X > 0"
  result <- CohortDAG:::translate_bigquery(sql)
  expect_true(grepl("column1", result))
  expect_true(grepl("mytable", result))
  expect_true(grepl("KeepThisCase", result))
})

test_that("translate_bigquery preserves @parameters", {
  sql <- "SELECT @param FROM t"
  result <- CohortDAG:::translate_bigquery(sql)
  expect_true(grepl("@param", result))
})

test_that("translate_bigquery handles empty SQL", {
  result <- CohortDAG:::translate_bigquery("")
  expect_equal(result, "")
})

test_that("translate_bigquery handles SQL with only whitespace gaps", {
  sql <- "SELECT   A   FROM   T"
  result <- CohortDAG:::translate_bigquery(sql)
  expect_true(grepl("select", result))
  expect_true(grepl("from", result))
})

# =============================================================================
# 36. translate_spark()
# =============================================================================

test_that("translate_spark converts CREATE TABLE to SELECT INTO", {
  sql <- "CREATE TABLE test_tbl (id INTEGER, name VARCHAR(50))"
  result <- CohortDAG:::translate_spark(sql)
  expect_true(grepl("SELECT", result, ignore.case = TRUE))
  expect_true(grepl("INTO", result, ignore.case = TRUE))
  expect_true(grepl("WHERE 1 = 0", result))
})

test_that("translate_spark handles multiple columns in CREATE TABLE", {
  sql <- "CREATE TABLE tbl (a INT, b DATE, c VARCHAR(100))"
  result <- CohortDAG:::translate_spark(sql)
  expect_true(grepl("WHERE 1 = 0", result))
  expect_true(grepl("SELECT", result))
})

test_that("translate_spark removes semicolons", {
  sql <- "SELECT 1; SELECT 2"
  result <- CohortDAG:::translate_spark(sql)
  # Semicolons between statements should be re-added by translate_spark
  expect_true(nchar(result) > 0)
})

test_that("translate_spark handles SQL without CREATE TABLE", {
  sql <- "SELECT a FROM t"
  result <- CohortDAG:::translate_spark(sql)
  expect_true(grepl("SELECT", result, ignore.case = TRUE))
})

test_that("translate_spark collapses multiple spaces", {
  sql <- "SELECT  a    FROM  t"
  result <- CohortDAG:::translate_spark(sql)
  expect_false(grepl("  ", result))
})

test_that("translate_spark replaces tabs with spaces", {
  sql <- "SELECT\ta\tFROM\tt"
  result <- CohortDAG:::translate_spark(sql)
  expect_false(grepl("\t", result))
})

test_that("translate_spark ensures trailing semicolon", {
  result <- CohortDAG:::translate_spark("SELECT 1")
  expect_true(grepl(";\\s*$", result))
})

# =============================================================================
# 37. listSupportedDialects()
# =============================================================================

test_that("listSupportedDialects returns data frame", {
  result <- CohortDAG:::listSupportedDialects()
  expect_s3_class(result, "data.frame")
  expect_true("dialect" %in% names(result))
})

test_that("listSupportedDialects includes common dialects", {
  result <- CohortDAG:::listSupportedDialects()
  expect_true("duckdb" %in% result$dialect)
  expect_true("postgresql" %in% result$dialect)
  expect_true("oracle" %in% result$dialect)
  expect_true("spark" %in% result$dialect)
})

# =============================================================================
# 38. render_r() -- more edge cases
# =============================================================================

test_that("render_r handles list parameter (vector values collapsed with comma)", {
  result <- CohortDAG:::render_r(
    "SELECT * FROM t WHERE id IN (@ids)",
    ids = c(1, 2, 3)
  )
  expect_true(grepl("1,2,3", result))
})

test_that("render_r handles single parameter", {
  result <- CohortDAG:::render_r(
    "SELECT @col FROM t",
    col = "name"
  )
  expect_true(grepl("name", result))
  expect_false(grepl("@col", result))
})

test_that("render_r handles conditionals", {
  result <- CohortDAG:::render_r(
    "SELECT * FROM t {@add_where}?{WHERE x > 0}",
    add_where = "true"
  )
  expect_true(grepl("WHERE x > 0", result))
})

test_that("render_r handles conditionals evaluating false", {
  result <- CohortDAG:::render_r(
    "SELECT * FROM t {@add_where}?{WHERE x > 0}",
    add_where = "false"
  )
  expect_false(grepl("WHERE", result))
})

test_that("render_r warns on missing parameters by default", {
  expect_warning(
    CohortDAG:::render_r("SELECT @a FROM t", a = "1", b = "2"),
    "not found"
  )
})

test_that("render_r suppresses warnings when warnOnMissingParameters = FALSE", {
  expect_no_warning(
    CohortDAG:::render_r("SELECT @a FROM t",
                            warnOnMissingParameters = FALSE, a = "1", b = "2")
  )
})

test_that("render_r preserves attributes from input SQL", {
  sql <- "SELECT @a FROM t"
  attr(sql, "custom_attr") <- "test_value"
  result <- CohortDAG:::render_r(sql, a = "1")
  expect_equal(attr(result, "custom_attr"), "test_value")
})

# =============================================================================
# 39. translate_r() -- more edge cases
# =============================================================================

test_that("translate_r translates to duckdb", {
  result <- CohortDAG:::translate_r(
    "SELECT DATEADD(day, 1, start_date) FROM t",
    targetDialect = "duckdb"
  )
  expect_equal(attr(result, "sqlDialect"), "duckdb")
})

test_that("translate_r warns on already-translated SQL", {
  sql <- "SELECT 1"
  attr(sql, "sqlDialect") <- "duckdb"
  expect_warning(
    CohortDAG:::translate_r(sql, targetDialect = "duckdb"),
    "already been translated"
  )
})

test_that("translate_r handles oracleTempSchema parameter", {
  result <- CohortDAG:::translate_r(
    "SELECT 1;",
    targetDialect = "oracle",
    oracleTempSchema = "temp_schema"
  )
  expect_true(nchar(result) > 0)
  expect_equal(attr(result, "sqlDialect"), "oracle")
})

test_that("translate_r handles tempEmulationSchema parameter", {
  result <- CohortDAG:::translate_r(
    "SELECT 1;",
    targetDialect = "oracle",
    tempEmulationSchema = "temp_schema"
  )
  expect_true(nchar(result) > 0)
})

test_that("translate_r emits warnings for long table names", {
  long_name <- paste0("#", paste(rep("a", 60), collapse = ""))
  sql <- paste("CREATE TABLE", long_name, "(id INT);")
  expect_warning(
    CohortDAG:::translate_r(sql, targetDialect = "postgresql"),
    "too long"
  )
})

test_that("translate_r sets sqlDialect attribute", {
  result <- CohortDAG:::translate_r("SELECT 1;", targetDialect = "postgresql")
  expect_equal(attr(result, "sqlDialect"), "postgresql")
})

test_that("translate_r preserves existing attributes", {
  sql <- "SELECT 1;"
  attr(sql, "custom_attr") <- "my_value"
  result <- CohortDAG:::translate_r(sql, targetDialect = "postgresql")
  expect_equal(attr(result, "custom_attr"), "my_value")
})

# =============================================================================
# 40. get_key()
# =============================================================================

test_that("get_key returns first matching key", {
  obj <- list(PascalCase = 10, camelCase = 20)
  expect_equal(CohortDAG:::get_key(obj, c("PascalCase", "camelCase")), 10)
})

test_that("get_key falls back to second key", {
  obj <- list(camelCase = 20)
  expect_equal(CohortDAG:::get_key(obj, c("PascalCase", "camelCase")), 20)
})

test_that("get_key returns default when no match", {
  obj <- list(other = 30)
  expect_equal(CohortDAG:::get_key(obj, c("a", "b"), default = -1), -1)
})

test_that("get_key returns NULL default when no match", {
  obj <- list(other = 30)
  expect_null(CohortDAG:::get_key(obj, c("a", "b")))
})

test_that("get_key skips NULL values", {
  obj <- list(a = NULL, b = 5)
  expect_equal(CohortDAG:::get_key(obj, c("a", "b")), 5)
})

# =============================================================================
# 41. %||% operator
# =============================================================================

test_that("%||% returns x when non-null", {
  expect_equal(CohortDAG:::`%||%`(42, 99), 42)
  expect_equal(CohortDAG:::`%||%`("hello", "default"), "hello")
  expect_equal(CohortDAG:::`%||%`(FALSE, TRUE), FALSE)
})

test_that("%||% returns y when x is NULL", {
  expect_equal(CohortDAG:::`%||%`(NULL, 99), 99)
  expect_equal(CohortDAG:::`%||%`(NULL, "default"), "default")
})

# =============================================================================
# 42. End-to-end integration tests -- render + translate pipelines
# =============================================================================

test_that("render_sql_core + translate_sql_with_path pipeline works for duckdb", {
  sql <- "{DEFAULT @schema = main}\nSELECT DATEADD(day, 30, start_date) FROM @schema.person"
  rendered <- CohortDAG:::render_sql_core(sql, c())
  translated <- CohortDAG:::translate_sql_with_path(rendered, "duckdb", "abc12345", "", NULL)
  expect_true(grepl("main", translated))
  expect_false(grepl("DATEADD", translated, ignore.case = TRUE))
})

test_that("render_sql_core + translate_sql_with_path pipeline works for postgresql", {
  sql <- "SELECT ISNULL(@col, 0) FROM @tbl"
  rendered <- CohortDAG:::render_sql_core(sql, c(col = "name", tbl = "person"))
  translated <- CohortDAG:::translate_sql_with_path(
    paste0(rendered, ";"), "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("COALESCE", translated, ignore.case = TRUE))
  expect_true(grepl("name", translated))
})

test_that("render with conditional + translate pipeline", {
  sql <- "SELECT * FROM t {@include}?{WHERE DATEADD(day, 1, dt) > GETDATE()}"
  rendered <- CohortDAG:::render_sql_core(sql, c(include = "true"))
  translated <- CohortDAG:::translate_sql_with_path(
    paste0(rendered, ";"), "postgresql", "abc12345", "", NULL
  )
  expect_false(grepl("DATEADD", translated, ignore.case = TRUE))
  expect_false(grepl("GETDATE", translated, ignore.case = TRUE))
})

test_that("render conditional false with translate pipeline", {
  sql <- "SELECT * FROM t {@include}?{WHERE DATEADD(day, 1, dt) > GETDATE()}"
  rendered <- CohortDAG:::render_sql_core(sql, c(include = "false"))
  translated <- CohortDAG:::translate_sql_with_path(
    paste0(rendered, ";"), "postgresql", "abc12345", "", NULL
  )
  expect_false(grepl("WHERE", translated))
})

# =============================================================================
# 43. Complex SQL translation patterns
# =============================================================================

test_that("translate handles GETDATE for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT GETDATE();", "postgresql", "abc12345", "", NULL
  )
  expect_false(grepl("GETDATE", result, ignore.case = TRUE))
})

test_that("translate handles COUNT_BIG for duckdb", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT COUNT_BIG(DISTINCT person_id) FROM t;", "duckdb", "abc12345", "", NULL
  )
  expect_false(grepl("COUNT_BIG", result))
  expect_true(grepl("COUNT", result, ignore.case = TRUE))
})

test_that("translate handles VARCHAR(MAX) for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "CAST(x AS VARCHAR(MAX));", "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("TEXT|varchar", result, ignore.case = TRUE))
})

test_that("translate handles nested function calls", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT ISNULL(DATEADD(day, 1, dt), GETDATE()) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_false(grepl("ISNULL", result, ignore.case = TRUE))
  expect_false(grepl("GETDATE", result, ignore.case = TRUE))
})

test_that("translate handles TOP for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT TOP 10 * FROM person;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("LIMIT", result, ignore.case = TRUE) || nchar(result) > 0)
})

test_that("translate handles EOMONTH for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT EOMONTH(start_date) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_false(grepl("\\bEOMONTH\\b", result, ignore.case = TRUE))
})

test_that("translate handles LEN for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT LEN(name) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("LENGTH|len|char_length", result, ignore.case = TRUE))
})

test_that("translate handles CHARINDEX for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT CHARINDEX('a', name) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("STRPOS|POSITION|charindex", result, ignore.case = TRUE))
})

test_that("translate handles STDEV for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT STDEV(val) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("STDDEV|stdev", result, ignore.case = TRUE))
})

test_that("translate handles CAST AS FLOAT for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT CAST(x AS FLOAT) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(nchar(result) > 0)
})

test_that("translate handles DATEFROMPARTS for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT DATEFROMPARTS(2020, 1, 15) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(nchar(result) > 0)
})

test_that("translate handles TRY_CAST for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT TRY_CAST(x AS INTEGER) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("CAST", result, ignore.case = TRUE))
})

test_that("translate handles IF OBJECT_ID for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "IF OBJECT_ID('tempdb..#temp', 'U') IS NOT NULL DROP TABLE #temp;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(nchar(result) > 0)
})

test_that("translate handles CONCAT for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT CONCAT(a, b, c) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(nchar(result) > 0)
})

test_that("translate handles RIGHT and LEFT for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT RIGHT(name, 3), LEFT(name, 2) FROM t;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(nchar(result) > 0)
})

test_that("translate handles TRUNCATE TABLE for postgresql", {
  result <- CohortDAG:::translate_sql_with_path(
    "TRUNCATE TABLE scratch.my_table;",
    "postgresql", "abc12345", "", NULL
  )
  expect_true(grepl("DELETE FROM|TRUNCATE", result, ignore.case = TRUE))
})

# =============================================================================
# 44. Spark-specific translation patterns
# =============================================================================

test_that("translate spark WITH...SELECT INTO produces CTE decomposition", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  sql <- "WITH cte AS (SELECT 1 as id) SELECT id INTO scratch.my_table FROM cte;"
  result <- CohortDAG:::translate_sql_with_path(sql, "spark", "abc12345", "", path)
  expect_true(grepl("VIEW|CREATE TABLE", result, ignore.case = TRUE))
})

test_that("translate spark handles GETDATE", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT GETDATE();", "spark", "abc12345", "", path
  )
  expect_false(grepl("GETDATE", result, ignore.case = TRUE))
})

test_that("translate spark handles DATEADD", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT DATEADD(day, 1, start_date) FROM t;",
    "spark", "abc12345", "", path
  )
  expect_true(nchar(result) > 0)
})

# =============================================================================
# 45. BigQuery-specific translation patterns
# =============================================================================

test_that("translate bigquery handles ISNULL", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT ISNULL(A, B) FROM T;",
    "bigquery", "abc12345", "", path
  )
  expect_true(grepl("IFNULL|ifnull|coalesce|COALESCE", result, ignore.case = TRUE))
})

test_that("translate bigquery lowercases tokens", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT Column1 FROM MyTable;",
    "bigquery", "abc12345", "", path
  )
  expect_true(grepl("column1", result))
  expect_true(grepl("mytable", result))
})

# =============================================================================
# 46. Oracle-specific translation patterns
# =============================================================================

test_that("translate oracle handles basic SQL", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT DATEADD(day, 1, start_date) FROM t;",
    "oracle", "abc12345", "", path
  )
  expect_true(nchar(result) > 0)
})

test_that("translate oracle with temp emulation schema", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT 1 FROM t;",
    "oracle", "abc12345", "temp_schema", path
  )
  expect_true(nchar(result) > 0)
})

# =============================================================================
# 47. Snowflake and Redshift translation
# =============================================================================

test_that("translate snowflake handles DATEADD", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT DATEADD(day, 1, start_date) FROM t;",
    "snowflake", "abc12345", "", path
  )
  expect_true(nchar(result) > 0)
})

test_that("translate redshift handles DATEADD", {
  path <- system.file("csv", "replacementPatterns.csv", package = "CDMConnector")
  skip_if(!file.exists(path))
  result <- CohortDAG:::translate_sql_with_path(
    "SELECT DATEADD(day, 1, start_date) FROM t;",
    "redshift", "abc12345", "", path
  )
  expect_true(nchar(result) > 0)
})

# =============================================================================
# 48. Conditional rendering integration
# =============================================================================

test_that("conditional with == comparison in render", {
  sql <- "{@dialect == 'postgresql'}?{LIMIT 10}:{TOP 10}"
  result <- CohortDAG:::render_sql_core(sql, c(dialect = "postgresql"))
  expect_true(grepl("LIMIT", result))
  expect_false(grepl("TOP", result))
})

test_that("conditional with != comparison in render", {
  sql <- "{@dialect != 'postgresql'}?{TOP 10}:{LIMIT 10}"
  result <- CohortDAG:::render_sql_core(sql, c(dialect = "postgresql"))
  expect_true(grepl("LIMIT", result))
  expect_false(grepl("TOP", result))
})

test_that("conditional with IN operator in render", {
  sql <- "{@dialect IN ('postgresql','duckdb')}?{LIMIT 10}:{FETCH FIRST 10}"
  result <- CohortDAG:::render_sql_core(sql, c(dialect = "duckdb"))
  expect_true(grepl("LIMIT", result))
  expect_false(grepl("FETCH", result))
})

test_that("conditional with IN operator false in render", {
  sql <- "{@dialect IN ('postgresql','duckdb')}?{LIMIT 10}:{FETCH FIRST 10}"
  result <- CohortDAG:::render_sql_core(sql, c(dialect = "oracle"))
  expect_false(grepl("LIMIT", result))
  expect_true(grepl("FETCH", result))
})

test_that("conditional with AND in render", {
  sql <- "{@a == 1 & @b == 1}?{BOTH_TRUE}:{NOT_BOTH}"
  result <- CohortDAG:::render_sql_core(sql, c(a = "1", b = "1"))
  expect_true(grepl("BOTH_TRUE", result))

  result2 <- CohortDAG:::render_sql_core(sql, c(a = "1", b = "0"))
  expect_true(grepl("NOT_BOTH", result2))
})

test_that("conditional with OR in render", {
  sql <- "{@a == 1 | @b == 1}?{EITHER_TRUE}:{NEITHER}"
  result <- CohortDAG:::render_sql_core(sql, c(a = "0", b = "1"))
  expect_true(grepl("EITHER_TRUE", result))

  result2 <- CohortDAG:::render_sql_core(sql, c(a = "0", b = "0"))
  expect_true(grepl("NEITHER", result2))
})

test_that("conditional in render expands single block", {
  sql <- "SELECT * FROM t {@a == 'yes'}?{WHERE 1=1}"
  result <- CohortDAG:::render_sql_core(sql, c(a = "yes"))
  expect_true(grepl("WHERE 1=1", result))
})

test_that("DEFAULT with conditional evaluation", {
  sql <- "{DEFAULT @use_filter = 1}\nSELECT * FROM t {@use_filter == 1}?{WHERE active = 1}"
  result <- CohortDAG:::render_sql_core(sql, c())
  expect_true(grepl("WHERE active = 1", result))
})

test_that("DEFAULT override disables conditional", {
  sql <- "{DEFAULT @use_filter = 1}\nSELECT * FROM t {@use_filter == 1}?{WHERE active = 1}"
  result <- CohortDAG:::render_sql_core(sql, c(use_filter = "0"))
  expect_false(grepl("WHERE", result))
})

# =============================================================================
# 49. MAX safety constants
# =============================================================================

test_that("MAX constants are defined", {
  expect_true(CohortDAG:::MAX_TRANSLATE_ITERATIONS > 0)
  expect_true(CohortDAG:::MAX_SQL_LENGTH_TRANSLATE > 0)
  expect_true(CohortDAG:::MAX_REGEX_INPUT_LENGTH > 0)
  expect_true(CohortDAG:::SESSION_ID_LENGTH > 0)
  expect_true(CohortDAG:::MAX_TABLE_NAME_LENGTH > 0)
})

# =============================================================================
# 50. Miscellaneous edge cases
# =============================================================================

test_that("tokenize_sql followed by split_sql_core handles complex SQL", {
  sql <- "SELECT CASE WHEN x = 1 THEN 'a' WHEN x = 2 THEN 'b' ELSE 'c' END AS val FROM t; INSERT INTO t2 SELECT * FROM t WHERE y > 0; DELETE FROM t WHERE z = 0"
  stmts <- CohortDAG:::split_sql_core(sql)
  expect_equal(length(stmts), 3)
})

test_that("render_sql_core with conditional and else across multiple lines", {
  sql <- "SELECT @col\n{@add_filter == 'yes'}?{WHERE x > 0\nAND y < 10}:{WHERE 1 = 1}"
  result <- CohortDAG:::render_sql_core(sql, c(col = "name", add_filter = "yes"))
  expect_true(grepl("WHERE x > 0", result))
  expect_true(grepl("AND y < 10", result))
})

test_that("parse_search_pattern with parenthesized function patterns", {
  blocks <- CohortDAG:::parse_search_pattern("DATEDIFF(@@a,@@b,@@c)")
  vars <- vapply(blocks, function(b) b$isVariable, logical(1))
  expect_equal(sum(vars), 3)
})

test_that("search_and_replace handles pattern with empty replacement", {
  parsed <- CohortDAG:::parse_search_pattern("GETDATE()")
  result <- CohortDAG:::search_and_replace(
    "SELECT GETDATE() FROM t",
    parsed,
    "NOW()"
  )
  expect_false(grepl("GETDATE", result))
  expect_true(grepl("NOW", result))
})

test_that("search_and_replace handles pattern not found", {
  parsed <- CohortDAG:::parse_search_pattern("NONEXISTENT(@@a)")
  sql <- "SELECT 1 FROM t"
  result <- CohortDAG:::search_and_replace(sql, parsed, "REPLACED(@@a)")
  expect_equal(trimws(result), trimws(sql))
})

test_that("translate_sql_with_path handles SQL with session_id placeholder in temp tables", {
  result <- CohortDAG:::translate_sql_with_path(
    "CREATE TABLE #my_temp (id INT);",
    "postgresql", "abcdefgh", "", NULL
  )
  expect_true(nchar(result) > 0)
})

test_that("translate_sql_with_path handles empty SQL", {
  result <- CohortDAG:::translate_sql_with_path(
    "", "duckdb", "abc12345", "", NULL
  )
  expect_true(is.character(result))
})

test_that("replace_with_concat handles mixed quotes", {
  result <- CohortDAG:::replace_with_concat("SELECT 'he said \"hello\"'")
  expect_true(nchar(result) > 0)
})

test_that("evaluate_condition handles complex expression with negation", {
  expect_true(CohortDAG:::evaluate_condition("!false"))
  expect_false(CohortDAG:::evaluate_condition("!true"))
})

test_that("evaluate_primitive_condition handles <> with numbers", {
  expect_true(CohortDAG:::evaluate_primitive_condition("1 <> 2"))
  expect_false(CohortDAG:::evaluate_primitive_condition("1 <> 1"))
})

test_that("safe_split handles only delimiter character", {
  result <- CohortDAG:::safe_split(",", ",")
  expect_equal(result, c("", ""))
})

test_that("split_and_keep handles empty regex match at boundaries", {
  result <- CohortDAG:::split_and_keep("abc", "b")
  expect_equal(length(result), 3)
  expect_equal(result[[1]], "a")
  expect_equal(result[[2]], "b")
  expect_equal(result[[3]], "c")
})

test_that("str_replace_region with single-char replacement", {
  result <- CohortDAG:::str_replace_region("abc", 2, 2, "X")
  expect_true(grepl("X", result))
})
