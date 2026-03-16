# CirceR Snowflake Nondeterminism Reprex

Script:

- `extras/circe_snowflake_nondeterminism_reprex.R`

Run:

```sh
Rscript extras/circe_snowflake_nondeterminism_reprex.R
```

Optional env vars:

- `CIRCE_REPRO_COHORTS`
  - comma-separated cohort basenames from `inst/cohorts`
  - default: `acute_respiratory_failure_in_inpatient_or_emergency_room,acute_urinary_tract_infections_uti`
- `CIRCE_REPRO_REPEATS`
  - number of repeated executions of the exact same Circe-generated SQL
  - default: `2`
- `CIRCE_REPRO_SAMPLE_N`
  - max diff rows written per pair
  - default: `20`
- `CIRCE_REPRO_COHORT_DIR`
  - default: `inst/cohorts`

Required Snowflake env vars:

- `SNOWFLAKE_CONNECTION_STRING`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_CDM_SCHEMA`
- `SNOWFLAKE_SCRATCH_SCHEMA`
- optional: `SNOWFLAKE_VOCAB_SCHEMA`

What it does:

1. Reads an ATLAS cohort JSON.
2. Builds SQL with `CirceR::buildCohortQuery()`.
3. Renders/translates it with `SqlRender` for Snowflake.
4. Connects with `DatabaseConnector`.
5. Executes the exact same translated SQL repeatedly against the same target table.
6. Compares the inserted cohort rows across runs.

Artifacts:

- `inst/benchmark-results/circe_snowflake_reprex_<timestamp>/circe_reprex_summary.csv`
- `inst/benchmark-results/circe_snowflake_reprex_<timestamp>/circe_reprex_rowdiff_sample.csv`
- one translated SQL file per cohort
- one `row_number()` snippet file per cohort

Interpretation:

- If repeated runs differ here, that is strong evidence of nondeterminism in the direct `CirceR + SqlRender` execution path on Snowflake.
- If repeated runs are identical here but `CDMConnector::generateCohortSet()` is not stable, then the nondeterminism is likely in `CDMConnector`'s execution/wrapping path rather than in bare `CirceR`.
