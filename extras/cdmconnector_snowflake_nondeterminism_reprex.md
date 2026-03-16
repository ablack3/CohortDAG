# CDMConnector Snowflake Nondeterminism Reprex

Script:

- `extras/cdmconnector_snowflake_nondeterminism_reprex.R`

Run:

```sh
Rscript extras/cdmconnector_snowflake_nondeterminism_reprex.R
```

Optional env vars:

- `CDMCONNECTOR_REPRO_COHORTS`
  - comma-separated cohort basenames from `inst/cohorts`
  - default: `acute_respiratory_failure_in_inpatient_or_emergency_room,acute_urinary_tract_infections_uti`
- `CDMCONNECTOR_REPRO_REPEATS`
  - repeated `generateCohortSet()` runs per cohort
  - default: `2`
- `CDMCONNECTOR_REPRO_SAMPLE_N`
  - max diff rows written per pair
  - default: `20`
- `CDMCONNECTOR_REPRO_COHORT_DIR`
  - default: `inst/cohorts`

Required Snowflake env vars:

- `SNOWFLAKE_CONNECTION_STRING`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_CDM_SCHEMA`
- `SNOWFLAKE_SCRATCH_SCHEMA`

What it does:

1. Connects to Snowflake with `DatabaseConnector`.
2. Builds a CDM object with `CDMConnector::cdmFromCon()`.
3. Reads one cohort JSON with `CDMConnector::readCohortSet()`.
4. Runs `CDMConnector::generateCohortSet()` repeatedly for the exact same cohort on the exact same data.
5. Collects the resulting cohort table after each run.
6. Compares row sets across runs.

Artifacts:

- `inst/benchmark-results/cdmconnector_snowflake_reprex_<timestamp>/cdmconnector_reprex_summary.csv`
- `inst/benchmark-results/cdmconnector_snowflake_reprex_<timestamp>/cdmconnector_reprex_rowdiff_sample.csv`

Interpretation:

- If repeated runs differ here, the nondeterminism is reproducible through `CDMConnector::generateCohortSet()` on Snowflake.
- This is the current strongest repro path for the instability we observed in the live benchmark.
