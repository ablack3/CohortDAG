# CohortDAG

> **Experimental software:** CohortDAG is experimental software and should be treated as such in production or analytical workflows.

`CohortDAG` generates OMOP CDM cohorts from ATLAS-style cohort definitions using a DAG-based batch execution plan.

## Attributions

This work relies heavily on [OHDSI/circe-be](https://github.com/OHDSI/circe-be),
by Chris Knoll and others, as well as
[OHDSI/SqlRender](https://github.com/OHDSI/SqlRender), by Martijn Schuemie and
others, and [OHDSI/circepy](https://github.com/OHDSI/circepy) by `@azimov`.
This package was generated using AI coding tools and is experimental code.

## Install from GitHub

```r
install.packages(c("remotes", "CDMConnector", "duckdb", "dplyr"))
remotes::install_github("darwin-eu/CohortDAG")
```

## Example

The example below creates a `cdm` object with `CDMConnector`, reads example cohort definitions bundled with `CDMConnector`, and generates cohorts with `generateCohortSet2()`.

```r
library(CDMConnector)
library(CohortDAG)
library(dplyr)

con <- DBI::dbConnect(duckdb::duckdb(), eunomiaDir())
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

cdm <- cdmFromCon(
  con = con,
  cdmSchema = "main",
  writeSchema = "main"
)

cohort_set <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))

cdm <- generateCohortSet2(
  cdm = cdm,
  cohortSet = cohort_set,
  name = "example_cohort"
)

generated_cohorts <- collect(cdm$example_cohort)
generated_cohorts

cohortCount(cdm$example_cohort)
attrition(cdm$example_cohort)
settings(cdm$example_cohort)
```

`name` must be lowercase, start with a letter, and contain only letters, numbers, and underscores.
