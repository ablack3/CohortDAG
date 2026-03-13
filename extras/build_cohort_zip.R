# Build inst/cohorts.zip from the archive cohort JSON files.
# Run this script from the package root directory.
#
# Usage:
#   Rscript extras/build_cohort_zip.R

source_dir <- file.path("_archive", "AtlasCohortGenerator", "inst", "cohorts")
if (!dir.exists(source_dir)) {
  stop("Source directory not found: ", source_dir,
       "\nRun this script from the CohortDAG package root.")
}

json_files <- list.files(source_dir, pattern = "\\.json$", full.names = FALSE)
message(sprintf("Found %d JSON files in %s", length(json_files), source_dir))

out_zip <- file.path("inst", "cohorts.zip")
dir.create("inst", showWarnings = FALSE)

# zip from within the source directory so paths inside the zip are flat
old_wd <- setwd(source_dir)
on.exit(setwd(old_wd), add = TRUE)

zip_path <- normalizePath(file.path(old_wd, out_zip), mustWork = FALSE)
if (file.exists(zip_path)) file.remove(zip_path)

utils::zip(zip_path, files = json_files, flags = "-q9")
setwd(old_wd)

info <- file.info(out_zip)
message(sprintf("Created %s (%.1f MB, %d files)",
                out_zip, info$size / 1e6, length(json_files)))
