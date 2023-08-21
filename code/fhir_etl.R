#!/usr/bin/env Rscript

# CoverCHILD FHIR ETL (JSON FHIR-Bundles -> normal-form list of DFs)
# @author: simeonplatte
# @date: 2023-07-28

# setup ----------------------------------------------------------------------------------------------------------------
## load & install ----
source("code/functions.R")
inst_pkgs("config", "httr")
load_inst_pkgs("tidyverse", "magrittr", "fhircrackr", "data.table", "tictoc")

cfg <- config::get(file = file.path("config", "fhir_cfg.yml"))
search_cfg <- config::get(file = file.path("config", "fhir_search_cfg.yml"))

tlog_path <- file.path("output", paste0("FHIR_timings_", format(Sys.time(), "%y%m%d-%H%M"), ".csv"))

## network settings ----
# proxy
walk(c("http_proxy", "https_proxy", "no_proxy"),
     \(x) if (!is.null(cfg[[x]])) do.call(Sys.setenv, set_names(list(cfg[[x]]), x)))
# ssl peer verification
if (!cfg$ssl_verify_peer) httr::set_config(httr::config(ssl_verifypeer = 0L))


#TODO: use parallel backend, e.g. doParallel for parallelization of searches
# - add config options for parallelization
# - Where will these scripts be executed? Single node only?

## check config ----
#TODO: protect config yml against type errors & provide default fallback values.
#      e.g. rectify bools with easyr::tobool


ctic("Run FHIR ETL.")
# retrieve & crack bundles  --------------------------------------------------------------------------------------------
## retrieve FHIR bundles (load or search) ----
ctic("Retrieve FHIR bundles (load or search).")
if (cfg$do_search) {
  # separate config entries so fhir_url() can check the elements
  search_cfg$named_params <-
    map(search_cfg$resource, \(x) deframe(separate_wider_delim(as_tibble(x), value, "=", names = c("name", "value"))))
  # extract base elements from selected elements to query
  if (!cfg$get_all_elements) search_cfg$base_elements <- map(search_cfg$elements, \(x) unique(str_split_i(x, "/", 1)))

  # generate FHIR search URLs
  fhir_search_urls <- imap(search_cfg$named_params, \(query_parts, resource_name) {
    fhir_url(url = cfg$serverbase,
             resource = str_to_title(resource_name),
             parameters = c(query_parts,
                            "_elements" = if (!cfg$get_all_elements) paste0(search_cfg$base_elements[[resource_name]],
                                                                            collapse = ","),
                            "_count" = cfg$max_bundle_size))
    })

  # run FHIR search(es)
  fhir_bundles <- map(fhir_search_urls, \(fhir_search_url) fhir_search(request = fhir_search_url,
                                                                       username = cfg$username,
                                                                       password = cfg$password,
                                                                       token = cfg$token,
                                                                       max_bundles = cfg$max_bundles,
                                                                       verbose = 2))
  fhir_bundles <- fhir_bundle_list(unlist(fhir_bundles, recursive = FALSE))
} else {
  fhir_bundles <- fhir_load(cfg$filebase, pattern = "\\.xml$")
}
ctoc_log(save = tlog_path)
gc()
gc()

## crack ----
# create FHIR design
fhir_elements <- fhir_design(imap(
  search_cfg$elements,
  \(elements, resource_name) fhir_table_description(resource = str_to_title(resource_name),
                                                    cols = if (!cfg$keep_all_elements) elements)))

ctic("Crack FHIR bundles.")
fhir_dfs <- fhir_crack(bundles = fhir_bundles,
                       design = fhir_elements,
                       sep = cfg$sep,
                       brackets = cfg$brackets,
                       rm_empty_cols = FALSE,
                       ncores = cfg$n_cores)
ctoc_log(save = tlog_path)
gc()
gc()

# melt
#TODO: This needs debugging/expansion, if multiple melt-groups per DF are present
#      or when grouping attribute is not on level 1.
ctic("Melt FHIR DFs.")
molten_dfs <- imap(fhir_dfs, \(df, df_name) {
  melt_groups <- find_melt_col_groups(df, cfg$brackets)
  if (length(melt_groups) > 1) warning(str_glue("DF '{df_name}' has more than 1 melt group: ",
                                                "{paste0(names(melt_groups), collapse = ', ')}. Inspect results!"))
  for (melt_group in names(melt_groups)) {
    cat(str_glue("Melting DF '{df_name}', group from attribute '{melt_group}':"),
                 paste0(melt_groups[[melt_group]], collapse = ", "), "\n")
    df <- fhir_melt(df,
                    melt_groups[[melt_group]],
                    brackets = cfg$brackets,
                    sep = cfg$sep,
                    id_name = paste0("resource_id_", melt_group),
                    all_columns = TRUE)
    }
  fhir_rm_indices(df, brackets = cfg$brackets)
  })
ctoc_log(save = tlog_path)
gc()
gc()

# bring dfs to global env for easier inspection
for (x in names(molten_dfs)) assign(paste0("df_", x), fhir_dfs[[x]])

ctoc_log(save = tlog_path)
