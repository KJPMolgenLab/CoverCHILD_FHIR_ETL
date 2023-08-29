#!/usr/bin/env Rscript

# CoverCHILD FHIR ETL (JSON FHIR-Bundles -> normal-form list of DFs)
# @author: simeonplatte
# @date: 2023-07-28

# setup ----------------------------------------------------------------------------------------------------------------
options(warn = 1L)

## load & install ----
source("code/functions.R")
inst_pkgs("config", "httr")
load_inst_pkgs("tidyverse", "magrittr", "fhircrackr", "data.table", "tictoc", "rlang")

cfg <- config::get(file = file.path("config", "fhir_cfg.yml"))
search_cfg <- config::get(file = file.path("config", "fhir_search_cfg.yml"))
# separate config entries so fhir_url() can check the elements
search_cfg$named_params <-
  map(search_cfg$parameters, \(x) {
    if (is.null(x)) NULL else deframe(separate_wider_delim(as_tibble(x), value, "=", names = c("name", "value")))
  })
# extract base elements from selected elements to query
if (!cfg$get_all_elements) search_cfg$base_elements <- map(search_cfg$elements, \(x) unique(str_split_i(x, "/", 1)))

# create filepath for logging of timings
tlog_path <- file.path(cfg$log_dir, paste0("FHIR_timings_", format(Sys.time(), "%y%m%d-%H%M"), ".csv"))

## network settings ----
# proxy
walk(c("http_proxy", "https_proxy", "no_proxy"),
     \(x) if (!is.null(cfg[[x]])) do.call(Sys.setenv, set_names(list(cfg[[x]]), x)))
# ssl peer verification
if (!cfg$ssl_verify_peer) httr::set_config(httr::config(ssl_verifypeer = 0L))


#TODO: use parallel backend, e.g. doParallel for parallelization of searches
# - add config options for parallelization
# - Where will these scripts be executed? Single node only?

#TODO: add 'do_search_from_step' to build on earlier search steps which were saved to disc

## check config ----
#TODO: protect config yml against type errors & provide default fallback values.
#      e.g. rectify bools with easyr::tobool

ctic("Run FHIR ETL.")
cat("Timings of this FHIR ETL run will be saved to:", tlog_path, "\n")


# retrieve & crack bundles  --------------------------------------------------------------------------------------------
ctic("Retrieve FHIR bundles (load or sequential searches).")
#TODO: update do_search logic
# if (!cfg$do_search) fhir_bundles <- fhir_load(cfg$filebase, pattern = "\\.xml$")

# gather all fhir_urls and downloaded bundles from subsequent searches
#TODO: Move to disk if necessary
fhir_search_urls <- list()
fhir_bundles <- list()
fhir_dfs <- list()
# molten_dfs <- list()
# out_dfs <- list()

## FHIR search 1: encounters ----
ctic("FHIR search 1: encounters")
this_search <- "encounter"

# url & search
ctic("FHIR search 1: encounters - Download & crack bundles")
fhir_search_urls[[this_search]] <- fhir_url_w_cfg(search_name = this_search)
fhir_page_count <- 1
save_path <- file.path(cfg$tmp_dir, "FHIR_DFs", paste0(this_search, "_", format(Sys.time(), "%y%m%d")))
dir.create(save_path, showWarnings = FALSE, recursive = TRUE)
while(is_useful_string(fhir_search_urls[[this_search]][["url"]])) {
  ctic(str_glue("FHIR search 1: encounters - Download bundle set {fhir_page_count}"))
  fhir_bundles[[this_search]] <- fhir_search_w_cfg(search_name = this_search)
  ctoc_log(save = tlog_path)

  # crack encounters
  ctic(str_glue("FHIR search 1: encounters - Crack bundle set {fhir_page_count}"))
  fhir_dfs[[this_search]] <- fhir_crack_w_cfg(search_name = this_search)
  gc(); gc()
  ctoc_log(save = tlog_path)

  saveRDS(fhir_dfs[[this_search]], file.path(save_path, paste0("DF_", this_search, "_", fhir_page_count, ".rds")))

  fhir_search_urls[[this_search]][["url"]] <- fhir_next_bundle_url()
  if (is_useful_string(fhir_search_urls[[this_search]][["url"]])) {
    names(fhir_search_urls[[this_search]][["url"]]) <- this_search
  }
  fhir_search_urls[[this_search]][["body"]] <- NULL
  fhir_page_count <- fhir_page_count + 1
  # if (fhir_page_count > 2) break
}
fhir_bundles[[this_search]] <- NULL
gc(); gc()
ctoc_log(save = tlog_path)

# load downloaded FHIR DFs
ctic("FHIR search 1: encounters - Load & combine downloaded DFs")
fhir_dfs[[this_search]] <- map(Sys.glob(file.path(save_path, "DF_encounter_*.rds")), readRDS) %>%
  bind_rows()
ctoc_log()

# melt encounters
ctic("FHIR search 1: encounters - Melt DF, extract subject IDs")
fhir_dfs[[this_search]] <- fhir_melt_loop_w_cfg(search_name = this_search)

# remove reference prefixes
fhir_dfs[[this_search]] <- fhir_dfs[[this_search]] %>%
  {mutate(., across(contains("reference"), \(x) remove_ref_prefix(x, resource = attr(., "search_name"))))}
# extract subject ids for next FHIR search
encounter_subject_id_string <- fhir_dfs[[this_search]]$subject.reference %>%
  {if_else(str_detect(., "/"), NA, .)} %>%
  na.omit() %>%
  unique() %>%
  paste0(collapse = ",")
gc(); gc()
ctoc_log(save = tlog_path)
ctoc_log(save = tlog_path)

## FHIR search 2: patients belonging to encounters ----
ctic("FHIR search 2: patients")
this_search <- "patient"

# url & search
ctic("FHIR search 2: patients - Download bundles")
fhir_search_urls[[this_search]] <- fhir_url_w_cfg(search_name = this_search,
                                                  parameters = c("_id" = encounter_subject_id_string))
fhir_bundles[[this_search]] <- fhir_search_w_cfg(search_name = this_search)
gc(); gc()
ctoc_log(save = tlog_path)

# crack patients
ctic("FHIR search 2: patients - Crack bundles")
fhir_dfs[[this_search]] <- fhir_crack_w_cfg(search_name = this_search)
fhir_bundles[[this_search]] <- NULL
gc(); gc()
ctoc_log(save = tlog_path)

# melt patients
ctic("FHIR search 2: patients - Melt DF")
fhir_dfs[[this_search]] <- fhir_melt_loop_w_cfg(search_name = this_search)
# remove reference prefixes
fhir_dfs[[this_search]] <- fhir_dfs[[this_search]] %>%
  {mutate(., across(contains("reference"), \(x) remove_ref_prefix(x, resource = attr(., "search_name"))))}
gc(); gc()
ctoc_log(save = tlog_path)
ctoc_log(save = tlog_path)

## filter encounters & patients to age criterion ----
underage_patients <- fhir_dfs$patient %>%
  select(patient_id = id, birthDate) %>%
  distinct() %>%
  inner_join(fhir_dfs$encounter %>% select(encounter_id = id, period.start, subject.reference) %>% distinct(),
             by = c(patient_id = "subject.reference")) %>%
  mutate(is_underage = as_date(period.start) < (as_date(birthDate) + years(18))) %>%
  filter(is_underage)
encounter_ids_to_keep <- unique(na.omit(underage_patients$encounter_id))
# extract encounter ids for next FHIR searches
encounter_ids_to_keep_string <- paste0(encounter_ids_to_keep, collapse = ",")
# encounter_ids_to_keep_string <- paste0("Encounter/", encounter_ids_to_keep, collapse = ",")

# filter encounter DF
fhir_dfs$encounter %<>% filter(id %in% encounter_ids_to_keep)
# filter patient DF
fhir_dfs$patient %<>% filter(id %in% unique(underage_patients$patient_id))
gc(); gc()

## FHIR search 3 & 4: conditions / procedures ----
fhir_searches <- c("condition", "procedure")
for (i in seq_along(fhir_searches)) {
  this_search <- fhir_searches[[i]]
  ctic(str_glue("FHIR search {i+2}: {this_search}s"))

  # url & search
  ctic(str_glue("FHIR search {i+2}: {this_search}s - Download bundles"))
  fhir_search_urls[[this_search]] <- fhir_url_w_cfg(search_name = this_search,
                                                    parameters = c("encounter" = encounter_ids_to_keep_string))
  fhir_bundles[[this_search]] <- fhir_search_w_cfg(search_name = this_search)
  gc(); gc()
  ctoc_log(save = tlog_path)

  # crack
  ctic(str_glue("FHIR search {i+2}: {this_search}s - Crack bundles"))
  fhir_dfs[[this_search]] <- fhir_crack_w_cfg(search_name = this_search)
  fhir_bundles[[this_search]] <- NULL
  gc(); gc()
  ctoc_log(save = tlog_path)

  # melt
  ctic(str_glue("FHIR search {i+2}: {this_search}s - Melt DF"))
  fhir_dfs[[this_search]] <- fhir_melt_loop_w_cfg(search_name = this_search)
  gc(); gc()
  ctoc_log(save = tlog_path)
  ctoc_log(save = tlog_path)
}
ctoc_log(save = tlog_path)

# bring dfs to global env for easier inspection
# for (x in names(fhir_dfs)) assign(paste0("df_", x), fhir_dfs[[x]])

if (cfg$clear_tmp_dir) file.remove(Sys.glob(file.path(save_path, "*")))
ctoc_log(save = tlog_path)
