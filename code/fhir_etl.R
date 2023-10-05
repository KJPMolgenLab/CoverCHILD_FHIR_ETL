#!/usr/bin/env Rscript

# CoverCHILD FHIR ETL (JSON FHIR-Bundles -> normal-form list of DFs)
# @author: simeonplatte
# @date: 2023-07-28

# setup ----------------------------------------------------------------------------------------------------------------
options(warn = 1L)

## load & install ----
source("code/functions.R")
inst_pkgs("config", "httr")
load_inst_pkgs("tidyverse", "lubridate", "magrittr", "fhircrackr", "data.table", "tictoc", "rlang")

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


# retrieve, crack & melt bundles  --------------------------------------------------------------------------------------
ctic("Retrieve, crack, melt FHIR data (load or sequential searches).")
#TODO: update do_search logic
# if (!cfg$do_search) {
#   loaded_df_encounter <- fhir_load(cfg$filebase, pattern = "\\.xml$") %>%
#     fhir_crack_w_cfg(resource = "Encounter") %>%
#     fhir_melt_loop_w_cfg()
# }

# gather all fhir_urls and downloaded bundles from subsequent searches
fhir_search_urls <- list()
fhir_dfs <- list()

## FHIR search 1: encounters ----
ctic("FHIR search 1: encounters")
this_search <- "encounter"
# url & batched search
fhir_search_urls[[this_search]] <- fhir_url_w_cfg(search_name = this_search)
fhir_dfs[[this_search]] <- fhir_batched_w_cfg(search_name = this_search, tlog_path = tlog_path)

# limit to encounters with contact to selected departments
encounter_ids_department <- fhir_dfs[[this_search]] %>%
  select(id, serviceType.coding.code, partOf.reference) %>%
  {if (!is.null(cfg$department_contacts)) filter(., serviceType.coding.code %in% cfg$department_contacts) else .} %>%
  reframe(ids = unique(na.omit(c(id, partOf.reference)))) %>%
  use_series(ids)
fhir_dfs[[this_search]] <- fhir_dfs[[this_search]] %>% filter(id %in% encounter_ids_department)

# extract subject ids for next FHIR search
encounter_subject_id_string <- fhir_dfs[[this_search]]$subject.reference %>%
  {if_else(str_detect(., "/"), NA, .)} %>%
  na.omit() %>%
  unique() %>%
  paste0(collapse = ",")
gc(); gc()
ctoc_log(save = tlog_path)


## FHIR search 2: patients belonging to encounters ----
ctic("FHIR search 2: patients")
this_search <- "patient"
# url & batched search
fhir_search_urls[[this_search]] <- fhir_url_w_cfg(search_name = this_search,
                                                  parameters = c("_id" = encounter_subject_id_string))
fhir_dfs[[this_search]] <- fhir_batched_w_cfg(search_name = this_search, tlog_path = tlog_path)
ctoc_log(save = tlog_path)


## filter encounters & patients to age criterion ----
ctic("Filtering encounters & patients to age criterion")
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

# filter encounter DF
fhir_dfs$encounter <- fhir_dfs$encounter %>% filter(id %in% encounter_ids_to_keep)
# filter patient DF
fhir_dfs$patient <- fhir_dfs$patient %>% filter(id %in% unique(underage_patients$patient_id))
gc(); gc()
ctoc_log(save = tlog_path)


## FHIR search 3 & 4: conditions / procedures ----
fhir_searches <- c("condition", "procedure")
for (i in seq_along(fhir_searches)) {
  this_search <- fhir_searches[[i]]
  ctic(str_glue("FHIR search {i+2}: {this_search}s"))

  # url & batched search
  fhir_search_urls[[this_search]] <- fhir_url_w_cfg(search_name = this_search,
                                                    parameters = c("encounter" = encounter_ids_to_keep_string))
  fhir_dfs[[this_search]] <- fhir_batched_w_cfg(search_name = this_search,
                                                remove_ref_prefixes = FALSE,
                                                tlog_path = tlog_path)
  ctoc_log(save = tlog_path)
}
ctoc_log(save = tlog_path)

# save results ---------------------------------------------------------------------------------------------------------



# cleanup --------------------------------------------------------------------------------------------------------------
if (cfg$clear_tmp_dir) unlink(file.path(cfg$tmp_dir, "*"), recursive = TRUE, expand = TRUE)
ctoc_log(save = tlog_path)
ctic.log(show = TRUE, save = FALSE) %>% arrange(start_s, finish_s) %>% print(n = Inf)
