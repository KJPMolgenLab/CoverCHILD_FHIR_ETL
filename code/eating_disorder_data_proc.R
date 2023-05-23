#!/usr/bin/env Rscript

# CoverCHILD eating disorder data processing
# @author: SP
# @date: 2023-02-09

# setup ----------------------------------------------------------------------------------------------------------------
if(!exists("do_source_data_creation")) do_source_data_creation <- FALSE
if(do_source_data_creation) {
  source("code/data_etl.R") # creates "data_exp" & "data_exp_sum" from scratch from raw data
} else {
  load("output/CoverCHILD_data_ETL_2023-06-01.RData.xz")
  load_inst_pkgs("tidyverse", "tools", "magrittr", "lubridate", "ggVennDiagram", "psych", "rlang", "glue", "janitor")
}

# settings -------------------------------------------------------------------------------------------------------------
study_start <- ymd("2016-01-01") # start of available data
study_end <- ymd("2022-02-28") # until start of Ukraine war

## AN pre/post comorbidities ----
an_comorb_cats <- exprs(
  str_starts(icd_code, "D50\\.[89]|E30\\.0|E83\\.38|I31\\.3|I34\\.[01]|K59\\.09|T69\\.1") ~ "AN_effect",
  str_starts(icd_code, str_c("A49\\.3", "B35\\.3", "B85\\.0", "D68\\.22", "E34\\.3", "E73\\.9", "E74\\.[13]",
                             "G40\\.6", "G43\\.1", "H10\\.1", "H61\\.0", "J03\\.9", "J06\\.9", "J18\\.8", "J30\\.1",
                             "J35\\.2", "J45\\.[019]", "K44\\.9", "K90\\.0", "L08\\.9", "L20\\.8", "L70\\.0",
                             "L89\\.(?:0[04]|1[09])", "N10", "N14\\.2", "N18\\.2", "N91\\.1", "N94\\.6", "R09\\.1",
                             "R79\\.8", "S00\\.05", "S50\\.88", "S51\\.[79]", "S52\\.50", "S70\\.88", "S93\\.40",
                             "T14\\.1", "T43\\.[26]", "T45\\.4", sep = "|")) ~ "Non_AN_effect"
  )


# create data ----------------------------------------------------------------------------------------------------------
# Esstörungskategorien
# l3: "Anorexie", "Bulimie", "Essstörung Sonst." -> l1: "Essstörung"
df_diag <- data_exp$diagnosis %>%
  filter(icd_type == "Entl.") %>%
  group_by(case_id, icd_code) %>%
  slice_max(icd_date, with_ties = FALSE) %>%
  ungroup() %>%
  mutate(icd_f50 = factor(if_else(icd_cat_l1 == "Essstörung", "F50+", "F50-"),
                          levels = c("F50-", "F50+"), ordered = TRUE),
         f50_type = factor(if_else(icd_cat_l1 == "Essstörung", icd_cat_l3, "F50-"),
                           levels = c("F50-", "Essstörung Sonst.", "Bulimie", "Anorexie"), ordered = TRUE),
         an_comorb_type = case_when(!!!an_comorb_cats) %>% as_factor())

df_ed <-
  # case
  data_exp$case %>%
  select(-source_dfs) %>%
  filter(between(adm_date, study_start, study_end),
         case_state != "ambulant") %>%
  # patient
  left_join(data_exp$patient %>% select(-source_dfs), by = "p_id") %>%
  # diagnosis
  inner_join(df_diag %>%
               summarise(icd_f50 = max(icd_f50, na.rm = TRUE),
                         f50_type = max(f50_type, na.rm = TRUE),
                         an_comorbs_an_effect = sum(an_comorb_type == "AN_effect", na.rm =  TRUE),
                         an_comorbs_non_an_effect = sum(an_comorb_type == "Non_AN_effect", na.rm =  TRUE),
                         .by = case_id),
             by = "case_id") %>%
  inner_join(data_exp_sum$diagnosis %>% select(-case_id_orig), by = "case_id") %>%
  # unify covid_pan+lockdown
  mutate(covid_lockd = if_else(covid_pan == "pre_covid", as.character(covid_pan), as.character(lockdown_status)) %>%
           replace_na("0") %>%
           ordered(levels = c("pre_covid", "0", "1")) %>%
           fct_recode(covid_open = "0", lockd = "1"),
         covid_lockd_i = if_else(covid_pan == "pre_covid", as.character(covid_pan), as.character(lockdown_period_i)) %>%
           replace_na("0") %>%
           ordered(levels = c("pre_covid", "0", "1", "2")) %>%
           fct_recode(covid_open = "0", lockd_1 = "1", lockd_2 = "2"))

pre_cov_dur <- min(df_ed$adm_date) %--% covid_start %>% as.numeric("years")
cov_dur <- covid_start %--% max(df_ed$adm_date) %>% as.numeric("years")

## save ----
if(do_save_objects) saveRDS(df_ed, str_glue("output/CoverCHILD_data+EDvars_{Sys.Date()}.rds"))
