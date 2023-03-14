#!/usr/bin/env Rscript

# CoverCHILD eating disorder data processing
# @author: SP
# @date: 2023-02-09

# setup ----------------------------------------------------------------------------------------------------------------
source("code/data_etl.R") # provides "data_exp" & "data_exp_sum"

study_start <- ymd("2016-01-01") # start of available data
study_end <- ymd("2022-02-28") # until start of Ukraine war

# Esstörungskategorien
# l3: "Anorexie", "Bulimie", "Essstörung Sonst." -> l1: "Essstörung"

df_diag <- data_exp$diagnosis %>%
  mutate(icd_f50 = factor(if_else(icd_cat_l1 == "Essstörung", "F50+", "F50-"),
                          levels = c("F50-", "F50+"), ordered = TRUE),
         f50_type = factor(if_else(icd_cat_l1 == "Essstörung", icd_cat_l3, "F50-"),
                           levels = c("F50-", "Essstörung Sonst.", "Bulimie", "Anorexie"), ordered = TRUE))

df_ed <-
  # case
  data_exp$case %>%
  select(-source_dfs) %>%
  filter(between(adm_date, study_start, study_end)) %>%
  # patient
  left_join(data_exp$patient %>% select(-source_dfs), by = "p_id") %>%
  # diagnosis
  inner_join(df_diag %>%
               summarise(icd_f50 = max(icd_f50, na.rm = TRUE),
                         f50_type = max(f50_type, na.rm = TRUE),
                         .by = case_id),
             by = "case_id") %>%
  inner_join(data_exp_sum$diagnosis %>% select(-case_id_orig), by = "case_id") %>%
  # unify covid_pan+lockdown
  mutate(covid_lockd = if_else(covid_pan == "pre_covid", as.character(covid_pan), as.character(lockdown_status)) %>%
           replace_na("0") %>%
           ordered(levels = c("pre_covid", "0", "1")) %>%
           fct_recode(covid_open = "0", lockd = "1"),
         covid_lockd_n = if_else(covid_pan == "pre_covid", as.character(covid_pan), as.character(lockdown_period_n)) %>%
           replace_na("0") %>%
           ordered(levels = c("pre_covid", "0", "1", "2")) %>%
           fct_recode(covid_open = "0", lockd_1 = "1", lockd_2 = "2"))

pre_cov_dur <- min(df_ed$adm_date) %--% covid_start %>% as.numeric("years")
cov_dur <- covid_start %--% max(df_ed$adm_date) %>% as.numeric("years")

