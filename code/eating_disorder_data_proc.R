#!/usr/bin/env Rscript

# CoverCHILD eating disorder data processing
# @author: SP
# @date: 2023-02-09

# setup ----------------------------------------------------------------------------------------------------------------
source("code/data_etl.R") # provides "df"

# join data
df_orbis <- full_join(df$Pers_Fall_V2_pseudonym %>%
                        filter(addr_type == "Erstanschrift") %>%
                        select(-fa_icd) %>%
                        distinct() %>%
                        group_by(case_id) %>%
                        mutate(plz = coalesce(plz, first(plz, na_rm = TRUE))) %>%
                        distinct() %>%
                        ungroup(),
                      df$ICD_V2 %>%
                        filter(icd_type == "Entl.") %>%
                        select(!c(icd_2_code, icd_2_label, icd_label)) %>%
                        distinct() %>%
                        group_by(case_id, icd_code) %>%
                        slice_max(icd_date) %>%
                        ungroup(),
                      multiple = "all") %>%
  # full_join(df$ICPM_V3, multiple = "all") %>%
  # rename(dis_date_Orbis = dis_date) %>%
  distinct() %>%
  mutate(across(where(is.factor), ~fct_drop(.x) %>% fct_na_level_to_value())) %>%
  select(where(~!all(is.na(.))))

df_p21 <- full_join(df$P21_Fall_V1_pseudonym %>%
                      select(!c(adm_reason, p_id)) %>%
                      distinct(),
                    df$P21_ICD_V1_pseudonym %>%
                      select(!c(icd_2_code, icd_2_loc, icd_loc)) %>%
                      distinct(),
                    multiple = "all") %>%
  # full_join(df$P21_OPS_V1_pseudonym, multiple = "all") %>%
  # rename(dis_date_p21 = dis_date) %>%
  distinct() %>%
  mutate(across(where(is.factor), ~fct_drop(.x) %>% fct_na_level_to_value())) %>%
  select(where(~!all(is.na(.))))

# find non-unique keys in y for patching rows
p21_cases_filter_icd_hn_n <- df_p21 %>%
  group_by(case_id, icd_code) %>%
  summarise(n = n()) %>% filter(n > 1) %>%
  use_series(case_id) %>% as.character()

df_patched <- df_orbis %>%
  mutate(across(where(is.factor), as.character)) %>%
  # add info from p21 data
  rows_patch(df_p21 %>%
               filter(!(case_id %in% p21_cases_filter_icd_hn_n & icd_hn == "N")) %>%
               select(!setdiff(names(df_p21), names(df_orbis))) %>%
               mutate(across(where(is.factor), as.character)),
             by = c("case_id", "icd_code"),
             unmatched = "ignore") %>%
  # add additional plz info
  rows_patch(df$Ergebnis_V2_PLZ_PID_Fall_pseudonym %>%
               select(-case_state_id) %>%
               mutate(across(where(is.factor), as.character)),
             by = "case_id") %>%
  # fill in unambiguous missing data per case
  group_by(case_id) %>%
  mutate(across(everything(),
                ~if(n_distinct(.x, na.rm = TRUE) == 1) {replace_na(.x, first(.x, na_rm = TRUE))} else {.x})) %>%
  ungroup() %>%
  distinct() %>%
  mutate(across(where(is.character), as_factor)) %>%
  mutate(across(where(is.factor), ~fct_drop(.x) %>% fct_na_level_to_value()))

# no varying entries per case_id except for icd_code & icd_date
df_patched %>%
  group_by(case_id) %>%
  select(!c(icd_code, icd_date)) %>%
  summarise(across(everything(), n_distinct)) %>% # counts NA as distinct value
  filter(if_any(where(is.integer), ~.x>1))

# saveRDS(df_patched, str_glue("output/CoverCHILD_data_{Sys.Date()}.rds"))

# df_full <- full_join(df_orbis, df_p21) %>%
#   distinct() %>%
#   mutate(across(where(is.factor), ~fct_drop(.x) %>% fct_na_level_to_value())) %>%
#   select(where(~!all(is.na(.))))

# temp <- full_join(df$Pers_Fall_V2_pseudonym %>%
#                     filter(addr_type == "Erstanschrift") %>%
#                     select(-fa_icd) %>%
#                     distinct() %>%
#                     group_by(case_id) %>%
#                     mutate(plz = coalesce(plz, first(plz, na_rm = TRUE))) %>%
#                     distinct() %>%
#                     ungroup() %>%
#                     rename(dis_date_Orbis = dis_date),
#                   df$P21_Fall_V1_pseudonym %>%
#                     select(!c(adm_reason, p_id)) %>%
#                     distinct() %>%
#                     rename(dis_date_p21 = dis_date))


# # OPS ------------------------------------------------------------------------------------------------------------------
#
# df$P21_OPS_V1_pseudonym$case_id %>% n_distinct()
#
# temp2 <- df$ICPM_V3 %>%
#   group_by(case_id) %>%
#   mutate(n = n()) %>%
#   filter(n > 1) %>%
#   select(!c(ops_date, ops_code, ops_label, n)) %>%
#   summarise(across(everything(), n_distinct)) %>%
#   filter(if_any(everything(), ~.x>1))
#
# df$ICPM_V3 %>% filter(case_id %in% temp2$case_id) %>% group_by(case_id) %>%
#   View()
#   summarise(across(everything(), n_distinct))
#
# df$ICD_V2$icd_type %>% fct_count()
#
# temp <- df$ICPM_V3 %>%
#   mutate(across(where(is.factor), ~fct_drop(.) %>% fct_na_level_to_value())) %>%
#   group_by(case_id, ops_code) %>%
#   mutate(n = n()) %>%
#   ungroup() %>%
#   arrange(desc(n), case_id, ops_date)
