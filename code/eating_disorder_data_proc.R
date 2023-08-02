#!/usr/bin/env Rscript

# CoverCHILD eating disorder data processing
# @author: SP
# @date: 2023-02-09

# setup ----------------------------------------------------------------------------------------------------------------
if(!exists("do_source_data_creation")) do_source_data_creation <- FALSE
if(do_source_data_creation) {
  source("code/data_etl.R") # creates "data_exp" & "data_exp_sum" from scratch from raw data
} else {
  load("output/CoverCHILD_data_ETL_2023-07-25.RData.xz")
  source("code/functions.R")
  load_inst_pkgs("tidyverse", "tools", "magrittr", "lubridate", "ggVennDiagram", "psych", "DescTools", "rlang", "glue",
                 "janitor")
  do_save_objects <- FALSE
}

# settings -------------------------------------------------------------------------------------------------------------
study_start <- ymd("2016-01-01", tz = "CET") # start of available data
study_end <- ymd("2022-02-28", tz = "CET") # until start of Ukraine war

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

psych_med_regex <- str_c("Chlorprothixen", "Circadin", "Equasym", "Escitalopram", "Fluoxetin", "Lorazepam", "Medikinet",
                         "Melatonin", "Melperon", "Midazolam", "Mirtazapin", "Pipamperon", "Quetiapin", "Risperidon",
                         "Sertralin", "Venlafaxin", sep = "|")


# create data ----------------------------------------------------------------------------------------------------------

## lockdown / school closure data ----
# confine lockdown data period to study period
df_covid_periods_ed <- df_covid_periods %>%
  mutate(period_end_date = if_else(period_end_date == max_na(period_end_date),
                                   study_end + hms("23:59:59"),
                                   period_end_date)) %>%
  group_by(period_i_no_hol, .add = TRUE) %>%
  mutate(period_no_hol_start_date = min_na(period_start_date),
         period_no_hol_end_date = max_na(period_end_date)) %>%
  ungroup() %>%
  mutate(across(ends_with("_date"),
                ~str_c(year(.), ".", week(.)) %>% fct_relevel(~str_sort(., numeric = TRUE)) %>% as.ordered(),
                .names = '{str_replace(.col, "_date", "_year_week")}'))

# periods of school closures with severity, but without holiday codes
df_schoolclosures <- df_covid_periods_ed %>%
  filter(state == "Hessen", measure == "school") %>%
  select(contains("no_hol")) %>%
  distinct() %>%
  filter(status_no_hol != 0) %>%
  mutate(status_no_hol = as_factor(status_no_hol),
         period_no_hol_end_year_week = fct_recode(period_no_hol_end_year_week, "2021.14" = "2021.13"))

# covid periods: open vs school closure (without severity)
df_covid_periods_is_lockd <- df_covid_periods_ed %>%
  summarise(across(c(period_start_date, period_start_year_week), min_na),
            across(c(period_end_date, period_end_year_week), max_na),
            .by = period_i_is_lockd) %>%
  rowwise() %>%
  mutate(period_interval = interval(period_start_date, period_end_date),
         period_days = list(seq(as_date(period_start_date), as_date(period_end_date), by = "days") %>%
                              `year<-`(0) %>%
                              unique()),
         period_start_year_week = fct_recode(period_start_year_week, "2021.25" = "2021.26"))


## Eating disorder base data ----
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

# Entlassmedikation
df_med <- data_exp$medication %>%
  select(case_id, med_start_date, med_end_date, med_nvl) %>%
  inner_join(data_exp$case %>% select(case_id, dis_date)) %>%
  # filter last week per case: med_end_date > dis_date-1week OR if NA med_start_date > dis_date-1week
  mutate(in_last_week = coalesce(med_end_date > (dis_date - weeks(1)), med_start_date > (dis_date - weeks(1)))) %>%
  filter(in_last_week) %>%
  mutate(psych_med = str_detect(med_nvl, psych_med_regex)) %>%
  summarise(psych_med = any(psych_med),
            .by = case_id)

# base DF for ED analyses
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
               summarise(icd_f50 = max_na(icd_f50),
                         f50_type = max_na(f50_type),
                         icd_dd = "Depression" %in% icd_cat_l3,
                         icd_ad = "Angststörung" %in% icd_cat_l3,
                         icd_f50_dd_ad = (icd_f50 == "F50+" & icd_dd & icd_ad),
                         an_comorbs_an_effect = sum_na(an_comorb_type == "AN_effect"),
                         an_comorbs_non_an_effect = sum_na(an_comorb_type == "Non_AN_effect"),
                         .by = case_id),
             by = "case_id") %>%
  inner_join(data_exp_sum$diagnosis %>% select(-case_id_orig), by = "case_id") %>%
  # medication
  left_join(df_med, by = "case_id") %>%
  # add variables
  arrange(adm_date) %>%
  mutate(
    # create covid lockdown severity from covid_pan + lockdown without holidays
    covid_lockd_sev = if_else(covid_pan == "pre_covid", as.character(covid_pan), as.character(lockd_status_no_hol)) %>%
      #TODO replace_na("0") %>%
      ordered(levels = c("pre_covid", "0", "1", "2")) %>%
      fct_recode(covid_open = "0", lockd_part = "1", lockd_full = "2"),
    # create covid period from covid_pan + is_lockdown period
    covid_period_i = if_else(covid_pan == "pre_covid", 0L, lockd_period_i_is_lockd) %>%
      ordered() %>% fct_inseq() %>%
      fct_recode(pre_covid_0 = "0", open_1 = "1", lockd_2 = "2", open_3 = "3", lockd_4 = "4", open_5 = "5"),

    # add broader admission date categories: month & quarter, date without year
    adm_date_year0 = `year<-`(adm_date, 0),
    adm_year = year(adm_date),
    adm_month = month(adm_date),
    adm_year_month = str_c(year(adm_date), ".", month(adm_date)) %>%
      fct_relevel(~str_sort(., numeric = TRUE)) %>%
      as.ordered(),
    adm_year_quarter = ordered(quarter(adm_date, type = "year.quarter")),

    # re_adm_soon where NAs (= first presentation) are recoded to FALSE
    re_adm_soon_nona = replace_na(re_adm_soon, FALSE),

    # re-admission periods for 2-year baseline comparison v1 (see descr Notebook)
    re_adm_period = if_else(covid_pan == "pre_covid",
                            if_else(adm_date < ymd("2018-03-01", tz = "CET"), "pre", "baseline"),
                            covid_pan)
    )

## derivatives: monthly, weekly, baseline-pseudoyear, 2-year-comparisons ----
# summarise function
sum_ed_abs_props <- function(...) {
  summarise(...,
            n_cases = n(),
            ED_abs = sum(icd_f50 == "F50+"),
            AN_abs = sum(f50_type == "Anorexie"),
            BN_abs = sum(f50_type == "Bulimie"),
            oED_abs = sum(f50_type == "Essstörung Sonst."),
            ED_prop = ED_abs/n(),
            AN_prop = AN_abs/n(),
            BN_prop = BN_abs/n(),
            oED_prop = oED_abs/n(),
            covid_lockd_sev = Mode(covid_lockd_sev),
            covid_is_lockd = Mode(lockd_status_is_lockd),
            covid_period_i = Mode(covid_period_i)
  )
}

# subset covid period only
df_ed_covid <- df_ed %>%
  filter(covid_pan != "pre_covid") %>%
  mutate(across(where(is.factor), fct_drop))

### monthly ----
# whole period
df_ed_monthly <- df_ed %>%
  group_by(adm_year_month, adm_year, adm_month) %>%
  sum_ed_abs_props() %>%
  ungroup()
# long format for plotting
df_ed_monthly_long <- df_ed_monthly %>%
  pivot_longer(starts_with(c("AN", "BN", "oED")), names_to = c("f50_type", ".value"), names_sep = "_")
# covid period only
df_ed_covid_monthly <- df_ed_covid %>%
  group_by(adm_year_month, adm_year, adm_month) %>%
  sum_ed_abs_props() %>%
  ungroup()

### weekly ----
# whole period
df_ed_weekly <- df_ed %>%
  group_by(adm_year_week, adm_year) %>%
  sum_ed_abs_props() %>%
  ungroup()
# covid period only
df_ed_covid_weekly <- df_ed_covid %>%
  group_by(adm_year_week, adm_year) %>%
  sum_ed_abs_props() %>%
  ungroup()
df_ed_covid_weekly_long <- df_ed_covid_weekly %>%
  pivot_longer(starts_with(c("AN", "BN", "oED")), names_to = c("f50_type", ".value"), names_sep = "_")

### baseline ----
# re-admissions: 2 year baseline vs 2 year covid, 2 year lookback each
df_ed_re <- df_ed %>% filter(adm_date >= ymd("2016-03-01"))
filter_df_ed_re <- function(period = "baseline") {
  if (period == "baseline") filter_1 <- "dur_covid"
  else if (period == "dur_covid") filter_1 <- "pre"
  else abort("period must be 'baseline' or 'dur_covid'.")
  df_ed_re %>%
    filter(re_adm_period != filter_1) %>%
    group_by(p_id) %>%
    arrange(adm_date) %>%
    mutate(is_first_case = case_id == first(case_id, na_rm = TRUE),
           re_adm_lag = difftime(adm_date, coalesce(lag(dis_date), lag(adm_date)), units = "days"),
           re_adm_soon = re_adm_lag < re_adm_span) %>%
    ungroup() %>%
    filter(re_adm_period == period)
}
df_ed_re_baseline <- filter_df_ed_re("baseline")
df_ed_re_covid <- filter_df_ed_re("dur_covid")

# pre-pandemic and pandemic period durations
pre_cov_dur <- min(df_ed$adm_date) %--% covid_start %>% as.numeric("days") %>% divide_by(30)
cov_dur <- covid_start %--% max(df_ed$adm_date) %>% as.numeric("days") %>% divide_by(30)


# save ----
if(do_save_objects) saveRDS(df_ed, str_glue("output/CoverCHILD_data+EDvars_{Sys.Date()}.rds"))
