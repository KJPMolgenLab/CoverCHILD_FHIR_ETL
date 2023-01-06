#!/usr/bin/env Rscript

# CoverCHILD data ETL
# @author: SP
# @date: 2022-11-04

# setup ------------------------------------------------------------------------
source("code/functions.R")
load_inst_pkgs("tidyverse", "tools", "magrittr", "lubridate", "ggVennDiagram", "psych")

# set wd for VS Code (doesn't align automatically to .Rproj)
old_wd <- set_wd()
outdir <- "output"
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# read data ------------------------------------------------------------------------------------------------------------
files <- Sys.glob("data/*.csv")
names(files) <- basename(file_path_sans_ext(files))

# specify column formats
# different date/time formats in the data sources:
dt_forms <- list(dt1 = col_datetime("%d.%m.%Y %H:%M"), # e.g. "02.01.2016 01:48"
                 dt2 = col_datetime("%Y%m%d%H%M")) # e.g. "201601020148"
col_formats <- list(
  ICD_V2 =
    cols("f", "f", "i", "f", dt_forms$dt1, dt_forms$dt1, "d", "f", "f", dt_forms$dt1, "f", "f", "f", "c", "f", "c"),
  ICPM_V3 = cols("f", "f", "f", "d", "d", "f", dt_forms$dt1, "f", "f", "f", "c"),
  KIJUPSY_Med_Detail_V2_pseudonym =
    cols("f", dt_forms$dt1, "f", "f", "D", "D", "f", "f", "c", "c", "l"),
  Labordaten_V3 = cols("f", dt_forms$dt1, "f", "f", dt_forms$dt1, "f", "c", "f", "d", "f", "c", "f", "c", "?", "?"),
  P21_FAB_V1_pseudonym = cols("f", "f", dt_forms$dt2, dt_forms$dt2),
  P21_Fall_V1_pseudonym =
    cols("f", "f", "i", "f", "f", "i", dt_forms$dt2, "f","f", dt_forms$dt2, "f", "d", "f", "f", "f", "i"),
  P21_ICD_V1_pseudonym = cols("f", "f", "i", "f", "f", "f", "f"),
  P21_OPS_V1_pseudonym = cols("f", "i", "f", "f", dt_forms$dt2),
  Pers_Fall_V2_pseudonym =
    cols("f", "f", "i", "d", "i", "f", "f", "f", dt_forms$dt1, dt_forms$dt1, "f", "f", "i", "f", "f"),
  Rezepte_Pack_Wirkstoff_V4_pseudonym = cols("f", "D", "t", "f", "f", "f", "f"))

data_raw <- imap(files, \(path, name) {
  encoding <- try(guess_encoding(path)$encoding[[1]])
  if (inherits(encoding, "try-error")) encoding <- "ISO-8859-1"
  read_delim(path, delim = ";", trim_ws = TRUE, col_types = col_formats[[name]],
             locale = locale(date_names = "de", date_format = "%d.%m.%Y", time_format = "%H:%M:%S",
                             decimal_mark =".", tz = "Europe/Berlin", encoding = encoding))
})

# preprocessing --------------------------------------------------------------------------------------------------------

# select dataframes to merge
# join_srcs <- c("P21_Fall_V1_pseudonym", "P21_ICD_V1_pseudonym")
# join_srcs <- c("ICD_V2", "Pers_Fall_V2_pseudonym", "P21_Fall_V1_pseudonym", "P21_ICD_V1_pseudonym")
join_srcs <- names(data_raw)

df <-
  data_raw[join_srcs] %>%
  map(~distinct(.) %>% # remove duplicate rows
        # rename(., any_of(col_names)) %>%
        select(where(~!all(is.na(.)))) # %>% # remove empty cols
        # mutate(across(.cols = where(~ is.POSIXt(.) || is.Date(.)), .fns = isoweek, .names = "{.col}_KW")) %>% # Kalenderwoche
      ) # %>%
  # reduce(inner_join) # by = "P21_Fallnummer_Pseudonym"

# convert to DFs in global env if desired
# for (x in names(df)) assign(x, data_raw[[x]])

# write_rds(df, file.path(outdir, "P21_Fall+ICD_sample_data.rds"))


# misc section - not needed atm -------------------------------------------------------------------------------------------------------

# save UTF-8 encoded
# outdir <- file.path("output", "data_utf8")
# dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
# for (x in names(data_raw)) write_csv2(data_raw[[x]], file.path(outdir, str_c(x, "_utf8.csv")))

# recode column names
# col_names <- c(case_id = "Fall_Pseudonym", # ICD_V2
#                p_id = "PID Pseudonym",
#                yob = "Geburtsjahr",
#                sex = "GE",
#                adm_date = "AUFNDAT",
#                dis_date = "ENTLDAT",
#                ret_time = "Verweildauer",
#                case_state = "FALLSTATUS",
#                icd_fa = "ICD_FA", # ?
#                icd_date = "ICD_DATUM",
#                icd_code = "ICD_CODE",
#                icd_type = "ICD_TYPE",
#                icd_hn = "ICD_HN",
#                icd_label = "ICD_BEZ",
#                icd_2_code = "SEK_ICD_CODE",
#                icd_2_label = "SEK_ICD_BEZ",
#                age_adm = "Alter_bei_Aufnahme", # ICPM_V3
#                age_treat = "Alter_bei_Maßnahme",
#                icpm_fa = "ICPM_FA",
#                icpm_date = "ICPM_DATUM",
#                icpm_code = "ICPM_CODE",
#                icpm_hn = "ICPM_HN",
#                loc = "LOKALISATION",
#                icpm_label = "ICPM_BEZ",
#                age_adm = "Alter_Aufnahme", # Pers_Fall_V2_pseudonym
#                plz = "PLZ-3stellig",
#                addr_type = "ADRESSART",
#                adm_reason = "AUFNAHMEGRUND",
#                adm_type = "AUFNAHMEART",
#                stay_type = "AUFENTHALTSART",
#                dis_type = "ENTLASSART")


# sandbox for individual dataframes

tmp <- df[[1]]
tmp$`PID Pseudonym` %>% unique() %>% length
tmp_desc <- tmp %>%
  group_by(`PID Pseudonym`, Fall_Pseudonym) %>%
  summarise(n_case = n()) %>%
  ungroup(Fall_Pseudonym) %>%
  mutate(cases_patient = n())

tmp_desc %>% slice_head(n = 1) %>% use_series(cases_patient) %>% qplot()
tmp_desc %>% arrange(desc(cases_patient)) %>% View()
tmp %>% filter(`PID Pseudonym` == "CC8748848558568597") %>% descr_mis() %>% select(1:4)

p21 <- df[["P21_ICD_V1_pseudonym"]]
p21 %>% filter(P21_Fallnummer_Pseudonym == "CC4725478752193819") %>% View()
p21 %>%
  filter(Diagnoseart == "HD") %>%
  group_by(P21_Fallnummer_Pseudonym) %>%
  summarise(n = n()) %>% arrange(desc(n)) %>% View()
