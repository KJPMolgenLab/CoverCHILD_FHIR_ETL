#!/usr/bin/env Rscript

# CoverCHILD data ETL
# @author: SP
# @date: 2022-11-04

# setup ------------------------------------------------------------------------
source("code/functions.R")
load_inst_pkgs("tidyverse", "tools", "magrittr", "lubridate", "ggVennDiagram", "psych")

# set wd for VS Code (doesn't align automatically to .Rproj)
old_wd <- set_wd() # function imported from functions.R
if(is.null(old_wd)) rm(old_wd)
outdir <- "output"
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# settings -------------------------------------------------------------------------------------------------------------
# specify column formats
# different date/time formats in the data sources:
dt_forms <- list(dt1 = col_datetime("%d.%m.%Y %H:%M"), # e.g. "02.01.2016 01:48"
                 dt2 = col_datetime("%Y%m%d%H%M")) # e.g. "201601020148"
col_formats <- list(
  ICD_V2 =
    cols("f", "f", "i", "f", dt_forms$dt1, dt_forms$dt1, "d", "f", "f", dt_forms$dt1, "f", "f", "f", "f", "f", "f"),
  ICPM_V3 = cols("f", "f", "f", "d", "d", "f", dt_forms$dt1, "f", "f", "f", "f"),
  KIJUPSY_Med_Detail_V2_pseudonym =
    cols("f", dt_forms$dt1, "f", "f", "D", "D", "f", "f", "f", "f", "l"),
  Labordaten_V3 = cols("f", dt_forms$dt1, "f", "f", dt_forms$dt1, "f", "f", "f", "d", "f", "f", "f", "f", "?", "?"),
  P21_FAB_V1_pseudonym = cols("f", "f", dt_forms$dt2, dt_forms$dt2),
  P21_Fall_V1_pseudonym =
    cols("f", "f", "i", "f", "f", "f", dt_forms$dt2, "f","f", dt_forms$dt2, "f", "d", "f", "f", "f", "i"),
  P21_ICD_V1_pseudonym = cols("f", "f", "f", "f", "f", "f", "f"),
  P21_OPS_V1_pseudonym = cols("f", "f", "f", "f", dt_forms$dt2),
  Pers_Fall_V2_pseudonym =
    cols("f", "f", "i", "d", "f", "f", "f", "f", dt_forms$dt1, dt_forms$dt1, "f", "f", "i", "f", "f"),
  Rezepte_Pack_Wirkstoff_V4_pseudonym = cols("f", "D", "t", "f", "f", "f", "f"))

# unify column names
col_names <- list(ICD_V2 = c(case_id = "Fall_Pseudonym",
                             p_id = "PID Pseudonym",
                             yob = "Geburtsjahr",
                             sex = "GE", # "M" "W" "D"
                             adm_date = "AUFNDAT",
                             dis_date = "ENTLDAT",
                             length_stay = "Verweildauer", # num
                             case_state = "FALLSTATUS", # "stationär" "ambulant" "teilstationär" "nachstationär"
                             fa_icd = "ICD_FA", # "Kinder- und Jugendpsychiatrie KIJUPSY" "PIA 92 - Kinder- und Jugendliche PIA KIJU" "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG" "HSA Psy. Kinder- und Jugendliche KIJU POLI"
                             icd_date = "ICD_DATUM",
                             icd_code = "ICD_CODE",
                             icd_type = "ICD_TYPE", # "FA En" "Aufn." "Entl." "Abr" "Beh." "Einw."
                             icd_hn = "ICD_HN", # "H" "N"
                             icd_label = "ICD_BEZ",
                             icd_2_code = "SEK_ICD_CODE",
                             icd_2_label = "SEK_ICD_BEZ"),
                  ICPM_V3 = c(case_id = "Fall_Pseudonym",
                              p_id = "PID Pseudonym",
                              case_state = "FALLSTATUS", # "ambulant" "stationär" "nachstationär" "teilstationär"
                              age_adm = "Alter_bei_Aufnahme",
                              age_treat = "Alter_bei_Maßnahme",
                              fa_ops = "ICPM_FA", # "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG" "Kinder- und Jugendpsychiatrie KIJUPSY" "HSA Psy. Kinder- und Jugendliche KIJU POLI" "PIA 92 - Kinder- und Jugendliche PIA KIJU"
                              ops_date = "ICPM_DATUM",
                              ops_code = "ICPM_CODE",
                              ops_hn = "ICPM_HN", # "H" "N"
                              ops_loc = "LOKALISATION", # "" "B"
                              ops_label = "ICPM_BEZ"),
                  KIJUPSY_Med_Detail_V2_pseudonym = c(case_id = "Fall_Pseudonym",
                                                      adm_date = "Aufnahmedatum",
                                                      case_state = "Fallstatus", # "teilstationär" "stationär" "ambulant" "nachstationär"
                                                      fa_med = "Fachabteilung", # "KIJUPSY"
                                                      med_start_date = "BeginnDatum",
                                                      med_end_date = "EndeDatum",
                                                      med_nvl = "Medikament_NVL",
                                                      med_unit = "Einheit",
                                                      med_admin_weekly = "F_Wochenstring",
                                                      med_admin_daily = "Tagesstring",
                                                      med_cbx_req = "cbxBedarfsmedikation"),
                  Labordaten_V3 = c(case_id = "Fall_Pseudonym",
                                    adm_date = "AUFNDAT",
                                    fa_lab = "OEBENEKURZ", # "KIJUPSY" "PIA KIJU" "KIJU POLI" "CA FREITAG"
                                    lab_prot_n = "PROTOKOLLNUMMER",
                                    lab_date = "BEFUNDDATUM",
                                    lab_code = "CODE",
                                    lab_label = "BEZEICHNUNG",
                                    lab_group = "Gruppenbezeichnung",
                                    lab_val = "WERT",
                                    lab_unit = "EINHEIT",
                                    lab_norm_val = "NORMALWERTE",
                                    lab_abnorm_dir = "ABNORMRICHTUNG",
                                    lab_val_text = "WERTTEXT"),
                  P21_FAB_V1_pseudonym = c(case_id = "P21_Fallnummer_Pseudonym",
                                           fa_p21 = "Fachabteilung", # "HA3000" "HA0003" "HA0002" "HA0001" "HA2900"
                                           adm_date = "FAB-Aufnahmedatum",
                                           dis_date = "FAB-Entlassungsdatum"),
                  P21_Fall_V1_pseudonym = c(case_id = "P21_Fallnummer_Pseudonym",
                                            p_id = "P21_PID_Pseudonym",
                                            yob = "Geburtsjahr",
                                            sex = "Geschlecht", # "m" "w" ""
                                            ik_insurer = "IK-der-Krankenkasse",
                                            plz = "PLZ",
                                            adm_date = "Aufnahmedatum",
                                            adm_event = "Aufnahmeanlass", # "N" "E" "V" "B" "A"
                                            adm_reason = "Aufnahmegrund", # "0107" "0301" "0101" "" "0307" "0201" "0407"
                                            dis_date = "Entlassungsdatum",
                                            dis_reason = "Entlassungsgrund", # "019" "012" "039" "229" "042" "011" "049" "029" "032" "022" "" "031" "179" "149" "069" "152" "041" "159" "089"
                                            age_adm = "Alter-in-Jahren-am-Aufnahmetag",
                                            ik_hospital = "IK-Verlegungs-KH",
                                            case_merge = "Fallzusammenführung", # "N" "J"
                                            case_merge_reason= "Fallzusammenführungsgrund", # "" "PW" "PR" "PM"
                                            leave_days_psy = "Beurlaubungstage-PSY"),
                  P21_ICD_V1_pseudonym = c(case_id = "P21_Fallnummer_Pseudonym",
                                           icd_hn = "Diagnoseart", # "HD" "ND"
                                           icd_version = "ICD-Version",
                                           icd_code = "ICD-Kode",
                                           icd_loc = "Lokalisation...5", # "" "L" "R" "B"
                                           icd_2_code = "Sekundär-Kode",
                                           icd_2_loc = "Lokalisation...7"), # "" "L" "R" "B"
                  P21_OPS_V1_pseudonym = c(case_id = "P21_Fallnummer_Pseudonym",
                                           ops_version = "OPS-Version",
                                           ops_code = "OPS-Kode",
                                           ops_loc = "Lokalisation", # "" "B" "L" "R"
                                           ops_date = "OPS-Datum"),
                  Pers_Fall_V2_pseudonym = c(p_id = "PID Pseudonym",
                                             sex = "GE", # "M" "W" "D"
                                             yob = "Geburtsjahr",
                                             age_adm = "Alter_Aufahme",
                                             plz = "PLZ-3stellig",
                                             addr_type = "ADRESSART", # "frühere Adresse" "Erstanschrift" "Rechnungsanschrift Ambu" "Zweitanschrift" "Ehemalige Erstanschrift" "Befundverschickung" "Rechnungsanschrift" "Info-Adresse"
                                             case_id = "Fall_Pseudonym",
                                             case_state = "FALLSTATUS", # "stationär" "ambulant" "teilstationär" "nachstationär"
                                             adm_date = "AUFNDAT",
                                             dis_date = "ENTLDAT",
                                             adm_reason = "AUFNAHMEGRUND", # "KH-Behandlung, vollstat." "" "KH-Behandlung, teilstat." KH-Behandlung, vollstat. nach vorstat."
                                             adm_type = "AUFNAHMEART", # "Notfall" "" "Normalfall"
                                             stay_type = "AUFENTHALTSART", # 1 NA
                                             dis_type = "ENTLASSART",
                                             fa_icd = "ICD_FA"), # "Kinder- und Jugendpsychiatrie KIJUPSY" "PIA 92 - Kinder- und Jugendliche PIA KIJU" "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG" "HSA Psy. Kinder- und Jugendliche KIJU POLI"
                  Rezepte_Pack_Wirkstoff_V4_pseudonym = c(case_id = "Fall_Pseudonym",
                                                          presc_date = "Rezept_Datum", # unify
                                                          presc_time = "Rezept_Zeit", # unify
                                                          presc_orderer = "Mediz_BST", # "PIA KIJU" "92-2" "CA FREITAG" "92-3" "92-1" "93-1" "KIJU POLI" "92-5" "92-4"
                                                          fa_presc = "Mediz_FA", # "KIJUPSY"
                                                          pres_package_info = "PackungInfo",
                                                          presc_drug = "WirkstoffBez")
                  )

# unify factor levels & labels
fa_lvls <- c("Kinder- und Jugendpsychiatrie KIJUPSY" = "KIJUPSY",
               "PIA 92 - Kinder- und Jugendliche PIA KIJU" = "PIA KIJU",
               "HSA Psy. Kinder- und Jugendliche KIJU POLI" = "KIJU POLI",
               "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG" = "CA FREITAG") #,
               # "HA3000" = ...,
               # "HA0003" = ...,
               # "HA0002" = ...,
               # "HA0001" = ...,
               # "HA2900" = ...)
hn_lvls <- c(HD = "H", ND = "N")
sex_lvls <- c(m = "M", w = "W", d = "D") # used right now is "toupper"

#TODO adm_reason_lvls = ... # recode & separate reason + type
#TODO add other codes from codebook (icd_type, fa_p21, adm_event, dis_reason, case_merge_reason, stay_type, +?)

# data exclusion
# ICD codes to be in-/excluded
filter_icd <- "[^XVZ]" # regex as defined by stringi, matched only at start of string

# icd_codes & ops_codes present in fewer than threshold proportion of cases will be lumped together as "Sonstige"
# and excluded if applicable
lump_threshold <- 0.0005
other_name <- "Sonstige"
other_exclude <- TRUE


# read & tidy data -----------------------------------------------------------------------------------------------------
files <- Sys.glob("data/*.csv")
names(files) <- basename(file_path_sans_ext(files))

# read data
data_raw <- imap(files, \(path, name) {
  encoding <- try(guess_encoding(path)$encoding[[1]])
  if(inherits(encoding, "try-error")) encoding <- "ISO-8859-1"
  read_delim(path, delim = ";", trim_ws = TRUE, col_types = col_formats[[name]],
             locale = locale(date_names = "de", date_format = "%d.%m.%Y", time_format = "%H:%M:%S",
                             decimal_mark =".", tz = "Europe/Berlin", encoding = encoding))
})

# tidy data
df <- data_raw %>%
  imap(~distinct(.) %>% # remove duplicate rows
         select(where(~!all(is.na(.)))) %>% # remove empty cols
         rename(., any_of(col_names[[.y]])) %>% # unify variable names

         filter(if_any(any_of("icd_code"), ~str_starts(., filter_icd))) %>% # keep/remove selected ICD codes
         mutate(# recode factor levels
                across(starts_with("fa_"), ~fct_relabel(., ~fa_lvls[.])),
                across(ends_with("_hn"), ~fct_relabel(., ~hn_lvls[.])),
                across(any_of("sex"), ~fct_relabel(., toupper)),

                # remove all occurences of "-" and "." to transfer icpm codes to ops codes
                across(any_of("ops_code"), ~fct_relabel(., ~str_remove_all(., "[\\-\\.]"))),

                # remove 2nd decimal place of diagnoses
                across(any_of("icd_code"), ~fct_relabel(., ~str_sub(., end = 5))),

                #TODO: move to: after merging (proportions might shift)
                # drop/lump rare diagnoses/treatmens
                across(any_of(c("icd_code", "ops_code")), ~fct_lump_prop(., lump_threshold, other_level = other_name))
                ) %>%
         # exclude icd_code & ops_code lump category
         { if(other_exclude) filter(., if_any(any_of(c("icd_code", "ops_code")), ~!str_detect(., other_name)))
           else . } %>%

         mutate(across(where(is.factor), fct_drop)) # drop unused factor levels after exclusion
      )

# expand data ----------------------------------------------------------------------------------------------------------


# create new variables -------------------------------------------------------------------------------------------------

# mutate(across(.cols = where(~ is.POSIXt(.) || is.Date(.)), .fns = isoweek, .names = "{.col}_KW")) %>% # Kalenderwoche


# convert to DFs in global env if desired
# for (x in names(df)) assign(x, df[[x]])
# write_rds(df, file.path(outdir, "P21_Fall+ICD_sample_data.rds"))


# merge data -----------------------------------------------------------------------------------------------------------

# select dataframes to merge
# join_srcs <- c("P21_Fall_V1_pseudonym", "P21_ICD_V1_pseudonym")
# join_srcs <- c("ICD_V2", "Pers_Fall_V2_pseudonym", "P21_Fall_V1_pseudonym", "P21_ICD_V1_pseudonym")
join_srcs <- names(data_raw)
# df[join_srcs] %<>% reduce(inner_join) # by = "P21_Fallnummer_Pseudonym"


# misc section - not needed atm ----------------------------------------------------------------------------------------

# save UTF-8 encoded
# outdir <- file.path("output", "data_utf8")
# dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
# for (x in names(data_raw)) write_csv2(data_raw[[x]], file.path(outdir, str_c(x, "_utf8.csv")))




# sandbox for individual dataframes

# tmp <- df[[1]]
# tmp$`PID Pseudonym` %>% unique() %>% length
# tmp_desc <- tmp %>%
#   group_by(`PID Pseudonym`, Fall_Pseudonym) %>%
#   summarise(n_case = n()) %>%
#   ungroup(Fall_Pseudonym) %>%
#   mutate(cases_patient = n())
#
# tmp_desc %>% slice_head(n = 1) %>% use_series(cases_patient) %>% qplot()
# tmp_desc %>% arrange(desc(cases_patient)) %>% View()
# tmp %>% filter(`PID Pseudonym` == "CC8748848558568597") %>% descr_mis() %>% select(1:4)
#
# p21 <- df[["P21_ICD_V1_pseudonym"]]
# p21 %>% filter(P21_Fallnummer_Pseudonym == "CC4725478752193819") %>% View()
# p21 %>%
#   filter(Diagnoseart == "HD") %>%
#   group_by(P21_Fallnummer_Pseudonym) %>%
#   summarise(n = n()) %>% arrange(desc(n)) %>% View()
