#!/usr/bin/env Rscript

# CoverCHILD data ETL
# @author: SP
# @date: 2022-11-04

# setup ------------------------------------------------------------------------
source("code/functions.R")
load_inst_pkgs("tidyverse", "tools", "magrittr", "lubridate", "ggVennDiagram", "psych", "rlang")

# set wd for VS Code (doesn't align automatically to .Rproj)
old_wd <- set_wd() # function imported from functions.R
if(is.null(old_wd)) rm(old_wd)
outdir <- "output"
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

# settings -------------------------------------------------------------------------------------------------------------
covid_start <- ymd("2020-03-01")

data_fs <- Sys.glob("data/*.csv")
names(data_fs) <- basename(file_path_sans_ext(data_fs))
lockdown_f <- "data/ext/ZPID_lockdown_measures_dataset-V6.0.csv"

# column formats
# different date/time formats in the data sources:
dt_forms <- list(dt1 = col_datetime("%d.%m.%Y %H:%M"), # e.g. "02.01.2016 01:48"
                 dt2 = col_datetime("%Y%m%d%H%M")) # e.g. "201601020148"
col_formats <- list(
  Ergebnis_V2_PLZ_PID_Fall_pseudonym = cols("f", "f", "f", "f", "f"),
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
col_names <- list(Ergebnis_V2_PLZ_PID_Fall_pseudonym = c(p_id = "Pseudonyme_PID",
                                                         case_id = "Pseudonyme_Fälle",
                                                         case_state_id = "FALLSTATUSID", # 1 11 13 12
                                                         case_state = "FALLSTATUS", # "ambulant" "stationär" "teilstationär" "nachstationär"
                                                         plz = "PLZ_3stellig"),
                  ICD_V2 = c(case_id = "Fall_Pseudonym",
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

# unify/recode factor levels & labels
# CAVE: order of names depends on used function: fct_recode needs new=old, fct_relabel needs old=new
# relabel
fa_lvls <- c("Kinder- und Jugendpsychiatrie KIJUPSY" = "KIJUPSY",
               "PIA 92 - Kinder- und Jugendliche PIA KIJU" = "PIA KIJU",
               "HSA Psy. Kinder- und Jugendliche KIJU POLI" = "KIJU POLI",
               "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG" = "CA FREITAG") #,
               # "HA3000" = ...,
               # "HA0003" = ...,
               # "HA0002" = ...,
               # "HA0001" = ...,
               # "HA2900" = ...)
hn_lvls <- c("HD" = "H", "ND" = "N")
sex_lvls <- c("m" = "M", "w" = "W", "d" = "D") # used right now is "toupper"
# recode
case_state_lvls <- c("ambulant" = "nachstationär")

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

# ICD categories in 3 levels
icd_cats_l3 <- exprs(
  # Neuronale Entwicklungsstörungen
  str_starts(icd_code, "F0") ~ "Organische Störung",
  str_starts(icd_code, "F84\\.[015]") ~ "Autismus-Spektrum-Störung",
  str_starts(icd_code, "F84\\.[2389]") ~ "Andere tiefgreifende Entwicklungsstörung",
  str_sub(icd_code, end = 6) %in% c("F80.1", "F80.2", "F80.28", "F80.9") ~ "Sprachstörung",
  str_starts(icd_code, "F81\\.[01]")  ~ "Lese-Rechtschreibstörung",
  str_starts(icd_code, "F81\\.2") ~ "Dyskalkulie",
  str_starts(icd_code, "F81\\.3") ~ "Kombinierte Störung schulischer Fertigkeiten",
  str_starts(icd_code, "F82") ~ "Motorische Störung",
  str_starts(icd_code, "F7") ~ "Intelligenz-assoziierte Störung",
  str_starts(icd_code, "F90|F98\\.8") ~ "Hyperkinetische Störung",
  str_starts(icd_code, "F98\\.[56]|F80\\.[08]") ~ "Sprechstörung",
  str_starts(icd_code, "F98\\.[01]|R32|R39\\.4[0128]") ~ "Ausscheidungsstörung",

  str_starts(icd_code, "F40|F41\\.[12389]|F93\\.[01289]|F94\\.0") ~ "Angststörung",

  str_starts(icd_code, "F42|F63\\.3|F45\\.2") ~ "Zwangsstörung",

  str_starts(icd_code, "F63\\.[0129]") ~ "Störung der Impulskontrolle",

  str_starts(icd_code, "F63\\.8") ~ "Pathologischer Medienkonsum",

  str_starts(icd_code, "F95") ~ "Tic-Störung",

  str_starts(icd_code, "F91|F92|F93\\.3") ~ "Störung des Sozialverhaltens",

  str_starts(icd_code, "F94\\.[1289]") ~ "Bindungsstörung",

  # Affektive Störungen
  str_starts(icd_code, "F3[23]|F34\\.[189]|F41\\.2|F48\\.[089]|F92\\.0") ~ "Depression",
  str_starts(icd_code, "F31|F30\\.[1289]|F34\\.0") ~ "Bipolare Störung / Manie",

  # Substanzbezogene Störungen
  str_starts(icd_code, "F10\\.0") ~ "Alkohol Intoxikation",
  str_starts(icd_code, "F10\\.[1-9]") ~ "Alkohol Sonst.",
  str_starts(icd_code, "F12\\.0") ~ "Cannabinoide Intoxikation",
  str_detect(icd_code, "F12\\.[1-9]") ~ "Cannabinoide Sonst.",
  str_starts(icd_code, "F17\\.0") ~ "Tabak Intoxikation",
  str_detect(icd_code, "F17\\.[1-9]") ~ "Tabak Sonst.",
  str_starts(icd_code, "F18\\.0") ~ "Lösungsmittel Intoxikation",
  str_starts(icd_code, "F18\\.[1-9]") ~ "Lösungsmittel Sonst.",
  str_starts(icd_code, "F1[134569]\\.0") ~ "schwerer zug. Substanzen Intoxikation",
  str_starts(icd_code, "F1[134569]\\.[1-9]") ~ "schwerer zug. Substanzen Sonst.",

  str_starts(icd_code, "F43|F62") ~ "Stress-assoziierte Störung",

  str_starts(icd_code, "F45\\.[013489]|F44|F48\\.[189]|F5[12]") ~ "Störung mit körperlicher Symptomatik",

  str_starts(icd_code, "F6[456]") ~ "Störung i. Zsh. m. Sexualerleben",

  # Essstörungen
  icd_code %in% c("F50.0", "F50.00", "F50.01", "F50.1") ~ "Anorexie",
  str_starts(icd_code, "F50\\.[23]") ~ "Bulimie",
  str_starts(icd_code, "F50\\.[4589]|F98\\.2") ~ "Essstörung Sonst.",

  str_starts(icd_code, "F6[012]|F21") ~ "Persönlichkeitsstörung",

  str_starts(icd_code, "F20\\.[01235689]F2[234589]") ~ "Psychotische Störung",

  # andere Kategorien
  str_starts(icd_code, "F") ~ "F Sonst.", #TODO inspect!
  str_starts(icd_code, "[^F]") ~ str_c(str_sub(icd_code, end = 2), " Gruppe")
  )

icd_cats_l2 <- exprs(
  icd_cat_l3 %in% c("Sprachstörung", "Lese-Rechtschreibstörung", "Dyskalkulie",
                    "Kombinierte Störung schulischer Fertigkeiten", "Motorische Störung") |
    str_starts(icd_code, "F81\\.[89]|F83|F88|F89") ~ "Entwicklungsstörung",

  str_starts(icd_cat_l3, "Alkohol |Cannabinoide |Tabak |Lösungsmittel ") ~ "Adoleszenten-assoziierte Substanz",

  str_starts(icd_cat_l3, "schwerer zug\\. Substanzen ") | str_starts(icd_code, "F55") ~
    "Nicht-adoleszenten-assoziierte Substanz",

  # andere Kategorien
  str_starts(icd_code, "[^F]") ~ str_c(str_sub(icd_code, end = 1), " Kapitel")

  # defaults to icd_cat_l3 value if not specified
  )

icd_cats_l1 = exprs(
  icd_cat_l2 %in% c("Autismus-Spektrum-Störung", "Andere tiefgreifende Entwicklungsstörung", "Entwicklungsstörung",
    "Intelligenz-assoziierte Störung", "Hyperkinetische Störung", "Sprechstörung", "Ausscheidungsstörung") ~
    "Neuronale Entwicklungsstörung",
  icd_cat_l3 %in% c("Depression", "Bipolare Störung / Manie") ~ "Affektive Störung",
  icd_cat_l2 %in% c("Adoleszenten-assoziierte Substanz", "Nicht-adoleszenten-assoziierte Substanz") ~
    "Substanzbezogene Störung",
  icd_cat_l3 %in% c("Anorexie", "Bulimie", "Essstörung Sonst.") ~ "Essstörung",

  # andere Kategorien
  str_starts(icd_code, "[^F]") ~ "somatische Diag."

  # defaults to icd_cat_l2 value if not specified
  )

#TODO implement list of treatment cats?

#TODO same case_id if <= 3 weeks between cases (+check for Covid-related discharges)

#TODO add calendar week to all POSIXct cols


# lockdown data --------------------------------------------------------------------------------------------------------
df_lockdown_long <- read_csv(lockdown_f, col_types = cols("f", "f", "f", .default = "i")) %>%
  rename(state_id = ...1) %>%
  rename_with(tolower) %>%
  filter(!is.na(state), !is.na(measure)) %>%
  pivot_longer(cols = !c(state_id, state, measure), names_to = "date", values_to = "status") %>%
  mutate(date = as_date(date, format = "%Y-%m-%d"))

df_lockdown_days <- df_lockdown_long %>%
  pivot_wider(names_from = "measure", values_from = "status")

lockdown_data_period <- df_lockdown_days$date %>% {interval(min(.), max(.))}

df_lockdown_periods <- df_lockdown_long %>%
  filter(status != 0) %>%
  group_by(state, measure, status) %>%
  mutate(period_n = cumsum((date-lag(date, default = first(date))) != 1)) %>%
  group_by(period_n, .add = TRUE) %>%
  summarise(date_start = min(date), date_end = max(date)) %>%
  ungroup() %>%
  mutate(date_period = interval(date_start, date_end))


# read & tidy data -----------------------------------------------------------------------------------------------------
data_raw <- imap(data_fs, \(path, name) {
  encoding <- try(guess_encoding(path)$encoding[[1]])
  if(inherits(encoding, "try-error")) encoding <- "ISO-8859-1"
  read_delim(path, delim = ";", trim_ws = TRUE, col_types = col_formats[[name]],
             locale = locale(date_names = "de", date_format = "%d.%m.%Y", time_format = "%H:%M:%S",
                             decimal_mark =".", tz = "Europe/Berlin", encoding = encoding))
})

# tidy data
data_tidy <- data_raw %>%
  imap(~distinct(.) %>% # remove duplicate rows
         select(where(~!all(is.na(.)))) %>% # remove empty cols
         rename(., any_of(col_names[[.y]])) %>% # unify variable names

         filter(if_any(any_of("icd_code"), ~str_starts(., filter_icd))) %>% # keep/remove selected ICD codes
         mutate(# recode factor levels
                across(starts_with("fa_"), ~fct_relabel(., ~fa_lvls[.])),
                across(ends_with("_hn"), ~fct_relabel(., ~hn_lvls[.])),
                across(any_of("case_state"), ~fct_recode(., !!!case_state_lvls)),
                across(any_of("sex"), ~fct_relabel(., toupper)),

                # remove all occurences of "-" and "." to transfer icpm codes to ops codes
                across(any_of("ops_code"), ~fct_relabel(., ~str_remove_all(., "[\\-\\.]"))),

                # remove 2nd decimal place of diagnoses
                #TODO: add setting if & where to cut
                # across(any_of("icd_code"), ~fct_relabel(., ~str_sub(., end = 5))),

                # drop/lump rare diagnoses/treatmens
                #TODO: move to: after merging (proportions might shift)
                # across(any_of(c("icd_code", "ops_code")), ~fct_lump_prop(., lump_threshold, other_level = other_name))
                ) %>%
         # exclude icd_code & ops_code lump category
         { if(other_exclude) filter(., if_any(any_of(c("icd_code", "ops_code")), ~!str_detect(., other_name)))
           else . } %>%
         mutate(across(where(is.factor), fct_drop)) # drop unused factor levels after all exclusion steps
      )

# expand data, add new variables ---------------------------------------------------------------------------------------
data_exp <- data_tidy %>%
  map(~ # add ICD categories
        { if("icd_code" %in% colnames(.)) {
          mutate(., icd_cat_l3 = case_when(!!!icd_cats_l3, .default = "check_me"), #TODO check "check_me"
                 icd_cat_l2 = case_when(!!!icd_cats_l2, .default = icd_cat_l3),
                 icd_cat_l1 = case_when(!!!icd_cats_l1, .default = icd_cat_l2)) %>%
            mutate(across(starts_with("icd_cat_l"), as_factor))
        } else . } %>%

        # add statuses based on case admission date
        { if("adm_date" %in% colnames(.)) {
          # covid pandemic status
          mutate(., covid_pan = factor(if_else(adm_date < covid_start, "pre_covid", "dur_covid"),
                                       levels = c("pre_covid", "dur_covid"), ordered = TRUE)) %>%
            # lockdown / school closure status
            left_join(df_lockdown_periods %>%
                        filter(state == "Hessen", measure == "school") %>%
                        select(lockdown_status = status, lockdown_period_n = period_n,
                               lockdown_date_start = date_start, lockdown_date_end = date_end),
                      by = join_by(between(adm_date, lockdown_date_start, lockdown_date_end))) %>%
            # select(!c(lockdown_date_start, lockdown_date_end)) %>%
            mutate(lockdown_status = if_else(is.na(lockdown_status) & (adm_date %within% lockdown_data_period),
                                             0, lockdown_status))
        } else . }
  )

# mutate(across(.cols = where(~ is.POSIXt(.) || is.Date(.)), .fns = isoweek, .names = "{.col}_KW")) %>% # Kalenderwoche


# convert to DFs in global env if desired
# for (x in names(data_tidy)) assign(x, data_tidy[[x]])
# write_rds(data_tidy, file.path(outdir, "P21_Fall+ICD_sample_data.rds"))


# merge data -----------------------------------------------------------------------------------------------------------

# select dataframes to merge
# join_srcs <- c("P21_Fall_V1_pseudonym", "P21_ICD_V1_pseudonym")
# join_srcs <- c("ICD_V2", "Pers_Fall_V2_pseudonym", "P21_Fall_V1_pseudonym", "P21_ICD_V1_pseudonym")
join_srcs <- names(data_raw)
# data_tidy[join_srcs] %<>% reduce(inner_join) # by = "P21_Fallnummer_Pseudonym"


# misc section - not needed atm ----------------------------------------------------------------------------------------

# save UTF-8 encoded
# outdir <- file.path("output", "data_utf8")
# dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
# for (x in names(data_raw)) write_csv2(data_raw[[x]], file.path(outdir, str_c(x, "_utf8.csv")))
