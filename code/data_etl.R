#!/usr/bin/env Rscript

# CoverCHILD data ETL
# @author: SP
# @date: 2022-11-04

# setup ----------------------------------------------------------------------------------------------------------------
source("code/functions.R")
load_inst_pkgs("tidyverse", "tools", "magrittr", "lubridate", "ggVennDiagram", "psych", "rlang", "glue", "janitor")

# set wd for VS Code (doesn't align automatically to .Rproj)
old_wd <- set_wd() # function imported from functions.R
if(is.null(old_wd)) rm(old_wd)

outdir <- "output"
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)


# settings -------------------------------------------------------------------------------------------------------------
## general ----
do_save_objects <- FALSE # save final dataframes in outdir

covid_start <- ymd("2020-03-01", tz = "CET")
covid_end <- ymd("2023-04-06", tz = "CET")

data_fs <- Sys.glob("data/*.csv")
names(data_fs) <- basename(file_path_sans_ext(data_fs))
lockdown_f <- "data/ext/ZPID_lockdown_measures_dataset-V6.0.csv"
lockdown_school_hesse_f <- "data/ext/ZPID_lockdown_Arora_Schule.csv"

# sort data sources by descending trustworthiness of information
data_fs <- data_fs[c("Ergebnis_V2_PLZ_PID_Fall_pseudonym", "Pers_Fall_V2_pseudonym", "ICD_V2", "ICPM_V3",
                     "KIJUPSY_Med_Detail_V2_pseudonym", "Labordaten_V3", "Rezepte_Pack_Wirkstoff_V4_pseudonym",
                     "P21_Fall_V1_pseudonym", "P21_ICD_V1_pseudonym", "P21_OPS_V1_pseudonym", "P21_FAB_V1_pseudonym")]

# minimal period between discharge and next admission for cases to be regarded as separate
same_case_span <- weeks(3)

# period between discharge and next admission for cases to be counted as a rapid readmission
# CAVE: specifying duration in months disables automatic rollover. Switch to weeks/days for precise interval
#       (see https://lubridate.tidyverse.org/reference/mplus.html)
re_adm_span <- months(6)

# window to look back when determining whether a case is a patient's first case
first_case_span <- years(2)

# data exclusion
# ICD codes to be in-/excluded
filter_icd <- "[^UVXZ]" # regex as defined by stringi, matched only at start of string

# icd_codes & ops_codes present in fewer than threshold proportion of cases will be lumped together as "Sonstige"
# and excluded if applicable
do_lump <- FALSE
lump_threshold <- 0.0005
other_name <- "Sonstige"
do_other_exclude <- FALSE

# Should factors in untransformed dataframes contain union of factor levels of same variable across all data sources?
do_unify_factor_lvls <- FALSE

## column formats ----
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

## unify column names ----
col_names <- list(
  Ergebnis_V2_PLZ_PID_Fall_pseudonym = c(
    p_id = "Pseudonyme_PID",
    case_id = "Pseudonyme_Fälle",
    case_state_id = "FALLSTATUSID", # 1 (=ambulant), 11 (=stationär), 13 (=teilstationär), 12 (=nachstationär)
    case_state = "FALLSTATUS", # "ambulant" "stationär" "teilstationär" "nachstationär"
    plz_main = "PLZ_3stellig"),
  ICD_V2 = c(
    case_id = "Fall_Pseudonym",
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
  ICPM_V3 = c(
    case_id = "Fall_Pseudonym",
    p_id = "PID Pseudonym",
    case_state = "FALLSTATUS", # "ambulant" "stationär" "nachstationär" "teilstationär"
    age_adm = "Alter_bei_Aufnahme",
    age_treat = "Alter_bei_Maßnahme",
    fa_ops = "ICPM_FA", # "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG" "Kinder- und Jugendpsychiatrie KIJUPSY" "HSA Psy. Kinder- und Jugendliche KIJU POLI" "PIA 92 - Kinder- und Jugendliche PIA KIJU"
    ops_date = "ICPM_DATUM",
    ops_code = "ICPM_CODE",
    ops_hn = "ICPM_HN", # "H" "N"
    ops_loc = "LOKALISATION", # "B"
    ops_label = "ICPM_BEZ"),
  KIJUPSY_Med_Detail_V2_pseudonym = c(
    case_id = "Fall_Pseudonym",
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
  Labordaten_V3 = c(
    case_id = "Fall_Pseudonym",
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
  P21_FAB_V1_pseudonym = c(
    case_id = "P21_Fallnummer_Pseudonym",
    fa_p21 = "Fachabteilung", # "HA3000" "HA0003" "HA0002" "HA0001" "HA2900"
    adm_date_fab = "FAB-Aufnahmedatum",
    dis_date_fab = "FAB-Entlassungsdatum"),
  P21_Fall_V1_pseudonym = c(
    case_id = "P21_Fallnummer_Pseudonym",
    p_id = "P21_PID_Pseudonym",
    yob = "Geburtsjahr",
    sex = "Geschlecht", # "m" "w"
    ik_insurer = "IK-der-Krankenkasse",
    plz_main = "PLZ",
    adm_date = "Aufnahmedatum",
    adm_event = "Aufnahmeanlass", # "N" "E" "V" "B" "A"
    adm_reason_type = "Aufnahmegrund", # "0107" "0301" "0101" "0307" "0201" "0407"
    dis_date = "Entlassungsdatum",
    dis_reason_work = "Entlassungsgrund", # "019" "012" "039" "229" "042" "011" "049" "029" "032" "022" "031" "179" "149" "069" "152" "041" "159" "089"
    age_adm = "Alter-in-Jahren-am-Aufnahmetag",
    ik_hospital = "IK-Verlegungs-KH",
    case_merge = "Fallzusammenführung", # "N" "J"
    case_merge_reason= "Fallzusammenführungsgrund", # "PW" "PR" "PM"
    leave_days_psy = "Beurlaubungstage-PSY"),
  P21_ICD_V1_pseudonym = c(
    case_id = "P21_Fallnummer_Pseudonym",
    icd_hn = "Diagnoseart", # "HD" "ND"
    icd_version = "ICD-Version",
    icd_code = "ICD-Kode",
    icd_loc = "Lokalisation...5", # "L" "R" "B"
    icd_2_code = "Sekundär-Kode",
    icd_2_loc = "Lokalisation...7"), # "L" "R" "B"
  P21_OPS_V1_pseudonym = c(
    case_id = "P21_Fallnummer_Pseudonym",
    ops_version = "OPS-Version",
    ops_code = "OPS-Kode",
    ops_loc = "Lokalisation", # "B" "L" "R"
    ops_date = "OPS-Datum"),
  Pers_Fall_V2_pseudonym = c(
    p_id = "PID Pseudonym",
    sex = "GE", # "M" "W" "D"
    yob = "Geburtsjahr",
    age_adm = "Alter_Aufahme",
    plz = "PLZ-3stellig",
    addr_type = "ADRESSART", # "frühere Adresse" "Erstanschrift" "Rechnungsanschrift Ambu" "Zweitanschrift" "Ehemalige Erstanschrift" "Befundverschickung" "Rechnungsanschrift" "Info-Adresse"
    case_id = "Fall_Pseudonym",
    case_state = "FALLSTATUS", # "stationär" "ambulant" "teilstationär" "nachstationär"
    adm_date = "AUFNDAT",
    dis_date = "ENTLDAT",
    adm_reason = "AUFNAHMEGRUND", # "KH-Behandlung, vollstat." "KH-Behandlung, teilstat." "KH-Behandlung, vollstat. nach vorstat."
    adm_type = "AUFNAHMEART", # "Notfall" "Normalfall"
    stay_type = "AUFENTHALTSART", # 1 NA
    dis_reason = "ENTLASSART",
    fa_icd = "ICD_FA"), # "Kinder- und Jugendpsychiatrie KIJUPSY" "PIA 92 - Kinder- und Jugendliche PIA KIJU" "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG" "HSA Psy. Kinder- und Jugendliche KIJU POLI"
  Rezepte_Pack_Wirkstoff_V4_pseudonym = c(
    case_id = "Fall_Pseudonym",
    presc_date = "Rezept_Datum",
    presc_time = "Rezept_Zeit",
    presc_orderer = "Mediz_BST", # "PIA KIJU" "92-2" "CA FREITAG" "92-3" "92-1" "93-1" "KIJU POLI" "92-5" "92-4"
    fa_presc = "Mediz_FA", # "KIJUPSY"
    presc_package_info = "PackungInfo",
    presc_drug = "WirkstoffBez")
  )

## normal form target DF variable assignment ----
vars_norm_dfs <- list(
  # 1 per patient
  patient = c("sex", "yob"),
  # 1 per case
  case = c("p_id", "case_state", "plz_prev", "plz_main", "plz_invoice_amb", "plz_second", "plz_prev_main",
           "plz_reports", "plz_invoice", "plz_info","adm_date", "dis_date", "length_stay", "age_adm", "ik_insurer",
           "adm_event", "adm_reason", "dis_reason", "dis_work", "ik_hospital", "leave_days_psy", "adm_type",
           "stay_type", "case_merge", "case_merge_reason", "addr_type"),
  # n per case
  diagnosis = c("fa_icd", "icd_date", "icd_code", "icd_type", "icd_hn", "icd_label", "icd_2_code", "icd_2_label",
                "icd_version", "icd_loc", "icd_2_loc"), # maybe exclude Pers_Fall
  treatment = c("age_treat", "fa_ops", "ops_date", "ops_code", "ops_hn", "ops_loc", "ops_label", "ops_version"),
  prescription = c("presc_date", "presc_orderer", "fa_presc", "presc_package_info", "presc_drug"),
  medication = c("fa_med", "med_start_date", "med_end_date", "med_nvl", "med_unit", "med_admin_weekly",
                 "med_admin_daily", "med_cbx_req"),
  laboratory = c("fa_lab", "lab_prot_n", "lab_date", "lab_code", "lab_label", "lab_group", "lab_val", "lab_unit",
                 "lab_norm_val", "lab_abnorm_dir", "lab_val_text"),
  external_stays = c("fa_p21", "adm_date_fab", "dis_date_fab"))

# Vars that should be uniform per case
unify_per_case <- list(first = c("yob", "sex", "ik_insurer", "adm_event", "adm_reason", "ik_hospital", "case_merge",
                                 "case_merge_reason", "leave_days_psy", "plz_main", "plz_prev","adm_type", "stay_type"),
                       last = c("dis_date", "dis_reason", "dis_type", "dis_work"),
                       min = c("adm_date", "age_adm"),
                       max = "case_state")

# how to collapse multiple values to a single entity
collapse_to <- "glue" # "glue" for string concatenation, "list" for a list

## adjust factors ----
### unify/recode factor levels & labels ----
# CAVE: order of names depends on used function: fct_recode needs new=old, fct_relabel needs old=new
# all recode
fa_lvls_rec <- c("KIJUPSY" = "Kinder- und Jugendpsychiatrie KIJUPSY",
                 "PIA KIJU" = "PIA 92 - Kinder- und Jugendliche PIA KIJU",
                 "KIJU POLI" = "HSA Psy. Kinder- und Jugendliche KIJU POLI",
                 "CA FREITAG" = "Prof. Dr. Christine Freitag, Privatsprechstunde CA FREITAG",
                 "Allg. Psychiatrie" = "HA2900",
                 "KIJUPSY" = "HA3000",
                 "Pseudo-FA: Rückverlegung" = "HA0001",
                 "Pseudo-FA: Wiederaufnahme nach ext. Aufenthalt" = "HA0002",
                 "Pseudo-FA: ext. Aufenthalt m. Abwesenheit über Mitternacht" = "HA0003")
hn_lvls_rec <- c("H" = "HD", "N" = "ND")
sex_lvls_rec <- c("M" = "m", "W" = "w", "D" = "d") # used right now is "toupper"
case_state_lvls_rec <- c("ambulant" = "nachstationär")
adm_reason_lvls_rec <- c("KH-Behandlung, vollstat." = "01",
                         "KH-Behandlung, vollstat. nach vorstat." = "02",
                         "KH-Behandlung, teilstat." = "03",
                         "Behandlung, vorst." = "04")
adm_type_lvls_rec <- c("Normalfall" = "01",
                       "Notfall" = "07")
dis_reason_lvls_rec <-
  c("Beh. regulär beendet" = "01",
    "Beh. regulär beendet, nachstat. Beh. vorgesehen" = "02",
    "Behand. aus sonst. Gründen beendet" = "03",
    "Beh. gegen ärztl. Rat beendet" = "04",
    "Verlegung in ein anderes KH" = "06",
    "Verlegung in ein anderes KH (Zusammenarb. §14 A5 S2)" = "08",
    "interne Verlegung zwischen den Geltungsbereichen" = "12",
    "externe Verlegung zur psychiatrischen Behandlung" = "13",
    "Beh. aus sonst. Gründen beendet, nachstat. Beh. vorgesehen" = "14",
    "Beh. gegen ärztl. Rat beendet, nachstat. Beh. vorgesehen" = "15",
    "interne Verlegung mit Wechsel zwischen den Entgeltbereichen" = "17",
    "Fallabschluss (interne Verlegung) bei Wechsel zwischen voll- und teilstationärer Behandlung" = "22")
dis_work_lvls_rec <- c("arbeitsfähig entlassen" = "1",
                       "arbeitsunfähig entlassen" = "2",
                       "keine Angabe" = "9")
addr_type_lvls_rec <- c("prev" = "frühere Adresse",
                        "main" = "Erstanschrift",
                        "invoice_amb" = "Rechnungsanschrift Ambu",
                        "second" = "Zweitanschrift",
                        "prev_main" = "Ehemalige Erstanschrift",
                        "reports" = "Befundverschickung",
                        "invoice" = "Rechnungsanschrift",
                        "info" = "Info-Adresse")

#TODO add other codes from codebook (icd_type, fa_p21, adm_event, case_merge_reason, stay_type, +?)

### order factor levels ----
case_state_order <- c("ambulant", "teilstationär", "nachstationär", "stationär")
icd_type_order <- c("Einw.", "Aufn.", "Beh.", "FA En", "Abr", "Entl.")
addr_type_order <- c("prev", "prev_main", "second", "invoice_amb", "info", "reports", "invoice", "main")

## ICD categories in 3 levels ----
icd_cats_l3 <- exprs(
  # Neuronale Entwicklungsstörungen
  str_starts(icd_code, "F0") ~ "Organische Störung",
  str_starts(icd_code, "F84\\.[015]") ~ "Autismus-Spektrum-Störung",
  str_starts(icd_code, "F84\\.[2389]") ~ "Andere tiefgreifende Entwicklungsstörung",
  str_starts(icd_code, "F80\\.[129]") ~ "Sprachstörung",
  str_starts(icd_code, "F81\\.[01]")  ~ "Lese-Rechtschreibstörung",
  str_starts(icd_code, "F81\\.2") ~ "Dyskalkulie",
  str_starts(icd_code, "F81\\.3") ~ "Kombinierte Störung schulischer Fertigkeiten",
  str_starts(icd_code, "F82") ~ "Motorische Störung",
  str_starts(icd_code, "F7|F84\\.4") ~ "Intelligenz-assoziierte Störung",
  str_starts(icd_code, "F90|F98\\.[89]") ~ "Hyperkinetische Störung",
  str_starts(icd_code, "F98\\.[56]|F80\\.[08]") ~ "Sprechstörung",
  str_starts(icd_code, "F98\\.[01]|R32|R39\\.4[0128]") ~ "Ausscheidungsstörung",

  str_starts(icd_code, "F40|F41|F93\\.[01289]|F94\\.0") ~ "Angststörung",

  str_starts(icd_code, "F42|F63\\.3|F45\\.2") ~ "Zwangsstörung",

  str_starts(icd_code, "F63\\.[0129]") ~ "Störung der Impulskontrolle",

  str_starts(icd_code, "F63\\.8") ~ "Pathologischer Medienkonsum",

  str_starts(icd_code, "F95") ~ "Tic-Störung",

  str_starts(icd_code, "F91|F92|F93\\.3") ~ "Störung des Sozialverhaltens",

  str_starts(icd_code, "F94\\.[1289]") ~ "Bindungsstörung",

  # Affektive Störungen
  str_starts(icd_code, "F3[23]|F34\\.[189]|F38\\.1|F41\\.2|F48\\.0|F92\\.0") ~ "Depression",
  str_starts(icd_code, "F3[01]|F34\\.0") ~ "Bipolare Störung / Manie",

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

  str_starts(icd_code, "F45\\.[013489]|F44|F48\\.1|F5[12]") ~ "Störung mit körperlicher Symptomatik",

  str_starts(icd_code, "F6[456]") ~ "Störung i. Zsh. m. Sexualerleben",

  # Essstörungen
  str_starts(icd_code, "F50\\.[01]") ~ "Anorexie",
  str_starts(icd_code, "F50\\.[23]") ~ "Bulimie",
  str_starts(icd_code, "F50\\.[4589]|F98\\.[23]") ~ "Essstörung Sonst.",

  str_starts(icd_code, "F6[012]|F21|F68\\.[18]") ~ "Persönlichkeitsstörung",

  str_starts(icd_code, "F2[0234589]") ~ "Psychotische Störung",

  # andere Kategorien
  str_starts(icd_code, "F") ~ "F Sonst.",
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

## treatment intensity/volume ----
ops_treat_intensity_cats <- exprs(str_starts(ops_code, "967") ~ "int_treat_ordered",
                                  str_starts(ops_code, "9693") ~ "int_treat_applied")
ops_treat_prof_vol_regex <- "(?<=9696)(?<prof>[13])(?<vol>[0-9a-g])"
ops_treat_prof_cats <- exprs(prof == "1" ~ "physician", prof == "3" ~ "psychologist")


# read & tidy data -----------------------------------------------------------------------------------------------------
## read raw data ----
data_raw <- imap(data_fs, \(path, name) {
  encoding <- try(guess_encoding(path)$encoding[[1]])
  if(inherits(encoding, "try-error")) encoding <- "ISO-8859-1"
  read_delim(path, delim = ";", trim_ws = TRUE, col_types = col_formats[[name]],
             na = c("", "NA", "na", "NaN", "-/-", "#NV", " "),
             locale = locale(date_names = "de", date_format = "%d.%m.%Y", time_format = "%H:%M:%S",
                             decimal_mark =".", tz = "CET", encoding = encoding))
  })

## recode p_ids in P21 data ----
# CAVE: p_id in P21 data differs from Orbis. Recoding P21 IDs to Orbis IDs
p21_p_id_lvls_rec <- data_raw$Pers_Fall_V2_pseudonym %>%
  select(p_id = `PID Pseudonym`, case_id = Fall_Pseudonym, adm_date = AUFNDAT, dis_date = ENTLDAT, sex = GE,
         yob = Geburtsjahr) %>%
  distinct() %>%
  inner_join(data_raw$P21_Fall_V1_pseudonym %>%
               select(p_id = P21_PID_Pseudonym, case_id = P21_Fallnummer_Pseudonym, adm_date = Aufnahmedatum,
                      dis_date = Entlassungsdatum, sex = Geschlecht, yob = Geburtsjahr) %>%
               distinct(),
             by = c("case_id"), suffix = c("_Orb", "_P21")) %>%
  mutate(across(where(is.factor), as.character)) %>%
  filter(yob_Orb == yob_P21 | is.na(yob_Orb) | is.na(yob_P21),
         sex_Orb == toupper(sex_P21) | is.na(sex_Orb) | is.na(sex_P21),
         adm_date_Orb == adm_date_P21 | is.na(adm_date_Orb) | is.na(adm_date_P21),
         p_id_Orb != p_id_P21) %>%
  group_by(case_id) %>%
  mutate(n_tot_case = n(),
         n_pid_Orb = n_distinct(p_id_Orb, na.rm = TRUE),
         n_pid_P21 = n_distinct(p_id_P21, na.rm = TRUE)) %>%
  ungroup() %>%
  filter(n_pid_Orb == 1 & n_pid_P21 == 1) %>%
  select(p_id_Orb, p_id_P21) %>%
  distinct() %>%
  deframe()

## tidy data ----
data_tidy <- data_raw %>%
  imap(~distinct(.) %>% # remove duplicate rows
         select(where(~!all(is.na(.)))) %>% # remove empty cols
         rename(., any_of(col_names[[.y]])) %>% # unify variable names

         { if("adm_reason_type" %in% colnames(.)) { # separate combined admission reason & workableness if present
           separate_wider_position(., adm_reason_type, c(adm_reason = 2, adm_type = 2))
         } else . } %>%

         { if("dis_reason_work" %in% colnames(.)) { # separate combined discharge reason & workableness if present
           separate_wider_position(., dis_reason_work, c(dis_reason = 2, dis_work = 1), too_few = "align_start")
         } else . } %>%

         { if(all(c("plz", "addr_type") %in% colnames(.))) { # widen plz+addr_type to plz_* cols
           select(., -any_of("fa_icd")) %>%
             distinct() %>%
             mutate(across(any_of("addr_type"), ~fct_recode(., !!!addr_type_lvls_rec))) %>%
             pivot_wider(names_from = addr_type, values_from = plz,
                         names_prefix = "plz_", values_fn = \(x) first(x, na_rm = TRUE)) %>%
             select(where(~!all(is.na(.))))
         } else . } %>%

         mutate(# recode & reorder factor levels
                across(starts_with("fa_"), ~fct_expand(., fa_lvls_rec) %>% fct_recode(!!!fa_lvls_rec)),
                across(ends_with("_hn"), ~fct_expand(., hn_lvls_rec) %>% fct_recode(!!!hn_lvls_rec)),
                across(any_of("case_state"),
                       ~fct_recode(., !!!case_state_lvls_rec) %>% ordered(levels = case_state_order)),
                across(any_of("sex"), ~fct_relabel(., toupper)),
                across(any_of("p_id"), ~fct_expand(., p21_p_id_lvls_rec) %>% fct_recode(!!!p21_p_id_lvls_rec)),
                across(any_of("adm_reason"),
                       ~fct_expand(., adm_reason_lvls_rec) %>% fct_recode(!!!adm_reason_lvls_rec)),
                across(any_of("adm_type"), ~fct_expand(., adm_type_lvls_rec) %>% fct_recode(!!!adm_type_lvls_rec)),
                across(any_of("dis_reason"),
                       ~fct_expand(., dis_reason_lvls_rec) %>% fct_recode(!!!dis_reason_lvls_rec)),
                across(any_of("dis_work"), ~fct_recode(., !!!dis_work_lvls_rec)),
                across(any_of("icd_type"), ~ordered(., levels = icd_type_order)),

                # remove all occurences of "-" and "." to transfer icpm codes to ops codes
                across(any_of("ops_code"), ~fct_relabel(., ~str_remove_all(., "[\\-\\.]"))),

                # unify prescription date+time column to datetime
                across(any_of("presc_date"), ~(ymd(., tz = "CET") + hms(presc_time))),

                # fill in length_stay where missing and both dates present
                across(any_of("length_stay"), ~coalesce(., as.numeric(dis_date-adm_date)))
                ) %>%
         select(-any_of(c("presc_time", "case_state_id"))) %>% # drop redundant cols

         # drop unused factor levels after all recoding & exclusion steps
         mutate(across(where(is.factor), ~fct_drop(.) %>% fct_na_level_to_value())) %>%
         distinct() # remove duplicate vars
      )

# union of factor levels of same variable across all data sources
if(do_unify_factor_lvls) {
  data_tidy %<>%
    map(~mutate(., across(where(is.factor),
                          ~fct_expand(., map(data_tidy, ~levels(.[[cur_column()]])) %>% list_c() %>% unique()))))
}


# transform to "normal form" database-like dataframes ------------------------------------------------------------------
norm_dfs_sources <- lapply(vars_norm_dfs, \(x) find_dfs_with_col(x, df_list = data_tidy))
data_norm <-
  imap(norm_dfs_sources, \(source_dfs, norm_name) {
    id_cols <- if(norm_name == "patient") "p_id" else "case_id"
    data_tidy[source_dfs] %>%
      imap(~select(., any_of(c(id_cols, vars_norm_dfs[[norm_name]]))) %>%
             distinct() %>%
             mutate("source_{.y}" := .y)
           ) %>%
      reduce(~ { full_join(.x, .y) %>%
          group_by(across(any_of(id_cols))) %>%
          mutate(across(everything(),
                        \(x) if(n_distinct(x, na.rm = TRUE) == 1) first(x, na_rm = TRUE) else x
          )) %>%
          ungroup() %>%
          distinct()}) %>%
      unite(source_dfs, starts_with("source_"), sep = ", ", na.rm = TRUE) %>%
      mutate(source_dfs = as_factor(source_dfs)) %>%
      distinct() %>%
      mutate(across(where(is.factor), ~fct_drop(.) %>% fct_na_level_to_value()))
  })
#TODO Warning message:
#   In full_join(.x, .y) :
# Each row in `x` is expected to match at most 1 row in `y`.
# ℹ Row 21055 of `x` matches multiple rows.
# ℹ Row 1 of `y` matches multiple rows in `x`.
# ℹ If a many-to-many relationship is expected, set `relationship = "many-to-many"` to silence this warning.

# case has multiple plz_main, dis_date, dis_reason after merge, stemming from conflicting info between Orbis & P21.
# Selecting first entry to prioritize Orbis over P21.
#TODO: save conflicts
data_norm$case %<>%
  arrange(p_id, case_id) %>%
  group_by(case_id) %>%
  slice_max(dis_date) %>%
  slice_head(n = 1) %>%
  ungroup()

norm_dfs_merge_success <-
  imap(data_norm, ~fct_count(.x$source_dfs, sort = TRUE, prop = TRUE) %T>% {cat(.y, "\n"); print(.); cat("\n")})
#TODO look into rows that are only in a single df, find pattern


# merge cases ----------------------------------------------------------------------------------------------------------
## create merge dict ----
# factor order of case_ids & dict to merge cases with <3 weeks between discharge and next admission
case_merge_dict <- data_norm$case %>%
  select(p_id, adm_date, dis_date, case_id_orig = case_id) %>%
  distinct() %>%
  arrange(p_id, adm_date) %>%
  group_by(p_id) %T>%

  # assign intermediate result: ordered case_id_orig factor levels
  {use_series(., case_id_orig) %>% fct_inorder() %>% head(0) %>% assign("case_id_orig_lvls_ordered", ., pos = 1)} %>%

  # If previous dis_date is missing, fall back on prev adm_date to compute at least an upper limit of time between
  # 2 adjacent cases
  mutate(case_id = cumsum(!replace_na((adm_date-same_case_span) < coalesce(lag(dis_date), lag(adm_date)), FALSE))) %>%
  group_by(case_id, .add = TRUE) %>%
  filter(n() > 1) %>%
  mutate(case_id = str_c(first(case_id_orig, na_rm = TRUE), "_m")) %>%
  ungroup() %>%
  select(case_id, case_id_orig) %>%
  mutate(case_id = as_factor(case_id)) %T>%

  # assign result: case_id recode dict
  { mutate(., across(everything(), as.character)) %>%
      deframe() %>%
      assign("case_id_lvls_rec", ., pos = 1) } %T>%
  # assign result: case_id recode nested list
  { summarise(., val = unique(case_id_orig) %>% fct_inorder() %>% fct_drop() %>% list(), .by = case_id) %>%
      deframe() %>%
      assign("case_id_lvls_by_new", ., pos = 1) }

## merge cases ----
data_norm_merged <- data_norm %>%
  map(~ { if("case_id" %in% colnames(.)) { # merge cases
          mutate(., case_id_orig = fct_c(case_id_orig_lvls_ordered, case_id),
                 case_id = fct_recode(case_id_orig, !!!case_id_lvls_rec)) %>%
            arrange(across(any_of(c("p_id", "case_id", "case_id_orig", "adm_date", "icd_date", "ops_date",
                                    "med_start_date", "med_end_date", "lab_date", "dis_date")))) %>%
            rename_with(~str_c(., "_orig"), any_of(list_c(unify_per_case))) %>%
            group_by(case_id) %>%
            # unify selected variables per case
            # (Although we want to get down to 1 row per case, we use mutate+distinct instead of summarise to be able
            #  to look for and catch errors in the data)
            mutate(across(any_of(str_c(unify_per_case$first, "_orig")), ~first(., na_rm = TRUE),
                          .names = '{str_remove(.col, "_orig")}'),
                   across(any_of(str_c(unify_per_case$last, "_orig")), ~last(., na_rm = TRUE),
                          .names = '{str_remove(.col, "_orig")}'),
                   across(any_of(str_c(unify_per_case$min, "_orig")), min_na, .names = '{str_remove(.col, "_orig")}'),
                   across(any_of(str_c(unify_per_case$max, "_orig")), max_na, .names = '{str_remove(.col, "_orig")}'),
                   across(any_of("length_stay"),
                          list(netto = ~(pick(case_id_orig, length_stay) %>%
                                           group_by(case_id_orig) %>%
                                           summarise(length_stay = first(length_stay, na_rm = TRUE)) %>%
                                           use_series(length_stay) %>%
                                           sum_na() %>%
                                           na_if(0)),
                               # technically, max+min should not be necessary as both dates are recoded to last/first
                               brutto = ~as.numeric(max_na(dis_date) - min_na(adm_date)))),
                   across(any_of("source_dfs"),
                          ~str_split(., ", ") %>% list_c() %>% collapse_na(sum_fun = collapse_to)),
                   # keep only _orig values when differing from new versions
                   across(ends_with("_orig"),
                          ~na_if(as.character(.), as.character(get(str_remove(cur_column(), "_orig")))) %>%
                            collapse_na(sum_fun = collapse_to)),
                   across(any_of("length_stay"), ~collapse_na(., sum_fun = collapse_to))
                   ) %>%
            ungroup() %>%
            distinct() %>%
            select(where(~!all(is.na(.)))) %>% # remove empty cols
            mutate(across(where(is.factor), fct_drop)) %>%  # drop unused factor levels
            relocate(ends_with("_orig"), .after = last_col()) # move "_orig" variables to back
        } else . }
      )


# filter & expand data, merge cases, add new variables -----------------------------------------------------------------
## lockdown data ----
# replace "lockdown_school_hesse_f" with "lockdown_f" for the ZPID dataset containing all measures and states
df_lockdown_long <- read_csv(lockdown_school_hesse_f, col_types = cols("f", "f", "f", .default = "i")) %>%
  rename(state_id = ...1) %>%
  rename_with(tolower) %>%
  filter(!is.na(state), !is.na(measure)) %>%
  pivot_longer(cols = !c(state_id, state, measure), names_to = "date", values_to = "status") %>%
  mutate(date = ymd(date))

df_lockdown_days <- df_lockdown_long %>%
  pivot_wider(names_from = "measure", values_from = "status")

lockdown_data_period <- df_lockdown_days$date %>% {interval(min_na(.), max_na(.))}

# extract covid lockdown status periods
df_covid_periods <- df_lockdown_long %>%
  # find continuous periods of statuses by state & measure
  group_by(state, measure, status) %>%
  mutate(period_i_all = cumsum((date-lag(date, default = first(date, na_rm = TRUE))) != 1)) %>%
  group_by(period_i_all, .add = TRUE) %>%
  summarise(period_start_date = min_na(date), period_end_date = max_na(date)) %>%
  group_by(state, measure) %>%
  arrange(period_start_date) %>%
  mutate(
    # recode periods
    period_i_all = row_number(),
    # extend first and last periods to span whole covid pandemic
    period_start_date = if_else(period_start_date == min_na(period_start_date), covid_start, period_start_date),
    period_end_date = if_else(period_end_date == max_na(period_end_date), covid_end, period_end_date) + hms("23:59:59"),
    # recode school holidays to fully closed when lying within a lockdown period or to open otherwise
    status_no_hol = case_when(status == 8 & lag(status, default = 0) != 0 & lead(status, default = 0) != 0 ~ 2L,
                              status == 8 ~ 0L,
                              .default = status),
    # find continuous periods of updated statuses by state & measure
    period_i_no_hol = cumsum((status_no_hol-lag(status_no_hol, default = first(status_no_hol))) != 0)+1L,
    # status based only on lockdown or no lockdown
    status_is_lockd = if_else(status_no_hol == 0, 0, 1),
    period_i_is_lockd = cumsum((status_is_lockd-lag(status_is_lockd, default = first(status_is_lockd))) != 0)+1L,

    # subperiod_i = str_c(period_i, status_group, sep = "_") %>% as.ordered(),
    ) %>%
  # add times based on no-holiday statuses
  group_by(period_i_no_hol, .add = TRUE) %>%
  mutate(period_no_hol_start_date = min_na(period_start_date),
         period_no_hol_end_date = max_na(period_end_date)) %>%
  ungroup() %>%
  # add time vars
  # status_date_period = interval(period_start_date, period_end_date),
  mutate(across(ends_with("_date"),
         ~str_c(year(.), ".", week(.)) %>% fct_relevel(~str_sort(., numeric = TRUE)) %>% as.ordered(),
         .names = '{str_replace(.col, "_date", "_year_week")}'))


## filter & expand normalized data ----
data_exp <- data_norm_merged %>%
  map(~ # drop/lump rare diagnoses/treatmens
        { if(do_lump) mutate(., across(any_of(c("icd_code", "ops_code")),
                                       ~fct_lump_prop(., lump_threshold, other_level = other_name)))
          else . } %>%
        # exclude icd_code & ops_code lump category
        { if(do_other_exclude) filter(., if_any(any_of(c("icd_code", "ops_code")), ~!str_detect(., other_name)))
          else . } %>%

        # filter selected ICD codes and add ICD categories
        { if("icd_code" %in% colnames(.)) {
          filter(., str_starts(icd_code, filter_icd)) %>% # keep/remove selected ICD codes
          mutate(icd_cat_l3 = case_when(!!!icd_cats_l3, .default = "check_me"), # "check_me" for not-captured cases
                 icd_cat_l2 = case_when(!!!icd_cats_l2, .default = icd_cat_l3),
                 icd_cat_l1 = case_when(!!!icd_cats_l1, .default = icd_cat_l2)) %>%
            mutate(across(starts_with("icd_cat_l"), as_factor))
        } else . } %>%

        # add treatment intensity and volume coding
        { if("ops_code" %in% colnames(.)) {
          mutate(., str_match(ops_code, ops_treat_prof_vol_regex)[, c("prof", "vol")] %>%
                   as_tibble() %>%
                   mutate(treat_prof = case_when(!!!ops_treat_prof_cats) %>% as_factor(),
                          treat_vol = strtoi(vol, 17L)+1,
                          .keep = "none"),
                 treat_int = case_when(!!!ops_treat_intensity_cats) %>% as_factor())
        } else . } %>%

        # add patient/case readmission lag and status
        { if(all(c("p_id", "case_id", "adm_date", "dis_date") %in% colnames(.))) {
          group_by(., p_id) %>%
            arrange(adm_date) %>%
            mutate(is_first_case_abs = case_id == first(case_id, na_rm = TRUE),
                   re_adm_lag = difftime(adm_date, coalesce(lag(dis_date), lag(adm_date)), units = "days"),
                   re_adm_soon = re_adm_lag < re_adm_span,
                   is_first_case_rel = (re_adm_lag > first_case_span) %>% replace_na(TRUE)) %>%
            ungroup()
        } else . } %>%

        # add statuses based on case admission date
        { if("adm_date" %in% colnames(.)) {
          # covid pandemic status
          mutate(., covid_pan = factor(if_else(adm_date < covid_start, "pre_covid", "dur_covid"),
                                       levels = c("pre_covid", "dur_covid"), ordered = TRUE)) %>%
            # lockdown / school closure status
            left_join(df_covid_periods %>%
                        filter(state == "Hessen", measure == "school") %>%
                        select(-c(state, measure)) %>%
                        rename_with(\(x) str_c("lockd_", x)),
                      by = join_by(between(adm_date, lockd_period_start_date, lockd_period_end_date)))
        } else . } %>%

        # add calendar week to all dates
        mutate(across(where(~ is.POSIXt(.) || is.Date(.)),
                      ~str_c(year(.), ".", week(.)) %>% fct_relevel(~str_sort(., numeric = TRUE)) %>% as.ordered(),
                      .names = '{str_replace(.col, "_date", "_week")}')) %>%

        mutate(across(where(is.factor), ~fct_drop(.) %>% fct_na_level_to_value()))
      )

# convert to DFs in global env if desired
# for (x in names(data_exp)) assign(x, data_exp[[x]])


# summarise data -------------------------------------------------------------------------------------------------------
# data_exp_sum <-
#   map(data_exp[!names(data_exp) %in% c("patient", "case")],
#       ~summarise(., across(everything(), first), .by = case_id) %>%
#         mutate(across(where(is.factor), ~fct_drop(.) %>% fct_na_level_to_value()))
#       )
# data_exp$case can have multiple rows per case after merging cases.

data_exp_sum <- list()

## diagnosis ----
data_exp_sum$diagnosis <- data_exp$diagnosis %>%
  #TODO instead of filtering only Entlassdiagnosen, code as ordered factor and take slice_max(icd_type),
  #     and add option to switch between the 2
  filter(icd_type == "Entl.") %>%
  group_by(case_id, icd_code) %>%
  slice_max(icd_date, with_ties = FALSE) %T>%

  # inspect if there are duplicated diagnoses per case
  {find_mult_per(., case_id, icd_code) %>%
      { if(ncol(.)>2) {
        assign("mult_diag_per_date_cases", ., pos = 1)
        warning("The diagnosis data contains duplicated icd codes with the same date. ",
                "Diagnosis summary statistics this will be biased. ",
                "Please check 'mult_diag_per_date_cases' for affected cases and varying columns.") }}} %>%

  group_by(case_id) %>%
  summarise(n_icd_f = sum(str_starts(unique(na.omit(icd_code)), "F")),
            n_icd_other = sum(str_starts(unique(na.omit(icd_code)), "[^F]")),
            across(c(icd_code, icd_date, starts_with("icd_cat_")), list(n = ~n_distinct(.x, na.rm = TRUE)),
                   .names = "{.fn}_{.col}"),
            across(c(icd_date, icd_week), list(first = min_na, last = max_na)),
            across(c(fa_icd, icd_hn, icd_version, case_id_orig, starts_with("icd_cat_")),
                   ~collapse_na(., sum_fun = collapse_to))
            ) %>%
  ungroup()

## treatment ----
data_exp_sum$treatment <- data_exp$treatment %>%
  summarise(across(c(age_treat), min_na),
            across(c(ops_date, ops_week), list(first = min_na, last = max_na)),
            across(c(fa_ops, ops_version, case_id_orig), ~collapse_na(., sum_fun = collapse_to)),
            # treatment intensity & volume
            n_int_treat_ord = sum_na(treat_int == "int_treat_ordered"),
            n_int_treat_app = sum_na(treat_int == "int_treat_applied"),
            n_treat_weekly_by_psy_max = "...", # "highest weekly treatment volume by psychologists"
            n_treat_weekly_by_med_max = "...", # ""highest weekly treatment volume by physician"
            n_treat_weekly_max = "...", # ""highest weekly treatment volume combined"
            n_treat_weekly_by_psy_mean = "...", # ""mean weekly treatment volume by psychologists"
            n_treat_weekly_by_med_mean = "...", # ""mean weekly treatment volume by physician"
            n_treat_weekly_mean = "...", # ""mean weekly treatment volume combined"
            n_treat_by_psy_sum = "...", # ""absolute treatment volume by psychologists"
            n_treat_by_med_sum = "...", # ""absolute treatment volume by physician"
            n_treat_sum = "...", # ""absolute treatment volume combined"
            .by = case_id
            )


# create codebooks -----------------------------------------------------------------------------------------------------
codebook_data_exp <- imap(data_exp, ~ create_codebook(.)) %>%
  bind_rows(.id = "dataframe") %>%
  mutate(across(dataframe, as_factor)) %>%
  # add respective original variable names
  left_join(list_c(col_names) %>%
              enframe(name = "variable_name", value = "variable_names_original") %>%
              summarise(variable_names_original = collapse_na(variable_names_original, sum_fun = collapse_to),
                        .by = variable_name),
            by = "variable_name") %>%
  # add respective variable source dataframes
  mutate(
    data_sources = find_dfs_with_col(variable_name, df_list = data_tidy) %>% collapse_na(sum_fun = collapse_to)) %>%
  relocate(variable_names_original, .after = variable_name) %>%
  arrange(dataframe, variable_name)

codebook_data_exp_sum <- imap(data_exp_sum, ~ create_codebook(.)) %>%
  bind_rows(.id = "dataframe") %>%
  mutate(across(dataframe, as_factor))


# save objects ---------------------------------------------------------------------------------------------------------
if(do_save_objects) {
  # Complete workspace
  save.image(file.path(outdir, str_glue("CoverCHILD_data_ETL_{Sys.Date()}.RData")))
  save.image(file.path(outdir, str_glue("CoverCHILD_data_ETL_{Sys.Date()}.RData.xz")), compress = "xz")
  # Dataframes to R Data Structure
  write_rds(data_exp, file.path(outdir, str_glue("CoverCHILD_data_exp_{Sys.Date()}.rds")))
  write_rds(data_exp_sum, file.path(outdir, str_glue("CoverCHILD_data_exp_sum_{Sys.Date()}.rds")))
  # Dataframes to CSV
  dir.create(file.path(outdir, "data_exp"), recursive = TRUE, showWarnings = FALSE)
  for (x in names(data_exp)) write_csv2(data_exp[[x]], file.path(outdir, "data_exp", str_c(x, "_exp.csv")))
  # codebooks
  write_csv2(codebook_data_exp, file.path(outdir, str_glue("CoverCHILD_codebook_{Sys.Date()}.csv")),
             na = "")
  write_csv2(codebook_data_exp_sum, file.path(outdir, str_glue("CoverCHILD_codebook_sum_{Sys.Date()}.csv")),
             na = "")
}
