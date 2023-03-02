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
do_save_objects <- FALSE # save final dataframes in outdir

covid_start <- ymd("2020-03-01")

data_fs <- Sys.glob("data/*.csv")
names(data_fs) <- basename(file_path_sans_ext(data_fs))
lockdown_f <- "data/ext/ZPID_lockdown_measures_dataset-V6.0.csv"

# data exclusion
# ICD codes to be in-/excluded
filter_icd <- "[^XVZ]" # regex as defined by stringi, matched only at start of string

# icd_codes & ops_codes present in fewer than threshold proportion of cases will be lumped together as "Sonstige"
# and excluded if applicable
do_lump <- FALSE
lump_threshold <- 0.0005
other_name <- "Sonstige"
do_other_exclude <- FALSE

# Should factors in untransformed dataframes contain union of factor levels of same variable across all data sources?
do_unify_factor_lvls <- FALSE

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
col_names <- list(
  Ergebnis_V2_PLZ_PID_Fall_pseudonym = c(
    p_id = "Pseudonyme_PID",
    case_id = "Pseudonyme_Fälle",
    case_state_id = "FALLSTATUSID", # 1 (=ambulant), 11 (=stationär), 13 (=teilstationär), 12 (=nachstationär)
    case_state = "FALLSTATUS", # "ambulant" "stationär" "teilstationär" "nachstationär"
    plz = "PLZ_3stellig"),
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
    plz = "PLZ",
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
    pres_package_info = "PackungInfo",
    presc_drug = "WirkstoffBez")
  )

# unify/recode factor levels & labels
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

#TODO adm_reason_lvls_rec = ... # recode & separate reason + type
#TODO add other codes from codebook (icd_type, fa_p21, adm_event, dis_reason, case_merge_reason, stay_type, +?)

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

# Vars that should be uniform per case
unify_per_case <- list(first = c("yob", "sex", "adm_date", "age_adm", "ik_insurer", "adm_event", "adm_reason",
                                 "ik_hospital", "case_merge", "case_merge_reason", "leave_days_psy", "case_state"),
                       last = c("dis_date", "dis_reason", "dis_type"))
same_case_span <- weeks(3)

#TODO list of treatment cats?


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
  summarise(start_date = min(date), end_date = max(date)) %>%
  ungroup() %>%
  mutate(date_period = interval(start_date, end_date))


# read & tidy data -----------------------------------------------------------------------------------------------------
data_raw <- imap(data_fs, \(path, name) {
  encoding <- try(guess_encoding(path)$encoding[[1]])
  if(inherits(encoding, "try-error")) encoding <- "ISO-8859-1"
  read_delim(path, delim = ";", trim_ws = TRUE, col_types = col_formats[[name]],
             locale = locale(date_names = "de", date_format = "%d.%m.%Y", time_format = "%H:%M:%S",
                             decimal_mark =".", tz = "Europe/Berlin", encoding = encoding))
  })

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
  mutate(n_tot_case = n(), n_pid_Orb = n_distinct(p_id_Orb), n_pid_P21 = n_distinct(p_id_P21)) %>%
  ungroup() %>%
  filter(n_pid_Orb == 1 & n_pid_P21 == 1) %>%
  select(p_id_Orb, p_id_P21) %>%
  distinct() %>%
  deframe()

# tidy data
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

         filter(if_any(any_of("icd_code"), ~str_starts(., filter_icd))) %>% # keep/remove selected ICD codes
         mutate(# recode factor levels
                across(starts_with("fa_"), ~fct_expand(., fa_lvls_rec) %>% fct_recode(!!!fa_lvls_rec)),
                across(ends_with("_hn"), ~fct_expand(., hn_lvls_rec) %>% fct_recode(!!!hn_lvls_rec)),
                across(any_of("case_state"), ~fct_recode(., !!!case_state_lvls_rec)),
                across(any_of("sex"), ~fct_relabel(., toupper)),
                across(any_of("p_id"), ~fct_expand(., p21_p_id_lvls_rec) %>% fct_recode(!!!p21_p_id_lvls_rec)),
                across(any_of("adm_reason"),
                       ~fct_expand(., adm_reason_lvls_rec) %>% fct_recode(!!!adm_reason_lvls_rec)),
                across(any_of("adm_type"), ~fct_expand(., adm_type_lvls_rec) %>% fct_recode(!!!adm_type_lvls_rec)),
                across(any_of("dis_reason"),
                       ~fct_expand(., dis_reason_lvls_rec) %>% fct_recode(!!!dis_reason_lvls_rec)),
                across(any_of("dis_work"), ~fct_expand(., dis_work_lvls_rec) %>% fct_recode(!!!dis_work_lvls_rec)),

                # remove all occurences of "-" and "." to transfer icpm codes to ops codes
                across(any_of("ops_code"), ~fct_relabel(., ~str_remove_all(., "[\\-\\.]"))),

                # unify prescription date+time column to datetime
                across(any_of("presc_date"), ~(ymd(.) + hms(presc_time)))
                ) %>%
         select(-any_of(c("presc_time", "case_state_id"))) %>%

         # drop/lump rare diagnoses/treatmens
         #TODO: do after merging (proportions might shift)?
         { if(do_lump) mutate(across(any_of(c("icd_code", "ops_code")),
                                     ~fct_lump_prop(., lump_threshold, other_level = other_name)))
           else . } %>%
         # exclude icd_code & ops_code lump category
         { if(do_other_exclude) filter(., if_any(any_of(c("icd_code", "ops_code")), ~!str_detect(., other_name)))
           else . } %>%

         mutate(across(where(is.factor), fct_drop)) # drop unused factor levels after all recoding & exclusion steps
      )

# union of factor levels of same variable across all data sources
if(do_unify_factor_lvls) {
  data_tidy %<>%
    map(~mutate(., across(where(is.factor),
                          ~fct_expand(., map(data_tidy, ~levels(.[[cur_column()]])) %>% list_c() %>% unique()))))
}


# expand data, merge cases, add new variables --------------------------------------------------------------------------
# factor order of case_ids & dict to merge cases with <3 weeks between discharge and next admission
case_merge_dict <-
  map(data_tidy[which(map_vec(data_tidy,
                              ~ length(intersect(c("p_id", "case_id", "adm_date", "dis_date"), names(.))) == 4))],
      ~select(., p_id, adm_date, dis_date, case_id_orig = case_id) %>% distinct()) %>%
  reduce(bind_rows) %>%
  distinct() %>%
  arrange(p_id, adm_date) %>%
  group_by(case_id_orig) %>%
  mutate(dis_date = max(dis_date)) %>% # solve different dis_dates present between Orbis & P21
  distinct() %>%
  group_by(p_id, .add = FALSE) %T>%

  # assign intermediate result: ordered case_id_orig factor levels
  {use_series(., case_id_orig) %>% fct_inorder() %>% head(0) %>% assign("case_id_orig_lvls_ordered", ., pos = 1) } %>%

  mutate(case_id = cumsum(!replace_na(adm_date < (lag(dis_date, default = origin) + same_case_span), FALSE))) %>%
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
  { summarise(., val = list(fct_inorder(unique(case_id_orig))), .by = case_id) %>%
      deframe() %>%
      assign("case_id_lvls_by_new", ., pos = 1) }

data_exp <- data_tidy %>%
  map(~ # merge cases
        mutate(., case_id_orig = fct_c(case_id_orig_lvls_ordered, case_id),
               case_id = fct_recode(case_id_orig, !!!case_id_lvls_rec)) %>%
        arrange(across(any_of(c("p_id", "case_id", "case_id_orig", "adm_date", "icd_date", "ops_date",
                                "med_start_date", "med_end_date", "lab_date", "dis_date")))) %>%
        rename_with(~str_c(., "_orig"), any_of(list_c(unify_per_case))) %>%
        group_by(case_id) %>%
        # unify selected variables per case
        mutate(across(any_of(str_c(unify_per_case$first, "_orig")), ~first(., na_rm = TRUE),
                      .names = '{str_remove(.col, "_orig")}'),
               across(any_of(str_c(unify_per_case$last, "_orig")), ~last(., na_rm = TRUE),
                      .names = '{str_remove(.col, "_orig")}'),
               across(any_of("length_stay"),
                      list(netto = ~(pick(case_id_orig, length_stay) %>%
                                       group_by(case_id_orig) %>%
                                       summarise(length_stay = first(length_stay, na_rm = TRUE)) %>%
                                       use_series(length_stay) %>%
                                       sum(na.rm = TRUE) %>%
                                       na_if(0)),
                           # technically, max()+min() should not be necessary as both dates are recoded to last/first
                           brutto = ~as.numeric(max(dis_date)-min(adm_date)))),
               # keep only _orig values when differing from new versions
               across(any_of(str_c(list_c(unify_per_case), "_orig")),
                      ~if_else(. == get(str_remove(cur_column(), "_orig")), NA, .))
               ) %>%
        ungroup() %>%
        select(!any_of("age_adm_orig")) %>% # remove age_adm_orig, since it can be reconstructed well enough from yob
        select(where(~!all(is.na(.)))) %>% # remove empty cols
        mutate(across(where(is.factor), fct_drop)) %>% # drop unused factor levels

        # add ICD categories
        { if("icd_code" %in% colnames(.)) {
          mutate(., icd_cat_l3 = case_when(!!!icd_cats_l3, .default = "check_me"), # "check_me" for not-captured cases
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
                               lockdown_start_date = start_date, lockdown_end_date = end_date),
                      by = join_by(between(adm_date, lockdown_start_date, lockdown_end_date))) %>%
            # select(!c(lockdown_start_date, lockdown_end_date)) %>%
            mutate(lockdown_status = if_else(is.na(lockdown_status) & (adm_date %within% lockdown_data_period),
                                             0, lockdown_status))
        } else . } %>%

        # add calendar week to all dates
        mutate(across(where(~ is.POSIXt(.) || is.Date(.)),
                      ~str_c(year(.), ".", week(.)) %>% fct_relevel(~str_sort(., numeric = TRUE)) %>% as.ordered(),
                      .names = '{str_replace(.col, "_date", "_week")}'))
      )

# convert to DFs in global env if desired
# for (x in names(data_exp)) assign(x, data_exp[[x]])


# summarise data -------------------------------------------------------------------------------------------------------

#TODO summarise diagnoses, treatments, medications

# remove 2nd decimal place of diagnoses
#TODO: add setting if & where to cut
# across(any_of("icd_code"), ~fct_relabel(., ~str_sub(., end = 5))),


# create codebook ------------------------------------------------------------------------------------------------------
codebook_data_exp <-
  imap(data_exp,
      ~ create_codebook(.) %>%
        left_join(enframe(col_names[[.y]], name = "variable_name", value = "variable_original"),
                  by = "variable_name") %>%
        relocate(variable_original, .after = variable_name) %>%
        arrange(fct_relevel(variable_original, col_names[[.y]]))
      ) %>%
  bind_rows(.id = "dataframe")


# save objects ---------------------------------------------------------------------------------------------------------
if(do_save_objects) {
  # R Data Structure
  write_rds(data_exp, file.path(outdir, str_glue("CoverCHILD_data_exp_{Sys.Date()}.rds")))
  # dataframe CSVs
  dir.create(file.path(outdir, "data_exp"), recursive = TRUE, showWarnings = FALSE)
  for (x in names(data_exp)) write_csv2(data_exp[[x]], file.path(outdir, "data_exp", str_c(x, "_exp.csv")))
  # codebook
  write_csv2(codebook_data_exp, file.path(outdir, str_glue("CoverCHILD_codebook_{Sys.Date()}.csv")),
             na = "")
}
