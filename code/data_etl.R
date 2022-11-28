#!/usr/bin/env Rscript

# CoverCHILD data ETL
# @author: SP
# @date: 2022-11-04

# set wd for VS Code (doesn't align automatically to .Rproj)
if (basename(getwd()) != "CoverCHILD") {
  old_wd <- getwd()
  setwd(dirname(dir(path = getwd(),
                    pattern = "CoverCHILD.Rproj",
                    recursive = TRUE, full.names = TRUE)))
}

library("tidyverse")
library("tools")
library("magrittr")
library("ggVennDiagram")

files <- Sys.glob("data/*.csv")
names(files) <- basename(file_path_sans_ext(files))
data_raw <- lapply(files, \(x) {
  encoding <- try(guess_encoding(x)$encoding[[1]])
  if (inherits(encoding, "try-error")) encoding <- "ISO-8859-1"
  read_csv2(x, locale = locale("de", encoding = encoding))
})

# convert to DFs in global env if desired
# for (x in names(data_raw)) assign(x, data_raw[[x]])

# save UTF-8 encoded
# outdir <- file.path("output", "data_utf8")
# dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
# for (x in names(data_raw)) write_csv2(data_raw[[x]], file.path(outdir, str_c(x, "_utf8.csv")))

# recode column names
col_names <- c(case_id = "Fall_Pseudonym", # ICD_V2
               p_id = "PID Pseudonym",
               yob = "Geburtsjahr",
               sex = "GE",
               adm_date = "AUFNDAT",
               dis_date = "ENTLDAT",
               ret_time = "Verweildauer",
               case_state = "FALLSTATUS",
               icd_fa = "ICD_FA", # ?
               icd_date = "ICD_DATUM",
               icd_code = "ICD_CODE",
               icd_type = "ICD_TYPE",
               icd_hn = "ICD_HN",
               icd_label = "ICD_BEZ",
               icd_2_code = "SEK_ICD_CODE",
               icd_2_label = "SEK_ICD_BEZ",
               age_adm = "Alter_bei_Aufnahme", # ICPM_V3
               age_treat = "Alter_bei_Maßnahme",
               icpm_fa = "ICPM_FA",
               icpm_date = "ICPM_DATUM",
               icpm_code = "ICPM_CODE",
               icpm_hn = "ICPM_HN",
               loc = "LOKALISATION",
               icpm_label = "ICPM_BEZ",
               age_adm = "Alter_Aufnahme", # Pers_Fall_V2_pseudonym
               plz = "PLZ-3stellig",
               addr_type = "ADRESSART",
               adm_reason = "AUFNAHMEGRUND",
               adm_type = "AUFNAHMEART",
               stay_type = "AUFENTHALTSART",
               dis_type = "ENTLASSART")

# P21 ------------------

ggVennDiagram(list(Orbis_ICD = unique(ICD_V2) %>% filter(FALLSTATUS == "stationär") %>% use_series(Fall_Pseudonym),
                   P21_ICD = unique(P21_ICD_V1_pseudonym$P21_Fallnummer_Pseudonym),
                   P21_plz = unique(P21_Fall_V1_pseudonym$P21_Fallnummer_Pseudonym)))


join_srcs <- c("ICD_V2", "Pers_Fall_V2_pseudonym")

df <-
  data_raw[join_srcs] %>%
  map(~rename(., any_of(col_names)) %>% distinct())

df[["Pers_Fall_V2_pseudonym"]] %>%
  filter(!is.na(plz), addr_type == "Erstanschrift") %>%
  use_series(p_id) %>%
  n_distinct()

df[["Pers_Fall_V2_pseudonym"]] %>%
  group_by(case_id, addr_type) %>%
  mutate(n = n()) %>%
  ungroup() %>%
  filter(n > 1) %>%
  arrange(desc(n)) %>%
  View()



Pers_Fall_V2_pseudonym %>% distinct() %>% nrow()

df2 <- df %>%
  reduce(full_join) %>%
  group_by(case_id) %>%
  summarise(n = n()) %>%
  ungroup() %>%
  filter(n > 1) %>%
  arrange(desc(n))

temp2 <- ICD_V2 %>%
  rename(any_of(col_names)) %>%
  filter(case_id == "CC3577569597159394")

temp2 %>%
  summarise(across(.fns = n_distinct))

joins <- intersect(names(ICPM_V3), names(ICD_V2))
full_join(select(ICD_V2, joins), select(ICPM_V3, joins))

ggVennDiagram::ggVennDiagram(map(list(ICD_V2, ICPM_V3, KIJUPSY_Med_Detail_V2_pseudonym, Labordaten_V3), names))

ICD_V2 %>% rename(any_of(col_names)) %>% names()

ICD_V2 %>% filter(starts_with("F50.", ICD_CODE))


temp <-
  Pers_Fall_V2_pseudonym %>%
  filter(!is.na(`PLZ-3stellig`)) %>%
  group_by(`PID Pseudonym`) %>%
  mutate(n = n_distinct(`PLZ-3stellig`)) %>%
  ungroup() %>%
  filter(n > 1) %>%
  group_by(`PID Pseudonym`, `PLZ-3stellig`) %>%
  slice_head(n = 1) %>%
  ungroup()

