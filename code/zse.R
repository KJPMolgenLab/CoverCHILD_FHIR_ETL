




{
  "itemname" : "timeline.coverchild.diags.icdcodes",
  "itemtype" : "stackedbarcharts",
  "data" : {
    "charts" : [ "allicdcodes" ],
    "bars" : ["2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023"],
    "stacks" : [
      "J20.5: Akute RSV-Bronchitis",
      "J21.0: Akute RSV-Bronchiolitis",
      "J12.1: RSV-Pneumonie",
      "B97.4!: RSV als Krankheitsursache"
    ],
    "values" : [ 
      [ 
        [80,44,13,1], 
        [52,45,7,1], 
        [49,50,19,1], 
        [59,64,14,6], 
        [35,44,4,1], 
        [61,68,9,38], 
        [28,59,12,30], 
        [25,12,11,21]
      ] 
    ]
  }
}


tmp <- data.frame(
  gender = c(1L, 2L, 1L, 2L, 2L, 2L),
  age = c(30, 34, 34, 33, 28, 26),
  welcoming = c(4L, 4L, 5L, 2L, 4L, 3L),
  proud = c(4L, 2L, 3L, 3L, 3L, 2L),
  tidy = c(4L, 4L, 4L, 2L, 4L, 4L),
  unique = c(4L, 4L, 5L, 4L, 4L, 3L)
)


df_tmp_J20_5 <- df_result_agg_year_pri %>% filter(grepl(paste(c("J20.5"),collapse = '|'), ICD.Primaercode))
df_tmp_J21_0 <- df_result_agg_year_pri %>% filter(grepl(paste(c("J21.0"),collapse = '|'), ICD.Primaercode))
df_tmp_J12_1 <- df_result_agg_year_pri %>% filter(grepl(paste(c("J12.1"),collapse = '|'), ICD.Primaercode))
df_tmp_B97_4 <- df_result %>% filter(!grepl(paste(c("J20.5","J21.0","J12.1"),collapse = '|'), ICD.Primaercode.EN))
df_tmp_B97_4 <- as.data.frame(df_tmp_B97_4 %>% group_by(Jahr = df_tmp_B97_4$Fall.Jahr, ICD.Sekundaercode = df_tmp_B97_4$ICD.Sekundaercode.EN) %>% summarise(Anzahl = n()) )

df_json_values <- df_tmp_J20_5 %>% select(-contains(c("Jahr","ICD","Anzahl")))
df_json_values$J20.5 <- df_tmp_J20_5$Anzahl
df_json_values$J21.0 <- df_tmp_J21_0$Anzahl
df_json_values$J12.1 <- df_tmp_J12_1$Anzahl
df_json_values$B97.4 <- df_tmp_B97_4$Anzahl

df_json_values <- t(df_json_values)

jsonlite::toJSON(
  list(
    itemname = "timeline.coverchild.diags.icdcodes",
    itemtype = "stackedbarcharts",
    data = list(
      charts = list("allicdcodes"),
      bars = sort(unique(year(df_patients_encounters_conditions_procedures$encounter.period.start))),
      stacks = list(
        "J20.5: Akute RSV-Bronchitis",
        "J21.0: Akute RSV-Bronchiolitis",
        "J12.1: RSV-Pneumonie",
        "B97.4!: RSV als Krankheitsursache"
      ),
      values = list(
        t(df_json_values)
      )
    )
  ),
  pretty = TRUE)


df_tmp <- df_result %>% filter(grepl(paste(c("J20.5"),collapse = '|'), ICD.Primaercode.EN))


df_tmp_J20_5 <- as.data.frame(df_tmp %>% group_by(Jahr = df_tmp$Fall.Jahr, ICD.Primaercode = df_tmp$ICD.Primaercode.EN) %>% summarise(Anzahl = n()) )



a<-conditions_raw %>% filter(grepl("P23",code.coding.code))
sort(unique(a$code.coding.code))

a<-conditions_tmp %>% filter(grepl("P23",code.coding.code))
sort(unique(a$code.coding.code))

sort(unique(conditions_tmp$condition.code.coding.code))

library(dplyr)
library(stringr)
library(tidyr)
#CoverChild
#ICD-10 zu RSV
#J20.5  Akute Bronchitis bei Respiratory-Syncytial-Viren (RS-Viren)
#J21.0  Akute Bronchiolitis durch Respiratory-Syncytial-Viren [RS-Viren]
#J12.1  Pneumonie durch Respiratory-Syncytial-Viren [RS-Viren]
#B97.4! Respiratory-Syncytial-Viren (RS-Viren) als Ursache von Krankheiten
#P23.0  Angeborene Pneumonie durch Viren (u.a. RS-Viren)

#1) Primärcode allein + in Kombi mit Sekundärcode
#J20.5 Akute Bronchitis bei Respiratory-Syncytial-Viren (RS-Viren)
#J21.0 Akute Bronchiolitis durch Respiratory-Syncytial-Viren [RS-Viren]
#J12.1 Pneumonie durch Respiratory-Syncytial-Viren [RS-Viren]
#P23.0 Angeborene Pneumonie durch Viren (u.a. RS-Viren)
#2) Primärcodes + Sekundärcode
#J00 Infektiöse Rhinitis/Rhinopharyngitis
#J01.8 Sonstige akute Sinusitis
#J01.9 Akute Sinusitis, nicht näher bezeichnet
#J02.8 Akute Pharyngitis durch sonstige näher bezeichnete Erreger
#J02.9 Akute Pharyngitis, nicht näher bezeichnet
#J03.8 Akute Tonsillitis durch sonstige näher bezeichnete Erreger
#J03.9 Akute Tonsillitis, nicht näher bezeichnet
#J04.0 Akute Laryngitis
#J04.1 Akute Tracheitis
#J04.2 Akute Laryngotracheitis
#J05.0 Akute obstruktive Laryngitis [Krupp]
#J05.1 Akute Epiglottitis
#J06.0 Akute Laryngopharyngitis
#J06.8 Sonstige akute Infektionen an mehreren Lokalisationen der oberen Atemwege
#J06.9 Akute Infektion der oberen Atemwege, nicht näher bezeichnet

#3) Sekundärcode
#B97.4!   Respiratory-Syncytial-Viren (RS-Viren) als Ursache von Krankheiten


#Zeitraum Patientenrekrutierung: 01.02.2016 - 31.01.2022 
#Alter bei Aufnahme bis Ende 17. LJ 
#Geschlecht (männlich, weiblich, divers)
#ICD-10: Fokus auf RSV-Diagnosen (siehe Wordfile ICD10_RSV_20231009)
#(OPS: Fokus auf OPS-Schlüssel zu Beatmung und Maßnahmen für den Blutkreislauf (?), aber NUR von Patienten mindestens einer der ausgewählten ICD10 Diagnosen/fallbezogen)

#Auswertungsstruktur												Siehe Differenzierung in Tabelle OPS_Codes				geht das?	
#  Demographische Daten		Diagnosen				Klinischer Verlauf									Krankheitsfolgen	
#Patienten-ID	Alter	Geschlecht	Primärdiagnose (ICD-10)		Sekundärdiagnose (ICD-10)		KH-Aufenthaltsdauer in Tagen			Labor		Prozeduren (OPS)				Tod ja/nein	Todesursache
#gesamt	Normalstation (S2,S3,S5,S6,S8)	Intensivstation (NeoITS, ITS, S4)	Max. CRP (mg/L)	Leukozytose (>13000 Leu/µl)	Beatmung		Maßnahmen für den Blutkreislauf			
#OPS ja/nein	pro OPS Dauer in Tagen	OPS ja/nein	pro OPS Dauer in Tagen		
#darunter zählen alle farblich markierten OPS-Codes zu Beatmung		darunter zählen Erhalt von Blutprodukten, Dialyseverfahren, Herz-Lungen-Maschine (siehe Markierungen in Tabelle OPS_Codes)			

source("code/fhir_etl.R")

conditions_raw <- fhir_dfs$condition
patients_raw <- fhir_dfs$patient
encounters_raw <- fhir_dfs$encounter
procedures_raw <- fhir_dfs$procedure
observations_raw <- fhir_dfs$observation

# check for custom reference_prefix
if (exists("subject_reference_prefix", where = cfg) && nchar(cfg$subject_reference_prefix) >= 1) {
  subject_reference_prefix <- cfg$subject_reference_prefix
} else {
  subject_reference_prefix <- "Patient/"
}
if (exists("encounter_reference_prefix", where = cfg) && nchar(cfg$encounter_reference_prefix) >= 1) {
  encounter_reference_prefix <- cfg$encounter_reference_prefix
} else {
  encounter_reference_prefix <- "Encounter/"
}

# check for custom icd_code_system
if (exists("icd_code_system", where = cfg) && nchar(cfg$icd_code_system) >= 1) {
  icd_code_system_custom <- cfg$icd_code_system
} else {
  icd_code_system_custom <- "http://fhir.de/CodeSystem/dimdi/icd-10-gm"
}

list_pri_icd_codes <- c("J12.1","J20.5", "J21.0", "P23.0")
list_pri_sec_icd_codes <- c("J00","J01.8","J01.9","J02.8","J02.9","J03.8","J03.9","J04.0","J04.1","J04.2","J05.0","J05.1","J06.0","J06.8","J06.9", "J38.5")
list_sec_icd_codes <- c("B97.4!")
list_pri_sec_icd_codes_combined <- paste(list_pri_sec_icd_codes, list_sec_icd_codes)

ops_codes_beatmung <- c("8-700","8-700.0","8-700.1","8-700.x","8-700.y","8-711","8-711.0","8-711.00","8-711.01","8-711.1","8-711.10","8-711.11","8-711.2","8-711.20","8-711.21","8-711.3","8-711.30","8-711.31","8-711.4","8-711.40","8-711.41","8-711.x","8-711.y","8-712","8-712.0","8-712.1","8-720")
ops_codes_blutkreislauf <- c("8-771","8-779","8-800.6","8-800.c","8-800.d","8-800.f","8-800.g","8-800.h","8-800.j","8-800.k","8-800.m","8-800.n","8-810","8-820","8-821","8-851","8-852","8-853","8-854","8-855","8-856","8-857","8-85a")

loinc_code_leuko <- c("6690-2")
loinc_code_crp <- c("1988-5")

patients_tmp <- patients_raw
colnames(patients_tmp) <- paste('patient', colnames(patients_tmp), sep = '.')
patients_tmp <- patients_tmp[patients_tmp$patient.birthDate > "1998-01-01", ]
patients_tmp <- mutate(patients_tmp, patient.birthDate = ifelse(nchar(patients_tmp$patient.birthDate) >= 10, patients_tmp$patient.birthDate, paste0(patients_tmp$patient.birthDate, "-01-01")))
patients_tmp <- patients_tmp %>% select(-contains(c("resource_id","meta")))

conditions_tmp <- conditions_raw
if (nrow(conditions_tmp) == 0) {
  stop('No conditions found...exiting')
}
conditions_tmp <- conditions_tmp[colSums(!is.na(conditions_tmp)) > 0]
conditions_tmp <- conditions_tmp %>% select(-contains(c("resource_id","meta",".extension")))
colnames(conditions_tmp) <- paste('condition', colnames(conditions_tmp), sep = '.')
# filter conditions by system to obtain only icd-10-gm system
conditions_tmp <- conditions_tmp[conditions_tmp$condition.code.coding.system == icd_code_system_custom, ]
conditions_tmp <- conditions_tmp[conditions_tmp$condition.recordedDate > "2016-01-01", ]
#conditions_tmp <- conditions_tmp %>% filter(grepl(paste(c(list_pri_icd_codes,list_pri_sec_icd_codes_combined),collapse = '|'), condition.code.coding.code))
conditions_tmp[c('condition.pri_icd_code','condition.sec_icd_code')] <- str_split_fixed(conditions_tmp$condition.code.coding.code, ' ', 2)
conditions_tmp$condition.sec_icd_code <- na_if(conditions_tmp$condition.sec_icd_code,"")
#conditions_tmp <- conditions_tmp %>% filter(condition.sec_icd_code %in% c(NA,list_sec_icd_codes))
conditions_tmp$condition.subject.reference <- sub(subject_reference_prefix, "", conditions_tmp[, "condition.subject.reference"])
conditions_tmp$condition.encounter.reference <- sub(encounter_reference_prefix, "", conditions_tmp[, "condition.encounter.reference"])


encounters_tmp <- encounters_raw
encounters_tmp <- subset(encounters_tmp, grepl(paste0(c("^inpatient encounter","^short stay"), collapse = "|"), class.display))
if (nrow(encounters_tmp) == 0) {
  encounters_tmp <- subset(encounters_raw, grepl("stationär", type.text))
  if (nrow(encounters_tmp) == 0) {
    encounters_tmp <- subset(encounters_raw, grepl(paste0(c("^IMP","^SS"), collapse = "|"), class.code))
    if (nrow(encounters_tmp) == 0) {
      stop('No conditions found...exiting')
    }
  }
}
encounters_tmp <- encounters_tmp %>% select(-contains(c("resource_id","meta")))
encounters_tmp <- encounters_tmp[colSums(!is.na(encounters_tmp)) > 0]
colnames(encounters_tmp) <- paste('encounter', colnames(encounters_tmp), sep = '.')
encounters_tmp$encounter.subject.reference <- sub(subject_reference_prefix, "", encounters_tmp[, "encounter.subject.reference"])
encounters_tmp <- encounters_tmp[encounters_tmp$encounter.period.start > "2016-01-01", ]
#encounters_tmp <- encounters_tmp %>% filter((encounter.diagnosis.use.coding.code != 'AD') %>% replace_na(TRUE))
#encounters_tmp <- encounters_tmp %>% filter((encounter.location.location.identifier.value == 'KIK') %>% replace_na(TRUE))
encounters_tmp <- subset(encounters_tmp, grepl(paste0(c("^KIK"), collapse = "|"), encounter.location.location.identifier.value))
#encounters_tmp3 <- encounters_tmp %>% filter((encounter.diagnosis.use.coding.code == 'AD'))
encounters_tmp$encounter.period.days <- as.numeric(difftime(encounters_tmp$encounter.period.end, encounters_tmp$encounter.period.start, units = "days"))
#encounters_tmp$encounter.location.period.days.normal <- ifelse(!grepl(paste(c("ITS","its"),collapse = '|'),encounters_tmp$encounter.location.location.identifier.value), as.numeric(difftime(encounters_tmp$encounter.location.period.end, encounters_tmp$encounter.location.period.start, units = "days")), NA)
#encounters_tmp$encounter.location.period.days.its <- ifelse(grepl(paste(c("ITS","its"),collapse = '|'),encounters_tmp$encounter.location.location.identifier.value), as.numeric(difftime(encounters_tmp$encounter.location.period.end, encounters_tmp$encounter.location.period.start, units = "days")), NA)
#encounters_tmp$encounter.location.period.days.fall <- as.numeric(difftime(encounters_tmp$encounter.location.period.end, encounters_tmp$encounter.location.period.start, units = "days"))
#encounters_tmp$encounter.location.period.days2 <- ifelse(encounters_tmp$encounter.location.period.days.fall < (encounters_tmp$encounter.location.period.days.its + encounters_tmp$encounter.location.period.days.normal),"+","fall")
#rm(encounters_tmp3)

procedures_tmp <- procedures_raw
procedures_tmp <- procedures_tmp[colSums(!is.na(procedures_tmp)) > 0]
procedures_tmp <- procedures_tmp %>% select(-contains(c("resource_id","meta")))
colnames(procedures_tmp) <- paste('procedure', colnames(procedures_tmp), sep = '.')
procedures_tmp <- procedures_tmp[procedures_tmp$procedure.performedDateTime > "2016-01-01", ]
#procedures_tmp <- procedures_tmp %>% filter(grepl(paste(c(ops_codes_beatmung,ops_codes_blutkreislauf),collapse = '|'), procedure.code.coding.code))
procedures_tmp[c('procedure.ops_code','procedure.snomed_code')] <- str_split_fixed(procedures_tmp$procedure.code.coding.code, ':::', 2)
procedures_tmp$procedure.ops_code.beatmung <- ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein")
procedures_tmp$procedure.ops_code.blutkreislauf <- ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein")
#procedures_tmp$procedure.ops_code.beatmung <- ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein"))
#procedures_tmp$procedure.ops_code.blutkreislauf <- ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein"))
procedures_tmp$procedure.subject.reference <- sub(subject_reference_prefix, "", procedures_tmp[, "procedure.subject.reference"])
procedures_tmp$procedure.encounter.reference <- sub(encounter_reference_prefix, "", procedures_tmp[, "procedure.encounter.reference"])


observations_tmp <- observations_raw
colnames(observations_tmp) <- paste('observation', colnames(observations_tmp), sep = '.')
observations_tmp <- observations_tmp[observations_tmp$observation.effectiveDateTime > "2016-01-01", ]
#observations_tmp <- observations_tmp %>% filter(grepl(paste(c(loinc_code_leuko,loinc_code_crp),collapse = '|'),observation.code.coding.code))
observations_tmp$observation.valueQuantity.value <- as.numeric(observations_tmp$observation.valueQuantity.value)
observations_tmp$observation.valueQuantity.value.leuko <- ifelse(grepl(paste(loinc_code_leuko,collapse = '|'),observations_tmp$observation.code.coding.code) & observations_tmp$observation.valueQuantity.value > 13, observations_tmp$observation.valueQuantity.value, NA)
observations_tmp$observation.valueQuantity.value.crp <- ifelse(grepl(paste(loinc_code_crp,collapse = '|'),observations_tmp$observation.code.coding.code), observations_tmp$observation.valueQuantity.value, NA)
observations_tmp$observation.subject.reference <- sub(subject_reference_prefix, "", observations_tmp[, "observation.subject.reference"])
observations_tmp$observation.encounter.reference <- sub(encounter_reference_prefix, "", observations_tmp[, "observation.encounter.reference"])
observations_tmp <- observations_tmp[colSums(!is.na(observations_tmp)) > 0]
observations_tmp <- observations_tmp %>% select(-contains(c("resource_id","meta")))
observations_tmp2 <- observations_tmp  %>% group_by(observation.encounter.reference) %>% summarise(observation.valueQuantity.value.crp.max = max(observation.valueQuantity.value.crp, na.rm=TRUE))
observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.encounter.reference")
observations_tmp2 <- observations_tmp  %>% group_by(observation.encounter.reference) %>% summarise(observation.valueQuantity.value.leuko.max = max(observation.valueQuantity.value.leuko, na.rm=TRUE))
observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.encounter.reference")
rm(observations_tmp2)

df_patients_encounters_conditions_procedures <- base::merge(conditions_tmp, encounters_tmp[ , c("encounter.diagnosis.condition.reference", "encounter.diagnosis.rank", "encounter.diagnosis.use.coding.code","encounter.diagnosis.use.coding.display")], by.x = "condition.id", by.y = "encounter.diagnosis.condition.reference", all.x=FALSE)
df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, encounters_tmp[ , c("encounter.serviceType.coding.code", "encounter.serviceType.coding.display", "encounter.location.location.identifier.value", "encounter.location.period.end", "encounter.location.period.start", "encounter.location.status", "encounter.partOf.reference","encounter.period.start", "encounter.period.days")], by.x = "condition.encounter.reference", by.y = "encounter.partOf.reference", all.x=FALSE)
df_patients_encounters_conditions_procedures$encounter.location.period.days.normal <- ifelse(!grepl(paste(c("ITS","its"),collapse = '|'),df_patients_encounters_conditions_procedures$encounter.location.location.identifier.value), as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.location.period.end, df_patients_encounters_conditions_procedures$encounter.location.period.start, units = "days")), NA)
df_patients_encounters_conditions_procedures$encounter.location.period.days.its <- ifelse(grepl(paste(c("ITS","its"),collapse = '|'),df_patients_encounters_conditions_procedures$encounter.location.location.identifier.value), as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.location.period.end, df_patients_encounters_conditions_procedures$encounter.location.period.start, units = "days")), NA)
df_patients_encounters_conditions_procedures$encounter.location.period.days.fall <- as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.location.period.end, df_patients_encounters_conditions_procedures$encounter.location.period.start, units = "days"))


diagnoses_role <- sort(unique(df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code))
for (i in seq_along(diagnoses_role)) {
  for (code in c("condition.pri_icd_code","condition.sec_icd_code")) {
    col_name <- paste0(code,".",tolower(str_replace(diagnoses_role[[i]],"-",".")))
    df_patients_encounters_conditions_procedures[col_name] <- ifelse(tolower(df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code) == tolower(diagnoses_role[[i]]), df_patients_encounters_conditions_procedures[[code]], NA)
    df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% group_by(condition.encounter.reference) %>% summarise(!!col_name := max(!!as.name(col_name), na.rm = TRUE))
    df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(col_name))
    df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "condition.encounter.reference")
  }
}

rm(df_patients_encounters_conditions_procedures2)

df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, procedures_tmp[, c("procedure.code.coding.display","procedure.encounter.reference","procedure.id","procedure.location.identifier.value","procedure.performedDateTime","procedure.status","procedure.ops_code","procedure.ops_code.beatmung","procedure.ops_code.blutkreislauf")], by.x = "condition.encounter.reference", by.y = "procedure.encounter.reference", all.x = TRUE)

df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, observations_tmp[, c("observation.encounter.reference","observation.id","observation.code.text","observation.effectiveDateTime","observation.code.coding.code","observation.interpretation.coding.code", "observation.status", "observation.valueQuantity.code", "observation.valueQuantity.comparator", "observation.valueQuantity.unit", "observation.valueQuantity.value", "observation.valueQuantity.value.crp.max", "observation.valueQuantity.value.leuko.max")], by.x = "condition.encounter.reference", by.y = "observation.encounter.reference", all.x = TRUE)

df_patients_encounters_conditions_procedures <- base::merge(patients_tmp, df_patients_encounters_conditions_procedures, by.x = "patient.id", by.y = "condition.subject.reference", all.x = FALSE)
df_patients_encounters_conditions_procedures$encounter.id <- df_patients_encounters_conditions_procedures$condition.encounter.reference
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% 
  select(-contains(c("resource_id","lastUpdated","meta.source","reference")))
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% 
  distinct(patient.id) %>% 
  group_by(patient.id) %>% 
  summarise(patient.id.pseudonym = paste("Patient",1:n())) %>% 
  mutate(
    patient.id.pseudonym = paste("Patient",str_pad(1:n(), 3, pad = "0"))
  )

df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% 
  group_by(encounter.id) %>% 
  reframe(
    encounter.location.period.days.normal = (encounter.location.period.days.normal),
    encounter.location.period.days.its = (encounter.location.period.days.its)
  ) %>% 
  distinct() %>%
  group_by(encounter.id) %>% 
  mutate(
    encounter.location.period.days.normal = sum(encounter.location.period.days.normal,na.rm=TRUE),
    encounter.location.period.days.its = sum(encounter.location.period.days.its,na.rm=TRUE)
  ) %>%
  distinct()

df_patients_encounters_conditions_procedures2[sapply(df_patients_encounters_conditions_procedures2, is.infinite)] <- NA
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% 
  select(-contains(c("encounter.location.period.days.normal","encounter.location.period.days.its","system","identifier","extension")))
df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "encounter.id")
rm(df_patients_encounters_conditions_procedures2)

df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>%
  mutate(
    encounter.period.start.patient.age.years = floor(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25),
    encounter.period.start.patient.age.days = interval(as.Date(df_patients_encounters_conditions_procedures$patient.birthDate), as.Date(df_patients_encounters_conditions_procedures$encounter.period.start)) %/% days(1),
    encounter.period.start.patient.age.months = interval(as.Date(df_patients_encounters_conditions_procedures$patient.birthDate), as.Date(df_patients_encounters_conditions_procedures$encounter.period.start)) %/% months(1),
    encounter.period.start.patient.age.dec = as.numeric(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25),
    encounter.period.start.patient.age = ifelse(encounter.period.start.patient.age.months == 0, paste0(encounter.period.start.patient.age.days,"d"), ifelse(encounter.period.start.patient.age.months <=23, paste0(encounter.period.start.patient.age.months,"m"), ifelse(encounter.period.start.patient.age.months >=24, paste0(encounter.period.start.patient.age.years,"y") ,NA ))),
    .after=patient.id
  )

df_result <- distinct(as.data.frame(
  df_patients_encounters_conditions_procedures %>% group_by(
    Patient.ID = df_patients_encounters_conditions_procedures$patient.id
    ,Patient.Alter = df_patients_encounters_conditions_procedures$encounter.period.start.patient.age
    ,Patient.Alter.sort = round(df_patients_encounters_conditions_procedures$encounter.period.start.patient.age.dec,2)
    ,Patient.Geschlecht = df_patients_encounters_conditions_procedures$patient.gender
    ,Fall.ID = df_patients_encounters_conditions_procedures$encounter.id
    ,ICD.Primaercode.BE = df_patients_encounters_conditions_procedures$condition.pri_icd_code.cc
    ,ICD.Sekundaercode.BE = df_patients_encounters_conditions_procedures$condition.sec_icd_code.cc
    ,ICD.Primaercode.EN = df_patients_encounters_conditions_procedures$condition.pri_icd_code.dd
    ,ICD.Sekundaercode.EN = df_patients_encounters_conditions_procedures$condition.sec_icd_code.dd
    ,Krankenhaus.Tage = coalesce(df_patients_encounters_conditions_procedures$encounter.period.days,NA)
    ,Normalstation.Tage = coalesce(ifelse(df_patients_encounters_conditions_procedures$encounter.period.days < df_patients_encounters_conditions_procedures$encounter.location.period.days.normal,df_patients_encounters_conditions_procedures$encounter.period.days,df_patients_encounters_conditions_procedures$encounter.location.period.days.normal),NA)
    ,Intensivstation.Tage = coalesce(df_patients_encounters_conditions_procedures$encounter.location.period.days.its,NA)
    ,Prozedur.Beatmung = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.beatmung,NA)
    ,Prozedur.Blutkreislauf = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.blutkreislauf,NA)
    ,Labor.CRP.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.crp.max,NA)
    ,Labor.Leuko.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.leuko.max,NA)
  ) %>% summarise(Anzahl = n()) ))


df_result2 <- df_result %>% 
  group_by(
    Patient.ID = as.integer(Patient.ID)
  ) %>% 
  summarise(
    Patient.ID.Pseudonym = paste("Patient")
  ) %>% 
  mutate(
    Patient.ID.Pseudonym = paste("Patient",str_pad(1:n(), 3, pad = "0"))
  )
df_result <- base::merge(df_result, df_result2, by = "Patient.ID")
df_result2 <- df_result %>% 
  group_by(
    Patient.ID = as.integer(Patient.ID),
    Fall.ID = as.integer(Fall.ID)) %>% 
  summarise(
    Fall.ID.Pseudonym = paste("Fall")) %>% 
  mutate(
    Fall.ID.Pseudonym = paste("Fall",str_pad(1:n(), 3, pad = "0")))
df_result <- base::merge(df_result, df_result2[,c("Fall.ID","Fall.ID.Pseudonym")], by = "Fall.ID")
rm(df_result2)
df_result$Fall.ID <- df_result$Fall.ID.Pseudonym
df_result$Patient.ID <- df_result$Patient.ID.Pseudonym
df_result$Labor.CRP.max <-round(df_result$Labor.CRP.max,2)
df_result$Labor.Leuko.max <-round(df_result$Labor.Leuko.max,2)
df_result <- df_result %>% 
  select(-contains(c("Pseudonym"))) %>% 
  select("Patient.ID","Fall.ID","Patient.Alter","Patient.Alter.sort","Patient.Geschlecht","ICD.Primaercode.BE","ICD.Sekundaercode.BE","ICD.Primaercode.EN","ICD.Sekundaercode.EN","Krankenhaus.Tage","Normalstation.Tage","Intensivstation.Tage","Prozedur.Beatmung","Prozedur.Blutkreislauf","Labor.CRP.max","Labor.Leuko.max")

df_result <- df_result[order(df_result$Patient.ID), ]

now <- format(Sys.time(), "%Y%m%d_%H%M%S")
write.csv2(df_result, file = paste0("output/",now,"_result.csv"), row.names = FALSE)



______________________________________________________________________________________

library(dplyr)
library(stringr)
#CoverChild
#ICD-10 zu RSV
#J20.5  Akute Bronchitis bei Respiratory-Syncytial-Viren (RS-Viren)
#J21.0  Akute Bronchiolitis durch Respiratory-Syncytial-Viren [RS-Viren]
#J12.1  Pneumonie durch Respiratory-Syncytial-Viren [RS-Viren]
#B97.4! Respiratory-Syncytial-Viren (RS-Viren) als Ursache von Krankheiten
#P23.0  Angeborene Pneumonie durch Viren (u.a. RS-Viren)

#1) Primärcode allein + in Kombi mit Sekundärcode
#J20.5 Akute Bronchitis bei Respiratory-Syncytial-Viren (RS-Viren)
#J21.0 Akute Bronchiolitis durch Respiratory-Syncytial-Viren [RS-Viren]
#J12.1 Pneumonie durch Respiratory-Syncytial-Viren [RS-Viren]
#P23.0 Angeborene Pneumonie durch Viren (u.a. RS-Viren)
#2) Primärcodes + Sekundärcode
#J00 Infektiöse Rhinitis/Rhinopharyngitis
#J01.8 Sonstige akute Sinusitis
#J01.9 Akute Sinusitis, nicht näher bezeichnet
#J02.8 Akute Pharyngitis durch sonstige näher bezeichnete Erreger
#J02.9 Akute Pharyngitis, nicht näher bezeichnet
#J03.8 Akute Tonsillitis durch sonstige näher bezeichnete Erreger
#J03.9 Akute Tonsillitis, nicht näher bezeichnet
#J04.0 Akute Laryngitis
#J04.1 Akute Tracheitis
#J04.2 Akute Laryngotracheitis
#J05.0 Akute obstruktive Laryngitis [Krupp]
#J05.1 Akute Epiglottitis
#J06.0 Akute Laryngopharyngitis
#J06.8 Sonstige akute Infektionen an mehreren Lokalisationen der oberen Atemwege
#J06.9 Akute Infektion der oberen Atemwege, nicht näher bezeichnet

#3) Sekundärcode
#B97.4!   Respiratory-Syncytial-Viren (RS-Viren) als Ursache von Krankheiten


#Zeitraum Patientenrekrutierung: 01.02.2016 - 31.01.2022 
#Alter bei Aufnahme bis Ende 17. LJ 
#Geschlecht (männlich, weiblich, divers)
#ICD-10: Fokus auf RSV-Diagnosen (siehe Wordfile ICD10_RSV_20231009)
#(OPS: Fokus auf OPS-Schlüssel zu Beatmung und Maßnahmen für den Blutkreislauf (?), aber NUR von Patienten mindestens einer der ausgewählten ICD10 Diagnosen/fallbezogen)

#Auswertungsstruktur												Siehe Differenzierung in Tabelle OPS_Codes				geht das?	
#  Demographische Daten		Diagnosen				Klinischer Verlauf									Krankheitsfolgen	
#Patienten-ID	Alter	Geschlecht	Primärdiagnose (ICD-10)		Sekundärdiagnose (ICD-10)		KH-Aufenthaltsdauer in Tagen			Labor		Prozeduren (OPS)				Tod ja/nein	Todesursache
#gesamt	Normalstation (S2,S3,S5,S6,S8)	Intensivstation (NeoITS, ITS, S4)	Max. CRP (mg/L)	Leukozytose (>13000 Leu/µl)	Beatmung		Maßnahmen für den Blutkreislauf			
#OPS ja/nein	pro OPS Dauer in Tagen	OPS ja/nein	pro OPS Dauer in Tagen		
#darunter zählen alle farblich markierten OPS-Codes zu Beatmung		darunter zählen Erhalt von Blutprodukten, Dialyseverfahren, Herz-Lungen-Maschine (siehe Markierungen in Tabelle OPS_Codes)			

#conditions_raw <- read.csv(file = paste0(getwd(),"/output/DF_condition_231027.csv"),sep = ",")
#patients_raw <- read.csv(file = paste0(getwd(),"/output/DF_patient_231027.csv"),sep = ",")
#encounters_raw <- read.csv(file = paste0(getwd(),"/output/DF_encounter_231027.csv"),sep = ",")
#procedures_raw <- read.csv(file = paste0(getwd(),"/output/DF_procedure_231027.csv"),sep = ",")
#observations_raw <- read.csv(file = paste0(getwd(),"/output/DF_observation_231027.csv"),sep = ",")

library(readr)
conditions_raw <- as.data.frame(read_csv(file = paste0(getwd(),"/output/DF_condition_231027.csv"), col_names=T, col_types = cols (.default = "c")))
patients_raw <- as.data.frame(read_csv(file = paste0(getwd(),"/output/DF_patient_231027.csv"), col_names=T, col_types = cols (.default = "c")))
encounters_raw <- as.data.frame(read_csv(file = paste0(getwd(),"/output/DF_encounter_231027.csv"), col_names=T, col_types = cols (.default = "c")))
procedures_raw <- as.data.frame(read_csv(file = paste0(getwd(),"/output/DF_procedure_231027.csv"), col_names=T, col_types = cols (.default = "c")))
observations_raw <- as.data.frame(read_csv(file = paste0(getwd(),"/output/DF_observation_231027.csv"), col_names=T, col_types = cols (.default = "c")))

#conditions_tmp <- conditions_raw %>% filter(grepl(paste(icd_codes,collapse = '|'), code.coding.code))

#df_patients_encounters_conditions_procedures
#df_conditions_patients <- base::merge(df_conditions_tmp, df_patients_tmp, by = "patient_id")

source("code/fhir_etl.R")

conditions_raw <- fhir_dfs$condition
patients_raw <- fhir_dfs$patient
encounters_raw <- fhir_dfs$encounter
procedures_raw <- fhir_dfs$procedure
observations_raw <- fhir_dfs$observation


#df_patients_encounters_conditions_procedures <- base::merge(conditions_raw, encounters_raw, by.x = "id", by.y = "subject.reference", all.x = TRUE)
#df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, procedures_raw, by.x = "subject.reference", by.y = "subject.reference", all.x = TRUE)
#df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, patients_raw, by.x = "subject.reference", by.y = "id", all.x = TRUE)

#df_conditions_patients <- base::merge(patients_raw, encounters_raw, by.x = "id", by.y = "subject.reference", all.x = TRUE)

# check for custom reference_prefix
#if (exists("subject_reference_prefix", where = conf) && nchar(conf$subject_reference_prefix) >= 1) {
#  subject_reference_prefix <- conf$subject_reference_prefix
#} else {
#  subject_reference_prefix <- "Patient/"
#}
#if (exists("encounter_reference_prefix", where = conf) && nchar(conf$encounter_reference_prefix) >= 1) {
#  encounter_reference_prefix <- conf$encounter_reference_prefix
#} else {
#  encounter_reference_prefix <- "Encounter/"
#}

subject_reference_prefix <- "Patient/"
encounter_reference_prefix <- "Encounter/"

list_pri_icd_codes <- c("J12.1","J20.5", "J21.0", "P23.0")
list_pri_sec_icd_codes <- c("J00","J01.8","J01.9","J02.8","J02.9","J03.8","J03.9","J04.0","J04.1","J04.2","J05.0","J05.1","J06.0","J06.8","J06.9", "J38.5")
list_sec_icd_codes <- c("B97.4!")
list_pri_sec_icd_codes_combined <- paste(list_pri_sec_icd_codes, list_sec_icd_codes)

ops_codes_beatmung <- c("8-700","8-700.0","8-700.1","8-700.x","8-700.y","8-711","8-711.0","8-711.00","8-711.01","8-711.1","8-711.10","8-711.11","8-711.2","8-711.20","8-711.21","8-711.3","8-711.30","8-711.31","8-711.4","8-711.40","8-711.41","8-711.x","8-711.y","8-712","8-712.0","8-712.1","8-720")
ops_codes_blutkreislauf <- c("8-771","8-779","8-800.6","8-800.c","8-800.d","8-800.f","8-800.g","8-800.h","8-800.j","8-800.k","8-800.m","8-800.n","8-810","8-820","8-821","8-851","8-852","8-853","8-854","8-855","8-856","8-857","8-85a")

loinc_code_leuko <- c("6690-2")
loinc_code_crp <- c("1988-5")

patients_tmp <- patients_raw
colnames(patients_tmp) <- paste('patient', colnames(patients_tmp), sep = '.')
patients_tmp <- patients_tmp[patients_tmp$patient.birthDate > "1998-01-01", ]
patients_tmp <- mutate(patients_tmp, patient.birthDate = ifelse(nchar(patients_tmp$patient.birthDate) >= 10, patients_tmp$patient.birthDate, paste0(patients_tmp$patient.birthDate, "-01-01")))
#patients_tmp <-  patients_tmp[c("id","birthDate","gender")]
patients_tmp <- patients_tmp %>% select(-contains(c("resource_id","meta")))

conditions_tmp <- conditions_raw
colnames(conditions_tmp) <- paste('condition', colnames(conditions_tmp), sep = '.')
conditions_tmp <- conditions_tmp[conditions_tmp$condition.recordedDate > "2016-01-01", ]
conditions_tmp <- conditions_tmp %>% filter(grepl(paste(c(list_pri_icd_codes,list_pri_sec_icd_codes_combined),collapse = '|'), condition.code.coding.code))
conditions_tmp[c('condition.pri_icd_code','condition.sec_icd_code')] <- str_split_fixed(conditions_tmp$condition.code.coding.code, ' ', 2)
conditions_tmp$condition.sec_icd_code <- na_if(conditions_tmp$condition.sec_icd_code,"")
#conditions_tmp <- conditions_tmp %>% mutate_all(na_if,"")
conditions_tmp <- conditions_tmp %>% filter(condition.sec_icd_code %in% c(NA,list_sec_icd_codes))
conditions_tmp$condition.subject.reference <- sub(subject_reference_prefix, "", conditions_tmp[, "condition.subject.reference"])
conditions_tmp$condition.encounter.reference <- sub(encounter_reference_prefix, "", conditions_tmp[, "condition.encounter.reference"])
conditions_tmp <- conditions_tmp[colSums(!is.na(conditions_tmp)) > 0]
conditions_tmp <- conditions_tmp %>% select(-contains(c("resource_id","meta")))


encounters_tmp <- encounters_raw
encounters_tmp <- subset(encounters_tmp, grepl(paste0(c("^inpatient encounter","^short stay"), collapse = "|"), class.display))
if (nrow(encounters_tmp) == 0) {
  encounters_tmp <- subset(encounters_tmp, grepl("stationär", type.text))
  if (nrow(encounters_tmp) == 0) {
    encounters_tmp <- subset(encounters_tmp, grepl(paste0(c("^IMP","^SS"), collapse = "|"), class.code))
    if (nrow(encounters_tmp) == 0) {
      stop('No conditions found...exiting')
    }
  }
}
colnames(encounters_tmp) <- paste('encounter', colnames(encounters_tmp), sep = '.')
encounters_tmp$encounter.subject.reference <- sub(subject_reference_prefix, "", encounters_tmp[, "encounter.subject.reference"])
encounters_tmp <- encounters_tmp[encounters_tmp$encounter.period.start > "2016-01-01", ]
encounters_tmp$encounter.period.days <- as.numeric(difftime(encounters_tmp$encounter.period.end, encounters_tmp$encounter.period.start, units = "days"))
encounters_tmp$encounter.location.period.days.normal <- ifelse(!grepl(paste(c("ITS","its"),collapse = '|'),encounters_tmp$encounter.location.location.identifier.value), as.numeric(difftime(encounters_tmp$encounter.location.period.end, encounters_tmp$encounter.location.period.start, units = "days")), NA)
encounters_tmp$encounter.location.period.days.its <- ifelse(grepl(paste(c("ITS","its"),collapse = '|'),encounters_tmp$encounter.location.location.identifier.value), as.numeric(difftime(encounters_tmp$encounter.location.period.end, encounters_tmp$encounter.location.period.start, units = "days")), NA)
encounters_tmp$encounter.location.period.days.fall <- as.numeric(difftime(encounters_tmp$encounter.location.period.end, encounters_tmp$encounter.location.period.start, units = "days"))
encounters_tmp$encounter.location.period.days2 <- ifelse(encounters_tmp$encounter.location.period.days.fall < (encounters_tmp$encounter.location.period.days.its + encounters_tmp$encounter.location.period.days.normal),"+","fall")
encounters_tmp <- encounters_tmp %>% select(-contains(c("resource_id","meta")))
encounters_tmp <- encounters_tmp[colSums(!is.na(encounters_tmp)) > 0]
#encounters_tmp <- base::merge(encounters_tmp, encounters_tmp, by.x = "encounter.partOf.reference", by.y = "encounter.id", all.x = FALSE)
#encounters_tmp <- base::merge(encounters_tmp, encounters_tmp, by.x = "encounter.id", by.y = "encounter.partOf.reference", all.x = FALSE)
#encounters_tmp.einrichtung <- subset(encounters_tmp, grepl("^einrichtungskontakt", encounter.type.coding.code))
#encounters_tmp.abteilung <- subset(encounters_tmp, !grepl("^einrichtungskontakt", encounter.type.coding.code))

procedures_tmp <- procedures_raw
colnames(procedures_tmp) <- paste('procedure', colnames(procedures_tmp), sep = '.')
procedures_tmp <- procedures_tmp[procedures_tmp$procedure.performedDateTime > "2016-01-01", ]
procedures_tmp <- procedures_tmp %>% filter(grepl(paste(c(ops_codes_beatmung,ops_codes_blutkreislauf),collapse = '|'), procedure.code.coding.code))
procedures_tmp[c('procedure.ops_code','procedure.snomed_code')] <- str_split_fixed(procedures_tmp$procedure.code.coding.code, ':::', 2)
#procedures_tmp$procedure.ops_code.beatmung <- ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein"))
procedures_tmp$procedure.ops_code.beatmung <- ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein")
#procedures_tmp$procedure.ops_code.blutkreislauf <- ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein"))
procedures_tmp$procedure.ops_code.blutkreislauf <- ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein")
#procedures_tmp <- procedures_tmp %>% mutate_all(na_if,"")
procedures_tmp$procedure.subject.reference <- sub(subject_reference_prefix, "", procedures_tmp[, "procedure.subject.reference"])
procedures_tmp$procedure.encounter.reference <- sub(encounter_reference_prefix, "", procedures_tmp[, "procedure.encounter.reference"])
procedures_tmp <- procedures_tmp[colSums(!is.na(procedures_tmp)) > 0]
procedures_tmp <- procedures_tmp %>% select(-contains(c("resource_id","meta")))

observations_tmp <- observations_raw
colnames(observations_tmp) <- paste('observation', colnames(observations_tmp), sep = '.')
observations_tmp <- observations_tmp[observations_tmp$observation.effectiveDateTime > "2016-01-01", ]
observations_tmp <- observations_tmp %>% filter(grepl(paste(c(loinc_code_leuko,loinc_code_crp),collapse = '|'),observation.code.coding.code))
observations_tmp$observation.valueQuantity.value <- as.numeric(observations_tmp$observation.valueQuantity.value)
observations_tmp$observation.valueQuantity.value.leuko <- ifelse(grepl(paste(loinc_code_leuko,collapse = '|'),observations_tmp$observation.code.coding.code) & observations_tmp$observation.valueQuantity.value > 13, observations_tmp$observation.valueQuantity.value, NA)
observations_tmp$observation.valueQuantity.value.crp <- ifelse(grepl(paste(loinc_code_crp,collapse = '|'),observations_tmp$observation.code.coding.code), observations_tmp$observation.valueQuantity.value, NA)
#observations_tmp <- observations_tmp %>% mutate_all(na_if,"")
observations_tmp$observation.subject.reference <- sub(subject_reference_prefix, "", observations_tmp[, "observation.subject.reference"])
observations_tmp$observation.encounter.reference <- sub(encounter_reference_prefix, "", observations_tmp[, "observation.encounter.reference"])
observations_tmp <- observations_tmp[colSums(!is.na(observations_tmp)) > 0]
observations_tmp <- observations_tmp %>% select(-contains(c("resource_id","meta")))
#observations_tmp <- subset(observations_tmp, !grepl("^http://snomed.info/sct", observation.valueCodeableConcept.coding.system))
observations_tmp2 <- observations_tmp  %>% group_by(observation.encounter.reference) %>% summarise(observation.valueQuantity.value.crp.max = max(observation.valueQuantity.value.crp, na.rm=TRUE))
observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.encounter.reference")
observations_tmp2 <- observations_tmp  %>% group_by(observation.encounter.reference) %>% summarise(observation.valueQuantity.value.leuko.max = max(observation.valueQuantity.value.leuko, na.rm=TRUE))
observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.encounter.reference")
rm(observations_tmp2)




#observations_tmp3 <- observations_tmp[, c("observation.encounter.reference","observation.id","observation.code.text","observation.effectiveDateTime","observation.code.coding.code","observation.interpretation.coding.code", "observation.status", "observation.valueQuantity.code", "observation.valueQuantity.comparator", "observation.valueQuantity.unit", "observation.valueQuantity.value", "observation.valueQuantity.value.crp", "observation.valueQuantity.value.leuko")]
#observations_tmp3$observation.valueQuantity.value <- as.numeric(observations_tmp3$observation.valueQuantity.value)
#observations_tmp3$observation.valueQuantity.value.leuko2 <- ifelse(grepl(paste(loinc_code_leuko,collapse = '|'),observations_tmp3$observation.code.coding.code) & (observations_tmp3$observation.valueQuantity.value >13), observations_tmp3$observation.valueQuantity.value, NA)
# max crp pro Fall
#observations_tmp3$observation.valueQuantity.value.maxcrp <- ifelse(grepl(paste(loinc_code_crp,collapse = '|'),observations_tmp3$observation.code.coding.code) & (observations_tmp3$observation.valueQuantity.value >13), observations_tmp3$observation.valueQuantity.value, NA)

#observations_tmp4 <- observations_tmp3 %>% group_by(observation.encounter.reference)






df_patients_encounters_conditions_procedures <- base::merge(conditions_tmp, encounters_tmp[ , c("encounter.diagnosis.condition.reference", "encounter.diagnosis.rank", "encounter.diagnosis.use.coding.code","encounter.diagnosis.use.coding.display")], by.x = "condition.id", by.y = "encounter.diagnosis.condition.reference", all.x=FALSE)
df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, encounters_tmp[ , c("encounter.serviceType.coding.code", "encounter.serviceType.coding.display", "encounter.location.location.identifier.value", "encounter.location.period.end", "encounter.location.period.start", "encounter.location.period.days.fall", "encounter.location.period.days.normal", "encounter.location.period.days.its", "encounter.location.status", "encounter.partOf.reference","encounter.period.start", "encounter.period.days")], by.x = "condition.encounter.reference", by.y = "encounter.partOf.reference", all.x=FALSE)




#names(df_patients_encounters_conditions_procedures)
#nrow(distinct(conditions_tmp2))

#conditions_tmp2 <- df_patients_encounters_conditions_procedures[, c("condition.id","condition.encounter.reference","condition.code.coding.code","condition.code.coding.display","condition.recordedDate","condition.subject.reference","condition.pri_icd_code","condition.sec_icd_code","encounter.serviceType.coding.code","encounter.serviceType.coding.display","encounter.location.location.identifier.value","encounter.location.period.end","encounter.location.period.start","encounter.location.status","encounter.diagnosis.rank","encounter.diagnosis.use.coding.code","encounter.diagnosis.use.coding.display")]

diagnoses_role <- sort(unique(df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code))
for (i in seq_along(diagnoses_role)) {
  for (code in c("condition.pri_icd_code","condition.sec_icd_code")) {
    col_name <- paste0(code,".",tolower(str_replace(diagnoses_role[[i]],"-",".")))
    df_patients_encounters_conditions_procedures[col_name] <- ifelse(tolower(df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code) == tolower(diagnoses_role[[i]]), df_patients_encounters_conditions_procedures[[code]], NA)
    df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% group_by(condition.encounter.reference) %>% summarise(!!col_name := max(!!as.name(col_name), na.rm = TRUE))
    #df_patients_encounters_conditions_procedures2[col_name] <- df_patients_encounters_conditions_procedures2$!!col_name
    #df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures2 %>% select(-contains(c("col_name")))
    df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(col_name))
    df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "condition.encounter.reference")
    #df_patients_encounters_conditions_procedures[paste0("condition.code.coding.code.",tolower(str_replace(diagnoses_role[[i]],'-','.')))] <- ifelse(tolower(df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code) == tolower(diagnoses_role[[i]]), df_patients_encounters_conditions_procedures$condition.code.coding.code, NA)
  }
}

rm(df_patients_encounters_conditions_procedures2)


#conditions_tmp2 <- conditions_tmp2 %>% select(-contains(c("Date", "condition.encounter.reference", "encounter.diagnosis.rank", "encounter.diagnosis.use.coding.code","encounter.diagnosis.use.coding.display")))




df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, procedures_tmp[, c("procedure.code.coding.display","procedure.encounter.reference","procedure.id","procedure.location.identifier.value","procedure.performedDateTime","procedure.status","procedure.ops_code","procedure.ops_code.beatmung","procedure.ops_code.blutkreislauf")], by.x = "condition.encounter.reference", by.y = "procedure.encounter.reference", all.x = TRUE)
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, observations_tmp[, c("observation.encounter.reference","observation.id","observation.code.text","observation.effectiveDateTime","observation.code.coding.code","observation.interpretation.coding.code", "observation.status", "observation.valueQuantity.code", "observation.valueQuantity.comparator", "observation.valueQuantity.unit", "observation.valueQuantity.value", "observation.valueQuantity.value.crp.max", "observation.valueQuantity.value.leuko.max")], by.x = "condition.encounter.reference", by.y = "observation.encounter.reference", all.x = TRUE)

df_patients_encounters_conditions_procedures <- base::merge(patients_tmp, df_patients_encounters_conditions_procedures, by.x = "patient.id", by.y = "condition.subject.reference", all.x = FALSE)
df_patients_encounters_conditions_procedures$encounter.id <- df_patients_encounters_conditions_procedures$condition.encounter.reference
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source","reference")))
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% distinct(patient.id) %>% group_by(patient.id) %>% summarise(patient.id.pseudonym = paste("Patient",1:n())) %>% mutate(patient.id.pseudonym = paste("Patient",str_pad(1:n(), 3, pad = "0")))
#df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "patient.id")

df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% 
  group_by(encounter.id) %>% 
  reframe(encounter.location.period.days.normal = (encounter.location.period.days.normal),encounter.location.period.days.its = (encounter.location.period.days.its)) %>% 
  distinct() %>%
  group_by(encounter.id) %>% 
  mutate(encounter.location.period.days.normal = sum(encounter.location.period.days.normal),encounter.location.period.days.its = sum(encounter.location.period.days.its)) %>%
  distinct()
#df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures  %>% group_by(encounter.id) %>% summarise(encounter.location.period.days.normal = max(encounter.location.period.days.normal, na.rm=TRUE),encounter.location.period.days.its = max(encounter.location.period.days.its, na.rm=TRUE))
df_patients_encounters_conditions_procedures2[sapply(df_patients_encounters_conditions_procedures2, is.infinite)] <- NA
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("encounter.location.period.days.normal","encounter.location.period.days.its","system","identifier","extension")))
df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "encounter.id")
rm(df_patients_encounters_conditions_procedures2)



#encounters_tmp2 <- encounters_tmp[,c("encounter.id", "encounter.period.start","encounter.location.period.start")]
#encounters_tmp2$eqal.start <- ifelse((encounters_tmp$encounter.period.start == encounters_tmp$encounter.location.period.start), 1, NA)
#df_patients_encounters_conditions_procedures <- add_column(df_patients_encounters_conditions_procedures, patient.age = floor(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.location.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25), .after = "patient.id")

df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>%
  mutate(
    encounter.period.start.patient.age = floor(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25),
    encounter.period.start.patient.age_dec = as.numeric(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25),
    encounter.period.start.patient.age.days = interval(as.Date(df_patients_encounters_conditions_procedures$patient.birthDate), as.Date(df_patients_encounters_conditions_procedures$encounter.period.start)) %/% days(1),
    encounter.period.start.patient.age.months = interval(as.Date(df_patients_encounters_conditions_procedures$patient.birthDate), as.Date(df_patients_encounters_conditions_procedures$encounter.period.start)) %/% months(1),
    .after=patient.id)

#Alter bei Aufnahme bis Ende 17. LJ 		Einzeln für jedes Lebensjahr; 1 Lebensmonat in Tagen, 2.bis 23 Lebensmonat in Monaten, ab 24. Lebensmonat in Jahren


library(lubridate)
as.period(df_patients_encounters_conditions_procedures$encounter.period.start.patient.age_dec, units = "year")

as.period(as.numeric(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate))))
as.period(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate), units = "days")

df_patients_encounters_conditions_procedures$encounter.period.start.patient.age.months <- interval(as.Date(df_patients_encounters_conditions_procedures$patient.birthDate), as.Date(df_patients_encounters_conditions_procedures$encounter.period.start)) %/% months(1)



#df_patients_encounters_conditions_procedures <- add_column(df_patients_encounters_conditions_procedures, encounter.period.start.patient.age = floor(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25), .after = "patient.id")
#df_patients_encounters_conditions_procedures <- add_column(df_patients_encounters_conditions_procedures, encounter.period.start.patient.age_dec = as.numeric(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25), .after = "patient.id")

#zse splitten in jahre
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[df_patients_encounters_conditions_procedures$encounter.period.start < "2020-02-01",]
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[df_patients_encounters_conditions_procedures$encounter.period.start > "2020-02-01",]

df_result <- distinct(as.data.frame(
  df_patients_encounters_conditions_procedures %>% group_by(
    Patient.ID = df_patients_encounters_conditions_procedures$patient.id
    ,Patient.Alter = df_patients_encounters_conditions_procedures$encounter.period.start.patient.age
    ,Patient.Alter.sort = round(df_patients_encounters_conditions_procedures$encounter.period.start.patient.age.dec,2)
    ,Patient.Geschlecht = df_patients_encounters_conditions_procedures$patient.gender
    ,Fall.ID = df_patients_encounters_conditions_procedures$encounter.id
    ,Fall.Jahr = year(df_patients_encounters_conditions_procedures$encounter.period.start)
    ,ICD.Primaercode.BE = df_patients_encounters_conditions_procedures$condition.pri_icd_code.cc
    ,ICD.Sekundaercode.BE = df_patients_encounters_conditions_procedures$condition.sec_icd_code.cc
    ,ICD.Primaercode.EN = df_patients_encounters_conditions_procedures$condition.pri_icd_code.dd
    ,ICD.Sekundaercode.EN = df_patients_encounters_conditions_procedures$condition.sec_icd_code.dd
    #,Station = df_patients_encounters_conditions_procedures$encounter.location.location.identifier.value
    ,Krankenhaus.Tage = coalesce(df_patients_encounters_conditions_procedures$encounter.period.days,NA)
    ,Normalstation.Tage = coalesce(ifelse(df_patients_encounters_conditions_procedures$encounter.period.days < df_patients_encounters_conditions_procedures$encounter.location.period.days.normal,df_patients_encounters_conditions_procedures$encounter.period.days,df_patients_encounters_conditions_procedures$encounter.location.period.days.normal),NA)
    ,Intensivstation.Tage = coalesce(df_patients_encounters_conditions_procedures$encounter.location.period.days.its,NA)
    ,Prozedur.Beatmung = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.beatmung,NA)
    ,Prozedur.Blutkreislauf = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.blutkreislauf,NA)
    ,Labor.CRP.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.crp.max,NA)
    ,Labor.Leuko.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.leuko.max,NA)
  ) %>% summarise(Anzahl = n()) ))


df_result2 <- df_result %>% 
  group_by(
    Patient.ID = as.integer(Patient.ID)
    ) %>% 
  summarise(
    Patient.ID.Pseudonym = paste("Patient")
    ) %>% 
  mutate(
    Patient.ID.Pseudonym = paste("Patient",str_pad(1:n(), 3, pad = "0"))
    )
df_result <- base::merge(df_result, df_result2, by = "Patient.ID")
df_result2 <- df_result %>% 
  group_by(
    Patient.ID = as.integer(Patient.ID),
    Fall.ID = as.integer(Fall.ID)) %>% 
  summarise(
    Fall.ID.Pseudonym = paste("Fall")) %>% 
  mutate(
    Fall.ID.Pseudonym = paste("Fall",str_pad(1:n(), 3, pad = "0")))
df_result <- base::merge(df_result, df_result2[,c("Fall.ID","Fall.ID.Pseudonym")], by = "Fall.ID")
rm(df_result2)
df_result$Fall.ID <- df_result$Fall.ID.Pseudonym
df_result$Patient.ID <- df_result$Patient.ID.Pseudonym
df_result$Labor.CRP.max <-round(df_result$Labor.CRP.max,2)
df_result$Labor.Leuko.max <-round(df_result$Labor.Leuko.max,2)
df_result <- df_result %>% 
  select(-contains(c("Pseudonym"))) %>% 
  select("Patient.ID","Fall.ID","Fall.Jahr","Patient.Alter","Patient.Alter.sort","Patient.Geschlecht","ICD.Primaercode.BE","ICD.Sekundaercode.BE","ICD.Primaercode.EN","ICD.Sekundaercode.EN","Krankenhaus.Tage","Normalstation.Tage","Intensivstation.Tage","Prozedur.Beatmung","Prozedur.Blutkreislauf","Labor.CRP.max","Labor.Leuko.max")
  #select("Patient.ID","Fall.ID","Patient.Alter","Patient.Geschlecht","ICD.Primaercode.AD","ICD.Sekundaercode.AD","ICD.Primaercode.ED","ICD.Sekundaercode.ED","Krankenhaus.Tage","Normalstation.Tage","Intensivstation.Tage","Prozedur.Beatmung","Prozedur.Blutkreislauf","Labor.CRP.max","Labor.Leuko.max")

df_result <- df_result[order(df_result$Patient.ID), ]

now <- format(Sys.time(), "%Y%m%d_%H%M%S")
write.csv2(df_result, file = paste0("output/",now,"_result.csv"), row.names = FALSE)

asdasdasd

Encounter finished
50x1000  1694.7s 
80x1000  1568.9s 1568.6s
100x1000 1696.6s 
50x10000 2364s   

Patients finished
50x1000  1823.4s 
80x1000  1653s   1652s
100x1000 1775.1s 
50x10000 2455.8s 

df_result_primaer_2015_2021 <- distinct(df_result_primaer_2015_2021, Klinikum, Diagn, Alter, .keep_all= TRUE)
  
#df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% distinct(patient.id) %>% group_by(patient.id) %>% summarise(patient.id.pseudonym = paste("Patient",1:n())) %>% mutate(patient.id.pseudonym = paste("Patient",str_pad(1:n(), 3, pad = "0")))
#df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "patient.id")



df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% distinct(encounter.id) %>% group_by(encounter.id) %>% summarise(encounter.id.pseudonym = paste("Patient",1:n())) %>% mutate(encounter.id.pseudonym = paste("Fall",1:n()))


paste(names(observations_raw), collapse = ',')


#df_patients_encounters_conditions_procedures <- base::merge(encounters_tmp, conditions_tmp, by.x = "encounter.id", by.y = "condition.encounter.reference", all.x = FALSE)
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

#df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, procedures_tmp, by.x = "encounter.id", by.y = "procedure.encounter.reference", all.x = FALSE)
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

#df_patients_encounters_conditions_procedures <- base::merge(patients_tmp, df_patients_encounters_conditions_procedures, by.x = "patient.id", by.y = "encounter.subject.reference", all.x = FALSE)
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source","reference")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

#---

#df_patients_encounters_conditions_procedures <- base::merge(conditions_tmp, encounters_tmp, by.x = "condition.encounter.reference", by.y = "encounter.id", all.x = FALSE)
#df_patients_encounters_conditions_procedures$encounter.id <- df_patients_encounters_conditions_procedures$condition.encounter.reference
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

#df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, procedures_tmp, by.x = "encounter.id", by.y = "procedure.encounter.reference", all.x = FALSE)
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

#df_patients_encounters_conditions_procedures <- base::merge(patients_tmp, df_patients_encounters_conditions_procedures, by.x = "patient.id", by.y = "encounter.subject.reference", all.x = FALSE)
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("resource_id","lastUpdated","meta.source",".reference","system")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]


df_patients_encounters_conditions_procedures <- add_column(df_patients_encounters_conditions_procedures, patient.age = floor(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25), .after = "patient.id")
df_patients_encounters_conditions_procedures <- add_column(df_patients_encounters_conditions_procedures, patient.age_dec = as.numeric(as.double(as.Date(df_patients_encounters_conditions_procedures$encounter.period.start) - as.Date(df_patients_encounters_conditions_procedures$patient.birthDate)) / 365.25), .after = "patient.id")


df_result <- as.data.frame(
  df_patients_encounters_conditions_procedures %>% group_by(
    Patient.ID = df_patients_encounters_conditions_procedures$patient.id
    ,Patient.Alter = df_patients_encounters_conditions_procedures$patient.age
    ,Patient.Geschlecht = df_patients_encounters_conditions_procedures$patient.gender
    ,Primaercode = df_patients_encounters_conditions_procedures$condition.pri_icd_code
    ,Sekundaercode = df_patients_encounters_conditions_procedures$condition.sec_icd_code
    ,Krankenhaus.Tage = 
    ,Normalstation.Tage = 
    ,Intensivstation.Tage = 
    ) %>% summarise(Anzahl = n()) )


#patienten und fälle dann einzeln aufführen

--


#df_patients_encounters_conditions_procedures <- distinct(df_patients_encounters_conditions_procedures, patient.id, .keep_all= TRUE)
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[df_patients_encounters_conditions_procedures$patient.id == "1002753", ]
1002753
sort(names(df_patients_encounters_conditions_procedures))

#Auswertungsstruktur												Siehe Differenzierung in Tabelle OPS_Codes				geht das?	
#  Demographische Daten		Diagnosen				Klinischer Verlauf									Krankheitsfolgen	
#Patienten-ID	Alter	Geschlecht	Primärdiagnose (ICD-10)		Sekundärdiagnose (ICD-10)		KH-Aufenthaltsdauer in Tagen			Labor		Prozeduren (OPS)				Tod ja/nein	Todesursache
#gesamt	Normalstation (S2,S3,S5,S6,S8)	Intensivstation (NeoITS, ITS, S4)	Max. CRP (mg/L)	Leukozytose (>13000 Leu/µl)	Beatmung		Maßnahmen für den Blutkreislauf			
#OPS ja/nein	pro OPS Dauer in Tagen	OPS ja/nein	pro OPS Dauer in Tagen		
#darunter zählen alle farblich markierten OPS-Codes zu Beatmung		darunter zählen Erhalt von Blutprodukten, Dialyseverfahren, Herz-Lungen-Maschine (siehe Markierungen in Tabelle OPS_Codes)	





#colnames(df_con_icd10codes_combined_ag_0_5_15_18_99_diagn2) <- c("Klinikum", "Jahr", "Diagn1", "Diagn2", "Alter", "Anzahl")



df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% 
  distinct(patient.id) %>% 
  group_by(patient.id) %>% 
  summarise(patient.id.pseudonym = paste("Patient",1:n())) %>% 
  mutate(
    patient.id.pseudonym = paste("Patient",str_pad(1:n(), 3, pad = "0"))
  )

df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% 
  group_by(encounter.id) %>% 
  reframe(
    encounter.period.days = (encounter.period.days),
    encounter.location.period.days.normal = (encounter.location.period.days.normal),
    encounter.location.period.days.its = (encounter.location.period.days.its)
  ) %>% 
  distinct() %>%
  group_by(encounter.id) %>% 
  mutate(
    encounter.period.days = sum(encounter.period.days),
    encounter.location.period.days.normal = ifelse(encounter.period.days < encounter.location.period.days.normal,sum(encounter.period.days),sum(encounter.location.period.days.normal)),
    encounter.location.period.days.its = sum(encounter.location.period.days.its)
  ) %>%
  distinct()

df_patients_encounters_conditions_procedures2[sapply(df_patients_encounters_conditions_procedures2, is.infinite)] <- NA
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% 
  select(-contains(c("encounter.location.period.days.normal","encounter.location.period.days.its","system","identifier","extension")))
df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "encounter.id")
rm(df_patients_encounters_conditions_procedures2)

df_patients_encounters_conditions_procedures$diff <- ifelse(df_patients_encounters_conditions_procedures$encounter.period.days.x != df_patients_encounters_conditions_procedures$encounter.period.days.y,"ungleich","gleich")









