if (!require("stringr")) {install.packages("stringr"); library(stringr)}
if (!require("tidyr")) {install.packages("tidyr"); library(tidyr)}
if (!require("dplyr")) {install.packages("dplyr"); library(dplyr)}
if (!require("lubridate")) {install.packages("lubridate"); library(lubridate)}

source("code/fhir_etl.R")

conditions_raw <- fhir_dfs$condition
patients_raw <- fhir_dfs$patient
encounters_raw <- fhir_dfs$encounter
procedures_raw <- fhir_dfs$procedure
observations_raw <- fhir_dfs$observation

##DEV
#conditions_raw <- readRDS("output/DF_condition_231222.rds")
#patients_raw <- readRDS("output/DF_patient_231222.rds")
#encounters_raw <- readRDS("output/DF_encounter_231222.rds")
#procedures_raw <- readRDS("output/DF_procedure_231222.rds")
#observations_raw <- readRDS("output/DF_observation_231222.rds")
##DEV

cfg <- config::get(file = file.path("config", "fhir_cfg.yml"))

# check for custom icd_code_system
if (exists("icd_code_system", where = cfg) && nchar(cfg$icd_code_system) >= 1) {
  icd_code_system_custom <- cfg$icd_code_system
} else {
  icd_code_system_custom <- "http://fhir.de/CodeSystem/dimdi/icd-10-gm"
}

list_pri_icd_codes <- c("J12.1","J20.5", "J21.0", "P23.0")
list_sec_icd_codes <- c("U07.1!","U99.0!","Z50.1!")
list_pri_sec_icd_codes_combined <- append(list_pri_icd_codes,as.vector(sort(outer(list_pri_icd_codes,list_sec_icd_codes, paste))))
list_pri_sec_B97.4_icd_codes <- c("B99","E84.0","J00","J01.2","J03.9","J06.8","J06.9","J18.0","J18.8","J18.9","J20.5","J20.9","J31.0","J38.5","J40","J45.0","J45.01","J45.02","J45.15","J45.9","J45.99","J96.09","J98.7","R05","R50.88","R50.9","R56.0","R56.8","Z11","Z22.8","Z29.0")
list_pri_sec_B97.4_icd_codes <- as.vector(sort(outer(list_pri_sec_B97.4_icd_codes,c("B97.4!"), paste)))

ops_codes_beatmung <- c("8-700","8-700.0","8-700.1","8-700.x","8-700.y","8-711","8-711.0","8-711.00","8-711.01","8-711.1","8-711.10","8-711.11","8-711.2","8-711.20","8-711.21","8-711.3","8-711.30","8-711.31","8-711.4","8-711.40","8-711.41","8-711.x","8-711.y","8-712","8-712.0","8-712.1","8-720")
ops_codes_blutkreislauf <- c("8-771","8-779","8-800.6","8-800.c","8-800.d","8-800.f","8-800.g","8-800.h","8-800.j","8-800.k","8-800.m","8-800.n","8-810","8-820","8-821","8-851","8-852","8-853","8-854","8-855","8-856","8-857","8-85a")

loinc_code_leuko <- c("6690-2")
loinc_code_crp <- c("1988-5")


patients_tmp <- patients_raw[
  c(
    "id",
    "birthDate",
    "gender",
    "address.postalCode",
    "address.country"
  )]
if (nrow(patients_tmp) == 0) {
  stop('No patients found...exiting')
}
colnames(patients_tmp) <- paste('patient', colnames(patients_tmp), sep = '.')
patients_tmp <- patients_tmp[patients_tmp$patient.birthDate > "1998-01-01", ]
patients_tmp <- mutate(patients_tmp, patient.birthDate = ifelse(nchar(patients_tmp$patient.birthDate) >= 10, patients_tmp$patient.birthDate, paste0(patients_tmp$patient.birthDate, "-01-01")))
patients_tmp <- patients_tmp[!is.na(patients_tmp$patient.address.postalCode),]
patients_tmp <- patients_tmp[!str_length(patients_tmp$patient.address.postalCode) >= 6,]
patients_tmp <- patients_tmp[!str_length(patients_tmp$patient.address.postalCode) <= 3,]
patients_tmp$patient.address.postalCode_full <- str_trim(patients_tmp$patient.address.postalCode)
patients_tmp$patient.address.postalCode <- substr(patients_tmp$patient.address.postalCode_full, 1, 3)
#rm(patients_raw)

conditions_tmp <- conditions_raw[
  c(
    "id",
    "code.coding.code",
    "code.coding.display",
    "code.coding.system",
    "recordedDate",
    "encounter.reference",
    "subject.reference"
  )]
if (nrow(conditions_tmp) == 0) {
  stop('No conditions found...exiting')
}
conditions_tmp <- conditions_tmp[colSums(!is.na(conditions_tmp)) > 0]
colnames(conditions_tmp) <- paste('condition', colnames(conditions_tmp), sep = '.')
# filter conditions by system to obtain only icd-10-gm system
conditions_tmp <- conditions_tmp %>% filter(condition.code.coding.system %in% c(NA,icd_code_system_custom))
conditions_tmp <- conditions_tmp[conditions_tmp$condition.recordedDate > "2016-01-01", ]
conditions_tmp$condition.code.coding.code <- toupper(conditions_tmp$condition.code.coding.code)
#conditions_tmp <- conditions_tmp %>% filter(grepl(paste(c(list_pri_sec_B97.4_icd_codes,list_pri_sec_icd_codes_combined),collapse = '|'), condition.code.coding.code))
conditions_tmp <- conditions_tmp %>% filter(grepl(paste(c("B97.4!",list_pri_sec_icd_codes_combined),collapse = '|'), condition.code.coding.code))
conditions_tmp$condition.pri_icd_code <- NA
conditions_tmp$condition.sec_icd_code <- NA
conditions_tmp[c('condition.pri_icd_code','condition.sec_icd_code')] <- str_split_fixed(conditions_tmp$condition.code.coding.code, ' ', 2)
conditions_tmp$condition.sec_icd_code <- na_if(conditions_tmp$condition.sec_icd_code,"")
#conditions_tmp <- conditions_tmp %>% filter(condition.sec_icd_code %in% c(NA,"B97.4!",list_sec_icd_codes))
#conditions_tmp$condition.subject.reference <- sub(subject_reference_prefix, "", conditions_tmp[, "condition.subject.reference"])
#conditions_tmp$condition.encounter.reference <- sub(encounter_reference_prefix, "", conditions_tmp[, "condition.encounter.reference"])
if (nrow(conditions_tmp) == 0) {
  stop('No conditions found matching incusion criteria...exiting')
}
#rm(conditions_raw)

if (exists("location.location.identifier.value", encounters_raw)) {
  encounters_tmp <- encounters_raw[
    c(
      "id",
      "period.start",
      "period.end",
      "subject.reference",
      "class.code",
      "class.display",
      "diagnosis.condition.reference",
      "diagnosis.use.coding.code",
      "location.location.identifier.value",
      "location.period.start",
      "location.period.end",
      "partOf.reference"
    )]
} else {
  encounters_tmp <- encounters_raw[
    c(
      "id",
      "period.start",
      "period.end",
      "subject.reference",
      "class.code",
      "class.display",
      "diagnosis.condition.reference",
      "diagnosis.use.coding.code"
    )]
}
encounters_tmp <- subset(encounters_tmp, grepl(paste0(c("^inpatient encounter","^short stay"), collapse = "|"), class.display))
if (nrow(encounters_tmp) == 0) {
  encounters_tmp <- subset(encounters_raw, grepl("stationär", type.text))
  if (nrow(encounters_tmp) == 0) {
    encounters_tmp <- subset(encounters_raw, grepl(paste0(c("^IMP","^SS"), collapse = "|"), class.code))
    if (nrow(encounters_tmp) == 0) {
      stop('No conditions found matching incusion criteria...exiting')
    }
  }
}
encounters_tmp <- encounters_tmp[colSums(!is.na(encounters_tmp)) > 0]
colnames(encounters_tmp) <- paste('encounter', colnames(encounters_tmp), sep = '.')
#encounters_tmp$encounter.subject.reference <- sub(subject_reference_prefix, "", encounters_tmp[, "encounter.subject.reference"])
#encounters_tmp$encounter.diagnosis.condition.reference <- sub(condition_reference_prefix, "", encounters_tmp[, "encounter.diagnosis.condition.reference"])
encounters_tmp <- encounters_tmp[encounters_tmp$encounter.period.start > "2016-01-01", ]
encounters_tmp <-encounters_tmp %>% filter((encounter.diagnosis.use.coding.code != 'AD') %>% replace_na(TRUE))
latest_period_startend_timestamp <- max(c(max(na.omit(encounters_tmp$encounter.period.start)),max(na.omit(encounters_tmp$encounter.period.end))))
encounters_tmp$encounter.period.end <- coalesce(encounters_tmp$encounter.period.end,latest_period_startend_timestamp)
encounters_tmp$encounter.period.days <- as.numeric(difftime(encounters_tmp$encounter.period.end, encounters_tmp$encounter.period.start, units = "days"))
#rm(encounters_raw)

if (exists("encounter.reference", procedures_raw)) {
  procedures_tmp <- procedures_raw[c(
    "id",
    "code.coding.code",
    "subject.reference",
    "performedDateTime",
    "encounter.reference"
  )]
} else {
  procedures_tmp <- procedures_raw[c(
    "id",
    "code.coding.code",
    "subject.reference",
    "performedDateTime"
  )]
}
procedures_tmp <- procedures_tmp[colSums(!is.na(procedures_tmp)) > 0]
colnames(procedures_tmp) <- paste('procedure', colnames(procedures_tmp), sep = '.')
procedures_tmp <- procedures_tmp[procedures_tmp$procedure.performedDateTime > "2016-01-01", ]
procedures_tmp <- procedures_tmp %>% filter(grepl(paste(c(ops_codes_beatmung,ops_codes_blutkreislauf),collapse = '|'), procedure.code.coding.code))
procedures_tmp[c('procedure.ops_code','procedure.snomed_code')] <- str_split_fixed(procedures_tmp$procedure.code.coding.code, ':::', 2)
procedures_tmp$procedure.ops_code.beatmung <- ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein")
procedures_tmp$procedure.ops_code.blutkreislauf <- ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein")
#procedures_tmp$procedure.ops_code.beatmung <- ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein"))
#procedures_tmp$procedure.ops_code.blutkreislauf <- ifelse(grepl(paste(ops_codes_blutkreislauf,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", ifelse(grepl(paste(ops_codes_beatmung,collapse = '|'),procedures_tmp$procedure.ops_code), "ja", "nein"))
#procedures_tmp$procedure.subject.reference <- sub(subject_reference_prefix, "", procedures_tmp[, "procedure.subject.reference"])
#procedures_tmp$procedure.encounter.reference <- sub(encounter_reference_prefix, "", procedures_tmp[, "procedure.encounter.reference"])
#rm(procedures_raw)

if (exists("encounter.reference", observations_raw)) {
  observations_tmp <- observations_raw[c(
    "id",
    "code.coding.code",
    "valueQuantity.value",
    "subject.reference",
    "effectiveDateTime",
    "encounter.reference"
  )]
} else {
  observations_tmp <- observations_raw[c(
    "id",
    "code.coding.code",
    "valueQuantity.value",
    "subject.reference",
    "effectiveDateTime"
  )]
}
colnames(observations_tmp) <- paste('observation', colnames(observations_tmp), sep = '.')
observations_tmp <- observations_tmp %>% filter(grepl(paste(c(loinc_code_leuko,loinc_code_crp),collapse = '|'),observation.code.coding.code))
observations_tmp <- observations_tmp[observations_tmp$observation.effectiveDateTime > "2016-01-01", ]
#observations_tmp <- observations_tmp %>% filter(grepl(paste(c(loinc_code_leuko,loinc_code_crp),collapse = '|'),observation.code.coding.code))
observations_tmp$observation.valueQuantity.value <- as.numeric(observations_tmp$observation.valueQuantity.value)
observations_tmp$observation.valueQuantity.value.leuko <- ifelse(grepl(paste(loinc_code_leuko,collapse = '|'),observations_tmp$observation.code.coding.code) & observations_tmp$observation.valueQuantity.value > 13, observations_tmp$observation.valueQuantity.value, NA)
observations_tmp$observation.valueQuantity.value.crp <- ifelse(grepl(paste(loinc_code_crp,collapse = '|'),observations_tmp$observation.code.coding.code), observations_tmp$observation.valueQuantity.value, NA)
#observations_tmp$observation.subject.reference <- sub(subject_reference_prefix, "", observations_tmp[, "observation.subject.reference"])
#observations_tmp$observation.encounter.reference <- sub(encounter_reference_prefix, "", observations_tmp[, "observation.encounter.reference"])
observations_tmp <- observations_tmp[colSums(!is.na(observations_tmp)) > 0]
if (exists("observation.encounter.reference", observations_tmp)) {
  observations_tmp2 <- observations_tmp  %>% group_by(observation.encounter.reference) %>% summarise(observation.valueQuantity.value.crp.max = max(observation.valueQuantity.value.crp, na.rm=TRUE))
  observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
  observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.encounter.reference")
  observations_tmp2 <- observations_tmp  %>% group_by(observation.encounter.reference) %>% summarise(observation.valueQuantity.value.leuko.max = max(observation.valueQuantity.value.leuko, na.rm=TRUE))
  observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
  observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.encounter.reference")
  rm(observations_tmp2)
} else {
  observations_tmp2 <- observations_tmp  %>% group_by(observation.subject.reference) %>% summarise(observation.valueQuantity.value.crp.max = max(observation.valueQuantity.value.crp, na.rm=TRUE))
  observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
  observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.subject.reference")
  observations_tmp2 <- observations_tmp  %>% group_by(observation.subject.reference) %>% summarise(observation.valueQuantity.value.leuko.max = max(observation.valueQuantity.value.leuko, na.rm=TRUE))
  observations_tmp2[sapply(observations_tmp2, is.infinite)] <- NA
  observations_tmp <- base::merge(observations_tmp, observations_tmp2, by = "observation.subject.reference")
  rm(observations_tmp2)
}
#rm(observations_raw)

df_patients_encounters_conditions_procedures <- base::merge(
  conditions_tmp, 
  encounters_tmp[ , 
                  c(
                    "encounter.diagnosis.condition.reference", 
                    #"encounter.diagnosis.rank", 
                    "encounter.diagnosis.use.coding.code"
                    #"encounter.diagnosis.use.coding.display"
                    )], 
  by.x = "condition.id", 
  by.y = "encounter.diagnosis.condition.reference", 
  all.x=FALSE) %>% distinct()

if (exists("encounter.location.location.identifier.value", df_patients_encounters_conditions_procedures)) {
  encounter_merge_cols <- c(
    "encounter.id",
    "encounter.serviceType.coding.code", 
    "encounter.serviceType.coding.display", 
    "encounter.location.location.identifier.value", 
    "encounter.location.period.end", 
    "encounter.location.period.start", 
    "encounter.location.status", 
    "encounter.partOf.reference",
    "encounter.period.start", 
    "encounter.period.days"
    )
} else {
  encounter_merge_cols <- c(
    "encounter.id",
    "encounter.subject.reference",
    "encounter.period.start", 
    "encounter.period.days"
    )
}

df_patients_encounters_conditions_procedures <- base::merge(
  df_patients_encounters_conditions_procedures, 
  encounters_tmp[ , 
                  encounter_merge_cols], 
  by.x = "condition.encounter.reference", 
  by.y = "encounter.id", 
  all.x=FALSE) %>% distinct()

if (exists("encounter.location.location.identifier.value", df_patients_encounters_conditions_procedures)) {
  df_patients_encounters_conditions_procedures$encounter.location.period.days.normal <- ifelse(
    !grepl(paste(c("ITS","its"),collapse = '|'),df_patients_encounters_conditions_procedures$encounter.location.location.identifier.value), 
    as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.location.period.end, df_patients_encounters_conditions_procedures$encounter.location.period.start, units = "days")), 
    NA
    )
  df_patients_encounters_conditions_procedures$encounter.location.period.days.its <- ifelse(
    grepl(paste(c("ITS","its"),collapse = '|'),df_patients_encounters_conditions_procedures$encounter.location.location.identifier.value), 
    as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.location.period.end, df_patients_encounters_conditions_procedures$encounter.location.period.start, units = "days")), 
    NA
    )
  df_patients_encounters_conditions_procedures$encounter.location.period.days.fall <- as.numeric(
    difftime(df_patients_encounters_conditions_procedures$encounter.location.period.end, df_patients_encounters_conditions_procedures$encounter.location.period.start, units = "days")
    )
}



#diagnoses_role <- sort(unique(df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code))
diagnoses_role <- c("DD","CC")
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% filter(grepl(paste(diagnoses_role,collapse = '|'), encounter.diagnosis.use.coding.code))
df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code <- ifelse(
 grepl(paste(c("dd","DD"),collapse = '|'),df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code), 
 "DD", 
 df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code
)
df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code <- ifelse(
  grepl(paste(c("cc","CC"),collapse = '|'),df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code), 
  "CC",
  df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code
)
for (i in seq_along(diagnoses_role)) {
  for (code in c("condition.pri_icd_code","condition.sec_icd_code")) {
    col_name <- paste0(code,".",tolower(str_replace(diagnoses_role[[i]],"-",".")))
    df_patients_encounters_conditions_procedures[col_name] <- ifelse(tolower(df_patients_encounters_conditions_procedures$encounter.diagnosis.use.coding.code) == tolower(diagnoses_role[[i]]), df_patients_encounters_conditions_procedures[[code]], NA)
    df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% group_by(condition.encounter.reference) %>% summarise(!!col_name := max(!!as.name(col_name), na.rm = TRUE)) 
    df_patients_encounters_conditions_procedures2 <- do.call(data.frame,lapply(df_patients_encounters_conditions_procedures2, function(x) replace(x, is.infinite(x),NA)))
    df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(col_name))
    df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "condition.encounter.reference")
  }
}
rm(df_patients_encounters_conditions_procedures2)

df_patients_encounters_conditions_procedures <- base::merge(
  df_patients_encounters_conditions_procedures, 
  procedures_tmp[, c(
    #"procedure.code.coding.display",
    "procedure.encounter.reference",
    "procedure.id",
    #"procedure.location.identifier.value",
    "procedure.performedDateTime",
    #"procedure.status",
    "procedure.ops_code",
    "procedure.ops_code.beatmung",
    "procedure.ops_code.blutkreislauf"
    )], 
  by.x = "condition.encounter.reference", 
  by.y = "procedure.encounter.reference", 
  all.x = TRUE) %>% distinct()

df_patients_encounters_conditions_procedures <- base::merge(
  df_patients_encounters_conditions_procedures, 
  observations_tmp[, c(
    "observation.encounter.reference",
    "observation.id",
    #"observation.code.text",
    "observation.subject.reference",
    "observation.effectiveDateTime",
    "observation.code.coding.code",
    #"observation.interpretation.coding.code",
    #"observation.status",
    #"observation.valueQuantity.code",
    #"observation.valueQuantity.comparator",
    #"observation.valueQuantity.unit",
    "observation.valueQuantity.value",
    "observation.valueQuantity.value.crp.max",
    "observation.valueQuantity.value.leuko.max"
  )], 
  by.x = "condition.encounter.reference", 
  by.y = "observation.encounter.reference", 
  all.x = TRUE) %>% distinct()

df_patients_encounters_conditions_procedures <- base::merge(
  patients_tmp, 
  df_patients_encounters_conditions_procedures, 
  by.x = "patient.id", 
  by.y = "condition.subject.reference", 
  all.x = FALSE) %>% distinct()

df_patients_encounters_conditions_procedures$encounter.id <- df_patients_encounters_conditions_procedures$condition.encounter.reference
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% 
#  select(-contains(c("resource_id","lastUpdated","meta.source","reference")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% 
  distinct(patient.id) %>% 
  group_by(patient.id) %>% 
  summarise(patient.id.pseudonym = paste("Patient",1:n())) %>% 
  mutate(
    patient.id.pseudonym = paste("Patient",str_pad(1:n(), 3, pad = "0"))
  )

if (exists("encounter.location.location.identifier.value", df_patients_encounters_conditions_procedures)) {
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
    ) %>% distinct()
}

df_patients_encounters_conditions_procedures2[sapply(df_patients_encounters_conditions_procedures2, is.infinite)] <- NA
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% 
  select(-contains(c("encounter.location.period.days.normal","encounter.location.period.days.its","system","identifier","extension")))
if (exists("encounter.location.location.identifier.value", df_patients_encounters_conditions_procedures)) {
  df_patients_encounters_conditions_procedures <- base::merge(
    df_patients_encounters_conditions_procedures, 
    df_patients_encounters_conditions_procedures2, 
    by = "encounter.id")
}
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

#zse splitten in jahre
#df_patients_encounters_conditions_procedures_lt2020 <- df_patients_encounters_conditions_procedures[df_patients_encounters_conditions_procedures$encounter.period.start < "2020-02-01",]
#df_patients_encounters_conditions_procedures_gt2020 <- df_patients_encounters_conditions_procedures[df_patients_encounters_conditions_procedures$encounter.period.start >= "2020-02-01",]

if (exists("encounter.location.location.identifier.value", df_patients_encounters_conditions_procedures)) {
  df_result <- distinct(as.data.frame(
    df_patients_encounters_conditions_procedures %>% group_by(
      Patient.ID = df_patients_encounters_conditions_procedures$patient.id
      ,Patient.Alter = df_patients_encounters_conditions_procedures$encounter.period.start.patient.age
      ,Patient.Alter.sort = round(df_patients_encounters_conditions_procedures$encounter.period.start.patient.age.dec,2)
      ,Patient.Geschlecht = df_patients_encounters_conditions_procedures$patient.gender
      ,Fall.ID = df_patients_encounters_conditions_procedures$encounter.id
      ,Fall.Jahr = year(df_patients_encounters_conditions_procedures$encounter.period.start)
      ,Fall.Monat = sprintf("%02d", month(df_patients_encounters_conditions_procedures$encounter.period.start))
      ,ICD.Primaercode.BE = coalesce(df_patients_encounters_conditions_procedures$condition.pri_icd_code.cc,NA)
      ,ICD.Sekundaercode.BE = coalesce(df_patients_encounters_conditions_procedures$condition.sec_icd_code.cc,NA)
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
} else {
  df_result <- distinct(as.data.frame(
    df_patients_encounters_conditions_procedures %>% group_by(
      Patient.ID = df_patients_encounters_conditions_procedures$patient.id
      ,Patient.Alter = df_patients_encounters_conditions_procedures$encounter.period.start.patient.age
      ,Patient.Alter.sort = round(df_patients_encounters_conditions_procedures$encounter.period.start.patient.age.dec,2)
      ,Patient.Geschlecht = df_patients_encounters_conditions_procedures$patient.gender
      ,Fall.ID = df_patients_encounters_conditions_procedures$encounter.id
      ,Fall.Jahr = year(df_patients_encounters_conditions_procedures$encounter.period.start)
      ,Fall.Monat = sprintf("%02d", month(df_patients_encounters_conditions_procedures$encounter.period.start))
      ,ICD.Primaercode.BE = coalesce(df_patients_encounters_conditions_procedures$condition.pri_icd_code.cc,NA)
      ,ICD.Sekundaercode.BE = coalesce(df_patients_encounters_conditions_procedures$condition.sec_icd_code.cc,NA)
      ,ICD.Primaercode.EN = df_patients_encounters_conditions_procedures$condition.pri_icd_code.dd
      ,ICD.Sekundaercode.EN = df_patients_encounters_conditions_procedures$condition.sec_icd_code.dd
      ,Krankenhaus.Tage = coalesce(df_patients_encounters_conditions_procedures$encounter.period.days,NA)
      #,Normalstation.Tage = coalesce(ifelse(df_patients_encounters_conditions_procedures$encounter.period.days < df_patients_encounters_conditions_procedures$encounter.location.period.days.normal,df_patients_encounters_conditions_procedures$encounter.period.days,df_patients_encounters_conditions_procedures$encounter.location.period.days.normal),NA)
      #,Intensivstation.Tage = coalesce(df_patients_encounters_conditions_procedures$encounter.location.period.days.its,NA)
      ,Prozedur.Beatmung = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.beatmung,NA)
      ,Prozedur.Blutkreislauf = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.blutkreislauf,NA)
      ,Labor.CRP.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.crp.max,NA)
      ,Labor.Leuko.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.leuko.max,NA)
    ) %>% summarise(Anzahl = n()) ))
}
df_result <- do.call(data.frame,lapply(df_result, function(x) replace(x, is.infinite(x),NA)))

df_result2 <- df_result %>% 
  group_by(
    Patient.ID = as.character(Patient.ID)
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
    Patient.ID = as.character(Patient.ID),
    Fall.ID = as.character(Fall.ID)) %>% 
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
if (exists("encounter.location.location.identifier.value", df_patients_encounters_conditions_procedures)) {
  df_result <- df_result %>% 
    select(-contains(c("Pseudonym"))) %>% 
    select("Patient.ID","Fall.ID","Fall.Jahr","Fall.Monat","Patient.Alter","Patient.Alter.sort","Patient.Geschlecht","ICD.Primaercode.BE","ICD.Sekundaercode.BE","ICD.Primaercode.EN","ICD.Sekundaercode.EN","Krankenhaus.Tage","Normalstation.Tage","Intensivstation.Tage","Prozedur.Beatmung","Prozedur.Blutkreislauf","Labor.CRP.max","Labor.Leuko.max")
} else {
  df_result <- df_result %>% 
    select(-contains(c("Pseudonym"))) %>% 
    select("Patient.ID","Fall.ID","Fall.Jahr","Fall.Monat","Patient.Alter","Patient.Alter.sort","Patient.Geschlecht","ICD.Primaercode.BE","ICD.Sekundaercode.BE","ICD.Primaercode.EN","ICD.Sekundaercode.EN","Krankenhaus.Tage","Prozedur.Beatmung","Prozedur.Blutkreislauf","Labor.CRP.max","Labor.Leuko.max")
}
  
df_result <- df_result[order(df_result$Patient.ID), ]
df_result_dev <- df_result
#df_result <- df_result_dev

df_result$ICD.Sekundaercode.EN <- ifelse(is.na(df_result$ICD.Primaercode.EN), df_result$ICD.Sekundaercode.BE, df_result$ICD.Sekundaercode.EN)
df_result$ICD.Primaercode.EN <- ifelse(is.na(df_result$ICD.Primaercode.EN), df_result$ICD.Primaercode.BE, df_result$ICD.Primaercode.EN)

df_result_agg_year_pri_sec_full <- as.data.frame(df_result %>% group_by(Jahr = df_result$Fall.Jahr, ICD.Primaercode.BE = df_result$ICD.Primaercode.BE, ICD.Sekundaercode.BE = df_result$ICD.Sekundaercode.BE, ICD.Primaercode.EN = df_result$ICD.Primaercode.EN, ICD.Sekundaercode.EN = df_result$ICD.Sekundaercode.EN) %>% summarise(Anzahl = n()) )
df_result_agg_year_pri_sec <- as.data.frame(df_result %>% group_by(Jahr = df_result$Fall.Jahr, ICD.Primaercode = df_result$ICD.Primaercode.EN, ICD.Sekundaercode = df_result$ICD.Sekundaercode.EN) %>% summarise(Anzahl = n()) )
df_result_agg_year_pri <- as.data.frame(df_result %>% group_by(Jahr = df_result$Fall.Jahr, ICD.Primaercode = df_result$ICD.Primaercode.EN) %>% summarise(Anzahl = n()) )
df_result_agg_year_month_full <- as.data.frame(df_result %>% group_by(Jahr = df_result$Fall.Jahr, Monat = df_result$Fall.Monat, ICD.Primaercode.BE = df_result$ICD.Primaercode.BE, ICD.Sekundaercode.BE = df_result$ICD.Sekundaercode.BE, ICD.Primaercode.EN = df_result$ICD.Primaercode.EN, ICD.Sekundaercode.EN = df_result$ICD.Sekundaercode.EN) %>% summarise(Anzahl = n()) )
df_result_agg_year_month <- as.data.frame(df_result %>% group_by(Jahr_Monat = paste0(df_result$Fall.Jahr,'-',df_result$Fall.Monat), ICD.Primaercode = df_result$ICD.Primaercode.EN, ICD.Sekundaercode = df_result$ICD.Sekundaercode.EN) %>% summarise(Anzahl = n()) )

df_tmp_J20_5 <- df_result_agg_year_pri %>% filter(grepl(paste(c("J20.5"),collapse = '|'), ICD.Primaercode))
df_tmp_J21_0 <- df_result_agg_year_pri %>% filter(grepl(paste(c("J21.0"),collapse = '|'), ICD.Primaercode))
df_tmp_J12_1 <- df_result_agg_year_pri %>% filter(grepl(paste(c("J12.1"),collapse = '|'), ICD.Primaercode))
df_tmp_P23_0 <- df_result_agg_year_pri %>% filter(grepl(paste(c("P23.0"),collapse = '|'), ICD.Primaercode))
df_tmp_B97_4 <- df_result %>% filter(!grepl(paste(c("J20.5","J21.0","J12.1"),collapse = '|'), ICD.Primaercode.EN))
df_tmp_B97_4 <- as.data.frame(df_tmp_B97_4 %>% group_by(Jahr = df_tmp_B97_4$Fall.Jahr, ICD.Sekundaercode = df_tmp_B97_4$ICD.Sekundaercode.EN) %>% summarise(Anzahl = n()) )

df_json_values <- df_tmp_J20_5 %>% select(-contains(c("Jahr","ICD","Anzahl")))
df_json_values$J20.5 <- df_tmp_J20_5$Anzahl
df_json_values$J21.0 <- df_tmp_J21_0$Anzahl
df_json_values$J12.1 <- df_tmp_J12_1$Anzahl
#df_json_values$P23.0 <- df_tmp_P23_0$Anzahl
df_json_values$B97.4 <- df_tmp_B97_4$Anzahl

df_json_values <- t(df_json_values)

dashboard_export <- jsonlite::toJSON(
  list(
    itemname = "timeline.coverchild.diags.icdcodes",
    itemtype = "stackedbarcharts",
    data = list(
      charts = list("allicdcodes"),
      bars = sort(unique(df_result_agg_year_pri$Jahr)),
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

now <- format(Sys.time(), "%Y%m%d_%H%M%S")
write.csv2(df_result_dev, file = paste0("output/",now,"_result.csv"), row.names = FALSE)
write.csv2(df_result_agg_year_pri, file = paste0("output/",now,"_result_agg_year_pri.csv"), row.names = FALSE)
write(dashboard_export, paste0("output/",now,"_dashboard_export.json"))
