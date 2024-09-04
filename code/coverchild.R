if (!require("stringr")) {install.packages("stringr"); library(stringr)}
if (!require("tidyr")) {install.packages("tidyr"); library(tidyr)}
if (!require("dplyr")) {install.packages("dplyr"); library(dplyr)}
if (!require("lubridate")) {install.packages("lubridate"); library(lubridate)}

if (rstudioapi::isAvailable()){
  setwd(dirname(dirname(rstudioapi::getSourceEditorContext()$path)))
  message("setting working directory to: ", getwd())
} else {
  setwd(getwd())
  message("setting working directory to: ", getwd())
}

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

if (file.exists(file.path("config", "coverchild_cfg.yml"))){
  cfg <- config::merge(config::get(file = file.path("config", "fhir_cfg.yml")),config::get(file = file.path("config", "coverchild_cfg.yml")))
} else {
  cfg <- config::get(file = file.path("config", "fhir_cfg.yml"))
}

if (exists("icd_code_system", where = cfg) && nchar(cfg$icd_code_system) >= 1) {
  icd_code_system_custom <- cfg$icd_code_system
} else {
  #icd_code_system_custom <- "http://fhir.de/CodeSystem/dimdi/icd-10-gm"
  icd_code_system_custom <- "http://fhir.de/CodeSystem/bfarm/icd-10-gm"
}

if (exists("dashboard", where = cfg)) {
  if (exists("send_data", where = cfg$dashboard) && cfg$dashboard$send_data) {
    dashboard_send_data <- cfg$dashboard$send_data
  } else {
    dashboard_send_data <- FALSE
  }
  
  if (exists("environment", where = cfg$dashboard) && nchar(cfg$dashboard$environment) >= 1) {
    dashboard_environment <- cfg$dashboard$environment
  } else {
    dashboard_environment <- "test"
  }
  
  if (exists("provider", where = cfg$dashboard) && nchar(cfg$dashboard$provider) >= 1) {
    dashboard_provider <- cfg$dashboard$provider
  } else {
    dashboard_provider <- "set provider name"
  }
  
  if (exists("author", where = cfg$dashboard) && nchar(cfg$dashboard$author) >= 1) {
    dashboard_author <- cfg$dashboard$author
  } else {
    dashboard_author <- "set author name"
  }
  
  if (exists("user", where = cfg$dashboard) && nchar(cfg$dashboard$user) >= 1) {
    dashboard_user <- cfg$dashboard$user
  } else {
    dashboard_user <- "numdashboard.ukbonn.de username"
  }
  
  if (exists("password", where = cfg$dashboard) && nchar(cfg$dashboard$password) >= 1) {
    dashboard_password <- cfg$dashboard$password
  } else {
    dashboard_password <- "numdashboard.ukbonn.de password"
  }
} else {
  dashboard_send_data <- FALSE
  dashboard_environment <- "test"
  dashboard_provider <- "set provider name"
  dashboard_author <- "set author name"
  dashboard_user <- "numdashboard.ukbonn.de username"
  dashboard_password <- "numdashboard.ukbonn.de password"
}

if (exists("patient", where = cfg)) {
  if (exists("filter_countrycode", where = cfg$patient) && nchar(cfg$patient$filter_countrycode) >= 1) {
    patient_filter_countrycode <- cfg$patient$filter_countrycode
  } else {
    patient_filter_countrycode <- "DE"
  }
} else {
  patient_filter_countrycode <- "DE"
}

list_pri_icd_codes <- c("J12.1","J20.5", "J21.0", "P23.0")
list_sec_icd_codes <- c("U07.1!","U99.0!","Z50.1!")
list_pri_sec_icd_codes_combined <- append(list_pri_icd_codes,as.vector(sort(outer(list_pri_icd_codes,list_sec_icd_codes, paste))))
list_pri_sec_B97.4_icd_codes <- c("B99","E84.0","J00","J01.2","J03.9","J06.8","J06.9","J18.0","J18.8","J18.9","J20.5","J20.9","J31.0","J38.5","J40","J45.0","J45.01","J45.02","J45.15","J45.9","J45.99","J96.09","J98.7","R05","R50.88","R50.9","R56.0","R56.8","Z11","Z22.8","Z29.0")
list_pri_sec_B97.4_icd_codes <- as.vector(sort(outer(list_pri_sec_B97.4_icd_codes,c("B97.4!"), paste)))

ops_codes_respiratory_support <- c("8-700","8-700.0","8-700.1","8-700.x","8-700.y","8-711","8-711.0","8-711.00","8-711.01","8-711.1","8-711.10","8-711.11","8-711.2","8-711.20","8-711.21","8-711.3","8-711.30","8-711.31","8-711.4","8-711.40","8-711.41","8-711.x","8-711.y","8-712","8-712.0","8-712.1","8-720")
ops_codes_circulatory_support <- c("8-771","8-779","8-800.6","8-800.c","8-800.d","8-800.f","8-800.g","8-800.h","8-800.j","8-800.k","8-800.m","8-800.n","8-810","8-820","8-821","8-851","8-852","8-853","8-854","8-855","8-856","8-857","8-85a")
ops_palivizumab <- c("6-004.0f","6-004.00","6-004.01","6-004.02","6-004.03","6-004.04","6-004.05","6-004.06","6-004.07","6-004.08","6-004.09","6-004.0a","6-004.0b","6-004.0c","6-004.0d","6-004.0e","6-004.0f","6-004.00","6-004.01","6-004.02","6-004.03","6-004.04","6-004.05","6-004.06","6-004.0e")
ops_palivizumab_neu <- c("")

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
patients_tmp$patient.address.postalCode <- trimws(patients_tmp$patient.address.postalCode)
if (patient_filter_countrycode == "DE") {
  patients_tmp <- patients_tmp %>% filter(patient.address.country %in% c(patient_filter_countrycode))
} else {
  patients_tmp <- patients_tmp[!is.na(patients_tmp$patient.address.postalCode),]
  patients_tmp <- patients_tmp[!str_length(patients_tmp$patient.address.postalCode) >= 6,]
  patients_tmp <- patients_tmp[!str_length(patients_tmp$patient.address.postalCode) <= 4,]
}
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

#conditions_tmp[71, 2] = "J06.9 B97.4! B97.0!"
#conditions_tmp[72, 2] = "J06.9 B97.4! B97.0! B97.1!"
#conditions_tmp[73, 2] = "J06.9 B97.4! B97.0! B97.1! B97.2!"

conditions_tmp[c('condition.pri_icd_code','condition.sec_icd_code')] <- str_split_fixed(conditions_tmp$condition.code.coding.code, ' ', 2)
conditions_tmp$condition.sec_icd_code1 <- conditions_tmp$condition.sec_icd_code
i <- 1
col_name <- paste0("condition.sec_icd_code",i)
count <- max(str_count(conditions_tmp$condition.sec_icd_code, " "))
if (count > 0) {
  for (i in 1:count) {
    col_name <- paste0("condition.sec_icd_code",i)
    i = i+1
    col_name_1 <- paste0("condition.sec_icd_code",i)
    conditions_tmp[col_name_1] <- NA
    conditions_tmp[c(col_name,col_name_1)] <- str_split_fixed(conditions_tmp[[col_name]], ' ', 2)
    conditions_tmp[col_name] <- na_if(conditions_tmp[[col_name]],"")
  }
}
#conditions_tmp$condition.sec_icd_code <- na_if(conditions_tmp$condition.sec_icd_code,"")
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
      "location.status",
      "partOf.reference",
      "serviceType.coding.code",
      "serviceType.coding.display"
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
procedures_tmp <- procedures_tmp %>% filter(grepl(paste(c(ops_codes_respiratory_support,ops_codes_circulatory_support),collapse = '|'), procedure.code.coding.code))
procedures_tmp[c('procedure.ops_code','procedure.snomed_code')] <- str_split_fixed(procedures_tmp$procedure.code.coding.code, ':::', 2)
procedures_tmp$procedure.ops_code.respiratory_support <- ifelse(grepl(paste(ops_codes_respiratory_support,collapse = '|'),procedures_tmp$procedure.ops_code), "yes", "no")
procedures_tmp$procedure.ops_code.circulatory_support <- ifelse(grepl(paste(ops_codes_circulatory_support,collapse = '|'),procedures_tmp$procedure.ops_code), "yes", "no")
#procedures_tmp$procedure.ops_code.respiratory_support <- ifelse(grepl(paste(ops_codes_respiratory_support,collapse = '|'),procedures_tmp$procedure.ops_code), "yes", ifelse(grepl(paste(ops_codes_circulatory_support,collapse = '|'),procedures_tmp$procedure.ops_code), "yes", "no"))
#procedures_tmp$procedure.ops_code.circulatory_support <- ifelse(grepl(paste(ops_codes_circulatory_support,collapse = '|'),procedures_tmp$procedure.ops_code), "yes", ifelse(grepl(paste(ops_codes_respiratory_support,collapse = '|'),procedures_tmp$procedure.ops_code), "yes", "no"))
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

if (exists("encounter.location.location.identifier.value", encounters_tmp)) {
  encounter_merge_cols <- c(
    "encounter.id",
    "encounter.serviceType.coding.code", 
    "encounter.serviceType.coding.display", 
    "encounter.location.location.identifier.value", 
    "encounter.location.period.end", 
    "encounter.location.period.start", 
    "encounter.location.status", 
    #"encounter.location.period.days.normal",
    #"encounter.location.period.days.its",
    #"encounter.location.period.days.fall",
    "encounter.partOf.reference",
    "encounter.period.start",
    "encounter.period.end"
    #"encounter.period.days"
  )
  df_patients_encounters_conditions_procedures <- base::merge(
    df_patients_encounters_conditions_procedures, 
    encounters_tmp[ , 
                    encounter_merge_cols], 
    by.x = "condition.encounter.reference", 
    by.y = "encounter.partOf.reference", 
    all.x=FALSE) %>% distinct()
  
  latest_period_startend_timestamp <- max(c(max(na.omit(df_patients_encounters_conditions_procedures$encounter.period.start)),max(na.omit(df_patients_encounters_conditions_procedures$encounter.period.end))))
  df_patients_encounters_conditions_procedures$encounter.period.end <- coalesce(df_patients_encounters_conditions_procedures$encounter.period.end,latest_period_startend_timestamp)
  df_patients_encounters_conditions_procedures$encounter.period.days <- as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.period.end, df_patients_encounters_conditions_procedures$encounter.period.start, units = "days"))
  
  df_patients_encounters_conditions_procedures$encounter.location.period.days.its <- ifelse(
    grepl(paste(c("ITS","its"),collapse = '|'),df_patients_encounters_conditions_procedures$encounter.location.location.identifier.value), 
    as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.location.period.end, df_patients_encounters_conditions_procedures$encounter.location.period.start, units = "days")), 
    NA
  )
  
  df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures  %>% group_by(condition.encounter.reference) %>% summarise(encounter.location.period.days.its = max(encounter.location.period.days.its, na.rm=TRUE), encounter.period.days = encounter.period.days) %>% distinct()
  df_patients_encounters_conditions_procedures2[sapply(df_patients_encounters_conditions_procedures2, is.infinite)] <- NA
  df_patients_encounters_conditions_procedures2$encounter.location.period.days.normal <- ifelse(
    !is.na(df_patients_encounters_conditions_procedures2$encounter.location.period.days.its),
    df_patients_encounters_conditions_procedures2$encounter.period.days - df_patients_encounters_conditions_procedures2$encounter.location.period.days.its,
    df_patients_encounters_conditions_procedures2$encounter.period.days)
  #df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures2 %>% drop_na(condition.encounter.reference)
  df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("encounter.location.period.days.its","encounter.period.days")))
  df_patients_encounters_conditions_procedures <- base::merge(df_patients_encounters_conditions_procedures, df_patients_encounters_conditions_procedures2, by = "condition.encounter.reference")
  rm(df_patients_encounters_conditions_procedures2)
  
} else {
  encounter_merge_cols <- c(
    "encounter.id",
    #"encounter.subject.reference",
    "encounter.period.start",
    "encounter.period.end"
    #"encounter.period.days"
  )
  df_patients_encounters_conditions_procedures <- base::merge(
    df_patients_encounters_conditions_procedures, 
    encounters_tmp[ , 
                    encounter_merge_cols], 
    by.x = "condition.encounter.reference", 
    by.y = "encounter.id", 
    all.x=FALSE) %>% distinct()
  
  latest_period_startend_timestamp <- max(c(max(na.omit(df_patients_encounters_conditions_procedures$encounter.period.start)),max(na.omit(df_patients_encounters_conditions_procedures$encounter.period.end))))
  df_patients_encounters_conditions_procedures$encounter.period.end <- coalesce(df_patients_encounters_conditions_procedures$encounter.period.end,latest_period_startend_timestamp)
  df_patients_encounters_conditions_procedures$encounter.period.days <- as.numeric(difftime(df_patients_encounters_conditions_procedures$encounter.period.end, df_patients_encounters_conditions_procedures$encounter.period.start, units = "days"))
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
    "procedure.ops_code.respiratory_support",
    "procedure.ops_code.circulatory_support"
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

df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("observation.subject.reference")))

df_patients_encounters_conditions_procedures <- base::merge(
  patients_tmp, 
  df_patients_encounters_conditions_procedures, 
  by.x = "patient.id", 
  by.y = "condition.subject.reference", 
  all.x = FALSE) %>% distinct()

df_patients_encounters_conditions_procedures$encounter.id <- df_patients_encounters_conditions_procedures$condition.encounter.reference
df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% select(-contains(c("condition.encounter.reference")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures %>% 
#  select(-contains(c("resource_id","lastUpdated","meta.source","reference")))
#df_patients_encounters_conditions_procedures <- df_patients_encounters_conditions_procedures[colSums(!is.na(df_patients_encounters_conditions_procedures)) > 0]

df_patients_encounters_conditions_procedures2 <- df_patients_encounters_conditions_procedures %>% 
  distinct(patient.id) %>% 
  group_by(patient.id) %>% 
  summarise(patient.id.pseudonym = paste("patient",1:n())) %>% 
  mutate(
    patient.id.pseudonym = paste("patient",str_pad(1:n(), 3, pad = "0"))
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
  select(-contains(c("encounter.location.period.days.normal","encounter.location.period.days.its")))
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
    encounter.period.start.patient.age = 
      ifelse(
        encounter.period.start.patient.age.months == 0, 
        paste0(encounter.period.start.patient.age.days,"d"), 
        ifelse(
          encounter.period.start.patient.age.months <=23, 
          paste0(encounter.period.start.patient.age.months,"m"), 
          ifelse(
            encounter.period.start.patient.age.months >=24, 
            paste0(encounter.period.start.patient.age.years,"y"),
            NA 
          )
        )
      ),
    encounter.period.start.patient.age.group = 
      ifelse(
        encounter.period.start.patient.age.days < 28,
        "0<28d",
        ifelse(
          encounter.period.start.patient.age.years >=3 & encounter.period.start.patient.age.years < 6,
          "3<6",
          ifelse(
            encounter.period.start.patient.age.years >= 6 & encounter.period.start.patient.age.years < 9,
            "6<9",
            ifelse(
              encounter.period.start.patient.age.years >= 9 & encounter.period.start.patient.age.years < 12,
              "9<12",
              ifelse(
                encounter.period.start.patient.age.years >= 12 & encounter.period.start.patient.age.years < 15,
                "12<15",
                ifelse(
                  encounter.period.start.patient.age.years >= 15 & encounter.period.start.patient.age.years < 18,
                  "15<18",
                  "18+"
                )
              )
            )
          )
        )
      ),
    encounter.period.start.patient.age.group_dashboard = 
      ifelse(
        encounter.period.start.patient.age.years < 3,
        "0<3",
        ifelse(
          encounter.period.start.patient.age.years >=3 & encounter.period.start.patient.age.years < 6,
          "3<6",
          ifelse(
            encounter.period.start.patient.age.years >= 6 & encounter.period.start.patient.age.years < 9,
            "6<9",
            ifelse(
              encounter.period.start.patient.age.years >= 9 & encounter.period.start.patient.age.years < 12,
              "9<12",
              ifelse(
                encounter.period.start.patient.age.years >= 12 & encounter.period.start.patient.age.years < 15,
                "12<15",
                ifelse(
                  encounter.period.start.patient.age.years >= 15 & encounter.period.start.patient.age.years < 18,
                  "15<18",
                  "18+"
                )
              )
            )
          )
        )
      ),
    .after=patient.id
  )

#zse splitten in jahre
#df_patients_encounters_conditions_procedures_lt2020 <- df_patients_encounters_conditions_procedures[df_patients_encounters_conditions_procedures$encounter.period.start < "2020-02-01",]
#df_patients_encounters_conditions_procedures_gt2020 <- df_patients_encounters_conditions_procedures[df_patients_encounters_conditions_procedures$encounter.period.start >= "2020-02-01",]

df_result <- distinct(as.data.frame(
  df_patients_encounters_conditions_procedures %>% group_by(
    patient.id = df_patients_encounters_conditions_procedures$patient.id
    ,patient.age = df_patients_encounters_conditions_procedures$encounter.period.start.patient.age
    ,patient.age.sort = round(df_patients_encounters_conditions_procedures$encounter.period.start.patient.age.dec,2)
    ,patient.age.group = df_patients_encounters_conditions_procedures$encounter.period.start.patient.age.group_dashboard
    ,patient.gender = df_patients_encounters_conditions_procedures$patient.gender
    ,patient.postalCode = df_patients_encounters_conditions_procedures$patient.address.postalCode
    ,patient.postalCode.full = df_patients_encounters_conditions_procedures$patient.address.postalCode_full
    ,encounter.id = df_patients_encounters_conditions_procedures$encounter.id
    ,encounter.year = year(df_patients_encounters_conditions_procedures$encounter.period.start)
    ,encounter.month = sprintf("%02d", month(df_patients_encounters_conditions_procedures$encounter.period.start))
    ,encounter.year.month = paste0(year(df_patients_encounters_conditions_procedures$encounter.period.start),'-',sprintf("%02d", month(df_patients_encounters_conditions_procedures$encounter.period.start)))
    ,encounter.year.quarter = paste0("Q",lubridate::quarter(df_patients_encounters_conditions_procedures$encounter.period.start, with_year = FALSE)," ",year(df_patients_encounters_conditions_procedures$encounter.period.start))
    ,icd.pri_code.cc = coalesce(df_patients_encounters_conditions_procedures$condition.pri_icd_code.cc,NA)
    ,icd.sec_code.cc = coalesce(df_patients_encounters_conditions_procedures$condition.sec_icd_code.cc,NA)
    ,icd.pri_code.dd = df_patients_encounters_conditions_procedures$condition.pri_icd_code.dd
    ,icd.sec_code.dd = df_patients_encounters_conditions_procedures$condition.sec_icd_code.dd
    ,days.hospital = coalesce(df_patients_encounters_conditions_procedures$encounter.period.days,NA)
    ,days.normal = coalesce(ifelse(df_patients_encounters_conditions_procedures$encounter.period.days < coalesce(df_patients_encounters_conditions_procedures$encounter.location.period.days.normal,NA),df_patients_encounters_conditions_procedures$encounter.period.days,coalesce(df_patients_encounters_conditions_procedures$encounter.location.period.days.normal,NA)),NA)
    ,days.its = coalesce(df_patients_encounters_conditions_procedures$encounter.location.period.days.its,NA)
    ,procedure.respiratory_support = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.respiratory_support,"no")
    ,procedure.circulatory_support = coalesce(df_patients_encounters_conditions_procedures$procedure.ops_code.circulatory_support,"no")
    ,laboratory.crp.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.crp.max,NA)
    ,laboratory.leuko.max = coalesce(df_patients_encounters_conditions_procedures$observation.valueQuantity.value.leuko.max,NA)
  ) %>% summarise(count = n()) ))
df_result <- do.call(data.frame,lapply(df_result, function(x) replace(x, is.infinite(x),NA)))

df_result2 <- df_result %>% 
  group_by(
    patient.id = as.character(patient.id)
  ) %>% 
  summarise(
    patient.id.pseudonym = paste("patient")
  ) %>% 
  mutate(
    patient.id.pseudonym = paste("patient",str_pad(1:n(), 3, pad = "0"))
  )
df_result <- base::merge(df_result, df_result2, by = "patient.id")
df_result2 <- df_result %>% 
  group_by(
    patient.id = as.character(patient.id),
    encounter.id = as.character(encounter.id)) %>% 
  summarise(
    encounter.id.pseudonym = paste("encounter")) %>% 
  mutate(
    encounter.id.pseudonym = paste("encounter",str_pad(1:n(), 3, pad = "0")))
df_result <- base::merge(df_result, df_result2[,c("encounter.id","encounter.id.pseudonym")], by = "encounter.id")
rm(df_result2)
df_result$encounter.id <- df_result$encounter.id.pseudonym
df_result$patient.id <- df_result$patient.id.pseudonym
df_result$laboratory.crp.max <-round(df_result$laboratory.crp.max,2)
df_result$laboratory.leuko.max <-round(df_result$laboratory.leuko.max,2)

df_result <- df_result %>% 
  select(-contains(c("pseudonym"))) %>% 
  select("patient.id","encounter.id","encounter.year","encounter.month","encounter.year.month","encounter.year.quarter","patient.age","patient.age.sort","patient.age.group","patient.gender","patient.postalCode","patient.postalCode.full","icd.pri_code.cc","icd.sec_code.cc","icd.pri_code.dd","icd.sec_code.dd","days.hospital","days.normal","days.its","procedure.respiratory_support","procedure.circulatory_support","laboratory.crp.max","laboratory.leuko.max")

df_result <- df_result[order(df_result$patient.id), ]
df_result_dev <- df_result
#df_result <- df_result_dev

df_result$icd.sec_code.dd <- ifelse(is.na(df_result$icd.pri_code.dd), df_result$icd.sec_code.cc, df_result$icd.sec_code.dd)
df_result$icd.pri_code.dd <- ifelse(is.na(df_result$icd.pri_code.dd), df_result$icd.pri_code.cc, df_result$icd.pri_code.dd)

df_result_agg_year_pri_sec_full <- as.data.frame(df_result %>% group_by(year = df_result$encounter.year, icd.pri_code.cc = df_result$icd.pri_code.cc, icd.sec_code.cc = df_result$icd.sec_code.cc, icd.pri_code.dd = df_result$icd.pri_code.dd, icd.sec_code.dd = df_result$icd.sec_code.dd) %>% summarise(count = n()) )
df_result_agg_year_pri_sec <- as.data.frame(df_result %>% group_by(year = df_result$encounter.year, icd.pri_code = df_result$icd.pri_code.dd, icd.sec_code = df_result$icd.sec_code.dd) %>% summarise(count = n()) )
df_result_agg_year_pri <- as.data.frame(df_result %>% group_by(year = df_result$encounter.year, icd.pri_code = df_result$icd.pri_code.dd) %>% summarise(count = n()) )
df_result_agg_year_month_full <- as.data.frame(df_result %>% group_by(year = df_result$encounter.year, month = df_result$encounter.month, icd.pri_code.cc = df_result$icd.pri_code.cc, icd.sec_code.cc = df_result$icd.sec_code.cc, icd.pri_code.dd = df_result$icd.pri_code.dd, icd.sec_code.dd = df_result$icd.sec_code.dd) %>% summarise(count = n()) )
df_result_agg_year_month <- as.data.frame(df_result %>% group_by(year_month = paste0(df_result$encounter.year,'-',df_result$encounter.month), icd.pri_code = df_result$icd.pri_code.dd, icd.sec_code = df_result$icd.sec_code.dd) %>% summarise(count = n()) )
df_result_agg_year_month_pri <- as.data.frame(df_result %>% group_by(year_month = paste0(df_result$encounter.year,'-',df_result$encounter.month), icd.pri_code = df_result$icd.pri_code.dd) %>% summarise(count = n()) )
df_result_agg_age <- as.data.frame(df_result %>% group_by(age.group = df_result$patient.age.group) %>% summarise(count = n()) %>% arrange(factor(age.group, levels = c("0<3","3<6","6<9","9<12","12<15","15<18"))))
df_result_agg_age$age.group.dashboard = c("age_<3","age_3<6","age_6<9","age_9<12","age_12<15","age_15<18")
df_result_agg_age_dashboard <- df_result_agg_age$count[1]
for (r in 2:length(df_result_agg_age$count)) {
  df_result_agg_age_dashboard <- rbind(df_result_agg_age_dashboard,df_result_agg_age$count[r])
}
df_result_agg_age_pri <- as.data.frame(df_result %>% group_by(age.group = df_result$patient.age.group, icd.pri_code = df_result$icd.pri_code.dd) %>% summarise(count = n()) %>% arrange(factor(age.group, levels = c("0<3","3<6","6<9","9<12","12<15","15<18"))))

df_tmp_J20_5_empty <- data.frame(age.group=c(df_result_agg_age$age.group,">18"), count=0)
df_tmp_J21_0_empty <- data.frame(age.group=c(df_result_agg_age$age.group,">18"), count=0)
df_tmp_J12_1_empty <- data.frame(age.group=c(df_result_agg_age$age.group,">18"), count=0)
df_tmp_P23_0_empty <- data.frame(age.group=c(df_result_agg_age$age.group,">18"), count=0)
df_tmp_B97_4_empty <- data.frame(age.group=c(df_result_agg_age$age.group,">18"), count=0)

df_tmp_J20_5 <- rbind(df_tmp_J20_5_empty,df_result_agg_age_pri %>% filter(grepl(paste(c("J20.5"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_J21_0 <- rbind(df_tmp_J21_0_empty,df_result_agg_age_pri %>% filter(grepl(paste(c("J21.0"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_J12_1 <- rbind(df_tmp_J12_1_empty,df_result_agg_age_pri %>% filter(grepl(paste(c("J12.1"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_P23_0 <- rbind(df_tmp_P23_0_empty,df_result_agg_age_pri %>% filter(grepl(paste(c("P23.0"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_B97_4 <- df_result %>% filter(!grepl(paste(c("J20.5","J21.0","J12.1"),collapse = '|'), icd.pri_code.dd))
df_tmp_B97_4 <- as.data.frame(df_tmp_B97_4 %>% group_by(age.group = df_tmp_B97_4$patient.age.group) %>% summarise(count = n()) )
df_tmp_B97_4 <- rbind(df_tmp_B97_4_empty,df_tmp_B97_4)

df_tmp_J20_5 <- df_tmp_J20_5 %>% group_by(age.group) %>% summarize(count = sum(count)) %>% arrange(factor(age.group, levels = c("0<3","3<6","6<9","9<12","12<15","15<18","18+"))) %>% mutate(age.group.dashboard = c("age_<3","age_3<6","age_6<9","age_9<12","age_12<15","age_15<18","age_18+"))
df_tmp_J21_0 <- df_tmp_J21_0 %>% group_by(age.group) %>% summarize(count = sum(count)) %>% arrange(factor(age.group, levels = c("0<3","3<6","6<9","9<12","12<15","15<18","18+"))) %>% mutate(age.group.dashboard = c("age_<3","age_3<6","age_6<9","age_9<12","age_12<15","age_15<18","age_18+"))
df_tmp_J12_1 <- df_tmp_J12_1 %>% group_by(age.group) %>% summarize(count = sum(count)) %>% arrange(factor(age.group, levels = c("0<3","3<6","6<9","9<12","12<15","15<18","18+"))) %>% mutate(age.group.dashboard = c("age_<3","age_3<6","age_6<9","age_9<12","age_12<15","age_15<18","age_18+"))
df_tmp_P23_0 <- df_tmp_P23_0 %>% group_by(age.group) %>% summarize(count = sum(count)) %>% arrange(factor(age.group, levels = c("0<3","3<6","6<9","9<12","12<15","15<18","18+"))) %>% mutate(age.group.dashboard = c("age_<3","age_3<6","age_6<9","age_9<12","age_12<15","age_15<18","age_18+"))
df_tmp_B97_4 <- df_tmp_B97_4 %>% group_by(age.group) %>% summarize(count = sum(count)) %>% arrange(factor(age.group, levels = c("0<3","3<6","6<9","9<12","12<15","15<18","18+"))) %>% mutate(age.group.dashboard = c("age_<3","age_3<6","age_6<9","age_9<12","age_12<15","age_15<18","age_18+"))

df_json_values_icdcodes_age_groups <- df_tmp_J20_5 %>% select(-contains(c("age.group","age.group.dashboard","count")))
df_json_values_icdcodes_age_groups$J20.5 <- df_tmp_J20_5$count
df_json_values_icdcodes_age_groups$J21.0 <- df_tmp_J21_0$count
df_json_values_icdcodes_age_groups$J12.1 <- df_tmp_J12_1$count
#df_json_values_icdcodes_age_groups$P23.0 <- df_tmp_P23_0$count
df_json_values_icdcodes_age_groups$B97.4 <- df_tmp_B97_4$count

df_json_values_icdcodes_age_groups <- t(df_json_values_icdcodes_age_groups)

df_result_agg_gender <- as.data.frame(df_result %>% group_by(gender = df_result$patient.gender) %>% summarise(count = n()))
df_result_agg_gender_pri <- as.data.frame(df_result %>% group_by(gender = df_result$patient.gender, icd.pri_code = df_result$icd.pri_code.dd) %>% summarise(count = n()))

df_tmp_J20_5_empty <- data.frame(gender=df_result_agg_gender$gender, count=0)
df_tmp_J21_0_empty <- data.frame(gender=df_result_agg_gender$gender, count=0)
df_tmp_J12_1_empty <- data.frame(gender=df_result_agg_gender$gender, count=0)
df_tmp_P23_0_empty <- data.frame(gender=df_result_agg_gender$gender, count=0)
df_tmp_B97_4_empty <- data.frame(gender=df_result_agg_gender$gender, count=0)

df_tmp_J20_5 <- rbind(df_tmp_J20_5_empty,df_result_agg_gender_pri %>% filter(grepl(paste(c("J20.5"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_J21_0 <- rbind(df_tmp_J21_0_empty,df_result_agg_gender_pri %>% filter(grepl(paste(c("J21.0"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_J12_1 <- rbind(df_tmp_J12_1_empty,df_result_agg_gender_pri %>% filter(grepl(paste(c("J12.1"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_P23_0 <- rbind(df_tmp_P23_0_empty,df_result_agg_gender_pri %>% filter(grepl(paste(c("P23.0"),collapse = '|'), icd.pri_code)) %>% select(-c(icd.pri_code)))
df_tmp_B97_4 <- df_result %>% filter(!grepl(paste(c("J20.5","J21.0","J12.1"),collapse = '|'), icd.pri_code.dd))
df_tmp_B97_4 <- as.data.frame(df_tmp_B97_4 %>% group_by(gender = df_tmp_B97_4$patient.gender) %>% summarise(count = n()) )
df_tmp_B97_4 <- rbind(df_tmp_B97_4_empty,df_tmp_B97_4)

df_tmp_J20_5 <- df_tmp_J20_5 %>% group_by(gender) %>% summarize(count = sum(count))
df_tmp_J21_0 <- df_tmp_J21_0 %>% group_by(gender) %>% summarize(count = sum(count))
df_tmp_J12_1 <- df_tmp_J12_1 %>% group_by(gender) %>% summarize(count = sum(count))
df_tmp_P23_0 <- df_tmp_P23_0 %>% group_by(gender) %>% summarize(count = sum(count))
df_tmp_B97_4 <- df_tmp_B97_4 %>% group_by(gender) %>% summarize(count = sum(count))

df_json_values_icdcodes_genders <- df_tmp_J20_5 %>% select(-contains(c("gender","count")))
df_json_values_icdcodes_genders$count <- df_result_agg_gender$count
df_json_values_icdcodes_genders$J20.5 <- df_tmp_J20_5$count
df_json_values_icdcodes_genders$J21.0 <- df_tmp_J21_0$count
df_json_values_icdcodes_genders$J12.1 <- df_tmp_J12_1$count
#df_json_values_icdcodes_genders$P23.0 <- df_tmp_P23_0$count
df_json_values_icdcodes_genders$B97.4 <- df_tmp_B97_4$count

df_json_values_icdcodes_genders <- t(df_json_values_icdcodes_genders)

lengthofstay_mean <- mean(df_result$days.hospital)

df_tmp_J20_5 <- df_result %>% filter(grepl(paste(c("J20.5"),collapse = '|'), icd.pri_code.dd)) %>% select(c(days.hospital))
df_tmp_J21_0 <- df_result %>% filter(grepl(paste(c("J21.0"),collapse = '|'), icd.pri_code.dd)) %>% select(c(days.hospital))
df_tmp_J12_1 <- df_result %>% filter(grepl(paste(c("J12.1"),collapse = '|'), icd.pri_code.dd)) %>% select(c(days.hospital))
df_tmp_P23_0 <- df_result %>% filter(grepl(paste(c("P23.0"),collapse = '|'), icd.pri_code.dd)) %>% select(c(days.hospital))
df_tmp_B97_4 <- df_result %>% filter(!grepl(paste(c("J20.5","J21.0","J12.1"),collapse = '|'), icd.pri_code.dd)) %>% select(c(days.hospital))

df_json_values_icdcodes_lengthofstay_mean <- tibble(J20.5=round(mean(df_tmp_J20_5$days.hospital),2))
df_json_values_icdcodes_lengthofstay_mean$J21.0 <- round(mean(df_tmp_J21_0$days.hospital),2)
df_json_values_icdcodes_lengthofstay_mean$J12.1 <- round(mean(df_tmp_J12_1$days.hospital),2)
#df_json_values_icdcodes_lengthofstay_mean$P23.0 <- round(mean(df_tmp_P23_0$days.hospital),2)
df_json_values_icdcodes_lengthofstay_mean$B97.4 <- round(mean(df_tmp_B97_4$days.hospital),2)

df_json_values_icdcodes_lengthofstay_mean <- t(df_json_values_icdcodes_lengthofstay_mean)

df_tmp_J20_5_empty <- data.frame(year=2016:2024, icd.pri_code="J20.5", count=0)
df_tmp_J21_0_empty <- data.frame(year=2016:2024, icd.pri_code="J21.0", count=0)
df_tmp_J12_1_empty <- data.frame(year=2016:2024, icd.pri_code="J12.1", count=0)
df_tmp_P23_0_empty <- data.frame(year=2016:2024, icd.pri_code="P23.0", count=0)
df_tmp_B97_4_empty <- data.frame(year=2016:2024, icd.sec_code="B97.4!", count=0)

df_tmp_J20_5 <- rbind(df_tmp_J20_5_empty,df_result_agg_year_pri %>% filter(grepl(paste(c("J20.5"),collapse = '|'), icd.pri_code)))
df_tmp_J21_0 <- rbind(df_tmp_J21_0_empty,df_result_agg_year_pri %>% filter(grepl(paste(c("J21.0"),collapse = '|'), icd.pri_code)))
df_tmp_J12_1 <- rbind(df_tmp_J12_1_empty,df_result_agg_year_pri %>% filter(grepl(paste(c("J12.1"),collapse = '|'), icd.pri_code)))
df_tmp_P23_0 <- rbind(df_tmp_P23_0_empty,df_result_agg_year_pri %>% filter(grepl(paste(c("P23.0"),collapse = '|'), icd.pri_code)))
df_tmp_B97_4 <- df_result %>% filter(!grepl(paste(c("J20.5","J21.0","J12.1"),collapse = '|'), icd.pri_code.dd))
df_tmp_B97_4 <- as.data.frame(df_tmp_B97_4 %>% group_by(year = df_tmp_B97_4$encounter.year, icd.sec_code = df_tmp_B97_4$icd.sec_code.dd) %>% summarise(count = n()) )
df_tmp_B97_4 <- rbind(df_tmp_B97_4_empty,df_tmp_B97_4)

df_tmp_J20_5 <- df_tmp_J20_5 %>% group_by(year) %>% summarize(count = sum(count))
df_tmp_J21_0 <- df_tmp_J21_0 %>% group_by(year) %>% summarize(count = sum(count))
df_tmp_J12_1 <- df_tmp_J12_1 %>% group_by(year) %>% summarize(count = sum(count))
df_tmp_P23_0 <- df_tmp_P23_0 %>% group_by(year) %>% summarize(count = sum(count))
df_tmp_B97_4 <- df_tmp_B97_4 %>% group_by(year) %>% summarize(count = sum(count))

df_json_values_icdcodes <- df_tmp_J20_5 %>% select(-contains(c("year","icd","count")))
df_json_values_icdcodes$J20.5 <- df_tmp_J20_5$count
df_json_values_icdcodes$J21.0 <- df_tmp_J21_0$count
df_json_values_icdcodes$J12.1 <- df_tmp_J12_1$count
#df_json_values_icdcodes$P23.0 <- df_tmp_P23_0$count
df_json_values_icdcodes$B97.4 <- df_tmp_B97_4$count

df_json_values_icdcodes <- t(df_json_values_icdcodes)

year_mon <- seq(ym("2016-01"), ym("2024-12"), by = "months") |>
  as.Date() |>
  format("%Y-%m") |>
  unique()

df_tmp_J20_5_empty <- data.frame(year_month=year_mon, icd.pri_code="J20.5", count=0)
df_tmp_J21_0_empty <- data.frame(year_month=year_mon, icd.pri_code="J21.0", count=0)
df_tmp_J12_1_empty <- data.frame(year_month=year_mon, icd.pri_code="J12.1", count=0)
df_tmp_P23_0_empty <- data.frame(year_month=year_mon, icd.pri_code="P23.0", count=0)
df_tmp_B97_4_empty <- data.frame(year_month=year_mon, icd.sec_code="B97.4!", count=0)

df_tmp_J20_5 <- rbind(df_tmp_J20_5_empty,df_result_agg_year_month_pri %>% filter(grepl(paste(c("J20.5"),collapse = '|'), icd.pri_code)))
df_tmp_J21_0 <- rbind(df_tmp_J21_0_empty,df_result_agg_year_month_pri %>% filter(grepl(paste(c("J21.0"),collapse = '|'), icd.pri_code)))
df_tmp_J12_1 <- rbind(df_tmp_J12_1_empty,df_result_agg_year_month_pri %>% filter(grepl(paste(c("J12.1"),collapse = '|'), icd.pri_code)))
df_tmp_P23_0 <- rbind(df_tmp_P23_0_empty,df_result_agg_year_month_pri %>% filter(grepl(paste(c("P23.0"),collapse = '|'), icd.pri_code)))
df_tmp_B97_4 <- df_result %>% filter(!grepl(paste(c("J20.5","J21.0","J12.1"),collapse = '|'), icd.pri_code.dd))
df_tmp_B97_4 <- as.data.frame(df_tmp_B97_4 %>% group_by(year_month = df_tmp_B97_4$encounter.year.month, icd.sec_code = df_tmp_B97_4$icd.sec_code.dd) %>% summarise(count = n()) )
df_tmp_B97_4 <- rbind(df_tmp_B97_4_empty,df_tmp_B97_4)

df_tmp_J20_5 <- df_tmp_J20_5 %>% group_by(year_month) %>% summarize(count = sum(count))
df_tmp_J21_0 <- df_tmp_J21_0 %>% group_by(year_month) %>% summarize(count = sum(count))
df_tmp_J12_1 <- df_tmp_J12_1 %>% group_by(year_month) %>% summarize(count = sum(count))
df_tmp_P23_0 <- df_tmp_P23_0 %>% group_by(year_month) %>% summarize(count = sum(count))
df_tmp_B97_4 <- df_tmp_B97_4 %>% group_by(year_month) %>% summarize(count = sum(count))

df_json_values_icdcodes_year_month <- df_tmp_J20_5 %>% select(-contains(c("year_month","icd","count")))
df_json_values_icdcodes_year_month$J20.5 <- df_tmp_J20_5$count
df_json_values_icdcodes_year_month$J21.0 <- df_tmp_J21_0$count
df_json_values_icdcodes_year_month$J12.1 <- df_tmp_J12_1$count
#df_json_values_icdcodes_year_month$P23.0 <- df_tmp_P23_0$count
df_json_values_icdcodes_year_month$B97.4 <- df_tmp_B97_4$count

df_json_values_icdcodes_year_month <- t(df_json_values_icdcodes_year_month)

dashboard_export_small <- jsonlite::toJSON(
  list(
    itemname = "timeline.coverchild.diags.icdcodes",
    itemtype = "stackedbarcharts",
    data = list(
      charts = list("allicdcodes"),
      bars = 2016:2024,
      stacks = list(
        "J20.5: acute rsv-bronchitis",
        "J21.0: acute rsv-bronchiolitis",
        "J12.1: rsv-pneumonia",
        "B97.4!: rsv caused disease"
      ),
      values = list(
        t(df_json_values_icdcodes)
      )
    )
  ),
  pretty = TRUE)

now <- format(Sys.time(), "%Y%m%d_%H%M%S")
write.csv2(df_result_dev, file = paste0("output/",now,"_result.csv"), row.names = FALSE)
write.csv2(df_result_agg_year_pri, file = paste0("output/",now,"_result_agg_year_pri.csv"), row.names = FALSE)

dashboard_export <- jsonlite::toJSON(
  c(
    provider = dashboard_provider,
    dashboard_dataset_version = "0.5.3",
    exporttimestamp = floor(as.numeric(Sys.time())),
    author = dashboard_author,
    dataitems = list(
      list(
        list(
          itemname = "kira.cumulative.diags.zipcode.all",
          itemtype = "list",
        #  data = NULL
          data = sort(df_result$patient.postalCode.full)
        ),
        list(
          itemname = "kira.cumulative.diags.age.disorders",
          itemtype = "stackedbarcharts",
          data = list(
            charts = list(
              "all_icd_codes"
            ),
            bars = list(
              c("age_<3","age_3<6","age_6<9","age_9<12","age_12<15","age_15<18","age_18+")
            ),
            stacks = list(
              c(
                "acute_rsv_bronchitis_j20.5",
                "acute_rsv_bronchiolitis_j21.0",
                "rsv_pneumonia_j12.1",
                "rsv_caused_disease_b97.4"
              )
            ),
            values = list(t(df_json_values_icdcodes_age_groups))
          )
        ),
        list(
          itemname = "kira.cumulative.diags.disorders.gender",
          itemtype = "piecharts",
          data = list(
            charts = c(
              "all_icd_codes",
              "acute_rsv_bronchitis_j20.5",
              "acute_rsv_bronchiolitis_j21.0",
              "rsv_pneumonia_j12.1",
              "rsv_caused_disease_b97.4"
            ),
            pies = sort(unique(df_result$patient.gender)),
            values = df_json_values_icdcodes_genders
          )
        ),
        list(
          itemname = "kira.cumulative.lengthofstay.disorders",
          itemtype = "stackedbarcharts",
          data = list(
            charts = list(
              "all_icd_codes"
            ),
            bars = list(
              c(
                "acute_rsv_bronchitis_j20.5",
                "acute_rsv_bronchiolitis_j21.0",
                "rsv_pneumonia_j12.1",
                "rsv_caused_disease_b97.4"
              )
            ),
            stacks = list(
              list(
                "mean_length_of_stay"
              )
            ),
            values = list(
              df_json_values_icdcodes_lengthofstay_mean
            )
          )
        ),
        list(
          itemname = "kira.timeline.diags.disorders",
          itemtype = "stackedbarcharts",
          data = list(
            charts = list(
              "all_icd_codes"
            ),
            bars = list(
              c(
                year_mon
              )
            ),
            stacks = list(
              c(
                "acute_rsv_bronchitis_j20.5",
                "acute_rsv_bronchiolitis_j21.0",
                "rsv_pneumonia_j12.1",
                "rsv_caused_disease_b97.4"
              )
            ),
            values = list(
              t(df_json_values_icdcodes_year_month)
            )
          )
        ),
        list(
          itemname = "kira.timeline.diags.icdcodes",
          itemtype = "stackedbarcharts",
          data = list(
            charts = list(
              "all_icd_codes"
            ),
            bars = list(
              c(
                "2016",
                "2017",
                "2018",
                "2019",
                "2020",
                "2021",
                "2022",
                "2023",
                "2024"
              )
            ),
            stacks = list(
              c(
                "acute_rsv_bronchitis_j20.5",
                "acute_rsv_bronchiolitis_j21.0",
                "rsv_pneumonia_j12.1",
                "rsv_caused_disease_b97.4"
              )
            ),
            values = list(
              t(df_json_values_icdcodes)
            )
          )
        )
      )
    )
  )
  , pretty = TRUE
  , auto_unbox = TRUE)

dashboard_export_1 <- jsonlite::toJSON(
  c(
    provider = dashboard_provider,
    dashboard_dataset_version = "0.5.3",
    exporttimestamp = floor(as.numeric(Sys.time())),
    author = dashboard_author,
    dataitems = list(
      list(
        list(
          itemtype = "stackedbarcharts",
          itemname = "kira.timeline.diags.icdcodes",
          data = list(
            charts = list(
              "all_icd_codes"
            ),
            bars = list(
              c(
                "2016",
                "2017",
                "2018",
                "2019",
                "2020",
                "2021",
                "2022",
                "2023",
                "2024"
              )
            ),
            stacks = list(
              c(
                "acute_rsv_bronchitis_j20.5",
                "acute_rsv_bronchiolitis_j21.0",
                "rsv_pneumonia_j12.1",
                "rsv_caused_disease_b97.4"
              )
            ),
            values = list(
              t(df_json_values_icdcodes)
            )
          )
        )
      )
    )
  )
  , pretty = TRUE
  , auto_unbox = TRUE)

df_json_values_icdcodes_1 <- df_json_values_icdcodes

df_json_values_icdcodes_1[df_json_values_icdcodes_1<5] <- NULL

dashboard_export

write(dashboard_export, paste0("output/",now,"_dashboard_export.json"))
if (dashboard_send_data) {
  # Loading packages
  library(httr)
  library(jsonlite)
  
  # Initializing API Call
  if (dashboard_environment == 'test') {
    destination <- "numdashboard-test.ukbonn.de"
  } else {
    destination <- "numdashboard.ukbonn.de"
  }
  call <- paste0("https://",destination,"/backend/",dashboard_provider,"/put")
  put_request <- PUT(url = call,
                     authenticate(dashboard_user, dashboard_password, type = "basic"),
                     #use_proxy("http://ukd-proxy.med.tu-dresden.de", port = 80),
                     content_type("application/json"),
                     body = dashboard_export_1,
                     encode = "json", 
                     config(
                       #verbose = TRUE, 
                       ssl_verifypeer = FALSE))
  
  stop_for_status(put_request)
  
}
content(put_request)

