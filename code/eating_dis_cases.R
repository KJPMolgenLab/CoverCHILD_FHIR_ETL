source("code/data_etl.R")
library(janitor)

covid_start <- ymd("2020-03-19")
pre_cov_dur <- min(df$ICD_V2$AUFNDAT) %--% covid_start %>% as.numeric("years")
cov_dur <- covid_start %--% max(df$ICD_V2$AUFNDAT) %>% as.numeric("years")

eat_dis_cases <-
  df$ICD_V2 %>%
  mutate(covid_pan = AUFNDAT >= covid_start) %>%
  filter(str_starts(ICD_CODE, "F50"), ICD_TYPE == "Entl.") %>%

  group_by(Fall_Pseudonym) %>%
  arrange(ICD_DATUM) %>%
  slice_head(n = 1) %>%
  ungroup() %>%

  group_by(covid_pan) %>%
  summarise(n_cases = n_distinct(Fall_Pseudonym),
            n_patients = n_distinct(`PID Pseudonym`)) %>%
  mutate(years = c(pre_cov_dur, cov_dur)) %>%
  adorn_totals() %>%
  mutate(n_cases_year = n_cases/years,
         n_patients_year = n_patients/years)

write_excel_csv(eat_dis_cases, file.path(outdir, "Eating_disorder_cases.csv"))

# aufteilen: alle, .0+1, .2+3, Rest
