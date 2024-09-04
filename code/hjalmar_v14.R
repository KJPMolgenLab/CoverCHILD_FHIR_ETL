# Notizen: 

# Der Datensatz "20231215_result_gesamt_2016_2023" (Originalrohdaten von der Analyse von Stephie) umbenannt in "dresden_rohdaten".
# 1 Jahr = 365.25 Tage
# Die Variable "Patient.Alter.sort" im Datensatz "dresden_rohdaten" muesste mehr Dezimalstellen enthalten
# Wie viele Dezimalstellen sollten wir inkludieren allgemein?
# Die letzten Daten (von 2024) muessten auch inkludiert werden.
# Patienten im Alter >=0 und >18 Jahren sollten inkludiert werden.
# TODO: Grafiken
# TODO: Wenn keine Werten fuer Variablen wie z.B. CRP vorhanden sind fuer eine Gruppe soll in output "No values recorded" stehen und nicht 0. 
# TODO: 0 sind verschoben in output
# TODO: Die ICD-variablen ueberlegen bzgl. Format (sekundaercodes koennten ggf. 2 Mal drin sein)


# Notwendige Packages

library(dplyr)
library(tidyr)
library(ggplot2)

dresden_rohdaten <- df_result

# Fuer eine vollstaendige Darstellung der Ergebnisse im Output.

#options(max.print = 10000)


# Umwandlung in numerische Werte zur Erstellung von Spalten.

# not needed
#dresden_rohdaten$Patient.Alter.sort <- as.numeric(gsub(",", ".", dresden_rohdaten$Patient.Alter.sort))
#dresden_rohdaten$Labor.CRP.max <- as.numeric(gsub(",", ".", dresden_rohdaten$Labor.CRP.max))
#dresden_rohdaten$Labor.Leuko.max <- as.numeric(gsub(",", ".", dresden_rohdaten$Labor.Leuko.max))


# Erstellung der Variable "age_group" im Datensatz "dresden_rohdaten"
# term newborn infants:(0 to 27 days) (bis inkl. Vom Tag der Geburt bis inkl. 1 tag vor 28. LTag)
# infants and toddlers (28 days to 23 months) (bis inkl. 1 tag vor 2. Geb)  (kleiner 0.0766598220... Jahre (1/365.25*28))
# children (2 to 11 years) (bis inkl. 1 tag vor 12 Geb)
# adolescents (12 to 18 years)  (bis inkl. 1 tag vor 18 Geb)

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(age_group = case_when(
    Patient.Alter.sort >= 0 & Patient.Alter.sort < (1/365.25*28) ~ "term newborn infants",   
    Patient.Alter.sort >= (1/365.25*28) & Patient.Alter.sort < 2 ~ "infants and toddlers",
    Patient.Alter.sort >= 2 & Patient.Alter.sort < 12 ~ "children",
    Patient.Alter.sort >= 12 & Patient.Alter.sort < 18 ~ "adolescents",
    TRUE ~ NA_character_
  ))


# Erstellung der Variable "pandemic_period" im Datensatz "dresden_rohdaten".
# Praepandemische Periode: 02.2018-02.2020
# Intrapandemische Periode: 03.2020-03.2022
# Postpandemische Periode: 04.2022-04.2024

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(pandemic_period = case_when(
    ((Fall.Jahr == 2018 & Fall.Monat >= 2) | Fall.Jahr == 2019 | (Fall.Jahr == 2020 & Fall.Monat <= 2)) ~ "Prepand. Period",
    ((Fall.Jahr == 2020 & Fall.Monat >= 3) | Fall.Jahr == 2021 | (Fall.Jahr == 2022 & Fall.Monat <= 3)) ~ "Intrapand. Period",
    ((Fall.Jahr == 2022 & Fall.Monat >= 4) | Fall.Jahr == 2023 | (Fall.Jahr == 2024 & Fall.Monat <= 4)) ~ "Postpand. Period",
    TRUE ~ "Other"
  ))


# Erstellung der Variable "year_of_life" (Lebensjahr) im Datensatz "dresden_rohdaten".

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(year_of_life = case_when(
    Patient.Alter.sort >= 0 & Patient.Alter.sort < 1 ~ "1",
    Patient.Alter.sort >= 1 & Patient.Alter.sort < 2 ~ "2",
    Patient.Alter.sort >= 2 & Patient.Alter.sort < 3 ~ "3",
    Patient.Alter.sort >= 3 & Patient.Alter.sort < 4 ~ "4",
    Patient.Alter.sort >= 4 & Patient.Alter.sort < 5 ~ "5",
    Patient.Alter.sort >= 5 & Patient.Alter.sort < 6 ~ "6",
    Patient.Alter.sort >= 6 & Patient.Alter.sort < 7 ~ "7",
    Patient.Alter.sort >= 7 & Patient.Alter.sort < 8 ~ "8",
    Patient.Alter.sort >= 8 & Patient.Alter.sort < 9 ~ "9",
    Patient.Alter.sort >= 9 & Patient.Alter.sort < 10 ~ "10",
    Patient.Alter.sort >= 10 & Patient.Alter.sort < 11 ~ "11",
    Patient.Alter.sort >= 11 & Patient.Alter.sort < 12 ~ "12",
    Patient.Alter.sort >= 12 & Patient.Alter.sort < 13 ~ "13",
    Patient.Alter.sort >= 13 & Patient.Alter.sort < 14 ~ "14",
    Patient.Alter.sort >= 14 & Patient.Alter.sort < 15 ~ "15",
    Patient.Alter.sort >= 15 & Patient.Alter.sort < 16 ~ "16",
    Patient.Alter.sort >= 16 & Patient.Alter.sort < 17 ~ "17",
    Patient.Alter.sort >= 17 & Patient.Alter.sort < 18 ~ "18",
    TRUE ~ NA_character_
  ),
  .after = Patient.Alter.sort)


# Erstellung der variable "month_of_life_first24" (Lebensmonat) im Datensatz "dresden_rohdaten".
# 1 Monat = 1/12 Jahre.
# Umfasst nur die ersten 24 Lebensmonate.

dresden_rohdaten <- dresden_rohdaten %>%      
  mutate(month_of_life_first24 = case_when(
    Patient.Alter.sort >= 0 & Patient.Alter.sort < (1/12) ~ "1",
    Patient.Alter.sort >= (1/12) & Patient.Alter.sort < (2/12) ~ "2",
    Patient.Alter.sort >= (2/12) & Patient.Alter.sort < (3/12) ~ "3",
    Patient.Alter.sort >= (3/12) & Patient.Alter.sort < (4/12) ~ "4",
    Patient.Alter.sort >= (4/12) & Patient.Alter.sort < (5/12) ~ "5",
    Patient.Alter.sort >= (5/12) & Patient.Alter.sort < (6/12) ~ "6",
    Patient.Alter.sort >= (6/12) & Patient.Alter.sort < (7/12) ~ "7",
    Patient.Alter.sort >= (7/12) & Patient.Alter.sort < (8/12) ~ "8",
    Patient.Alter.sort >= (8/12) & Patient.Alter.sort < (9/12) ~ "9",
    Patient.Alter.sort >= (9/12) & Patient.Alter.sort < (10/12) ~ "10",
    Patient.Alter.sort >= (10/12) & Patient.Alter.sort < (11/12) ~ "11",
    Patient.Alter.sort >= (11/12) & Patient.Alter.sort < (12/12) ~ "12",
    Patient.Alter.sort >= (12/12) & Patient.Alter.sort < (13/12) ~ "13",
    Patient.Alter.sort >= (13/12) & Patient.Alter.sort < (14/12) ~ "14",
    Patient.Alter.sort >= (14/12) & Patient.Alter.sort < (15/12) ~ "15",
    Patient.Alter.sort >= (15/12) & Patient.Alter.sort < (16/12) ~ "16",
    Patient.Alter.sort >= (16/12) & Patient.Alter.sort < (17/12) ~ "17",
    Patient.Alter.sort >= (17/12) & Patient.Alter.sort < (18/12) ~ "18",
    Patient.Alter.sort >= (18/12) & Patient.Alter.sort < (19/12) ~ "19",
    Patient.Alter.sort >= (19/12) & Patient.Alter.sort < (20/12) ~ "20",
    Patient.Alter.sort >= (20/12) & Patient.Alter.sort < (21/12) ~ "21",
    Patient.Alter.sort >= (21/12) & Patient.Alter.sort < (22/12) ~ "22",
    Patient.Alter.sort >= (22/12) & Patient.Alter.sort < (23/12) ~ "23",
    Patient.Alter.sort >= (23/12) & Patient.Alter.sort < (24/12) ~ "24",
    TRUE ~ NA_character_
  ))


# Erstellung der variable "quarter" im Datensatz "dresden_rohdaten".

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(quarter = case_when(
    ((Fall.Jahr == 2016 & Fall.Monat == 1) | (Fall.Jahr == 2016 & Fall.Monat == 2) | (Fall.Jahr == 2016 & Fall.Monat == 3)) ~ "Q1 2016",
    ((Fall.Jahr == 2016 & Fall.Monat == 4) | (Fall.Jahr == 2016 & Fall.Monat == 5) | (Fall.Jahr == 2016 & Fall.Monat == 6)) ~ "Q2 2016",
    ((Fall.Jahr == 2016 & Fall.Monat == 7) | (Fall.Jahr == 2016 & Fall.Monat == 8) | (Fall.Jahr == 2016 & Fall.Monat == 9)) ~ "Q3 2016",
    ((Fall.Jahr == 2016 & Fall.Monat == 10) | (Fall.Jahr == 2016 & Fall.Monat == 11) | (Fall.Jahr == 2016 & Fall.Monat == 12)) ~ "Q4 2016",
    ((Fall.Jahr == 2017 & Fall.Monat == 1) | (Fall.Jahr == 2017 & Fall.Monat == 2) | (Fall.Jahr == 2017 & Fall.Monat == 3)) ~ "Q1 2017",
    ((Fall.Jahr == 2017 & Fall.Monat == 4) | (Fall.Jahr == 2017 & Fall.Monat == 5) | (Fall.Jahr == 2017 & Fall.Monat == 6)) ~ "Q2 2017",
    ((Fall.Jahr == 2017 & Fall.Monat == 7) | (Fall.Jahr == 2017 & Fall.Monat == 8) | (Fall.Jahr == 2017 & Fall.Monat == 9)) ~ "Q3 2017",
    ((Fall.Jahr == 2017 & Fall.Monat == 10) | (Fall.Jahr == 2017 & Fall.Monat == 11) | (Fall.Jahr == 2017 & Fall.Monat == 12)) ~ "Q4 2017",
    ((Fall.Jahr == 2018 & Fall.Monat == 1) | (Fall.Jahr == 2018 & Fall.Monat == 2) | (Fall.Jahr == 2018 & Fall.Monat == 3)) ~ "Q1 2018",
    ((Fall.Jahr == 2018 & Fall.Monat == 4) | (Fall.Jahr == 2018 & Fall.Monat == 5) | (Fall.Jahr == 2018 & Fall.Monat == 6)) ~ "Q2 2018",
    ((Fall.Jahr == 2018 & Fall.Monat == 7) | (Fall.Jahr == 2018 & Fall.Monat == 8) | (Fall.Jahr == 2018 & Fall.Monat == 9)) ~ "Q3 2018",
    ((Fall.Jahr == 2018 & Fall.Monat == 10) | (Fall.Jahr == 2018 & Fall.Monat == 11) | (Fall.Jahr == 2018 & Fall.Monat == 12)) ~ "Q4 2018",
    ((Fall.Jahr == 2019 & Fall.Monat == 1) | (Fall.Jahr == 2019 & Fall.Monat == 2) | (Fall.Jahr == 2019 & Fall.Monat == 3)) ~ "Q1 2019",
    ((Fall.Jahr == 2019 & Fall.Monat == 4) | (Fall.Jahr == 2019 & Fall.Monat == 5) | (Fall.Jahr == 2019 & Fall.Monat == 6)) ~ "Q2 2019",
    ((Fall.Jahr == 2019 & Fall.Monat == 7) | (Fall.Jahr == 2019 & Fall.Monat == 8) | (Fall.Jahr == 2019 & Fall.Monat == 9)) ~ "Q3 2019",
    ((Fall.Jahr == 2019 & Fall.Monat == 10) | (Fall.Jahr == 2019 & Fall.Monat == 11) | (Fall.Jahr == 2019 & Fall.Monat == 12)) ~ "Q4 2019",
    ((Fall.Jahr == 2020 & Fall.Monat == 1) | (Fall.Jahr == 2020 & Fall.Monat == 2) | (Fall.Jahr == 2020 & Fall.Monat == 3)) ~ "Q1 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 4) | (Fall.Jahr == 2020 & Fall.Monat == 5) | (Fall.Jahr == 2020 & Fall.Monat == 6)) ~ "Q2 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 7) | (Fall.Jahr == 2020 & Fall.Monat == 8) | (Fall.Jahr == 2020 & Fall.Monat == 9)) ~ "Q3 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 10) | (Fall.Jahr == 2020 & Fall.Monat == 11) | (Fall.Jahr == 2020 & Fall.Monat == 12)) ~ "Q4 2020",
    ((Fall.Jahr == 2021 & Fall.Monat == 1) | (Fall.Jahr == 2021 & Fall.Monat == 2) | (Fall.Jahr == 2021 & Fall.Monat == 3)) ~ "Q1 2021",
    ((Fall.Jahr == 2021 & Fall.Monat == 4) | (Fall.Jahr == 2021 & Fall.Monat == 5) | (Fall.Jahr == 2021 & Fall.Monat == 6)) ~ "Q2 2021",
    ((Fall.Jahr == 2021 & Fall.Monat == 7) | (Fall.Jahr == 2021 & Fall.Monat == 8) | (Fall.Jahr == 2021 & Fall.Monat == 9)) ~ "Q3 2021",
    ((Fall.Jahr == 2021 & Fall.Monat == 10) | (Fall.Jahr == 2021 & Fall.Monat == 11) | (Fall.Jahr == 2021 & Fall.Monat == 12)) ~ "Q4 2021",
    ((Fall.Jahr == 2022 & Fall.Monat == 1) | (Fall.Jahr == 2022 & Fall.Monat == 2) | (Fall.Jahr == 2022 & Fall.Monat == 3)) ~ "Q1 2022",
    ((Fall.Jahr == 2022 & Fall.Monat == 4) | (Fall.Jahr == 2022 & Fall.Monat == 5) | (Fall.Jahr == 2022 & Fall.Monat == 6)) ~ "Q2 2022",
    ((Fall.Jahr == 2022 & Fall.Monat == 7) | (Fall.Jahr == 2022 & Fall.Monat == 8) | (Fall.Jahr == 2022 & Fall.Monat == 9)) ~ "Q3 2022",
    ((Fall.Jahr == 2022 & Fall.Monat == 10) | (Fall.Jahr == 2022 & Fall.Monat == 11) | (Fall.Jahr == 2022 & Fall.Monat == 12)) ~ "Q4 2022",
    ((Fall.Jahr == 2023 & Fall.Monat == 1) | (Fall.Jahr == 2023 & Fall.Monat == 2) | (Fall.Jahr == 2023 & Fall.Monat == 3)) ~ "Q1 2023",
    ((Fall.Jahr == 2023 & Fall.Monat == 4) | (Fall.Jahr == 2023 & Fall.Monat == 5) | (Fall.Jahr == 2023 & Fall.Monat == 6)) ~ "Q2 2023",
    ((Fall.Jahr == 2023 & Fall.Monat == 7) | (Fall.Jahr == 2023 & Fall.Monat == 8) | (Fall.Jahr == 2023 & Fall.Monat == 9)) ~ "Q3 2023",
    ((Fall.Jahr == 2023 & Fall.Monat == 10) | (Fall.Jahr == 2023 & Fall.Monat == 11) | (Fall.Jahr == 2023 & Fall.Monat == 12)) ~ "Q4 2023",
    ((Fall.Jahr == 2024 & Fall.Monat == 1) | (Fall.Jahr == 2024 & Fall.Monat == 2) | (Fall.Jahr == 2024 & Fall.Monat == 3)) ~ "Q1 2024",
    ((Fall.Jahr == 2024 & Fall.Monat == 4) | (Fall.Jahr == 2024 & Fall.Monat == 5) | (Fall.Jahr == 2024 & Fall.Monat == 6)) ~ "Q2 2024",
    ((Fall.Jahr == 2024 & Fall.Monat == 7) | (Fall.Jahr == 2024 & Fall.Monat == 8) | (Fall.Jahr == 2024 & Fall.Monat == 9)) ~ "Q3 2024",
    ((Fall.Jahr == 2024 & Fall.Monat == 10) | (Fall.Jahr == 2024 & Fall.Monat == 11) | (Fall.Jahr == 2024 & Fall.Monat == 12)) ~ "Q4 2024",
    TRUE ~ "Other"
  ),
  .after = Fall.Jahr.Viertel)


# Erstellung der variable "season" im Datensatz "dresden_rohdaten".
# Metrologische Jahreszeiten nach dem Deutschen Wetterdienst DWD.
# Fruehling: Maerz-Mai. Sommer: Juni-August. Herbst: September-November. Winter: Dezember-Februar. 

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(season = case_when(
    ((Fall.Jahr == 2016 & Fall.Monat == 3) | (Fall.Jahr == 2016 & Fall.Monat == 4) | (Fall.Jahr == 2016 & Fall.Monat == 5)) ~ "Spring 2016",
    ((Fall.Jahr == 2016 & Fall.Monat == 6) | (Fall.Jahr == 2016 & Fall.Monat == 7) | (Fall.Jahr == 2016 & Fall.Monat == 8)) ~ "Summer 2016",
    ((Fall.Jahr == 2016 & Fall.Monat == 9) | (Fall.Jahr == 2016 & Fall.Monat == 10) | (Fall.Jahr == 2016 & Fall.Monat == 11)) ~ "Fall 2016",
    ((Fall.Jahr == 2016 & Fall.Monat == 12) | (Fall.Jahr == 2017 & Fall.Monat == 1) | (Fall.Jahr == 2017 & Fall.Monat == 2)) ~ "Winter 2016/2017",
    ((Fall.Jahr == 2017 & Fall.Monat == 3) | (Fall.Jahr == 2017 & Fall.Monat == 4) | (Fall.Jahr == 2017 & Fall.Monat == 5)) ~ "Spring 2017",
    ((Fall.Jahr == 2017 & Fall.Monat == 6) | (Fall.Jahr == 2017 & Fall.Monat == 7) | (Fall.Jahr == 2017 & Fall.Monat == 8)) ~ "Summer 2017",
    ((Fall.Jahr == 2017 & Fall.Monat == 9) | (Fall.Jahr == 2017 & Fall.Monat == 10) | (Fall.Jahr == 2017 & Fall.Monat == 11)) ~ "Fall 2017",
    ((Fall.Jahr == 2017 & Fall.Monat == 12) | (Fall.Jahr == 2018 & Fall.Monat == 1) | (Fall.Jahr == 2018 & Fall.Monat == 2)) ~ "Winter 2017/2018",
    ((Fall.Jahr == 2018 & Fall.Monat == 3) | (Fall.Jahr == 2018 & Fall.Monat == 4) | (Fall.Jahr == 2018 & Fall.Monat == 5)) ~ "Spring 2018",
    ((Fall.Jahr == 2018 & Fall.Monat == 6) | (Fall.Jahr == 2018 & Fall.Monat == 7) | (Fall.Jahr == 2018 & Fall.Monat == 8)) ~ "Summer 2018",
    ((Fall.Jahr == 2018 & Fall.Monat == 9) | (Fall.Jahr == 2018 & Fall.Monat == 10) | (Fall.Jahr == 2018 & Fall.Monat == 11)) ~ "Fall 2018",
    ((Fall.Jahr == 2018 & Fall.Monat == 12) | (Fall.Jahr == 2019 & Fall.Monat == 1) | (Fall.Jahr == 2019 & Fall.Monat == 2)) ~ "Winter 2018/2019",
    ((Fall.Jahr == 2019 & Fall.Monat == 3) | (Fall.Jahr == 2019 & Fall.Monat == 4) | (Fall.Jahr == 2019 & Fall.Monat == 5)) ~ "Spring 2019",
    ((Fall.Jahr == 2019 & Fall.Monat == 6) | (Fall.Jahr == 2019 & Fall.Monat == 7) | (Fall.Jahr == 2019 & Fall.Monat == 8)) ~ "Summer 2019",
    ((Fall.Jahr == 2019 & Fall.Monat == 9) | (Fall.Jahr == 2019 & Fall.Monat == 10) | (Fall.Jahr == 2019 & Fall.Monat == 11)) ~ "Fall 2019",
    ((Fall.Jahr == 2019 & Fall.Monat == 12) | (Fall.Jahr == 2020 & Fall.Monat == 1) | (Fall.Jahr == 2020 & Fall.Monat == 2)) ~ "Winter 2019/2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 3) | (Fall.Jahr == 2020 & Fall.Monat == 4) | (Fall.Jahr == 2020 & Fall.Monat == 5)) ~ "Spring 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 6) | (Fall.Jahr == 2020 & Fall.Monat == 7) | (Fall.Jahr == 2020 & Fall.Monat == 8)) ~ "Summer 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 9) | (Fall.Jahr == 2020 & Fall.Monat == 10) | (Fall.Jahr == 2020 & Fall.Monat == 11)) ~ "Fall 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 12) | (Fall.Jahr == 2021 & Fall.Monat == 1) | (Fall.Jahr == 2021 & Fall.Monat == 2)) ~ "Winter 2020/2021",
    ((Fall.Jahr == 2020 & Fall.Monat == 3) | (Fall.Jahr == 2020 & Fall.Monat == 4) | (Fall.Jahr == 2020 & Fall.Monat == 5)) ~ "Spring 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 6) | (Fall.Jahr == 2020 & Fall.Monat == 7) | (Fall.Jahr == 2020 & Fall.Monat == 8)) ~ "Summer 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 9) | (Fall.Jahr == 2020 & Fall.Monat == 10) | (Fall.Jahr == 2020 & Fall.Monat == 11)) ~ "Fall 2020",
    ((Fall.Jahr == 2020 & Fall.Monat == 12) | (Fall.Jahr == 2021 & Fall.Monat == 1) | (Fall.Jahr == 2021 & Fall.Monat == 2)) ~ "Winter 2020/2021",
    ((Fall.Jahr == 2021 & Fall.Monat == 3) | (Fall.Jahr == 2021 & Fall.Monat == 4) | (Fall.Jahr == 2021 & Fall.Monat == 5)) ~ "Spring 2021",
    ((Fall.Jahr == 2021 & Fall.Monat == 6) | (Fall.Jahr == 2021 & Fall.Monat == 7) | (Fall.Jahr == 2021 & Fall.Monat == 8)) ~ "Summer 2021",
    ((Fall.Jahr == 2021 & Fall.Monat == 9) | (Fall.Jahr == 2021 & Fall.Monat == 10) | (Fall.Jahr == 2021 & Fall.Monat == 11)) ~ "Fall 2021",
    ((Fall.Jahr == 2021 & Fall.Monat == 12) | (Fall.Jahr == 2022 & Fall.Monat == 1) | (Fall.Jahr == 2022 & Fall.Monat == 2)) ~ "Winter 2021/2022",
    ((Fall.Jahr == 2022 & Fall.Monat == 3) | (Fall.Jahr == 2022 & Fall.Monat == 4) | (Fall.Jahr == 2022 & Fall.Monat == 5)) ~ "Spring 2022",
    ((Fall.Jahr == 2022 & Fall.Monat == 6) | (Fall.Jahr == 2022 & Fall.Monat == 7) | (Fall.Jahr == 2022 & Fall.Monat == 8)) ~ "Summer 2022",
    ((Fall.Jahr == 2022 & Fall.Monat == 9) | (Fall.Jahr == 2022 & Fall.Monat == 10) | (Fall.Jahr == 2022 & Fall.Monat == 11)) ~ "Fall 2022",
    ((Fall.Jahr == 2022 & Fall.Monat == 12) | (Fall.Jahr == 2023 & Fall.Monat == 1) | (Fall.Jahr == 2023 & Fall.Monat == 2)) ~ "Winter 2022/2023",
    ((Fall.Jahr == 2023 & Fall.Monat == 3) | (Fall.Jahr == 2023 & Fall.Monat == 4) | (Fall.Jahr == 2023 & Fall.Monat == 5)) ~ "Spring 2023",
    ((Fall.Jahr == 2023 & Fall.Monat == 6) | (Fall.Jahr == 2023 & Fall.Monat == 7) | (Fall.Jahr == 2023 & Fall.Monat == 8)) ~ "Summer 2023",
    ((Fall.Jahr == 2023 & Fall.Monat == 9) | (Fall.Jahr == 2023 & Fall.Monat == 10) | (Fall.Jahr == 2023 & Fall.Monat == 11)) ~ "Fall 2023",
    ((Fall.Jahr == 2023 & Fall.Monat == 12) | (Fall.Jahr == 2024 & Fall.Monat == 1) | (Fall.Jahr == 2024 & Fall.Monat == 2)) ~ "Winter 2023/2024",
    ((Fall.Jahr == 2024 & Fall.Monat == 3) | (Fall.Jahr == 2024 & Fall.Monat == 4) | (Fall.Jahr == 2024 & Fall.Monat == 5)) ~ "Spring 2024",
    ((Fall.Jahr == 2024 & Fall.Monat == 6) | (Fall.Jahr == 2024 & Fall.Monat == 7) | (Fall.Jahr == 2023 & Fall.Monat == 4)) ~ "Summer 2024",
    ((Fall.Jahr == 2024 & Fall.Monat == 9) | (Fall.Jahr == 2024 & Fall.Monat == 10) | (Fall.Jahr == 2024 & Fall.Monat == 11)) ~ "Fall 2024",
    ((Fall.Jahr == 2024 & Fall.Monat == 12) | (Fall.Jahr == 2025 & Fall.Monat == 1) | (Fall.Jahr == 2025 & Fall.Monat == 2)) ~ "Winter 2024/2025",
    TRUE ~ "Other"
  ))


# Erstellung der Variable "icd_combo_treatment" im Datensatz "dresden_rohdaten".
# Umfasst die verschiedenen vordefinierten Kombinationen von ICD-Codes in Dokument ICD10_RSV_20231009.
# Handelt sich um die Behandlungsdiagnose.

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(icd_combo_treatment = case_when(
    ICD.Primaercode.BE == "J00" & ICD.Sekundaercode.BE == "B97.4!" ~ "J00 B97.4!",
    ICD.Primaercode.BE == "J01.8" & ICD.Sekundaercode.BE == "B97.4!" ~ "J01.8 B97.4!",
    ICD.Primaercode.BE == "J01.9" & ICD.Sekundaercode.BE == "B97.4!" ~ "J01.9 B97.4!",
    ICD.Primaercode.BE == "J02.8" & ICD.Sekundaercode.BE == "B97.4!" ~ "J02.8 B97.4!",
    ICD.Primaercode.BE == "J02.9" & ICD.Sekundaercode.BE == "B97.4!" ~ "J02.9 B97.4!",
    ICD.Primaercode.BE == "J03.8" & ICD.Sekundaercode.BE == "B97.4!" ~ "J03.8 B97.4!",
    ICD.Primaercode.BE == "J03.9" & ICD.Sekundaercode.BE == "B97.4!" ~ "J03.9 B97.4!",
    ICD.Primaercode.BE == "J04.0" & ICD.Sekundaercode.BE == "B97.4!" ~ "J04.0 B97.4!",
    ICD.Primaercode.BE == "J04.1" & ICD.Sekundaercode.BE == "B97.4!" ~ "J04.1 B97.4!",
    ICD.Primaercode.BE == "J04.2" & ICD.Sekundaercode.BE == "B97.4!" ~ "J04.2 B97.4!",
    ICD.Primaercode.BE == "J05.0" & ICD.Sekundaercode.BE == "B97.4!" ~ "J05.0 B97.4!",
    ICD.Primaercode.BE == "J05.1" & ICD.Sekundaercode.BE == "B97.4!" ~ "J05.1 B97.4!",
    ICD.Primaercode.BE == "J06.0" & ICD.Sekundaercode.BE == "B97.4!" ~ "J06.0 B97.4!",
    ICD.Primaercode.BE == "J06.8" & ICD.Sekundaercode.BE == "B97.4!" ~ "J06.8 B97.4!",
    ICD.Primaercode.BE == "J06.9" & ICD.Sekundaercode.BE == "B97.4!" ~ "J06.9 B97.4!",
    TRUE ~ "Other"
  ))


# Erstellung der Variable "icd_combo_discharge" im Datensatz "dresden_rohdaten".
# Umfasst die verschiedenen vordefinierten Kombinationen von ICD-Codes in Dokument ICD10_RSV_20231009.
# Handelt sich um Entlassdiagnose.

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(icd_combo_discharge = case_when(
    ICD.Primaercode.EN == "J00" & ICD.Sekundaercode.EN == "B97.4!" ~ "J00 B97.4!",
    ICD.Primaercode.EN == "J01.8" & ICD.Sekundaercode.EN == "B97.4!" ~ "J01.8 B97.4!",
    ICD.Primaercode.EN == "J01.9" & ICD.Sekundaercode.EN == "B97.4!" ~ "J01.9 B97.4!",
    ICD.Primaercode.EN == "J02.8" & ICD.Sekundaercode.EN == "B97.4!" ~ "J02.8 B97.4!",
    ICD.Primaercode.EN == "J02.9" & ICD.Sekundaercode.EN == "B97.4!" ~ "J02.9 B97.4!",
    ICD.Primaercode.EN == "J03.8" & ICD.Sekundaercode.EN == "B97.4!" ~ "J03.8 B97.4!",
    ICD.Primaercode.EN == "J03.9" & ICD.Sekundaercode.EN == "B97.4!" ~ "J03.9 B97.4!",
    ICD.Primaercode.EN == "J04.0" & ICD.Sekundaercode.EN == "B97.4!" ~ "J04.0 B97.4!",
    ICD.Primaercode.EN == "J04.1" & ICD.Sekundaercode.EN == "B97.4!" ~ "J04.1 B97.4!",
    ICD.Primaercode.EN == "J04.2" & ICD.Sekundaercode.EN == "B97.4!" ~ "J04.2 B97.4!",
    ICD.Primaercode.EN == "J05.0" & ICD.Sekundaercode.EN == "B97.4!" ~ "J05.0 B97.4!",
    ICD.Primaercode.EN == "J05.1" & ICD.Sekundaercode.EN == "B97.4!" ~ "J05.1 B97.4!",
    ICD.Primaercode.EN == "J06.0" & ICD.Sekundaercode.EN == "B97.4!" ~ "J06.0 B97.4!",
    ICD.Primaercode.EN == "J06.8" & ICD.Sekundaercode.EN == "B97.4!" ~ "J06.8 B97.4!",
    ICD.Primaercode.EN == "J06.9" & ICD.Sekundaercode.EN == "B97.4!" ~ "J06.9 B97.4!",
    TRUE ~ "Other"
  ))


# Aggregiert Datensatz nach den Variablen "age_group" und "pandemic_period".

dresden_aggregated_data <- dresden_rohdaten %>%
  group_by(age_group, pandemic_period) %>%
  summarise(
    `term newborn infants` = sum(age_group == "term newborn infants", na.rm = TRUE),
    `infants and toddlers` = sum(age_group == "infants and toddlers", na.rm = TRUE),
    `children` = sum(age_group == "children", na.rm = TRUE),
    `adolescents` = sum(age_group == "adolescents", na.rm = TRUE),
    `prepand. period` = sum(pandemic_period == "Prepand. Period", na.rm = TRUE),
    `intrapand. period` = sum(pandemic_period == "Intrapand. Period", na.rm = TRUE),
    `postpand. period` = sum(pandemic_period == "Postpand. Period", na.rm = TRUE),
    `1. year of life` = sum(year_of_life == "1", na.rm = TRUE),
    `2. year of life` = sum(year_of_life == "2", na.rm = TRUE),
    `3. year of life` = sum(year_of_life == "3", na.rm = TRUE),
    `4. year of life` = sum(year_of_life == "4", na.rm = TRUE),
    `5. year of life` = sum(year_of_life == "5", na.rm = TRUE),
    `6. year of life` = sum(year_of_life == "6", na.rm = TRUE),
    `7. year of life` = sum(year_of_life == "7", na.rm = TRUE),
    `8. year of life` = sum(year_of_life == "8", na.rm = TRUE),
    `9. year of life` = sum(year_of_life == "9", na.rm = TRUE),
    `10. year of life` = sum(year_of_life == "10", na.rm = TRUE),
    `11. year of life` = sum(year_of_life == "11", na.rm = TRUE),
    `12. year of life` = sum(year_of_life == "12", na.rm = TRUE),
    `13. year of life` = sum(year_of_life == "13", na.rm = TRUE),
    `14. year of life` = sum(year_of_life == "14", na.rm = TRUE),
    `15. year of life` = sum(year_of_life == "15", na.rm = TRUE),
    `16. year of life` = sum(year_of_life == "16", na.rm = TRUE),
    `17. year of life` = sum(year_of_life == "17", na.rm = TRUE),
    `18. year of life` = sum(year_of_life == "18", na.rm = TRUE),
    `1. month of life` = sum(month_of_life_first24 == "1", na.rm = TRUE),
    `2. month of life` = sum(month_of_life_first24 == "2", na.rm = TRUE),
    `3. month of life` = sum(month_of_life_first24 == "3", na.rm = TRUE),
    `4. month of life` = sum(month_of_life_first24 == "4", na.rm = TRUE),
    `5. month of life` = sum(month_of_life_first24 == "5", na.rm = TRUE),
    `6. month of life` = sum(month_of_life_first24 == "6", na.rm = TRUE),
    `7. month of life` = sum(month_of_life_first24 == "7", na.rm = TRUE),
    `8. month of life` = sum(month_of_life_first24 == "8", na.rm = TRUE),
    `9. month of life` = sum(month_of_life_first24 == "9", na.rm = TRUE),
    `10. month of life` = sum(month_of_life_first24 == "10", na.rm = TRUE),
    `11. month of life` = sum(month_of_life_first24 == "11", na.rm = TRUE),
    `12. month of life` = sum(month_of_life_first24 == "12", na.rm = TRUE),
    `13. month of life` = sum(month_of_life_first24 == "13", na.rm = TRUE),
    `14. month of life` = sum(month_of_life_first24 == "14", na.rm = TRUE),
    `15. month of life` = sum(month_of_life_first24 == "15", na.rm = TRUE),
    `16. month of life` = sum(month_of_life_first24 == "16", na.rm = TRUE),
    `17. month of life` = sum(month_of_life_first24 == "17", na.rm = TRUE),
    `18. month of life` = sum(month_of_life_first24 == "18", na.rm = TRUE),
    `19. month of life` = sum(month_of_life_first24 == "19", na.rm = TRUE),
    `20. month of life` = sum(month_of_life_first24 == "20", na.rm = TRUE),
    `21. month of life` = sum(month_of_life_first24 == "21", na.rm = TRUE),
    `22. month of life` = sum(month_of_life_first24 == "22", na.rm = TRUE),
    `23. month of life` = sum(month_of_life_first24 == "23", na.rm = TRUE),
    `24. month of life` = sum(month_of_life_first24 == "24", na.rm = TRUE),
    `2016` = sum(Fall.Jahr == 2016, na.rm = TRUE),
    `2017` = sum(Fall.Jahr == 2017, na.rm = TRUE),
    `2018` = sum(Fall.Jahr == 2018, na.rm = TRUE),
    `2019` = sum(Fall.Jahr == 2019, na.rm = TRUE),
    `2020` = sum(Fall.Jahr == 2020, na.rm = TRUE),
    `2021` = sum(Fall.Jahr == 2021, na.rm = TRUE),
    `2022` = sum(Fall.Jahr == 2022, na.rm = TRUE),
    `2023` = sum(Fall.Jahr == 2023, na.rm = TRUE),
    `2024` = sum(Fall.Jahr == 2024, na.rm = TRUE),
    `Q1 2016` = sum(quarter == "Q1 2016", na.rm = TRUE),
    `Q2 2016` = sum(quarter == "Q2 2016", na.rm = TRUE),
    `Q3 2016` = sum(quarter == "Q3 2016", na.rm = TRUE),
    `Q4 2016` = sum(quarter == "Q4 2016", na.rm = TRUE),
    `Q1 2017` = sum(quarter == "Q1 2017", na.rm = TRUE),
    `Q2 2017` = sum(quarter == "Q2 2017", na.rm = TRUE),
    `Q3 2017` = sum(quarter == "Q3 2017", na.rm = TRUE),
    `Q4 2017` = sum(quarter == "Q4 2017", na.rm = TRUE),
    `Q1 2018` = sum(quarter == "Q1 2018", na.rm = TRUE),
    `Q2 2018` = sum(quarter == "Q2 2018", na.rm = TRUE),
    `Q3 2018` = sum(quarter == "Q3 2018", na.rm = TRUE),
    `Q4 2018` = sum(quarter == "Q4 2018", na.rm = TRUE),
    `Q1 2019` = sum(quarter == "Q1 2019", na.rm = TRUE),
    `Q2 2019` = sum(quarter == "Q2 2019", na.rm = TRUE),
    `Q3 2019` = sum(quarter == "Q3 2019", na.rm = TRUE),
    `Q4 2019` = sum(quarter == "Q4 2019", na.rm = TRUE),
    `Q1 2020` = sum(quarter == "Q1 2020", na.rm = TRUE),
    `Q2 2020` = sum(quarter == "Q2 2020", na.rm = TRUE),
    `Q3 2020` = sum(quarter == "Q3 2020", na.rm = TRUE),
    `Q4 2020` = sum(quarter == "Q4 2020", na.rm = TRUE),
    `Q1 2021` = sum(quarter == "Q1 2021", na.rm = TRUE),
    `Q2 2021` = sum(quarter == "Q2 2021", na.rm = TRUE),
    `Q3 2021` = sum(quarter == "Q3 2021", na.rm = TRUE),
    `Q4 2021` = sum(quarter == "Q4 2021", na.rm = TRUE),
    `Q1 2022` = sum(quarter == "Q1 2022", na.rm = TRUE),
    `Q2 2022` = sum(quarter == "Q2 2022", na.rm = TRUE),
    `Q3 2022` = sum(quarter == "Q3 2022", na.rm = TRUE),
    `Q4 2022` = sum(quarter == "Q4 2022", na.rm = TRUE),
    `Q1 2023` = sum(quarter == "Q1 2023", na.rm = TRUE),
    `Q2 2023` = sum(quarter == "Q2 2023", na.rm = TRUE),
    `Q3 2023` = sum(quarter == "Q3 2023", na.rm = TRUE),
    `Q4 2023` = sum(quarter == "Q4 2023", na.rm = TRUE),
    `Q1 2024` = sum(quarter == "Q1 2024", na.rm = TRUE),
    `Q2 2024` = sum(quarter == "Q2 2024", na.rm = TRUE),
    `Q3 2024` = sum(quarter == "Q3 2024", na.rm = TRUE),
    `Q4 2024` = sum(quarter == "Q4 2024", na.rm = TRUE),
    `Spring 2016` = sum(season == "Spring 2016", na.rm = TRUE),
    `Summer 2016` = sum(season == "Summer 2016", na.rm = TRUE),
    `Fall 2016` = sum(season == "Fall 2016", na.rm = TRUE),
    `Winter 2016/2017` = sum(season == "Winter 2016/2017", na.rm = TRUE),
    `Spring 2017` = sum(season == "Spring 2017", na.rm = TRUE),
    `Summer 2017` = sum(season == "Summer 2017", na.rm = TRUE),
    `Fall 2017` = sum(season == "Fall 2017", na.rm = TRUE),
    `Winter 2017/2018` = sum(season == "Winter 2017/2018", na.rm = TRUE),
    `Spring 2018` = sum(season == "Spring 2018", na.rm = TRUE),
    `Summer 2018` = sum(season == "Summer 2018", na.rm = TRUE),
    `Fall 2018` = sum(season == "Fall 2018", na.rm = TRUE),
    `Winter 2018/2019` = sum(season == "Winter 2018/2019", na.rm = TRUE),
    `Spring 2019` = sum(season == "Spring 2019", na.rm = TRUE),
    `Summer 2019` = sum(season == "Summer 2019", na.rm = TRUE),
    `Fall 2019` = sum(season == "Fall 2019", na.rm = TRUE),
    `Winter 2019/2020` = sum(season == "Winter 2019/2020", na.rm = TRUE),
    `Spring 2020` = sum(season == "Spring 2020", na.rm = TRUE),
    `Summer 2020` = sum(season == "Summer 2020", na.rm = TRUE),
    `Fall 2020` = sum(season == "Fall 2020", na.rm = TRUE),
    `Winter 2020/2021` = sum(season == "Winter 2020/2021", na.rm = TRUE),
    `Spring 2021` = sum(season == "Spring 2021", na.rm = TRUE),
    `Summer 2021` = sum(season == "Summer 2021", na.rm = TRUE),
    `Fall 2021` = sum(season == "Fall 2021", na.rm = TRUE),
    `Winter 2021/2022` = sum(season == "Winter 2021/2022", na.rm = TRUE),
    `Spring 2022` = sum(season == "Spring 2022", na.rm = TRUE),
    `Summer 2022` = sum(season == "Summer 2022", na.rm = TRUE),
    `Fall 2022` = sum(season == "Fall 2022", na.rm = TRUE),
    `Winter 2022/2023` = sum(season == "Winter 2022/2023", na.rm = TRUE),
    `Spring 2023` = sum(season == "Spring 2023", na.rm = TRUE),
    `Summer 2023` = sum(season == "Summer 2023", na.rm = TRUE),
    `Fall 2023` = sum(season == "Fall 2023", na.rm = TRUE),
    `Winter 2023/2024` = sum(season == "Winter 2023/2024", na.rm = TRUE),
    `Spring 2024` = sum(season == "Spring 2024", na.rm = TRUE),
    `Summer 2024` = sum(season == "Summer 2024", na.rm = TRUE),
    `Fall 2024` = sum(season == "Fall 2024", na.rm = TRUE),
    `Winter 2024/2025` = sum(season == "Winter 2024/2025", na.rm = TRUE),
    `J20.5 treatment diagnosis` = sum(ICD.Primaercode.BE == "J20.5", na.rm = TRUE),
    `J21.0 treatment diagnosis` = sum(ICD.Primaercode.BE == "J21.0", na.rm = TRUE),
    `J12.1 treatment diagnosis` = sum(ICD.Primaercode.BE == "J12.1", na.rm = TRUE),
    `P23.0 treatment diagnosis` = sum(ICD.Primaercode.BE == "P23.0", na.rm = TRUE),
    `B97.4! treatment diagnosis` = sum(ICD.Sekundaercode.BE == "B97.4!", na.rm = TRUE),
    `J20.5 discharge diagnosis` = sum(ICD.Primaercode.EN == "J20.5", na.rm = TRUE),
    `J21.0 discharge diagnosis` = sum(ICD.Primaercode.EN == "J21.0", na.rm = TRUE),
    `J12.1 discharge diagnosis` = sum(ICD.Primaercode.EN == "J12.1", na.rm = TRUE),
    `P23.0 discharge diagnosis` = sum(ICD.Primaercode.EN == "P23.0", na.rm = TRUE),
    `B97.4! discharge diagnosis` = sum(ICD.Sekundaercode.EN == "B97.4!", na.rm = TRUE),
    `J00 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J00 B97.4!", na.rm = TRUE),
    `J01.8 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J01.8 B97.4!", na.rm = TRUE),
    `J01.9 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J01.9 B97.4!", na.rm = TRUE),
    `J02.8 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J02.8 B97.4!", na.rm = TRUE),
    `J02.9 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J02.9 B97.4!", na.rm = TRUE),
    `J03.8 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J03.8 B97.4!", na.rm = TRUE),
    `J03.9 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J03.9 B97.4!", na.rm = TRUE),
    `J04.0 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J04.0 B97.4!", na.rm = TRUE),
    `J04.1 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J04.1 B97.4!", na.rm = TRUE),
    `J04.2 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J04.2 B97.4!", na.rm = TRUE),
    `J05.0 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J05.0 B97.4!", na.rm = TRUE),
    `J05.1 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J05.1 B97.4!", na.rm = TRUE),
    `J06.0 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J06.0 B97.4!", na.rm = TRUE),
    `J06.8 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J06.8 B97.4!", na.rm = TRUE),
    `J06.9 B97.4! treatment diagnosis` = sum(icd_combo_treatment == "J06.9 B97.4!", na.rm = TRUE),
    `J00 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J00 B97.4!", na.rm = TRUE),
    `J01.8 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J01.8 B97.4!", na.rm = TRUE),
    `J01.9 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J01.9 B97.4!", na.rm = TRUE),
    `J02.8 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J02.8 B97.4!", na.rm = TRUE),
    `J02.9 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J02.9 B97.4!", na.rm = TRUE),
    `J03.8 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J03.8 B97.4!", na.rm = TRUE),
    `J03.9 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J03.9 B97.4!", na.rm = TRUE),
    `J04.0 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J04.0 B97.4!", na.rm = TRUE),
    `J04.1 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J04.1 B97.4!", na.rm = TRUE),
    `J04.2 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J04.2 B97.4!", na.rm = TRUE),
    `J05.0 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J05.0 B97.4!", na.rm = TRUE),
    `J05.1 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J05.1 B97.4!", na.rm = TRUE),
    `J06.0 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J06.0 B97.4!", na.rm = TRUE),
    `J06.8 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J06.8 B97.4!", na.rm = TRUE),
    `J06.9 B97.4! discharge diagnosis` = sum(icd_combo_discharge == "J06.9 B97.4!", na.rm = TRUE),
    male = sum(Patient.Geschlecht == "male", na.rm = TRUE),
    female = sum(Patient.Geschlecht == "female", na.rm = TRUE),
    `mean hospital days` = mean(Krankenhaus.Tage, na.rm = TRUE),
    `median hospital days` = median(Krankenhaus.Tage, na.rm = TRUE),
    `min hospital days` = min(Krankenhaus.Tage, na.rm = TRUE),
    `max hospital days` = max(Krankenhaus.Tage, na.rm = TRUE),
    `quartile1 hospital days` = quantile(Krankenhaus.Tage, 0.25, na.rm = TRUE),
    `quartile3 hospital days` = quantile(Krankenhaus.Tage, 0.75, na.rm = TRUE),
    `mean ns days` = mean(Normalstation.Tage, na.rm = TRUE),
    `median ns days` = median(Normalstation.Tage, na.rm = TRUE),
    `min ns days` = min(Normalstation.Tage, na.rm = TRUE),
    `max ns days` = max(Normalstation.Tage, na.rm = TRUE),
    `quartile1 ns days` = quantile(Normalstation.Tage, 0.25, na.rm = TRUE),
    `quartile3 ns days` = quantile(Normalstation.Tage, 0.75, na.rm = TRUE),
    `mean is days` = mean(Intensivstation.Tage, na.rm = TRUE),
    `median is days` = median(Intensivstation.Tage, na.rm = TRUE),
    `max is days` = max(Intensivstation.Tage, na.rm = TRUE),
    `min is days` = min(Intensivstation.Tage, na.rm = TRUE),
    `quartile1 is days` = quantile(Intensivstation.Tage, 0.25, na.rm = TRUE),
    `quartile3 is days` = quantile(Intensivstation.Tage, 0.75, na.rm = TRUE),
    `respiratory support` = sum(Prozedur.Beatmung == "ja", na.rm = TRUE),
    `circulatory support` = sum(Prozedur.Blutkreislauf == "ja", na.rm = TRUE),
    `mean max crp` = mean(Labor.CRP.max, na.rm = TRUE),
    `median max crp` = median(Labor.CRP.max, na.rm = TRUE),
    `min max crp` = ifelse(sum(!is.na(Labor.CRP.max)) == 0, NA, min(Labor.CRP.max, na.rm = TRUE)),
    `max max crp` = ifelse(sum(!is.na(Labor.CRP.max)) == 0, NA, max(Labor.CRP.max, na.rm = TRUE)),
    `quartile1 crp` = quantile(Labor.CRP.max, 0.25, na.rm = TRUE),
    `quartile3 crp` = quantile(Labor.CRP.max, 0.75, na.rm = TRUE),
    `mean max leuko` = mean(Labor.Leuko.max, na.rm = TRUE),
    `median max leuko` = median(Labor.Leuko.max, na.rm = TRUE),
    `min max leuko` = ifelse(sum(!is.na(Labor.Leuko.max)) == 0, NA, min(Labor.Leuko.max, na.rm = TRUE)),
    `max max leuko` = ifelse(sum(!is.na(Labor.Leuko.max)) == 0, NA, max(Labor.Leuko.max, na.rm = TRUE)),
    `quartile1 leuko` = quantile(Labor.Leuko.max, 0.25, na.rm = TRUE),
    `quartile3 leuko` = quantile(Labor.Leuko.max, 0.75, na.rm = TRUE)
  ) %>%
  replace(is.na(.),0) %>%
  as.data.frame()


# Zeigt den aggregierten Datensatz im Terminal an.

print(dresden_aggregated_data)


transform <- as.data.frame(t(dresden_aggregated_data))
