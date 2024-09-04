# Notizen: 

# Der Datensatz "20231215_result_gesamt_2016_2023" (Originalrohdaten von der Analyse von Stephie) umbenannt in "dresden_rohdaten".
# 1 year = 365.25 Tage
# Die Variable "patient.age.sort" im Datensatz "dresden_rohdaten" muesste mehr Dezimalstellen enthalten um genauer unterschieden zu koennen zwischen den Altersgruppen.
# Die letzten Daten (von 2024) muessten auch inkludiert werden.
# patienten im Alter >=0 und >18 yearen sollten inkludiert werden.
# TODO: Grafiken schoener gestalten
# TODO: Wenn keine Werten fuer Variablen wie z.B. CRP vorhanden sind fuer eine Gruppe soll im Output/Grafik "No values recorded" o.ae. stehen und nicht 0. 
# TODO: Die 0s sind verschoben im Output (in den Transformierten Datensaetzen)
# TODO: Die ICD-variablen ueberlegen bzgl. Format (sekundaercodes koennten ggf. 2 Mal drin sein)


# Notwendige Packages

library(dplyr)
library(tidyr)
library(ggplot2)

dresden_rohdaten <- df_result

# Fuer eine vollstaendige Darstellung der Ergebnisse im Console.

options(max.print = 1000000000)


# Umwandlung in numerische Werte zur Erstellung von Spalten.

# not needed
#dresden_rohdaten$patient.age.sort <- as.numeric(gsub(",", ".", dresden_rohdaten$patient.age.sort))
#dresden_rohdaten$laboratory.crp.max <- as.numeric(gsub(",", ".", dresden_rohdaten$laboratory.crp.max))
#dresden_rohdaten$laboratory.leuko.max <- as.numeric(gsub(",", ".", dresden_rohdaten$laboratory.leuko.max))


# Erstellung der Variable "age_group" im Datensatz "dresden_rohdaten"
# term newborn infants:(0 to 27 days) (bis inkl. Vom Tag der Geburt bis inkl. 1 tag vor 28. LTag) (kleiner 0.0766598220... yeare (1/365.25*28))
# infants and toddlers (28 days to 23 months) (bis inkl. 1 tag vor 2. Geb)  
# children (2 to 11 years) (bis inkl. 1 tag vor 12 Geb)
# adolescents (12 to 18 years)  (bis inkl. 1 tag vor 18 Geb)

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(age_group = case_when(
    patient.age.sort >= 0 & patient.age.sort < (1/365.25*28) ~ "term newborn infants",   
    patient.age.sort >= (1/365.25*28) & patient.age.sort < 2 ~ "infants and toddlers",
    patient.age.sort >= 2 & patient.age.sort < 12 ~ "children",
    patient.age.sort >= 12 & patient.age.sort < 18 ~ "adolescents",
    TRUE ~ NA_character_
  ))


# Erstellung der Variable "pandemic_period" im Datensatz "dresden_rohdaten".
# Praepandemische Periode: 02.2018-02.2020
# Intrapandemische Periode: 03.2020-03.2022
# Postpandemische Periode: 04.2022-04.2024

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(pandemic_period = case_when(
    ((encounter.year == 2018 & encounter.month >= 2) | encounter.year == 2019 | (encounter.year == 2020 & encounter.month <= 2)) ~ "Prepand. Period",
    ((encounter.year == 2020 & encounter.month >= 3) | encounter.year == 2021 | (encounter.year == 2022 & encounter.month <= 3)) ~ "Intrapand. Period",
    ((encounter.year == 2022 & encounter.month >= 4) | encounter.year == 2023 | (encounter.year == 2024 & encounter.month <= 4)) ~ "Postpand. Period",
    TRUE ~ "Other"
  ))


# Erstellung der Variable "year_of_life" (Lebensjahr) im Datensatz "dresden_rohdaten".

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(year_of_life = case_when(
    patient.age.sort >= 0 & patient.age.sort < 1 ~ "1",
    patient.age.sort >= 1 & patient.age.sort < 2 ~ "2",
    patient.age.sort >= 2 & patient.age.sort < 3 ~ "3",
    patient.age.sort >= 3 & patient.age.sort < 4 ~ "4",
    patient.age.sort >= 4 & patient.age.sort < 5 ~ "5",
    patient.age.sort >= 5 & patient.age.sort < 6 ~ "6",
    patient.age.sort >= 6 & patient.age.sort < 7 ~ "7",
    patient.age.sort >= 7 & patient.age.sort < 8 ~ "8",
    patient.age.sort >= 8 & patient.age.sort < 9 ~ "9",
    patient.age.sort >= 9 & patient.age.sort < 10 ~ "10",
    patient.age.sort >= 10 & patient.age.sort < 11 ~ "11",
    patient.age.sort >= 11 & patient.age.sort < 12 ~ "12",
    patient.age.sort >= 12 & patient.age.sort < 13 ~ "13",
    patient.age.sort >= 13 & patient.age.sort < 14 ~ "14",
    patient.age.sort >= 14 & patient.age.sort < 15 ~ "15",
    patient.age.sort >= 15 & patient.age.sort < 16 ~ "16",
    patient.age.sort >= 16 & patient.age.sort < 17 ~ "17",
    patient.age.sort >= 17 & patient.age.sort < 18 ~ "18",
    TRUE ~ NA_character_
  ),
  .after = patient.age.sort)


# Erstellung der variable "month_of_life_first24" (Lebensmonat) im Datensatz "dresden_rohdaten".
# 1 month = 1/12 yeare.
# Umfasst nur die ersten 24 Lebensmonate.

dresden_rohdaten <- dresden_rohdaten %>%      
  mutate(month_of_life_first24 = case_when(
    patient.age.sort >= 0 & patient.age.sort < (1/12) ~ "1",
    patient.age.sort >= (1/12) & patient.age.sort < (2/12) ~ "2",
    patient.age.sort >= (2/12) & patient.age.sort < (3/12) ~ "3",
    patient.age.sort >= (3/12) & patient.age.sort < (4/12) ~ "4",
    patient.age.sort >= (4/12) & patient.age.sort < (5/12) ~ "5",
    patient.age.sort >= (5/12) & patient.age.sort < (6/12) ~ "6",
    patient.age.sort >= (6/12) & patient.age.sort < (7/12) ~ "7",
    patient.age.sort >= (7/12) & patient.age.sort < (8/12) ~ "8",
    patient.age.sort >= (8/12) & patient.age.sort < (9/12) ~ "9",
    patient.age.sort >= (9/12) & patient.age.sort < (10/12) ~ "10",
    patient.age.sort >= (10/12) & patient.age.sort < (11/12) ~ "11",
    patient.age.sort >= (11/12) & patient.age.sort < (12/12) ~ "12",
    patient.age.sort >= (12/12) & patient.age.sort < (13/12) ~ "13",
    patient.age.sort >= (13/12) & patient.age.sort < (14/12) ~ "14",
    patient.age.sort >= (14/12) & patient.age.sort < (15/12) ~ "15",
    patient.age.sort >= (15/12) & patient.age.sort < (16/12) ~ "16",
    patient.age.sort >= (16/12) & patient.age.sort < (17/12) ~ "17",
    patient.age.sort >= (17/12) & patient.age.sort < (18/12) ~ "18",
    patient.age.sort >= (18/12) & patient.age.sort < (19/12) ~ "19",
    patient.age.sort >= (19/12) & patient.age.sort < (20/12) ~ "20",
    patient.age.sort >= (20/12) & patient.age.sort < (21/12) ~ "21",
    patient.age.sort >= (21/12) & patient.age.sort < (22/12) ~ "22",
    patient.age.sort >= (22/12) & patient.age.sort < (23/12) ~ "23",
    patient.age.sort >= (23/12) & patient.age.sort < (24/12) ~ "24",
    TRUE ~ NA_character_
  ))


# Erstellung der variable "quarter" im Datensatz "dresden_rohdaten".

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(quarter = case_when(
    ((encounter.year == 2016 & encounter.month == 1) | (encounter.year == 2016 & encounter.month == 2) | (encounter.year == 2016 & encounter.month == 3)) ~ "Q1 2016",
    ((encounter.year == 2016 & encounter.month == 4) | (encounter.year == 2016 & encounter.month == 5) | (encounter.year == 2016 & encounter.month == 6)) ~ "Q2 2016",
    ((encounter.year == 2016 & encounter.month == 7) | (encounter.year == 2016 & encounter.month == 8) | (encounter.year == 2016 & encounter.month == 9)) ~ "Q3 2016",
    ((encounter.year == 2016 & encounter.month == 10) | (encounter.year == 2016 & encounter.month == 11) | (encounter.year == 2016 & encounter.month == 12)) ~ "Q4 2016",
    ((encounter.year == 2017 & encounter.month == 1) | (encounter.year == 2017 & encounter.month == 2) | (encounter.year == 2017 & encounter.month == 3)) ~ "Q1 2017",
    ((encounter.year == 2017 & encounter.month == 4) | (encounter.year == 2017 & encounter.month == 5) | (encounter.year == 2017 & encounter.month == 6)) ~ "Q2 2017",
    ((encounter.year == 2017 & encounter.month == 7) | (encounter.year == 2017 & encounter.month == 8) | (encounter.year == 2017 & encounter.month == 9)) ~ "Q3 2017",
    ((encounter.year == 2017 & encounter.month == 10) | (encounter.year == 2017 & encounter.month == 11) | (encounter.year == 2017 & encounter.month == 12)) ~ "Q4 2017",
    ((encounter.year == 2018 & encounter.month == 1) | (encounter.year == 2018 & encounter.month == 2) | (encounter.year == 2018 & encounter.month == 3)) ~ "Q1 2018",
    ((encounter.year == 2018 & encounter.month == 4) | (encounter.year == 2018 & encounter.month == 5) | (encounter.year == 2018 & encounter.month == 6)) ~ "Q2 2018",
    ((encounter.year == 2018 & encounter.month == 7) | (encounter.year == 2018 & encounter.month == 8) | (encounter.year == 2018 & encounter.month == 9)) ~ "Q3 2018",
    ((encounter.year == 2018 & encounter.month == 10) | (encounter.year == 2018 & encounter.month == 11) | (encounter.year == 2018 & encounter.month == 12)) ~ "Q4 2018",
    ((encounter.year == 2019 & encounter.month == 1) | (encounter.year == 2019 & encounter.month == 2) | (encounter.year == 2019 & encounter.month == 3)) ~ "Q1 2019",
    ((encounter.year == 2019 & encounter.month == 4) | (encounter.year == 2019 & encounter.month == 5) | (encounter.year == 2019 & encounter.month == 6)) ~ "Q2 2019",
    ((encounter.year == 2019 & encounter.month == 7) | (encounter.year == 2019 & encounter.month == 8) | (encounter.year == 2019 & encounter.month == 9)) ~ "Q3 2019",
    ((encounter.year == 2019 & encounter.month == 10) | (encounter.year == 2019 & encounter.month == 11) | (encounter.year == 2019 & encounter.month == 12)) ~ "Q4 2019",
    ((encounter.year == 2020 & encounter.month == 1) | (encounter.year == 2020 & encounter.month == 2) | (encounter.year == 2020 & encounter.month == 3)) ~ "Q1 2020",
    ((encounter.year == 2020 & encounter.month == 4) | (encounter.year == 2020 & encounter.month == 5) | (encounter.year == 2020 & encounter.month == 6)) ~ "Q2 2020",
    ((encounter.year == 2020 & encounter.month == 7) | (encounter.year == 2020 & encounter.month == 8) | (encounter.year == 2020 & encounter.month == 9)) ~ "Q3 2020",
    ((encounter.year == 2020 & encounter.month == 10) | (encounter.year == 2020 & encounter.month == 11) | (encounter.year == 2020 & encounter.month == 12)) ~ "Q4 2020",
    ((encounter.year == 2021 & encounter.month == 1) | (encounter.year == 2021 & encounter.month == 2) | (encounter.year == 2021 & encounter.month == 3)) ~ "Q1 2021",
    ((encounter.year == 2021 & encounter.month == 4) | (encounter.year == 2021 & encounter.month == 5) | (encounter.year == 2021 & encounter.month == 6)) ~ "Q2 2021",
    ((encounter.year == 2021 & encounter.month == 7) | (encounter.year == 2021 & encounter.month == 8) | (encounter.year == 2021 & encounter.month == 9)) ~ "Q3 2021",
    ((encounter.year == 2021 & encounter.month == 10) | (encounter.year == 2021 & encounter.month == 11) | (encounter.year == 2021 & encounter.month == 12)) ~ "Q4 2021",
    ((encounter.year == 2022 & encounter.month == 1) | (encounter.year == 2022 & encounter.month == 2) | (encounter.year == 2022 & encounter.month == 3)) ~ "Q1 2022",
    ((encounter.year == 2022 & encounter.month == 4) | (encounter.year == 2022 & encounter.month == 5) | (encounter.year == 2022 & encounter.month == 6)) ~ "Q2 2022",
    ((encounter.year == 2022 & encounter.month == 7) | (encounter.year == 2022 & encounter.month == 8) | (encounter.year == 2022 & encounter.month == 9)) ~ "Q3 2022",
    ((encounter.year == 2022 & encounter.month == 10) | (encounter.year == 2022 & encounter.month == 11) | (encounter.year == 2022 & encounter.month == 12)) ~ "Q4 2022",
    ((encounter.year == 2023 & encounter.month == 1) | (encounter.year == 2023 & encounter.month == 2) | (encounter.year == 2023 & encounter.month == 3)) ~ "Q1 2023",
    ((encounter.year == 2023 & encounter.month == 4) | (encounter.year == 2023 & encounter.month == 5) | (encounter.year == 2023 & encounter.month == 6)) ~ "Q2 2023",
    ((encounter.year == 2023 & encounter.month == 7) | (encounter.year == 2023 & encounter.month == 8) | (encounter.year == 2023 & encounter.month == 9)) ~ "Q3 2023",
    ((encounter.year == 2023 & encounter.month == 10) | (encounter.year == 2023 & encounter.month == 11) | (encounter.year == 2023 & encounter.month == 12)) ~ "Q4 2023",
    ((encounter.year == 2024 & encounter.month == 1) | (encounter.year == 2024 & encounter.month == 2) | (encounter.year == 2024 & encounter.month == 3)) ~ "Q1 2024",
    ((encounter.year == 2024 & encounter.month == 4) | (encounter.year == 2024 & encounter.month == 5) | (encounter.year == 2024 & encounter.month == 6)) ~ "Q2 2024",
    ((encounter.year == 2024 & encounter.month == 7) | (encounter.year == 2024 & encounter.month == 8) | (encounter.year == 2024 & encounter.month == 9)) ~ "Q3 2024",
    ((encounter.year == 2024 & encounter.month == 10) | (encounter.year == 2024 & encounter.month == 11) | (encounter.year == 2024 & encounter.month == 12)) ~ "Q4 2024",
    TRUE ~ "Other"
  ),
  .after = encounter.year.quarter)


# Erstellung der variable "season" im Datensatz "dresden_rohdaten".
# Metrologische yeareszeiten nach dem Deutschen Wetterdienst DWD.
# Fruehling: Maerz-Mai. Sommer: Juni-August. Herbst: September-November. Winter: Dezember-Februar (Folgejahr). 

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(season = case_when(
    ((encounter.year == 2016 & encounter.month == 3) | (encounter.year == 2016 & encounter.month == 4) | (encounter.year == 2016 & encounter.month == 5)) ~ "Spring 2016",
    ((encounter.year == 2016 & encounter.month == 6) | (encounter.year == 2016 & encounter.month == 7) | (encounter.year == 2016 & encounter.month == 8)) ~ "Summer 2016",
    ((encounter.year == 2016 & encounter.month == 9) | (encounter.year == 2016 & encounter.month == 10) | (encounter.year == 2016 & encounter.month == 11)) ~ "Fall 2016",
    ((encounter.year == 2016 & encounter.month == 12) | (encounter.year == 2017 & encounter.month == 1) | (encounter.year == 2017 & encounter.month == 2)) ~ "Winter 2016/2017",
    ((encounter.year == 2017 & encounter.month == 3) | (encounter.year == 2017 & encounter.month == 4) | (encounter.year == 2017 & encounter.month == 5)) ~ "Spring 2017",
    ((encounter.year == 2017 & encounter.month == 6) | (encounter.year == 2017 & encounter.month == 7) | (encounter.year == 2017 & encounter.month == 8)) ~ "Summer 2017",
    ((encounter.year == 2017 & encounter.month == 9) | (encounter.year == 2017 & encounter.month == 10) | (encounter.year == 2017 & encounter.month == 11)) ~ "Fall 2017",
    ((encounter.year == 2017 & encounter.month == 12) | (encounter.year == 2018 & encounter.month == 1) | (encounter.year == 2018 & encounter.month == 2)) ~ "Winter 2017/2018",
    ((encounter.year == 2018 & encounter.month == 3) | (encounter.year == 2018 & encounter.month == 4) | (encounter.year == 2018 & encounter.month == 5)) ~ "Spring 2018",
    ((encounter.year == 2018 & encounter.month == 6) | (encounter.year == 2018 & encounter.month == 7) | (encounter.year == 2018 & encounter.month == 8)) ~ "Summer 2018",
    ((encounter.year == 2018 & encounter.month == 9) | (encounter.year == 2018 & encounter.month == 10) | (encounter.year == 2018 & encounter.month == 11)) ~ "Fall 2018",
    ((encounter.year == 2018 & encounter.month == 12) | (encounter.year == 2019 & encounter.month == 1) | (encounter.year == 2019 & encounter.month == 2)) ~ "Winter 2018/2019",
    ((encounter.year == 2019 & encounter.month == 3) | (encounter.year == 2019 & encounter.month == 4) | (encounter.year == 2019 & encounter.month == 5)) ~ "Spring 2019",
    ((encounter.year == 2019 & encounter.month == 6) | (encounter.year == 2019 & encounter.month == 7) | (encounter.year == 2019 & encounter.month == 8)) ~ "Summer 2019",
    ((encounter.year == 2019 & encounter.month == 9) | (encounter.year == 2019 & encounter.month == 10) | (encounter.year == 2019 & encounter.month == 11)) ~ "Fall 2019",
    ((encounter.year == 2019 & encounter.month == 12) | (encounter.year == 2020 & encounter.month == 1) | (encounter.year == 2020 & encounter.month == 2)) ~ "Winter 2019/2020",
    ((encounter.year == 2020 & encounter.month == 3) | (encounter.year == 2020 & encounter.month == 4) | (encounter.year == 2020 & encounter.month == 5)) ~ "Spring 2020",
    ((encounter.year == 2020 & encounter.month == 6) | (encounter.year == 2020 & encounter.month == 7) | (encounter.year == 2020 & encounter.month == 8)) ~ "Summer 2020",
    ((encounter.year == 2020 & encounter.month == 9) | (encounter.year == 2020 & encounter.month == 10) | (encounter.year == 2020 & encounter.month == 11)) ~ "Fall 2020",
    ((encounter.year == 2020 & encounter.month == 12) | (encounter.year == 2021 & encounter.month == 1) | (encounter.year == 2021 & encounter.month == 2)) ~ "Winter 2020/2021",
    ((encounter.year == 2021 & encounter.month == 3) | (encounter.year == 2021 & encounter.month == 4) | (encounter.year == 2021 & encounter.month == 5)) ~ "Spring 2021",
    ((encounter.year == 2021 & encounter.month == 6) | (encounter.year == 2021 & encounter.month == 7) | (encounter.year == 2021 & encounter.month == 8)) ~ "Summer 2021",
    ((encounter.year == 2021 & encounter.month == 9) | (encounter.year == 2021 & encounter.month == 10) | (encounter.year == 2021 & encounter.month == 11)) ~ "Fall 2021",
    ((encounter.year == 2021 & encounter.month == 12) | (encounter.year == 2022 & encounter.month == 1) | (encounter.year == 2022 & encounter.month == 2)) ~ "Winter 2021/2022",
    ((encounter.year == 2022 & encounter.month == 3) | (encounter.year == 2022 & encounter.month == 4) | (encounter.year == 2022 & encounter.month == 5)) ~ "Spring 2022",
    ((encounter.year == 2022 & encounter.month == 6) | (encounter.year == 2022 & encounter.month == 7) | (encounter.year == 2022 & encounter.month == 8)) ~ "Summer 2022",
    ((encounter.year == 2022 & encounter.month == 9) | (encounter.year == 2022 & encounter.month == 10) | (encounter.year == 2022 & encounter.month == 11)) ~ "Fall 2022",
    ((encounter.year == 2022 & encounter.month == 12) | (encounter.year == 2023 & encounter.month == 1) | (encounter.year == 2023 & encounter.month == 2)) ~ "Winter 2022/2023",
    ((encounter.year == 2023 & encounter.month == 3) | (encounter.year == 2023 & encounter.month == 4) | (encounter.year == 2023 & encounter.month == 5)) ~ "Spring 2023",
    ((encounter.year == 2023 & encounter.month == 6) | (encounter.year == 2023 & encounter.month == 7) | (encounter.year == 2023 & encounter.month == 8)) ~ "Summer 2023",
    ((encounter.year == 2023 & encounter.month == 9) | (encounter.year == 2023 & encounter.month == 10) | (encounter.year == 2023 & encounter.month == 11)) ~ "Fall 2023",
    ((encounter.year == 2023 & encounter.month == 12) | (encounter.year == 2024 & encounter.month == 1) | (encounter.year == 2024 & encounter.month == 2)) ~ "Winter 2023/2024",
    ((encounter.year == 2024 & encounter.month == 3) | (encounter.year == 2024 & encounter.month == 4) | (encounter.year == 2024 & encounter.month == 5)) ~ "Spring 2024",
    ((encounter.year == 2024 & encounter.month == 6) | (encounter.year == 2024 & encounter.month == 7) | (encounter.year == 2024 & encounter.month == 8)) ~ "Summer 2024",
    ((encounter.year == 2024 & encounter.month == 9) | (encounter.year == 2024 & encounter.month == 10) | (encounter.year == 2024 & encounter.month == 11)) ~ "Fall 2024",
    ((encounter.year == 2024 & encounter.month == 12) | (encounter.year == 2025 & encounter.month == 1) | (encounter.year == 2025 & encounter.month == 2)) ~ "Winter 2024/2025",
    TRUE ~ "Other"
  ))


#Erstellung der Variable "season_total", sodass alle Daten nach yeareszeit aggregiert werden koennen. 

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(season_total = case_when(
    ((encounter.month == 3) | (encounter.month == 4) | (encounter.month == 5)) ~ "Spring",
    ((encounter.month == 6) | (encounter.month == 7) | (encounter.month == 8)) ~ "Summer",
    ((encounter.month == 9) | (encounter.month == 10) | (encounter.month == 11)) ~ "Fall",
    ((encounter.month == 12) | (encounter.month == 1) | (encounter.month == 2)) ~ "Winter",
    TRUE ~ "Other"
  ))

#Erstellung der Variable "seasonbased_year" um die Daten nach year und yeareszeit gleichzeitig aggregieren zu koennen.

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(seasonbased_year = case_when(
    ((encounter.year == 2016 & encounter.month == 3) | (encounter.year == 2016 & encounter.month == 4) | (encounter.year == 2016 & encounter.month == 5) | (encounter.year == 2016 & encounter.month == 6) | (encounter.year == 2016 & encounter.month == 7) | (encounter.year == 2016 & encounter.month == 8) | (encounter.year == 2016 & encounter.month == 9) | (encounter.year == 2016 & encounter.month == 10) | (encounter.year == 2016 & encounter.month == 11) | (encounter.year == 2016 & encounter.month == 12) | (encounter.year == 2017 & encounter.month == 1) | (encounter.year == 2017 & encounter.month == 2)) ~ "2016",
    ((encounter.year == 2017 & encounter.month == 3) | (encounter.year == 2017 & encounter.month == 4) | (encounter.year == 2017 & encounter.month == 5) | (encounter.year == 2017 & encounter.month == 6) | (encounter.year == 2017 & encounter.month == 7) | (encounter.year == 2017 & encounter.month == 8) | (encounter.year == 2017 & encounter.month == 9) | (encounter.year == 2017 & encounter.month == 10) | (encounter.year == 2017 & encounter.month == 11) | (encounter.year == 2017 & encounter.month == 12) | (encounter.year == 2018 & encounter.month == 1) | (encounter.year == 2018 & encounter.month == 2)) ~ "2017",
    ((encounter.year == 2018 & encounter.month == 3) | (encounter.year == 2018 & encounter.month == 4) | (encounter.year == 2018 & encounter.month == 5) | (encounter.year == 2018 & encounter.month == 6) | (encounter.year == 2018 & encounter.month == 7) | (encounter.year == 2018 & encounter.month == 8) | (encounter.year == 2018 & encounter.month == 9) | (encounter.year == 2018 & encounter.month == 10) | (encounter.year == 2018 & encounter.month == 11) | (encounter.year == 2018 & encounter.month == 12) | (encounter.year == 2019 & encounter.month == 1) | (encounter.year == 2019 & encounter.month == 2)) ~ "2018",
    ((encounter.year == 2019 & encounter.month == 3) | (encounter.year == 2019 & encounter.month == 4) | (encounter.year == 2019 & encounter.month == 5) | (encounter.year == 2019 & encounter.month == 6) | (encounter.year == 2019 & encounter.month == 7) | (encounter.year == 2019 & encounter.month == 8) | (encounter.year == 2019 & encounter.month == 9) | (encounter.year == 2019 & encounter.month == 10) | (encounter.year == 2019 & encounter.month == 11) | (encounter.year == 2019 & encounter.month == 12) | (encounter.year == 2020 & encounter.month == 1) | (encounter.year == 2020 & encounter.month == 2)) ~ "2019",
    ((encounter.year == 2020 & encounter.month == 3) | (encounter.year == 2020 & encounter.month == 4) | (encounter.year == 2020 & encounter.month == 5) | (encounter.year == 2020 & encounter.month == 6) | (encounter.year == 2020 & encounter.month == 7) | (encounter.year == 2020 & encounter.month == 8) | (encounter.year == 2020 & encounter.month == 9) | (encounter.year == 2020 & encounter.month == 10) | (encounter.year == 2020 & encounter.month == 11) | (encounter.year == 2020 & encounter.month == 12) | (encounter.year == 2021 & encounter.month == 1) | (encounter.year == 2021 & encounter.month == 2)) ~ "2020",
    ((encounter.year == 2021 & encounter.month == 3) | (encounter.year == 2021 & encounter.month == 4) | (encounter.year == 2021 & encounter.month == 5) | (encounter.year == 2021 & encounter.month == 6) | (encounter.year == 2021 & encounter.month == 7) | (encounter.year == 2021 & encounter.month == 8) | (encounter.year == 2021 & encounter.month == 9) | (encounter.year == 2021 & encounter.month == 10) | (encounter.year == 2021 & encounter.month == 11) | (encounter.year == 2021 & encounter.month == 12) | (encounter.year == 2022 & encounter.month == 1) | (encounter.year == 2022 & encounter.month == 2)) ~ "2021",
    ((encounter.year == 2022 & encounter.month == 3) | (encounter.year == 2022 & encounter.month == 4) | (encounter.year == 2022 & encounter.month == 5) | (encounter.year == 2022 & encounter.month == 6) | (encounter.year == 2022 & encounter.month == 7) | (encounter.year == 2022 & encounter.month == 8) | (encounter.year == 2022 & encounter.month == 9) | (encounter.year == 2022 & encounter.month == 10) | (encounter.year == 2022 & encounter.month == 11) | (encounter.year == 2022 & encounter.month == 12) | (encounter.year == 2023 & encounter.month == 1) | (encounter.year == 2023 & encounter.month == 2)) ~ "2022",
    ((encounter.year == 2023 & encounter.month == 3) | (encounter.year == 2023 & encounter.month == 4) | (encounter.year == 2023 & encounter.month == 5) | (encounter.year == 2023 & encounter.month == 6) | (encounter.year == 2023 & encounter.month == 7) | (encounter.year == 2023 & encounter.month == 8) | (encounter.year == 2023 & encounter.month == 9) | (encounter.year == 2023 & encounter.month == 10) | (encounter.year == 2023 & encounter.month == 11) | (encounter.year == 2023 & encounter.month == 12) | (encounter.year == 2024 & encounter.month == 1) | (encounter.year == 2024 & encounter.month == 2)) ~ "2023",
    ((encounter.year == 2024 & encounter.month == 3) | (encounter.year == 2024 & encounter.month == 4) | (encounter.year == 2024 & encounter.month == 5) | (encounter.year == 2024 & encounter.month == 6) | (encounter.year == 2024 & encounter.month == 7) | (encounter.year == 2024 & encounter.month == 8) | (encounter.year == 2024 & encounter.month == 9) | (encounter.year == 2024 & encounter.month == 10) | (encounter.year == 2024 & encounter.month == 11) | (encounter.year == 2024 & encounter.month == 12) | (encounter.year == 2025 & encounter.month == 1) | (encounter.year == 2025 & encounter.month == 2)) ~ "2024",
    TRUE ~ "Other"
  ))


# Erstellung der Variable "icd_combo_treatment" im Datensatz "dresden_rohdaten".
# Umfasst die verschiedenen vordefinierten Kombinationen von ICD-Codes in Dokument ICD10_RSV_20231009.
# Handelt sich um die Behandlungsdiagnose.

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(icd_combo_treatment = case_when(
    icd.pri_code.cc == "J00" & icd.sec_code.cc == "B97.4!" ~ "J00 B97.4!",
    icd.pri_code.cc == "J01.8" & icd.sec_code.cc == "B97.4!" ~ "J01.8 B97.4!",
    icd.pri_code.cc == "J01.9" & icd.sec_code.cc == "B97.4!" ~ "J01.9 B97.4!",
    icd.pri_code.cc == "J02.8" & icd.sec_code.cc == "B97.4!" ~ "J02.8 B97.4!",
    icd.pri_code.cc == "J02.9" & icd.sec_code.cc == "B97.4!" ~ "J02.9 B97.4!",
    icd.pri_code.cc == "J03.8" & icd.sec_code.cc == "B97.4!" ~ "J03.8 B97.4!",
    icd.pri_code.cc == "J03.9" & icd.sec_code.cc == "B97.4!" ~ "J03.9 B97.4!",
    icd.pri_code.cc == "J04.0" & icd.sec_code.cc == "B97.4!" ~ "J04.0 B97.4!",
    icd.pri_code.cc == "J04.1" & icd.sec_code.cc == "B97.4!" ~ "J04.1 B97.4!",
    icd.pri_code.cc == "J04.2" & icd.sec_code.cc == "B97.4!" ~ "J04.2 B97.4!",
    icd.pri_code.cc == "J05.0" & icd.sec_code.cc == "B97.4!" ~ "J05.0 B97.4!",
    icd.pri_code.cc == "J05.1" & icd.sec_code.cc == "B97.4!" ~ "J05.1 B97.4!",
    icd.pri_code.cc == "J06.0" & icd.sec_code.cc == "B97.4!" ~ "J06.0 B97.4!",
    icd.pri_code.cc == "J06.8" & icd.sec_code.cc == "B97.4!" ~ "J06.8 B97.4!",
    icd.pri_code.cc == "J06.9" & icd.sec_code.cc == "B97.4!" ~ "J06.9 B97.4!",
    TRUE ~ "not applicable"
  ))


# Erstellung der Variable "icd_combo_discharge" im Datensatz "dresden_rohdaten".
# Umfasst die verschiedenen vordefinierten Kombinationen von ICD-Codes in Dokument ICD10_RSV_20231009.
# Handelt sich um Entlassdiagnose.

dresden_rohdaten <- dresden_rohdaten %>%
  mutate(icd_combo_discharge = case_when(
    icd.pri_code.dd == "J00" & icd.sec_code.dd == "B97.4!" ~ "J00 B97.4!",
    icd.pri_code.dd == "J01.8" & icd.sec_code.dd == "B97.4!" ~ "J01.8 B97.4!",
    icd.pri_code.dd == "J01.9" & icd.sec_code.dd == "B97.4!" ~ "J01.9 B97.4!",
    icd.pri_code.dd == "J02.8" & icd.sec_code.dd == "B97.4!" ~ "J02.8 B97.4!",
    icd.pri_code.dd == "J02.9" & icd.sec_code.dd == "B97.4!" ~ "J02.9 B97.4!",
    icd.pri_code.dd == "J03.8" & icd.sec_code.dd == "B97.4!" ~ "J03.8 B97.4!",
    icd.pri_code.dd == "J03.9" & icd.sec_code.dd == "B97.4!" ~ "J03.9 B97.4!",
    icd.pri_code.dd == "J04.0" & icd.sec_code.dd == "B97.4!" ~ "J04.0 B97.4!",
    icd.pri_code.dd == "J04.1" & icd.sec_code.dd == "B97.4!" ~ "J04.1 B97.4!",
    icd.pri_code.dd == "J04.2" & icd.sec_code.dd == "B97.4!" ~ "J04.2 B97.4!",
    icd.pri_code.dd == "J05.0" & icd.sec_code.dd == "B97.4!" ~ "J05.0 B97.4!",
    icd.pri_code.dd == "J05.1" & icd.sec_code.dd == "B97.4!" ~ "J05.1 B97.4!",
    icd.pri_code.dd == "J06.0" & icd.sec_code.dd == "B97.4!" ~ "J06.0 B97.4!",
    icd.pri_code.dd == "J06.8" & icd.sec_code.dd == "B97.4!" ~ "J06.8 B97.4!",
    icd.pri_code.dd == "J06.9" & icd.sec_code.dd == "B97.4!" ~ "J06.9 B97.4!",
    TRUE ~ "not applicable"
  ))


# Aggregiert Datensatz nach den Variablen "age_group" und "pandemic_period".

aggregated_pand_age <- dresden_rohdaten %>%
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
    `2016` = sum(encounter.year == 2016, na.rm = TRUE),
    `2017` = sum(encounter.year == 2017, na.rm = TRUE),
    `2018` = sum(encounter.year == 2018, na.rm = TRUE),
    `2019` = sum(encounter.year == 2019, na.rm = TRUE),
    `2020` = sum(encounter.year == 2020, na.rm = TRUE),
    `2021` = sum(encounter.year == 2021, na.rm = TRUE),
    `2022` = sum(encounter.year == 2022, na.rm = TRUE),
    `2023` = sum(encounter.year == 2023, na.rm = TRUE),
    `2024` = sum(encounter.year == 2024, na.rm = TRUE),
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
    `Spring` = sum(season_total == "Spring", na.rm = TRUE),
    `Summer` = sum(season_total == "Summer", na.rm = TRUE),
    `encounter` = sum(season_total == "encounter", na.rm = TRUE),
    `Winter` = sum(season_total == "Winter", na.rm = TRUE),
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
    `J20.5 treatment diagnosis` = sum(icd.pri_code.cc == "J20.5", na.rm = TRUE),
    `J21.0 treatment diagnosis` = sum(icd.pri_code.cc == "J21.0", na.rm = TRUE),
    `J12.1 treatment diagnosis` = sum(icd.pri_code.cc == "J12.1", na.rm = TRUE),
    `P23.0 treatment diagnosis` = sum(icd.pri_code.cc == "P23.0", na.rm = TRUE),
    `B97.4! treatment diagnosis` = sum(icd.sec_code.cc == "B97.4!", na.rm = TRUE),
    `J20.5 discharge diagnosis` = sum(icd.pri_code.dd == "J20.5", na.rm = TRUE),
    `J21.0 discharge diagnosis` = sum(icd.pri_code.dd == "J21.0", na.rm = TRUE),
    `J12.1 discharge diagnosis` = sum(icd.pri_code.dd == "J12.1", na.rm = TRUE),
    `P23.0 discharge diagnosis` = sum(icd.pri_code.dd == "P23.0", na.rm = TRUE),
    `B97.4! discharge diagnosis` = sum(icd.sec_code.dd == "B97.4!", na.rm = TRUE),
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
    `male` = sum(patient.gender == "male", na.rm = TRUE),
    `female` = sum(patient.gender == "female", na.rm = TRUE),
    `mean hospital days` = mean(days.hospital, na.rm = TRUE),
    `median hospital days` = median(days.hospital, na.rm = TRUE),
    `min hospital days` = min(days.hospital, na.rm = TRUE),
    `max hospital days` = max(days.hospital, na.rm = TRUE),
    `quartile1 hospital days` = quantile(days.hospital, 0.25, na.rm = TRUE),
    `quartile3 hospital days` = quantile(days.hospital, 0.75, na.rm = TRUE),
    `mean ns days` = mean(days.normal, na.rm = TRUE),
    `median ns days` = median(days.normal, na.rm = TRUE),
    `min ns days` = min(days.normal, na.rm = TRUE),
    `max ns days` = max(days.normal, na.rm = TRUE),
    `quartile1 ns days` = quantile(days.normal, 0.25, na.rm = TRUE),
    `quartile3 ns days` = quantile(days.normal, 0.75, na.rm = TRUE),
    `mean is days` = mean(days.its, na.rm = TRUE),
    `median is days` = median(days.its, na.rm = TRUE),
    `max is days` = max(days.its, na.rm = TRUE),
    `min is days` = min(days.its, na.rm = TRUE),
    `quartile1 is days` = quantile(days.its, 0.25, na.rm = TRUE),
    `quartile3 is days` = quantile(days.its, 0.75, na.rm = TRUE),
    `respiratory support` = sum(procedure.respiratory_support  == "yes", na.rm = TRUE),
    `circulatory support` = sum(procedure.circulatory_support  == "yes", na.rm = TRUE),
    `mean max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, mean(laboratory.crp.max, na.rm = TRUE)),
    `median max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, median(laboratory.crp.max, na.rm = TRUE)),
    `min max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, min(laboratory.crp.max, na.rm = TRUE)),
    `max max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, max(laboratory.crp.max, na.rm = TRUE)),
    `quartile1 crp` = ifelse(all(is.na(laboratory.crp.max)), NA, quantile(laboratory.crp.max, 0.25, na.rm = TRUE)),
    `quartile3 crp` = ifelse(all(is.na(laboratory.crp.max)), NA, quantile(laboratory.crp.max, 0.75, na.rm = TRUE)),
    `mean max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, mean(laboratory.leuko.max, na.rm = TRUE)),
    `median max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, median(laboratory.leuko.max, na.rm = TRUE)),
    `min max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, min(laboratory.leuko.max, na.rm = TRUE)),
    `max max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, max(laboratory.leuko.max, na.rm = TRUE)),
    `quartile1 leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, quantile(laboratory.leuko.max, 0.25, na.rm = TRUE)),
    `quartile3 leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, quantile(laboratory.leuko.max, 0.75, na.rm = TRUE))
  ) %>%
  ungroup() %>%
  mutate(across(-c(`mean max leuko`, `median max leuko`, `min max leuko`, `max max leuko`, `quartile1 leuko`, `mean max crp`, `median max crp`, `quartile3 leuko`,`min max crp`, `max max crp`, `quartile1 crp`, `quartile3 crp`), ~ replace_na(., 0))) %>%
  as.data.frame()


# Aggregiert Datensatz nach den Variablen "seasonbased_year" und "season_total" (year und yearesszeit).

aggregated_year_season <- dresden_rohdaten %>%
  group_by(seasonbased_year, season_total) %>%
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
    `2016` = sum(encounter.year == 2016, na.rm = TRUE),
    `2017` = sum(encounter.year == 2017, na.rm = TRUE),
    `2018` = sum(encounter.year == 2018, na.rm = TRUE),
    `2019` = sum(encounter.year == 2019, na.rm = TRUE),
    `2020` = sum(encounter.year == 2020, na.rm = TRUE),
    `2021` = sum(encounter.year == 2021, na.rm = TRUE),
    `2022` = sum(encounter.year == 2022, na.rm = TRUE),
    `2023` = sum(encounter.year == 2023, na.rm = TRUE),
    `2024` = sum(encounter.year == 2024, na.rm = TRUE),
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
    `Spring` = sum(season_total == "Spring", na.rm = TRUE),
    `Summer` = sum(season_total == "Summer", na.rm = TRUE),
    `Fall` = sum(season_total == "Fall", na.rm = TRUE),
    `Winter` = sum(season_total == "Winter", na.rm = TRUE),
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
    `J20.5 treatment diagnosis` = sum(icd.pri_code.cc == "J20.5", na.rm = TRUE),
    `J21.0 treatment diagnosis` = sum(icd.pri_code.cc == "J21.0", na.rm = TRUE),
    `J12.1 treatment diagnosis` = sum(icd.pri_code.cc == "J12.1", na.rm = TRUE),
    `P23.0 treatment diagnosis` = sum(icd.pri_code.cc == "P23.0", na.rm = TRUE),
    `B97.4! treatment diagnosis` = sum(icd.sec_code.cc == "B97.4!", na.rm = TRUE),
    `J20.5 discharge diagnosis` = sum(icd.pri_code.dd == "J20.5", na.rm = TRUE),
    `J21.0 discharge diagnosis` = sum(icd.pri_code.dd == "J21.0", na.rm = TRUE),
    `J12.1 discharge diagnosis` = sum(icd.pri_code.dd == "J12.1", na.rm = TRUE),
    `P23.0 discharge diagnosis` = sum(icd.pri_code.dd == "P23.0", na.rm = TRUE),
    `B97.4! discharge diagnosis` = sum(icd.sec_code.dd == "B97.4!", na.rm = TRUE),
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
    male = sum(patient.gender == "male", na.rm = TRUE),
    female = sum(patient.gender == "female", na.rm = TRUE),
    `mean hospital days` = mean(days.hospital, na.rm = TRUE),
    `median hospital days` = median(days.hospital, na.rm = TRUE),
    `min hospital days` = min(days.hospital, na.rm = TRUE),
    `max hospital days` = max(days.hospital, na.rm = TRUE),
    `quartile1 hospital days` = quantile(days.hospital, 0.25, na.rm = TRUE),
    `quartile3 hospital days` = quantile(days.hospital, 0.75, na.rm = TRUE),
    `mean ns days` = mean(days.normal, na.rm = TRUE),
    `median ns days` = median(days.normal, na.rm = TRUE),
    `min ns days` = min(days.normal, na.rm = TRUE),
    `max ns days` = max(days.normal, na.rm = TRUE),
    `quartile1 ns days` = quantile(days.normal, 0.25, na.rm = TRUE),
    `quartile3 ns days` = quantile(days.normal, 0.75, na.rm = TRUE),
    `mean is days` = mean(days.its, na.rm = TRUE),
    `median is days` = median(days.its, na.rm = TRUE),
    `max is days` = max(days.its, na.rm = TRUE),
    `min is days` = min(days.its, na.rm = TRUE),
    `quartile1 is days` = quantile(days.its, 0.25, na.rm = TRUE),
    `quartile3 is days` = quantile(days.its, 0.75, na.rm = TRUE),
    `respiratory support` = sum(procedure.respiratory_support  == "ja", na.rm = TRUE),
    `circulatory support` = sum(procedure.circulatory_support  == "ja", na.rm = TRUE),
    `mean max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, mean(laboratory.crp.max, na.rm = TRUE)),
    `median max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, median(laboratory.crp.max, na.rm = TRUE)),
    `min max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, min(laboratory.crp.max, na.rm = TRUE)),
    `max max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, max(laboratory.crp.max, na.rm = TRUE)),
    `quartile1 crp` = ifelse(all(is.na(laboratory.crp.max)), NA, quantile(laboratory.crp.max, 0.25, na.rm = TRUE)),
    `quartile3 crp` = ifelse(all(is.na(laboratory.crp.max)), NA, quantile(laboratory.crp.max, 0.75, na.rm = TRUE)),
    `mean max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, mean(laboratory.leuko.max, na.rm = TRUE)),
    `median max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, median(laboratory.leuko.max, na.rm = TRUE)),
    `min max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, min(laboratory.leuko.max, na.rm = TRUE)),
    `max max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, max(laboratory.leuko.max, na.rm = TRUE)),
    `quartile1 leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, quantile(laboratory.leuko.max, 0.25, na.rm = TRUE)),
    `quartile3 leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, quantile(laboratory.leuko.max, 0.75, na.rm = TRUE))
  ) %>%
  ungroup() %>%
  mutate(across(-c(`mean max leuko`, `median max leuko`, `min max leuko`, `max max leuko`, `quartile1 leuko`, `mean max crp`, `median max crp`, `quartile3 leuko`,`min max crp`, `max max crp`, `quartile1 crp`, `quartile3 crp`), ~ replace_na(., 0))) %>%
  as.data.frame()


# Aggregiert Datensatz nach eine selbst gewaehlte zusaetzliche Variable aus dresden_rohdaten. Die Variable kann nach "free_variable <-" in "" eingefuegt werden. Wenn dies nicht erwuenscht ist kann hier "not applicable" eingeschrieben werden. 
# TODO: Auch ermoeglichen, den Daten nach den anderen Variablen aus den anderen aggregierten Datensaetzen zu aggregieren, und nicht nur nach den Variablen in dresden_rohdaten. 

free_variable <- "not applicable"

if (free_variable != "not applicable") {
  aggregated_free <- dresden_rohdaten %>%
    group_by(across(all_of(free_variable))) %>%
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
      `2016` = sum(encounter.year == 2016, na.rm = TRUE),
      `2017` = sum(encounter.year == 2017, na.rm = TRUE),
      `2018` = sum(encounter.year == 2018, na.rm = TRUE),
      `2019` = sum(encounter.year == 2019, na.rm = TRUE),
      `2020` = sum(encounter.year == 2020, na.rm = TRUE),
      `2021` = sum(encounter.year == 2021, na.rm = TRUE),
      `2022` = sum(encounter.year == 2022, na.rm = TRUE),
      `2023` = sum(encounter.year == 2023, na.rm = TRUE),
      `2024` = sum(encounter.year == 2024, na.rm = TRUE),
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
      `Spring` = sum(season_total == "Spring", na.rm = TRUE),
      `Summer` = sum(season_total == "Summer", na.rm = TRUE),
      `encounter` = sum(season_total == "encounter", na.rm = TRUE),
      `Winter` = sum(season_total == "Winter", na.rm = TRUE),
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
      `J20.5 treatment diagnosis` = sum(icd.pri_code.cc == "J20.5", na.rm = TRUE),
      `J21.0 treatment diagnosis` = sum(icd.pri_code.cc == "J21.0", na.rm = TRUE),
      `J12.1 treatment diagnosis` = sum(icd.pri_code.cc == "J12.1", na.rm = TRUE),
      `P23.0 treatment diagnosis` = sum(icd.pri_code.cc == "P23.0", na.rm = TRUE),
      `B97.4! treatment diagnosis` = sum(icd.sec_code.cc == "B97.4!", na.rm = TRUE),
      `J20.5 discharge diagnosis` = sum(icd.pri_code.dd == "J20.5", na.rm = TRUE),
      `J21.0 discharge diagnosis` = sum(icd.pri_code.dd == "J21.0", na.rm = TRUE),
      `J12.1 discharge diagnosis` = sum(icd.pri_code.dd == "J12.1", na.rm = TRUE),
      `P23.0 discharge diagnosis` = sum(icd.pri_code.dd == "P23.0", na.rm = TRUE),
      `B97.4! discharge diagnosis` = sum(icd.sec_code.dd == "B97.4!", na.rm = TRUE),
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
      male = sum(patient.gender == "male", na.rm = TRUE),
      female = sum(patient.gender == "female", na.rm = TRUE),
      `mean hospital days` = mean(days.hospital, na.rm = TRUE),
      `median hospital days` = median(days.hospital, na.rm = TRUE),
      `min hospital days` = min(days.hospital, na.rm = TRUE),
      `max hospital days` = max(days.hospital, na.rm = TRUE),
      `quartile1 hospital days` = quantile(days.hospital, 0.25, na.rm = TRUE),
      `quartile3 hospital days` = quantile(days.hospital, 0.75, na.rm = TRUE),
      `mean ns days` = mean(days.normal, na.rm = TRUE),
      `median ns days` = median(days.normal, na.rm = TRUE),
      `min ns days` = min(days.normal, na.rm = TRUE),
      `max ns days` = max(days.normal, na.rm = TRUE),
      `quartile1 ns days` = quantile(days.normal, 0.25, na.rm = TRUE),
      `quartile3 ns days` = quantile(days.normal, 0.75, na.rm = TRUE),
      `mean is days` = mean(days.its, na.rm = TRUE),
      `median is days` = median(days.its, na.rm = TRUE),
      `max is days` = max(days.its, na.rm = TRUE),
      `min is days` = min(days.its, na.rm = TRUE),
      `quartile1 is days` = quantile(days.its, 0.25, na.rm = TRUE),
      `quartile3 is days` = quantile(days.its, 0.75, na.rm = TRUE),
      `respiratory support` = sum(procedure.respiratory_support  == "ja", na.rm = TRUE),
      `circulatory support` = sum(procedure.circulatory_support  == "ja", na.rm = TRUE),
      `mean max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, mean(laboratory.crp.max, na.rm = TRUE)),
      `median max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, median(laboratory.crp.max, na.rm = TRUE)),
      `min max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, min(laboratory.crp.max, na.rm = TRUE)),
      `max max crp` = ifelse(all(is.na(laboratory.crp.max)), NA, max(laboratory.crp.max, na.rm = TRUE)),
      `quartile1 crp` = ifelse(all(is.na(laboratory.crp.max)), NA, quantile(laboratory.crp.max, 0.25, na.rm = TRUE)),
      `quartile3 crp` = ifelse(all(is.na(laboratory.crp.max)), NA, quantile(laboratory.crp.max, 0.75, na.rm = TRUE)),
      `mean max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, mean(laboratory.leuko.max, na.rm = TRUE)),
      `median max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, median(laboratory.leuko.max, na.rm = TRUE)),
      `min max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, min(laboratory.leuko.max, na.rm = TRUE)),
      `max max leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, max(laboratory.leuko.max, na.rm = TRUE)),
      `quartile1 leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, quantile(laboratory.leuko.max, 0.25, na.rm = TRUE)),
      `quartile3 leuko` = ifelse(all(is.na(laboratory.leuko.max)), NA, quantile(laboratory.leuko.max, 0.75, na.rm = TRUE))
    ) %>%
    ungroup() %>%
    mutate(across(-c(`mean max leuko`, `median max leuko`, `min max leuko`, `max max leuko`, `quartile1 leuko`, `mean max crp`, `median max crp`, `quartile3 leuko`,`min max crp`, `max max crp`, `quartile1 crp`, `quartile3 crp`), ~ replace_na(., 0))) %>%
    as.data.frame()
  print(aggregated_free)
  transformed_aggregated_free <- as.data.frame(t(aggregated_free))
} else {
  rm(aggregated_free)
  rm(transformed_aggregated_free)
}


# Erstellt Datensaetze zur Uebersicht.

transformed_aggregated_pand_age <- as.data.frame(t(aggregated_pand_age))
transformed_aggregated_year_season <- as.data.frame(t(aggregated_year_season))


#Funktion fuer Grafikerstellung (Barchart mit Aggregierung nach Altersgruppe und Pand. Periode)

bar_chart_pand_age <- function(data, y_variable) {
  data_long <- data %>% 
    pivot_longer(cols = all_of(y_variable), names_to = "variable", values_to = "value") %>%
    filter(pandemic_period != "Other") %>%
    mutate(value = as.numeric(value),
           pandemic_period = factor(pandemic_period, levels = c("Prepand. Period", "Intrapand. Period", "Postpand. Period"))) %>%
    complete(age_group, pandemic_period, fill = list(value = 0))
  
  ggplot(data_long, aes(x = pandemic_period, y = value, fill = age_group)) + 
    geom_bar(stat = "identity", position = position_dodge(preserve = "single")) + 
    labs(title = paste(y_variable, "(frequency)"),
         x = "",
         y = "",
         fill = "age group") +
    theme_minimal() + 
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    scale_x_discrete(expand = expansion(mult = c(0.001, 0.001)))
}


#Funktion fuer Grafikerstellung (Barchart mit Aggregierung nach yeareszeit und year)

bar_chart_year_season <- function(data, y_variable) {
  data_long <- data %>% 
    pivot_longer(cols = all_of(y_variable), names_to = "variable", values_to = "value") %>%
    filter(seasonbased_year != "Other") %>%
    mutate(value = as.numeric(value),
           seasonbased_year = factor(seasonbased_year, levels = c("2016", "2017", "2018", "2019", "2020", "2021", "2022", "2023", "2024")),
           season_total = factor(season_total, level = c("Spring", "Summer", "Fall", "Winter"))) %>%
    complete(season_total, seasonbased_year, fill = list(value = 0))
  
  ggplot(data_long, aes(x = seasonbased_year, y = value, fill = season_total)) +
    geom_bar(stat = "identity", position = position_dodge(preserve = "single")) + 
    labs(title = paste(y_variable, "(frequency)"),
         x = "",
         y = "",
         fill = "season") +
    theme_minimal() + 
    theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
    scale_x_discrete(expand = expansion(mult = c(0.001, 0.001)))
}


# Generiert die 2 Barcharts. Hier kann auch gewaehlt werden, welche Variablen aus den aggregierten Datensaetzen in den Grafiken dargestellt sein sollen. Hier sind als Beispiel "male" und "respiratory support" ausgewaehlt. 

bar_chart_year_season(aggregated_year_season, "male")
bar_chart_pand_age(aggregated_pand_age, "respiratory support")


# Zeigt den aggregierten Datensaetzen im Console an.

print(aggregated_pand_age)
print(aggregated_year_season)
