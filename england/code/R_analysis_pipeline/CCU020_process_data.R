#clear environment
rm(list = ls())

#Load packages
library(odbc)
library(dplyr)
library(data.table)
library(DBI)

# set target folder
setwd("/mnt/efs/a.handy/dars_nic_391419_j3w9t_collab/CCU020")

# connect to databricks instance
con = dbConnect( odbc::odbc(), "Databricks", timeout = 60, 
                 PWD=rstudioapi::askForPassword("Please enter your Databricks personal access token"))

# setup time indices that require processing
time_indices = c("2020_01_01", "2020_07_01", "2021_01_01", "2021_05_01")
#time_indices = c("2020_01_01")

#set today's date for file versioning
today_date = format(Sys.time(), "%d_%m_%Y")

#conduct data processing for each time index and save data object
for (index in time_indices){
  
  query = paste('SELECT * FROM dars_nic_391419_j3w9t_collab.ccu020_20210701_', index, '_study_population_cov', sep="")
  
  # load data
  data = dbGetQuery(con,query)
  
  # check dimensions of the data
  print(head(data))
  print(paste("Number of rows: ", nrow(data)))
  print(paste("Number of individuals: ", length(unique(data$NHS_NUMBER_DEID))))
  
  # check for any remaining duplicate ids 
  print(length(unique(data$NHS_NUMBER_DEID)) == nrow(data))
  
  #PROCESS DRUG VARIABLES--------------------
  
  #replace all n/a's with 0 in drug flag columns
  drugs = c("antiplatelets", "aspirin", "clopidogrel", "dipyridamole", "ticagrelor", "prasugrel", "doacs", "apixaban", "rivaroxaban", "dabigatran", "edoxaban", "warfarin", "antihypertensives", "lipid_regulating_drugs", "insulin", "sulphonylurea", "metformin", "other_diabetic_drugs", "proton_pump_inhibitors", "nsaids", "corticosteroids", "other_immunosuppressants")
  data = data %>% mutate_at(drugs, ~replace(., is.na(.), 0))
  
  #add aggregate drug categories
  data = mutate(data, anticoagulants = if_else( (doacs == 1) | (warfarin == 1),1,0))
  
  data = mutate(data, ac_only = if_else( (anticoagulants == 1) & (antiplatelets == 0), 1,0), 
                ap_only = if_else( (anticoagulants == 0) & (antiplatelets == 1), 1,0), 
                ac_and_ap = if_else( (anticoagulants == 1) & (antiplatelets == 1), 1,0),
                any_at = if_else( (anticoagulants == 1) | (antiplatelets == 1), 1,0),
                no_at = if_else( (anticoagulants == 0) & (antiplatelets == 0), 1,0),
  )
  
  #check aggregated categories == 100%
  ac_only_n = nrow(filter(data, ac_only == 1))
  ap_only_n = nrow(filter(data, ap_only == 1))
  ac_and_ap_n = nrow(filter(data, ac_and_ap == 1))
  no_at_n = nrow(filter(data, no_at == 1))
  
  print(paste("AC only: ", ac_only_n))
  print(paste("AP only: ", ap_only_n))
  print(paste("AC and AP: ", ac_and_ap_n))
  print(paste("No AT: ", no_at_n))
  print(paste("Categories == full data set?", nrow(data) == (ac_only_n + ap_only_n + ac_and_ap_n + no_at_n)))
  
  #create category groupings for mutually exclusive drug categories
  data = mutate(data, drug_cat = case_when(
    ac_only == 1 ~ "ac_only",
    ap_only == 1 ~ "ap_only", 
    ac_and_ap == 1 ~ "ac_and_ap",
    no_at == 1 ~ "no_at"
  )
  )
  
  #PROCESS AGE AND SEX VARIABLES --------------------
  
  #add columns for chadsvasc and hasbled categories inferred from other variables (e.g. age / gender)
  data = mutate(data, female = if_else( sex == "2",1,0), 
                age65_74 = if_else( (age_at_cohort_start < 75) & (age_at_cohort_start > 64),1,0), 
                agegte75 = if_else(age_at_cohort_start >= 75,1,0), 
                agegt65 = if_else(age_at_cohort_start > 65,1,0))
  
  #convert agegte75 stroke binary flag into 2 points for chadsvasc scoring
  data = mutate(data, stroke_chads_pts = if_else ( stroke_chads == 1,2,0), 
                agegte75_pts = if_else (agegte75 == 1,2,0) )
  
  #setup categorical variables 
  
  #age categories
  data = mutate(data, age_cat = case_when(
    (age_at_cohort_start < 75) & (age_at_cohort_start > 64) ~ "65-74",
    age_at_cohort_start >= 75 ~ ">=75", 
    age_at_cohort_start < 65 ~ "<65"
  )
  )
  
  data$age_cat = factor(data$age_cat, levels = c("<65", "65-74", ">=75"))
  
  #setup standardized variable
  data$age_z = (data$age_at_cohort_start - mean(data$age_at_cohort_start)) / sd(data$age_at_cohort_start)
  
  #PROCESS ETHNICITY VARIABLES--------------------
  
  #add ethnicity flags
  data = mutate(data, eth_white = if_else(ethnicity == "White",1,0), 
                eth_asian = if_else(ethnicity == "Asian or Asian British",1,0), 
                eth_black = if_else(ethnicity == "Black or Black British",1,0),
                eth_mixed = if_else(ethnicity == "Mixed",1,0),
                eth_other = if_else(ethnicity == "Other Ethnic Groups",1,0)
  )
  
  #ethnicity categories - set reference category
  data$ethnicity_cat = factor(data$ethnicity, levels = c("White", "Black or Black British", "Asian or Asian British", "Mixed", "Other Ethnic Groups"))
  
  #PROCESS GEOGRAPHIC VARIABLES--------------------
  
  #add region flags
  data = mutate(data, reg_se = if_else(region_name == "South East",1,0), 
                reg_nw = if_else(region_name == "North West",1,0), 
                reg_ee = if_else(region_name == "East of England",1,0),
                reg_sw = if_else(region_name == "South West",1,0),
                reg_yh = if_else(region_name == "Yorkshire and The Humber",1,0),
                reg_wm = if_else(region_name == "West Midlands",1,0),
                reg_em = if_else(region_name == "East Midlands",1,0),
                reg_ln = if_else(region_name == "London",1,0),
                reg_ne = if_else(region_name == "North East",1,0)
  )
  
  #region categories
  data$region_cat = factor(data$region_name, levels = c("South East", "North West", "East of England", "South West", "Yorkshire and The Humber", "West Midlands", "East Midlands", "London", "North East"))
  
  #imd decile categories
  data$imd_decile_cat = factor(data$imd_decile)
  
  #PROCESS AF VARIABLES--------------------
  
  #add AF time since first diagnosis
  
  cohort_start_date = as.Date(gsub("_", "-", index))
  
  #replace individuals where af diagnosis is before date of birth with imputation of mean af_first_diagnosis date
  print(paste("Confirm individuals in data pre af imputation: ", nrow(data)))
  data = mutate(data, af_first_diagnosis = if_else( af_first_diagnosis < date_of_birth,mean(af_first_diagnosis),af_first_diagnosis))
  print(paste("Confirm individuals in data post af imputation: ", nrow(data)))
  
  
  data$yrs_since_af_diagnosis = as.numeric(cohort_start_date - data$af_first_diagnosis) / 365
  
  data = mutate(data, af_lt2yrs = if_else( yrs_since_af_diagnosis < 2,1,0), 
                af_2_to_5yrs = if_else( (yrs_since_af_diagnosis >= 2) & (yrs_since_af_diagnosis < 5),1,0), 
                af_gte5yrs = if_else(yrs_since_af_diagnosis >= 5,1,0) 
  )
  
  #years since af diagnosis categories
  data = mutate(data, yrs_since_af_diagnosis_cat = case_when(
    (yrs_since_af_diagnosis >= 2) & (yrs_since_af_diagnosis < 5) ~ "2-5yrs",
    yrs_since_af_diagnosis < 2 ~ "<2yrs", 
    yrs_since_af_diagnosis >= 5 ~ ">=5yrs"
  )
  )
  
  data$yrs_since_af_diagnosis_cat = factor(data$yrs_since_af_diagnosis_cat, levels = c("<2yrs", "2-5yrs", ">=5yrs"))
  
  #process fall variable
  #replace all n/a's with 0 in fall
  data = data %>% mutate_at(c("fall"), ~replace(., is.na(.), 0))
  
  #PROCESS CHADSVASC VARIABLES--------------------
  
  #replace all n/a's with 0 in chadsvasc component columns
  chads_component_columns = c("vascular_disease_chads", "stroke_chads_pts", "congestive_heart_failure_chads", "diabetes_chads", "hypertension_chads", "female", "age65_74", "agegte75_pts")
  data = data %>% mutate_at(chads_component_columns, ~replace(., is.na(.), 0))
  #also convert binary flag stroke_chads n/a's to 0 - not used in sum
  data = data %>% mutate_at(c("stroke_chads"), ~replace(., is.na(.), 0))
  
  #calculate chadsvasc score for each individual
  data$chadsvasc_score = rowSums(data[, chads_component_columns])
  
  #create categorical variable
  data$chadsvasc_cat = as.factor(data$chadsvasc_score)
  
  print(paste("Avg chadsvasc :", mean(data$chadsvasc_score)))
  
  
  #PROCESS HASBLED VARIABLES--------------------
  
  #replace all n/a's with 0 in hasbled component columns
  hasbled_component_columns = c("renal_disease_hasbled", "liver_disease_hasbled", "stroke_hasbled", "bleeding_hasbled", "alcohol_hasbled", "bleeding_medications_hasbled", "uncontrolled_hypertension_hasbled", "agegt65")
  data = data %>% mutate_at(hasbled_component_columns, ~replace(., is.na(.), 0))
  
  #calculate hasbled score for each individual
  data$hasbled_score = rowSums(data[, hasbled_component_columns])
  
  print(paste("Avg hasbled :", mean(data$hasbled_score)))
  
  #create categorical variable
  data$hasbled_cat = as.factor(data$hasbled_score)
  
  #create category for multi-morbid chadsvasc and hasbled scores
  data = mutate(data, chadsvasc_gte3 = if_else( chadsvasc_score >= 3, 1,0),
                hasbled_gte3 = if_else( hasbled_score >= 3, 1,0),
  )
  
  #PROCESSS OTHER LIFESTYLE VARIABLES --------------------
  
  #bmi - replace na's with mean (confirm best strategy)
  print(paste("Replace (n) individuals with na bmi: ", nrow(filter(data, is.na(bmi)))))
  data = data %>% mutate(data, bmi_impute = if_else( is.na(bmi), mean(data[,"bmi"], na.rm = TRUE),bmi))
  print(paste("Confirm (n) individuals with na bmi: ", nrow(filter(data, is.na(bmi_impute)))))
  
  #setup standardized variable
  data$bmi_z = (data$bmi_impute - mean(data$bmi_impute)) / sd(data$bmi_impute)
  
  #smoking status - replace na's with 0 
  data = data %>% mutate_at(c("smoking_status"), ~replace(., is.na(.), 0))
  
  #APPLY CHADSVASC INCLUSION CRITERIA --------------------
  
  #remove individuals with chadsvasc <2
  print(paste("Confirm individuals in data pre chadsvasc <2 filter: ", nrow(data)))
  print(paste("Remove (n) individuals with chadsvasc <2: ", nrow(filter(data, chadsvasc_score < 2))))
  data_chadsgte2 = data %>% filter(chadsvasc_score >= 2) 
  print(paste("Individuals (n) with chadsvasc >= 2: ", nrow(data_chadsgte2)))
  
  #save as rds
  filename1 = paste("data/CCU020_base_cohort_", index, "_", today_date, ".rds", sep="")
  saveRDS(data_chadsgte2,filename1)
  
  #APPLY COVID INCLUSION CRITERIA (ONLY TO JAN 1ST 2020 COHORT) --------------------
  if (index == "2020_01_01"){
  
    #Pre-vaccine and all (but could be more flexible)
    cut_off_all = "2021-05-01"
    cut_off_pre_vaccine = "2020-12-01"
    cohort_cut_off_dates = c(cut_off_all, cut_off_pre_vaccine)
    
    for (cut_off_date in cohort_cut_off_dates){
      
      #reset cut_off_date as date format (converts back to integer in loops...)
      #set explicitly to default origin of 1970-01-01 if handles as integer 
      cut_off_date = as.Date(cut_off_date,origin="1970-01-01")
      print(cut_off_date)
      #remove individuals without a positive covid diagnosis
      print(paste("Confirm individuals in data with chadsvasc >= 2 pre covid filter: ", nrow(data_chadsgte2)))
      print(paste("Remove (n) individuals without covid: ", nrow(filter(data_chadsgte2, is.na(covid_infection)))))
      data_cov = data_chadsgte2 %>% filter(!is.na(covid_infection)) 
      print(paste("Individuals (n) with chadsvasc >= 2 and covid: ", nrow(data_cov)))
      
      #PROCESS COVID OUTCOME VARIABLES --------------------
      
      #check for any remaining data on covid infection which is after cohort end date and remove if required
      print(paste("Confirm individuals with covid pre cohort end date filter (covid infection date): ", nrow(data_cov)))
      
      data_cov$cohort_end_date = cut_off_date
      
      data_cov = data_cov %>% filter(covid_infection_date <= cohort_end_date)
      
      print(paste("Confirm individuals with covid post cohort end date filter (covid infection date): ", nrow(data_cov)))
      
      #calculate follow-up time
      data_cov = data_cov %>% mutate(fu_date_death = if_else (covid_death == 1, date_of_death, cohort_end_date), 
                                     fu_date_hosp = if_else (covid_hospitalisation == 1, covid_hospitalisation_date, cohort_end_date)
      )
      
      data_cov$fu_time_death <- as.numeric(data_cov$fu_date_death - data_cov$covid_infection_date, units = "days")
      data_cov$fu_time_hosp <- as.numeric(data_cov$fu_date_hosp - data_cov$covid_infection_date, units = "days")
      
      #check for any remaining data on covid outcomes which is after cohort end date and remove if required
      print(paste("Confirm individuals with covid pre cohort end date filter (covid hospitalisation and covid death): ", nrow(data_cov)))
      
      data_cov = data_cov %>% filter(fu_date_death <= cohort_end_date) %>% filter(fu_date_hosp <= cohort_end_date)
      
      print(paste("Confirm individuals with covid post cohort end date filter (covid hospitalisation and covid death): ", nrow(data_cov)))
      
      #add vaccine status
      
      print("Add vaccine status")
      #extract YYYYMMDD (first 8 characters), format for as.Date and convert
      #no longer required due to pre-processing in Databricks
      # clean_dates = function(row){
      #   
      #   if (!is.na(row)){
      #     #print(row)
      #     date = as.character(paste(substr(row, 1, 4), "-", substr(row, 5, 6), "-", substr(row, 7, 8), sep=""))
      #     date_clean = format(as.Date(date,origin="1970-01-01"))
      #     #print(date_clean)
      #     return(date_clean)
      #   } else {
      #     #print(row)
      #     return(NA)
      #   }
      #   
      # }
      # 
      # data_cov$covid_first_vaccine_date_clean = mapply(clean_dates, data_cov$covid_first_vaccine_date)
      
      #set vaccine status to 1 if vaccine before covid infection
      data_cov = data_cov %>% mutate(vaccine_status = if_else ( ( (covid_first_vaccine_date < covid_infection_date) & !is.na(covid_first_vaccine_date) ), 1, 0))
      
      print(paste("Confirm number of individuals in covid data after all transformations", nrow(data_cov)))
      
      #SAVE AND EXPORT DATASETS--------------------
      cohort_end_date_label = gsub("-", "_", cut_off_date)
      filename_cov = paste("data/CCU020_cov_cohort_", index, "_", cohort_end_date_label, "_", today_date, ".rds", sep="")
      saveRDS(data_cov,filename_cov)
   }
  } else {
    print(paste("No covid cohort created for: ", index))
  }
}




