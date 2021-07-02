rm(list = ls())

#Load packages
library(dplyr)
library(data.table)
library(ggplot2)
library(corrplot)
library(survival)
library(survminer)
library(scales)

# set target folder
setwd("/mnt/efs/a.handy/dars_nic_391419_j3w9t_collab/CCU020")

#load functions
source("code/CCU020_fn_run_regression.R", echo = TRUE)
source("code/CCU020_fn_build_forest_plot.R", echo = TRUE)
source("code/CCU020_fn_correlation_check.R", echo = TRUE)
source("code/CCU020_fn_build_comparison_plot.R", echo = TRUE)

# setup time indices that require processing
cohort_start_dates = c("2020_01_01")
cohort_end_dates = c("2021_05_01", "2020_12_01")
#cohort_end_dates = c("2021_05_01")

#set today's date for file versioning
today_date = format(Sys.time(), "%d_%m_%Y")

#setup categories
splits = c("total", "any_at", "ac_only", "ap_only", "ac_and_ap", "no_at")

for (cut_off in cohort_end_dates){
  for (index in cohort_start_dates){
    print(paste("Starting analysis for cohort starting:", index, "and finishing: ", cut_off))
    
    #load data
    input_filename = paste("data/CCU020_cov_cohort_", index, "_", cut_off, "_02_07_2021.rds", sep="")
    data = readRDS(input_filename)
    
    print(paste("Individuals for covid analysis: ", nrow(data)))
    
    #BUILD SUMMARY CHARACTERISTICS TABLE--------------------
    
    #setup summary table for each index and input data to support get() function  
    df_txt_name = "data"
    
    table2_summary = data.frame()
    
    #set dynamic denominator to allow for switch to individual drugs
    
    #calculate denominator across splits
    sum_list = c()
    
    for(split in splits) {
      if ((split != "total") & (split != "any_at")){
        new_value <- sum(get(df_txt_name)[split])
        sum_list <- c(sum_list, new_value)
      } else{
        print("dont add to list")
      }
    }
    denominator = Reduce(f = "+", x = sum_list, accumulate = FALSE)
    
    
    for (split in splits){
      print(split)
      
      #handle case of no split
      if (split == "total"){
        split_df = get(df_txt_name)
      } else {
        split_df = get(df_txt_name) %>% filter(get(df_txt_name)[split] == 1)
      }
      
      
      #set cohort start date for time index in table
      cohort_start_date = as.Date(gsub("_", "-", index))
      
      #set cohort end date for time index in table
      cohort_end_date = as.Date(gsub("_", "-", cut_off))
      
      #create function to generate clean labels for n and pct columns
      clean_table_text = function(col_n, col_pct) {
        clean_var = paste(col_n, " (", round(col_pct,3)*100, "%)", sep="")
        return(clean_var)
      }
      
      #individuals 
      n_split =  nrow(split_df)
      n_split_pct = nrow(split_df) / denominator
      n_split_clean = clean_table_text(n_split, n_split_pct)
      
      #age
      split_mean_age = mean(split_df$age_at_cohort_start)
      
      #sex
      female_n = sum(split_df$female)
      female_pct = female_n / nrow(split_df)
      female_clean = clean_table_text(female_n, female_pct)
      
      #ethnicity categories
      eth_white_n = sum(split_df$eth_white)
      eth_white_pct = eth_white_n / nrow(split_df)
      eth_white_clean = clean_table_text(eth_white_n, eth_white_pct)
      
      eth_asian_n = sum(split_df$eth_asian)
      eth_asian_pct = eth_asian_n / nrow(split_df)
      eth_asian_clean = clean_table_text(eth_asian_n, eth_asian_pct)
      
      eth_black_n = sum(split_df$eth_black)
      eth_black_pct = eth_black_n / nrow(split_df)
      eth_black_clean = clean_table_text(eth_black_n, eth_black_pct)
      
      eth_mixed_n = sum(split_df$eth_mixed)
      eth_mixed_pct = eth_mixed_n / nrow(split_df)
      eth_mixed_clean = clean_table_text(eth_mixed_n, eth_mixed_pct)
      
      eth_other_n = sum(split_df$eth_other)
      eth_other_pct = eth_other_n / nrow(split_df)
      eth_other_clean = clean_table_text(eth_other_n, eth_other_pct)
      
      #geographic categories
      reg_se_n = sum(split_df$reg_se)
      reg_se_pct = reg_se_n / nrow(split_df)
      reg_se_clean = clean_table_text(reg_se_n, reg_se_pct)
      
      reg_nw_n = sum(split_df$reg_nw)
      reg_nw_pct = reg_nw_n / nrow(split_df)
      reg_nw_clean = clean_table_text(reg_nw_n, reg_nw_pct)
      
      reg_ee_n = sum(split_df$reg_ee)
      reg_ee_pct = reg_ee_n / nrow(split_df)
      reg_ee_clean = clean_table_text(reg_ee_n, reg_ee_pct)
      
      reg_sw_n = sum(split_df$reg_sw)
      reg_sw_pct = reg_sw_n / nrow(split_df)
      reg_sw_clean = clean_table_text(reg_sw_n, reg_sw_pct)
      
      reg_yh_n = sum(split_df$reg_yh)
      reg_yh_pct = reg_yh_n / nrow(split_df)
      reg_yh_clean = clean_table_text(reg_yh_n, reg_yh_pct)
      
      reg_wm_n = sum(split_df$reg_wm)
      reg_wm_pct = reg_wm_n / nrow(split_df)
      reg_wm_clean = clean_table_text(reg_wm_n, reg_wm_pct)
      
      reg_em_n = sum(split_df$reg_em)
      reg_em_pct = reg_em_n / nrow(split_df)
      reg_em_clean = clean_table_text(reg_em_n, reg_em_pct)
      
      reg_ln_n = sum(split_df$reg_ln)
      reg_ln_pct = reg_ln_n / nrow(split_df)
      reg_ln_clean = clean_table_text(reg_ln_n, reg_ln_pct)
      
      reg_ne_n = sum(split_df$reg_ne)
      reg_ne_pct = reg_ne_n / nrow(split_df)
      reg_ne_clean = clean_table_text(reg_ne_n, reg_ne_pct)
      
      #imd deciles
      split_imd_scores = split_df %>% group_by(imd_decile) %>% summarise(n = n())
      
      imd_1_n = as.numeric(split_imd_scores[1,2])
      imd_1_pct = as.numeric(imd_1_n / nrow(split_df))
      imd_1_clean = clean_table_text(imd_1_n, imd_1_pct)
      
      imd_2_n = as.numeric(split_imd_scores[2,2])
      imd_2_pct = as.numeric(imd_2_n / nrow(split_df))
      imd_2_clean = clean_table_text(imd_2_n, imd_2_pct)
      
      imd_3_n = as.numeric(split_imd_scores[3,2])
      imd_3_pct = as.numeric(imd_3_n / nrow(split_df))
      imd_3_clean = clean_table_text(imd_3_n, imd_3_pct)
      
      imd_4_n = as.numeric(split_imd_scores[4,2])
      imd_4_pct = as.numeric(imd_4_n / nrow(split_df))
      imd_4_clean = clean_table_text(imd_4_n, imd_4_pct)
      
      imd_5_n = as.numeric(split_imd_scores[5,2])
      imd_5_pct = as.numeric(imd_5_n / nrow(split_df))
      imd_5_clean = clean_table_text(imd_5_n, imd_5_pct)
      
      imd_6_n = as.numeric(split_imd_scores[6,2])
      imd_6_pct = as.numeric(imd_6_n / nrow(split_df))
      imd_6_clean = clean_table_text(imd_6_n, imd_6_pct)
      
      imd_7_n = as.numeric(split_imd_scores[7,2])
      imd_7_pct = as.numeric(imd_7_n / nrow(split_df))
      imd_7_clean = clean_table_text(imd_7_n, imd_7_pct)
      
      imd_8_n = as.numeric(split_imd_scores[8,2])
      imd_8_pct = as.numeric(imd_8_n / nrow(split_df))
      imd_8_clean = clean_table_text(imd_8_n, imd_8_pct)
      
      imd_9_n = as.numeric(split_imd_scores[9,2])
      imd_9_pct = as.numeric(imd_9_n / nrow(split_df))
      imd_9_clean = clean_table_text(imd_9_n, imd_9_pct)
      
      imd_10_n = as.numeric(split_imd_scores[10,2])
      imd_10_pct = as.numeric(imd_10_n / nrow(split_df))
      imd_10_clean = clean_table_text(imd_10_n, imd_10_pct)
      
      #chadsvasc disease components 
      vascular_disease_chads_n = sum(split_df$vascular_disease_chads)
      vascular_disease_chads_pct = vascular_disease_chads_n / nrow(split_df)
      vascular_disease_chads_clean = clean_table_text(vascular_disease_chads_n, vascular_disease_chads_pct)
      
      stroke_chads_n = sum(split_df$stroke_chads)
      stroke_chads_pct = stroke_chads_n / nrow(split_df)
      stroke_chads_clean = clean_table_text(stroke_chads_n, stroke_chads_pct)
      
      chf_chads_n = sum(split_df$congestive_heart_failure_chads)
      chf_chads_pct = chf_chads_n / nrow(split_df)
      chf_chads_clean = clean_table_text(chf_chads_n, chf_chads_pct)
      
      diabetes_chads_n = sum(split_df$diabetes_chads)
      diabetes_chads_pct = diabetes_chads_n / nrow(split_df)
      diabetes_chads_clean = clean_table_text(diabetes_chads_n, diabetes_chads_pct)
      
      hypertension_chads_n = sum(split_df$hypertension_chads)
      hypertension_chads_pct = hypertension_chads_n / nrow(split_df)
      hypertension_chads_clean = clean_table_text(hypertension_chads_n, hypertension_chads_pct)
      
      #chadsvasc score
      chadsvasc_avg = mean(split_df$chadsvasc_score)
      
      #hasbled disease components
      renal_disease_hasbled_n = sum(split_df$renal_disease_hasbled)
      renal_disease_hasbled_pct = renal_disease_hasbled_n / nrow(split_df)
      renal_disease_hasbled_clean = clean_table_text(renal_disease_hasbled_n, renal_disease_hasbled_pct)
      
      liver_disease_hasbled_n = sum(split_df$liver_disease_hasbled)
      liver_disease_hasbled_pct = liver_disease_hasbled_n / nrow(split_df)
      liver_disease_hasbled_clean = clean_table_text(liver_disease_hasbled_n, liver_disease_hasbled_pct)
      
      stroke_hasbled_n = sum(split_df$stroke_hasbled)
      stroke_hasbled_pct = stroke_hasbled_n / nrow(split_df)
      stroke_hasbled_clean = clean_table_text(stroke_hasbled_n, stroke_hasbled_pct)
      
      bleeding_hasbled_n = sum(split_df$bleeding_hasbled)
      bleeding_hasbled_pct = bleeding_hasbled_n / nrow(split_df)
      bleeding_hasbled_clean = clean_table_text(bleeding_hasbled_n, bleeding_hasbled_pct)
      
      alcohol_hasbled_n = sum(split_df$alcohol_hasbled)
      alcohol_hasbled_pct = alcohol_hasbled_n / nrow(split_df)
      alcohol_hasbled_clean = clean_table_text(alcohol_hasbled_n, alcohol_hasbled_pct)
      
      # labile_inr_hasbled_n = sum(split_df$labile_inr_hasbled)
      # labile_inr_hasbled_pct = labile_inr_hasbled_n / nrow(split_df)
      # labile_inr_hasbled_clean = clean_table_text(labile_inr_hasbled_n, labile_inr_hasbled_pct)
      
      bleeding_medications_hasbled_n = sum(split_df$bleeding_medications_hasbled)
      bleeding_medications_hasbled_pct = bleeding_medications_hasbled_n / nrow(split_df)
      bleeding_medications_hasbled_clean = clean_table_text(bleeding_medications_hasbled_n, bleeding_medications_hasbled_pct)
      
      uncontrolled_hypertension_hasbled_n = sum(split_df$uncontrolled_hypertension_hasbled)
      uncontrolled_hypertension_hasbled_pct = uncontrolled_hypertension_hasbled_n / nrow(split_df)
      uncontrolled_hypertension_hasbled_clean = clean_table_text(uncontrolled_hypertension_hasbled_n, uncontrolled_hypertension_hasbled_pct)
      
      #hasbled score
      hasbled_avg = mean(split_df$hasbled_score)
      
      #covid hospitalisation
      covid_hospitalisation_n = sum(split_df$covid_hospitalisation)
      covid_hospitalisation_pct = covid_hospitalisation_n / nrow(split_df)
      covid_hospitalisation_clean = clean_table_text(covid_hospitalisation_n, covid_hospitalisation_pct)
      
      #covid death
      covid_death_n = sum(split_df$covid_death)
      covid_death_pct = covid_death_n / nrow(split_df)
      covid_death_clean = clean_table_text(covid_death_n, covid_death_pct)
      
      #antihypertensives
      antihypertensives_n = sum(split_df$antihypertensives)
      antihypertensives_pct = antihypertensives_n / nrow(split_df)
      antihypertensives_clean = clean_table_text(antihypertensives_n, antihypertensives_pct)
      
      #lipid regulating drugs
      lipid_regulating_drugs_n = sum(split_df$lipid_regulating_drugs)
      lipid_regulating_drugs_pct = lipid_regulating_drugs_n / nrow(split_df)
      lipid_regulating_drugs_clean = clean_table_text(lipid_regulating_drugs_n, lipid_regulating_drugs_pct)
      
      #insulin
      insulin_n = sum(split_df$insulin)
      insulin_pct = insulin_n / nrow(split_df)
      insulin_clean = clean_table_text(insulin_n, insulin_pct)

      #sulphonylurea
      sulphonylurea_n = sum(split_df$sulphonylurea)
      sulphonylurea_pct = sulphonylurea_n / nrow(split_df)
      sulphonylurea_clean = clean_table_text(sulphonylurea_n, sulphonylurea_pct)

      #metformin
      metformin_n = sum(split_df$metformin)
      metformin_pct = metformin_n / nrow(split_df)
      metformin_clean = clean_table_text(metformin_n, metformin_pct)

      #other_diabetic_drugs
      other_diabetic_drugs_n = sum(split_df$other_diabetic_drugs)
      other_diabetic_drugs_pct = other_diabetic_drugs_n / nrow(split_df)
      other_diabetic_drugs_clean = clean_table_text(other_diabetic_drugs_n, other_diabetic_drugs_pct)
      
      #proton_pump_inhibitors
      proton_pump_inhibitors_n = sum(split_df$proton_pump_inhibitors)
      proton_pump_inhibitors_pct = proton_pump_inhibitors_n / nrow(split_df)
      proton_pump_inhibitors_clean = clean_table_text(proton_pump_inhibitors_n, proton_pump_inhibitors_pct)
      
      #nsaids
      nsaids_n = sum(split_df$nsaids)
      nsaids_pct = nsaids_n / nrow(split_df)
      nsaids_clean = clean_table_text(nsaids_n, nsaids_pct)
      
      #corticosteroids
      corticosteroids_n = sum(split_df$corticosteroids)
      corticosteroids_pct = corticosteroids_n / nrow(split_df)
      corticosteroids_clean = clean_table_text(corticosteroids_n, corticosteroids_pct)
      
      #other_immunosuppressants
      other_immunosuppressants_n = sum(split_df$other_immunosuppressants)
      other_immunosuppressants_pct = other_immunosuppressants_n / nrow(split_df)
      other_immunosuppressants_clean = clean_table_text(other_immunosuppressants_n, other_immunosuppressants_pct)
      
      #bmi
      split_mean_bmi = mean(split_df$bmi_impute)
      
      #smoking status
      smoking_status_n = sum(split_df$smoking_status)
      smoking_status_pct = smoking_status_n / nrow(split_df)
      smoking_status_clean = clean_table_text(smoking_status_n, smoking_status_pct)
      
      #vaccine status
      vaccine_status_n = sum(split_df$vaccine_status)
      vaccine_status_pct = vaccine_status_n / nrow(split_df)
      vaccine_status_clean = clean_table_text(vaccine_status_n, vaccine_status_pct)
      
      new_entry = data.frame(cohort_start_date = cohort_start_date, 
                             cohort_end_date = cohort_end_date,
                             drug_category = split, 
                             individuals_n = n_split, 
                             individuals_pct = n_split_pct, 
                             individuals_clean = n_split_clean,
                             mean_age = split_mean_age,
                             female_n = female_n,
                             female_pct = female_pct,
                             female_clean = female_clean,
                             eth_white_n = eth_white_n,
                             eth_white_pct = eth_white_pct,
                             eth_white_clean = eth_white_clean,
                             eth_asian_n = eth_asian_n,
                             eth_asian_pct = eth_asian_pct,
                             eth_asian_clean = eth_asian_clean,
                             eth_black_n = eth_black_n, 
                             eth_black_pct = eth_black_pct,
                             eth_black_clean = eth_black_clean,
                             eth_mixed_n = eth_mixed_n,
                             eth_mixed_pct = eth_mixed_pct,
                             eth_mixed_clean = eth_mixed_clean,
                             eth_other_n = eth_other_n,
                             eth_other_pct = eth_other_pct,
                             eth_other_clean = eth_other_clean,
                             reg_se_n = reg_se_n, 
                             reg_se_pct = reg_se_pct,
                             reg_se_clean = reg_se_clean,
                             reg_nw_n = reg_nw_n,
                             reg_nw_pct = reg_nw_pct,
                             reg_nw_clean = reg_nw_clean, 
                             reg_ee_n = reg_ee_n,
                             reg_ee_pct = reg_ee_pct,
                             reg_ee_clean = reg_ee_clean,
                             reg_sw_n = reg_sw_n,
                             reg_sw_pct = reg_sw_pct,
                             reg_sw_clean = reg_sw_clean, 
                             reg_yh_n = reg_yh_n,
                             reg_yh_pct = reg_yh_pct,
                             reg_yh_clean = reg_yh_clean,
                             reg_wm_n = reg_wm_n,
                             reg_wm_pct = reg_wm_pct,
                             reg_wm_clean = reg_wm_clean,
                             reg_em_n = reg_em_n,
                             reg_em_pct = reg_em_pct,
                             reg_em_clean = reg_em_clean,
                             reg_ln_n = reg_ln_n,
                             reg_ln_pct = reg_ln_pct,
                             reg_ln_clean = reg_ln_clean,
                             reg_ne_n = reg_ne_n,
                             reg_ne_pct = reg_ne_pct,
                             reg_ne_clean = reg_ne_clean,
                             imd_1_n = imd_1_n, 
                             imd_1_pct = imd_1_pct, 
                             imd_1_clean = imd_1_clean,
                             imd_2_n = imd_2_n,
                             imd_2_pct = imd_2_pct,
                             imd_2_clean = imd_2_clean,
                             imd_3_n = imd_3_n,
                             imd_3_pct = imd_3_pct,
                             imd_3_clean = imd_3_clean,
                             imd_4_n = imd_4_n,
                             imd_4_pct = imd_4_pct,
                             imd_4_clean = imd_4_clean,
                             imd_5_n = imd_5_n,
                             imd_5_pct = imd_5_pct,
                             imd_5_clean = imd_5_clean,
                             imd_6_n = imd_6_n,
                             imd_6_pct = imd_6_pct,
                             imd_6_clean = imd_6_clean,
                             imd_7_n = imd_7_n,
                             imd_7_pct = imd_7_pct,
                             imd_7_clean = imd_7_clean,
                             imd_8_n = imd_8_n,
                             imd_8_pct = imd_8_pct,
                             imd_8_clean  = imd_8_clean,
                             imd_9_n = imd_9_n,
                             imd_9_pct = imd_9_pct,
                             imd_9_clean = imd_9_clean,
                             imd_10_n = imd_10_n,
                             imd_10_pct = imd_10_pct,
                             imd_10_clean = imd_10_clean,
                             vascular_disease_chads_n = vascular_disease_chads_n,
                             vascular_disease_chads_pct = vascular_disease_chads_pct,
                             vascular_disease_chads_clean = vascular_disease_chads_clean,
                             stroke_chads_n = stroke_chads_n,
                             stroke_chads_pct = stroke_chads_pct, 
                             stroke_chads_clean = stroke_chads_clean,
                             chf_chads_n = chf_chads_n, 
                             chf_chads_pct = chf_chads_pct, 
                             chf_chads_clean = chf_chads_clean,
                             diabetes_chads_n = diabetes_chads_n, 
                             diabetes_chads_pct = diabetes_chads_pct, 
                             diabetes_chads_clean = diabetes_chads_clean,
                             hypertension_chads_n = hypertension_chads_n, 
                             hypertension_chads_pct = hypertension_chads_pct,
                             hypertension_chads_clean = hypertension_chads_clean,
                             mean_chadsvasc = chadsvasc_avg,
                             renal_disease_hasbled_n = renal_disease_hasbled_n, 
                             renal_disease_hasbled_pct = renal_disease_hasbled_pct,
                             renal_disease_hasbled_clean = renal_disease_hasbled_clean,
                             liver_disease_hasbled_n = liver_disease_hasbled_n,
                             liver_disease_hasbled_pct = liver_disease_hasbled_pct, 
                             liver_disease_hasbled_clean = liver_disease_hasbled_clean,
                             stroke_hasbled_n = stroke_hasbled_n, 
                             stroke_hasbled_pct = stroke_hasbled_pct, 
                             stroke_hasbled_clean = stroke_hasbled_clean,
                             bleeding_hasbled_n = bleeding_hasbled_n,
                             bleeding_hasbled_pct = bleeding_hasbled_pct,
                             bleeding_hasbled_clean = bleeding_hasbled_clean,
                             alcohol_hasbled_n = alcohol_hasbled_n, 
                             alcohol_hasbled_pct = alcohol_hasbled_pct, 
                             alcohol_hasbled_clean = alcohol_hasbled_clean,
                             bleeding_medications_hasbled_n = bleeding_medications_hasbled_n, 
                             bleeding_medications_hasbled_pct = bleeding_medications_hasbled_pct,
                             bleeding_medications_hasbled_clean = bleeding_medications_hasbled_clean,
                             uncontrolled_hypertension_hasbled_n = uncontrolled_hypertension_hasbled_n, 
                             uncontrolled_hypertension_hasbled_pct = uncontrolled_hypertension_hasbled_pct,
                             uncontrolled_hypertension_hasbled_clean = uncontrolled_hypertension_hasbled_clean,
                             mean_hasbled = hasbled_avg,
                             covid_hospitalisation_n = covid_hospitalisation_n, 
                             covid_hospitalisation_pct = covid_hospitalisation_pct,
                             covid_hospitalisation_clean = covid_hospitalisation_clean,
                             covid_death_n = covid_death_n, 
                             covid_death_pct = covid_death_pct,
                             covid_death_clean = covid_death_clean,
                             antihypertensives_n = antihypertensives_n, 
                             antihypertensives_pct = antihypertensives_pct,
                             antihypertensives_clean = antihypertensives_clean,
                             lipid_regulating_drugs_n = lipid_regulating_drugs_n, 
                             lipid_regulating_drugs_pct = lipid_regulating_drugs_pct,
                             lipid_regulating_drugs_clean = lipid_regulating_drugs_clean,
                             insulin_n = insulin_n, 
                             insulin_pct = insulin_pct,
                             insulin_clean = insulin_clean,
                             sulphonylurea_n = sulphonylurea_n, 
                             sulphonylurea_pct = sulphonylurea_pct,
                             sulphonylurea_clean = sulphonylurea_clean,
                             metformin_n = metformin_n, 
                             metformin_pct = metformin_pct, 
                             metformin_clean = metformin_clean,
                             other_diabetic_drugs_n = other_diabetic_drugs_n, 
                             other_diabetic_drugs_pct = other_diabetic_drugs_pct, 
                             other_diabetic_drugs_clean = other_diabetic_drugs_clean,
                             proton_pump_inhibitors_n = proton_pump_inhibitors_n, 
                             proton_pump_inhibitors_pct = proton_pump_inhibitors_pct,
                             proton_pump_inhibitors_clean = proton_pump_inhibitors_clean,
                             nsaids_n = nsaids_n, 
                             nsaids_pct = nsaids_pct,
                             nsaids_clean = nsaids_clean,
                             corticosteroids_n = corticosteroids_n, 
                             corticosteroids_pct = corticosteroids_pct, 
                             corticosteroids_clean = corticosteroids_clean,
                             other_immunosuppressants_n = other_immunosuppressants_n, 
                             other_immunosuppressants_pct = other_immunosuppressants_pct, 
                             other_immunosuppressants_clean = other_immunosuppressants_clean,
                             mean_bmi = split_mean_bmi,
                             smoking_status_n = smoking_status_n,
                             smoking_status_pct = smoking_status_pct,
                             smoking_status_clean = smoking_status_clean,
                             vaccine_status_n = vaccine_status_n, 
                             vaccine_status_pct = vaccine_status_pct,
                             vaccine_status_clean
                             )
      
      table2_summary = rbind(table2_summary, new_entry)
    }
    
    
    #export summary table for each index
    table2_index_filename = paste("output/summary_characteristics_cov_raw_",index,"_", cut_off, "_", today_date, ".csv", sep="")
    write.csv(table2_summary, table2_index_filename, row.names=F, quote=F)
    
    #export polished summary table for manuscript
    table2_summary_t = setNames(data.frame(t(table2_summary[,-3])), table2_summary[,3])
    
    target_rows = c("individuals_clean", "mean_age", "female_clean", "eth_white_clean", "eth_asian_clean", "eth_black_clean",
                    "eth_mixed_clean", "eth_other_clean", "reg_se_clean", "reg_nw_clean", "reg_ee_clean",
                    "reg_sw_clean", "reg_yh_clean", "reg_wm_clean", "reg_em_clean", "reg_ln_clean", "reg_ne_clean",
                    "imd_1_clean", "imd_2_clean", "imd_3_clean","imd_4_clean", "imd_5_clean", "imd_6_clean", 
                    "imd_7_clean", "imd_8_clean", "imd_9_clean", "imd_10_clean","vascular_disease_chads_clean", "stroke_chads_clean", "chf_chads_clean", "diabetes_chads_clean",
                    "hypertension_chads_clean", "mean_chadsvasc", "renal_disease_hasbled_clean", "liver_disease_hasbled_clean", "stroke_hasbled_clean", "bleeding_hasbled_clean", 
                    "alcohol_hasbled_clean", "bleeding_medications_hasbled_clean", "uncontrolled_hypertension_hasbled_clean",
                    "mean_hasbled","covid_hospitalisation_clean", "covid_death_clean",
                    "antihypertensives_clean", "lipid_regulating_drugs_clean", "insulin_clean", "sulphonylurea_clean",
                    "metformin_clean", "other_diabetic_drugs_clean", "proton_pump_inhibitors_clean", "nsaids_clean", "corticosteroids_clean",
                    "other_immunosuppressants_clean", "mean_bmi", "smoking_status_clean", "vaccine_status_clean")
    
    table2_clean = table2_summary_t[c(target_rows), ]
    
    print(table2_clean)
    table2_clean_index_filename = paste("output/summary_characteristics_cov_clean_",index,"_", cut_off, "_", today_date, ".csv", sep="")
    write.csv(table2_clean, table2_clean_index_filename, row.names=T, quote=F)
  
    #RUN MULTIVARIABLE LOGISTIC REGRESSION FOR COVID DEATHS AND HOSPITALISATION --------------------
    
    #NOTE: logic relies on this order due to data subsetting logic below 
    exposures = c("any_at", "ac_only", "doacs")
    outcomes = c("covid_death", "covid_hospitalisation")
    
    #setup tables for exposure comparison
    exp_comp_basic = data.frame()
    exp_comp_prop = data.frame()
    exp_comp_cox = data.frame()
    
    for (exposure in exposures){
      
      #check rows before filter
      print(paste("Rows prior to filter for ", exposure, " :", nrow(data)))
      
      #subset data for relevant exposure
      if (exposure == "ac_only"){
        
        data = data %>% filter(any_at == 1) %>% filter(ac_and_ap == 0)
        
        
      } else if (exposure == "doacs"){
        
        data = data %>% filter(ac_only == 1) %>% filter(ac_and_ap == 0) %>% filter( !((warfarin == 1) & (doacs == 1)) )
        
      } else {
        
        data = data
      }
      
      #check that data filtering has worked for the exposure
      print(paste("Rows post filter for ", exposure, " :", nrow(data)))
      
      target_variables = c(exposure, "age_z", "female", "ethnicity_cat", "region_cat", "imd_decile_cat",
                           "congestive_heart_failure_chads", "hypertension_chads", "vascular_disease_chads", "diabetes_chads", 
                           "liver_disease_hasbled", "renal_disease_hasbled", "alcohol_hasbled", "stroke_hasbled", "bleeding_hasbled", "uncontrolled_hypertension_hasbled", "antihypertensives", "lipid_regulating_drugs", 
                           "proton_pump_inhibitors", "nsaids", "corticosteroids", "other_immunosuppressants", "bmi_z", "smoking_status", "vaccine_status")
      
      #for correlation analysis
      target_variables_numeric = c("age_z", "female", "eth_white", "eth_asian", "eth_black", "eth_mixed", "eth_other", "reg_se","reg_nw", "reg_ee", "reg_sw", "reg_yh", "reg_wm", "reg_em", "reg_ln", "reg_ne",
                                   "imd_decile","congestive_heart_failure_chads", "hypertension_chads", "vascular_disease_chads", "diabetes_chads",
                                   "liver_disease_hasbled", "renal_disease_hasbled", "alcohol_hasbled","stroke_hasbled", "bleeding_hasbled", "uncontrolled_hypertension_hasbled","antihypertensives", "lipid_regulating_drugs", 
                                   "proton_pump_inhibitors", "nsaids", "corticosteroids", "other_immunosuppressants", "bmi_z", "smoking_status", "vaccine_status")
      
      #remove vaccine status variable from analysis where no vaccines have been administered
      if (cohort_end_date <= as.Date("2020-12-01")) {
        target_variables = target_variables[-length(target_variables)]
        target_variables_numeric = target_variables_numeric[-length(target_variables_numeric)]
      } else {
        print("Vaccine status kept in analysis")
      }
      
      #run correlation test
      
      cor_data = prep_correlation_check(data, target_variables_numeric)
      corrplot_filename = paste("output/covid_corr_check_", exposure, "_", index, "_", cut_off, "_", today_date, ".png", sep="")
      png(height=1200, width=1500, pointsize=15, file=corrplot_filename)
      corrplot(cor_data, method="number", type="upper", tl.col="black")
      dev.off()
      
      for (outcome in outcomes){
        
        #RUN BASIC REGRESSION ANALYSIS --------------------
        
        multivariable_res = run_regression(exposure, outcome, target_variables, data, method = "basic")
        
        #build forest plot
        multivariable_basic_outputs = build_forest_plot(multivariable_res, exposure, outcome, analysis = "covid", method = "basic")
        multivariable_basic_forest_plot_table = multivariable_basic_outputs[[1]]
        multivariable_basic_forest_plot_graphic = multivariable_basic_outputs[[2]]
        
        #export multivariable results table for each index
        multivariable_table_filename = paste("output/covid_multivariable_res_basic_table_", exposure, "_", outcome, "_", index, "_", cut_off, "_", today_date, ".csv", sep="")
        write.csv(multivariable_basic_forest_plot_table, multivariable_table_filename, row.names=F, quote=F)
        
        #export forest plot for each index
        multivariable_forest_plot_filename = paste("output/covid_multivariable_basic_forest_plot_", exposure, "_", outcome, "_", index, "_", cut_off, "_", today_date, ".png", sep="")
        ggsave(multivariable_forest_plot_filename, multivariable_basic_forest_plot_graphic, device = "png", width = 9, height = 10)
        
        #select just the current exposure and outcome result
        exp_res_basic = multivariable_basic_forest_plot_table %>% filter(var == exposure)
        exp_res_basic$outcome = gsub("_", " ", outcome)
        exp_comp_basic = rbind(exp_comp_basic, exp_res_basic)
        
        #RUN PROPENSITY SCORE ANALYSIS --------------------
        
        #estimate propensity score
        prop_covariates = paste(target_variables[2:length(target_variables)], collapse = ' + ')
        prop_formula = paste(exposure, " ~ ", prop_covariates, sep="")
        prop_md = glm(formula = prop_formula,data = data, family = "binomial")
        
        data$prop_score <- predict(prop_md, type = "response")
        
        #plot distribution of propensity scores
        df_txt_name = "data"
        exposure_actual = get(df_txt_name)[exposure]
        prs_df = data.frame(pr_score = data$prop_score,
                             exposure_actual = exposure_actual)
        
        labs = c(paste("Exposed to: ", exposure), paste("Not exposed to: ", exposure))
        propensity_dist_plot = prs_df %>%
          mutate(exposure_actual = ifelse(exposure_actual == 1, labs[1], labs[2])) %>%
          ggplot(aes(x = pr_score)) +
          geom_histogram(color = "white") +
          facet_wrap(~exposure_actual) +
          xlab(paste("Probability of exposure to ", exposure)) +
          theme_bw()
        
        #export propensity distribution plot 
        propensity_dist_plot_filename = paste("output/covid_propensity_distribution_plot_", exposure, "_", outcome, "_", index, "_", cut_off, "_", today_date, ".png", sep="")
        ggsave(propensity_dist_plot_filename, propensity_dist_plot)
        
        #fit new model with propensity score variable
        data$prop_score_z = (data$prop_score - mean(data$prop_score)) / sd(data$prop_score)
        
        multivariable_res_prop = run_regression(exposure, outcome, target_variables, data, method = "propensity")
        
        #build forest plot
        multivariable_prop_outputs = build_forest_plot(multivariable_res_prop, exposure, outcome, analysis = "covid", method = "propensity")
        multivariable_prop_forest_plot_table = multivariable_prop_outputs[[1]]
        multivariable_prop_forest_plot_graphic = multivariable_prop_outputs[[2]]
        
        #export multivariable results table for each index
        multivariable_prop_table_filename = paste("output/covid_multivariable_res_prop_table_", exposure, "_", outcome, "_", index, "_", cut_off, "_", today_date, ".csv", sep="")
        write.csv(multivariable_prop_forest_plot_table, multivariable_prop_table_filename, row.names=F, quote=F)
        
        #export forest plot for each index
        multivariable_prop_forest_plot_filename = paste("output/covid_multivariable_prop_forest_plot_", exposure, "_", outcome, "_", index, "_", cut_off, "_",today_date, ".png", sep="")
        ggsave(multivariable_prop_forest_plot_filename, multivariable_prop_forest_plot_graphic, device = "png", width = 9, height = 10)
        
        #select just the current exposure and outcome result
        exp_res_prop = multivariable_prop_forest_plot_table %>% filter(var == exposure)
        exp_res_prop$outcome = gsub("_", " ", outcome)
        exp_comp_prop = rbind(exp_comp_prop, exp_res_prop)
        
        #RUN TIME TO EVENT ANALYSIS --------------------
      
        #fit cox model and create summary table
        multivariable_res_cox = run_regression(exposure, outcome, target_variables, data, method = "cox")
        
        #build forest plot
        multivariable_cox_outputs = build_forest_plot(multivariable_res_cox, exposure, outcome, analysis = "covid", method = "cox")
        multivariable_cox_forest_plot_table = multivariable_cox_outputs[[1]]
        multivariable_cox_forest_plot_graphic = multivariable_cox_outputs[[2]]
        
        #export multivariable results table for each index
        multivariable_cox_table_filename = paste("output/covid_multivariable_res_cox_table_", exposure, "_", outcome, "_", index, "_", cut_off, "_", today_date, ".csv", sep="")
        write.csv(multivariable_cox_forest_plot_table, multivariable_cox_table_filename, row.names=F, quote=F)
        
        #export forest plot for each index
        multivariable_cox_forest_plot_filename = paste("output/covid_multivariable_cox_forest_plot_", exposure, "_", outcome, "_", index, "_", cut_off, "_", today_date, ".png", sep="")
        ggsave(multivariable_cox_forest_plot_filename, multivariable_cox_forest_plot_graphic, device = "png", width = 9, height = 10)
        
        #select just the current exposure and outcome result
        exp_res_cox = multivariable_cox_forest_plot_table %>% filter(var == exposure)
        exp_res_cox$outcome = gsub("_", " ", outcome)
        exp_comp_cox = rbind(exp_comp_cox, exp_res_cox)
        
      }
      #end outcomes loop
      
    }
    #end exposures loop
    
    #CREATE EXPOSURE COMPARISON EXHIBITS --------------------
    print("Create exposure comparison exhibits")
    
    #build comparison table for basic multivariable regression
    exp_comp_basic_outputs = build_comparison_plot(exp_comp_basic, method = "basic")
    exp_comp_basic_plot_table = exp_comp_basic_outputs[[1]]
    exp_comp_basic_plot_graphic = exp_comp_basic_outputs[[2]]
    
    #export forest plot input table
    exp_comp_basic_forest_plot_table_filename = paste("output/covid_exp_comp_basic_forest_plot_table_", index, "_", cut_off, "_", today_date, ".csv", sep="")
    write.csv(exp_comp_basic_plot_table, exp_comp_basic_forest_plot_table_filename)
    
    #export forest plot for each time index (if required)
    exp_comp_basic_forest_plot_filename = paste("output/covid_exp_comp_basic_forest_plot_", index, "_", cut_off, "_", today_date, ".png", sep="")
    ggsave(exp_comp_basic_forest_plot_filename, exp_comp_basic_plot_graphic, device = "png", width = 11, height = 6)
    
    #build comparison table for multivariable regression with propensity score
    exp_comp_prop_outputs = build_comparison_plot(exp_comp_prop, method = "propensity")
    exp_comp_prop_plot_table = exp_comp_prop_outputs[[1]]
    exp_comp_prop_plot_graphic = exp_comp_prop_outputs[[2]]
    
    #export forest plot input table
    exp_comp_prop_forest_plot_table_filename = paste("output/covid_exp_comp_prop_forest_plot_table_", index, "_", cut_off, "_", today_date, ".csv", sep="")
    write.csv(exp_comp_prop_plot_table, exp_comp_prop_forest_plot_table_filename)
    
    #export forest plot for each time index (if required)
    exp_comp_prop_forest_plot_filename = paste("output/covid_exp_comp_prop_forest_plot_", index, "_", cut_off, "_", today_date, ".png", sep="")
    ggsave(exp_comp_prop_forest_plot_filename, exp_comp_prop_plot_graphic, device = "png", width = 11, height = 6)
    
    #build comparison table for cox regression
    exp_comp_cox_outputs = build_comparison_plot(exp_comp_cox, method = "cox")
    exp_comp_cox_plot_table = exp_comp_cox_outputs[[1]]
    exp_comp_cox_plot_graphic = exp_comp_cox_outputs[[2]]
    
    #export forest plot input table
    exp_comp_cox_forest_plot_table_filename = paste("output/covid_exp_comp_cox_forest_plot_table_", index, "_", cut_off, "_", today_date, ".csv", sep="")
    write.csv(exp_comp_cox_plot_table, exp_comp_cox_forest_plot_table_filename)
    
    #export forest plot 
    exp_comp_cox_forest_plot_filename = paste("output/covid_exp_comp_cox_forest_plot_", index, "_", cut_off, "_", today_date, ".png", sep="")
    ggsave(exp_comp_cox_forest_plot_filename, exp_comp_cox_plot_graphic, device = "png", width = 11, height = 6)
    
  }
  #end cohort start loop
}
#end cohort cut off loop



