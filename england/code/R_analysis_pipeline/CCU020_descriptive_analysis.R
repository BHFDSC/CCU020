#clear environment
rm(list = ls())

#Load packages
library(dplyr)
library(data.table)
library(ggplot2)
library(RColorBrewer)

# set target folder
setwd("/mnt/efs/a.handy/dars_nic_391419_j3w9t_collab/CCU020")

#setup categories
drug_cats = c("total", "any_at", "ac_only", "ap_only", "ac_and_ap", "no_at")

drug_ind = c("aspirin", "clopidogrel", "dipyridamole", "ticagrelor", "prasugrel", "apixaban", "rivaroxaban", "dabigatran", "edoxaban", "warfarin")

cat_vars = c("age_cat", "female", "ethnicity_cat", "region_cat", "imd_decile_cat", 
             "vascular_disease_chads", "stroke_chads", "congestive_heart_failure_chads", "diabetes_chads", "hypertension_chads",
             "renal_disease_hasbled", "liver_disease_hasbled", "bleeding_hasbled", "stroke_hasbled", "alcohol_hasbled", "uncontrolled_hypertension_hasbled", "yrs_since_af_diagnosis_cat", "fall")
cont_vars = c("chadsvasc_score", "hasbled_score")

#combine into a list of categories
categories = list(drug_cats, drug_ind)

for (i in 1:length(categories)) {
  splits = categories[[i]]
  
  if ("total" %in% splits){
    cat_flag = "cat"
    title_text = "Individual antithrombotic prescriptions by category Jan 2020 - May 2021"
  } else {
    cat_flag = "ind"
    title_text = "Individual antithrombotic prescriptions by drug Jan 2020 - May 2021"
  }

  # setup time indices that require processing
  time_indices = c("2020_01_01", "2020_07_01", "2021_01_01", "2021_05_01")
  #time_indices = c("2020_01_01")
  
  #setup joint summary table for all indices
  table1_all_time_indices = data.frame()
  
  #set today's date for file versioning
  today_date = format(Sys.time(), "%d_%m_%Y")
  
  for (index in time_indices){
    
    #load the saved data as rds
    input_filename = paste("data/CCU020_base_cohort_", index, "_16_08_2021.rds", sep="")
    data = readRDS(input_filename)
  
    #BUILD SUMMARY CHARACTERISTICS TABLE--------------------
    
    #setup summary table for each index and input data to support get() function  
    df_txt_name = "data"
  
    table1_summary = data.frame()
    
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
      
      #create function to generate clean labels for n and pct columns
      clean_table_text = function(col_n, col_pct) {
        clean_var = paste(col_n, " (", round(col_pct,3)*100, "%)", sep="")
        return(clean_var)
      }
      
      #create function to generate clean labels for mean and sd columns
      clean_cont_text = function(col_mean, col_sd) {
        clean_var = paste(round(col_mean, 1), " (+/- ", round(col_sd,1), ")", sep="")
        return(clean_var)
      }
      
      
      #individuals 
      n_split =  nrow(split_df)
      n_split_pct = nrow(split_df) / denominator
      n_split_clean = clean_table_text(n_split, n_split_pct)
      
      #age
      split_mean_age = mean(split_df$age_at_cohort_start)
      age_sd = sd(split_df$age_at_cohort_start)
      age_clean = clean_cont_text(split_mean_age, age_sd)
      
      #age categories
      age65_74_n = sum(split_df$age65_74) 
      age65_74_pct = age65_74_n / nrow(split_df)
      age65_74_clean = clean_table_text(age65_74_n, age65_74_pct)
      
      agegte75_n = sum(split_df$agegte75) 
      agegte75_pct = agegte75_n / nrow(split_df)
      agegte75_clean = clean_table_text(agegte75_n, agegte75_pct)
      
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
      chadsvasc_sd = sd(split_df$chadsvasc_score)
      chadsvasc_clean = clean_cont_text(chadsvasc_avg, chadsvasc_sd)
      
      split_chadsvasc_scores = split_df %>% group_by(chadsvasc_score) %>% summarise(n = n())
      chadsvasc_2_n = as.numeric(split_chadsvasc_scores[1,2])
      chadsvasc_2_pct = as.numeric(chadsvasc_2_n / nrow(split_df))
      chadsvasc_2_clean = clean_table_text(chadsvasc_2_n, chadsvasc_2_pct)
      
      chadsvasc_3_n = as.numeric(split_chadsvasc_scores[2,2])
      chadsvasc_3_pct = as.numeric(chadsvasc_3_n / nrow(split_df))
      chadsvasc_3_clean = clean_table_text(chadsvasc_3_n, chadsvasc_3_pct)
      
      chadsvasc_4_n = as.numeric(split_chadsvasc_scores[3,2])
      chadsvasc_4_pct = as.numeric(chadsvasc_4_n / nrow(split_df))
      chadsvasc_4_clean = clean_table_text(chadsvasc_4_n, chadsvasc_4_pct)
      
      chadsvasc_5_n = as.numeric(split_chadsvasc_scores[4,2])
      chadsvasc_5_pct = as.numeric(chadsvasc_5_n / nrow(split_df))
      chadsvasc_5_clean = clean_table_text(chadsvasc_5_n, chadsvasc_5_pct)
      
      chadsvasc_6_n = as.numeric(split_chadsvasc_scores[5,2])
      chadsvasc_6_pct = as.numeric(chadsvasc_6_n / nrow(split_df))
      chadsvasc_6_clean = clean_table_text(chadsvasc_6_n, chadsvasc_6_pct)
      
      chadsvasc_7_n = as.numeric(split_chadsvasc_scores[6,2])
      chadsvasc_7_pct = as.numeric(chadsvasc_7_n / nrow(split_df))
      chadsvasc_7_clean = clean_table_text(chadsvasc_7_n, chadsvasc_7_pct)
      
      chadsvasc_8_n = as.numeric(split_chadsvasc_scores[7,2])
      chadsvasc_8_pct = as.numeric(chadsvasc_8_n / nrow(split_df))
      chadsvasc_8_clean = clean_table_text(chadsvasc_8_n, chadsvasc_8_pct)
      
      chadsvasc_9_n = as.numeric(split_chadsvasc_scores[8,2])
      chadsvasc_9_pct = as.numeric(chadsvasc_9_n / nrow(split_df))
      chadsvasc_9_clean = clean_table_text(chadsvasc_9_n, chadsvasc_9_pct)
      
      chadsvasc_6_plus_n = nrow(split_df) - (chadsvasc_2_n + chadsvasc_3_n + chadsvasc_4_n + chadsvasc_5_n)
      chadsvasc_6_plus_pct = 1 - (chadsvasc_2_pct + chadsvasc_3_pct + chadsvasc_4_pct + chadsvasc_5_pct)
      chadsvasc_6_plus_clean = clean_table_text(chadsvasc_6_plus_n, chadsvasc_6_plus_pct)
      
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
      hasbled_sd = sd(split_df$hasbled_score)
      hasbled_clean = clean_cont_text(hasbled_avg, hasbled_sd)
      
      split_hasbled_scores = split_df %>% group_by(hasbled_score) %>% summarise(n = n())
      
      hasbled_score_0_n = as.numeric(split_hasbled_scores[1,2])
      hasbled_score_0_pct = as.numeric(hasbled_score_0_n / nrow(split_df))
      hasbled_score_0_clean = clean_table_text(hasbled_score_0_n, hasbled_score_0_pct)
      
      hasbled_score_1_n = as.numeric(split_hasbled_scores[2,2])
      hasbled_score_1_pct = as.numeric(hasbled_score_1_n / nrow(split_df))
      hasbled_score_1_clean = clean_table_text(hasbled_score_1_n, hasbled_score_1_pct)
      
      hasbled_score_2_n = as.numeric(split_hasbled_scores[3,2])
      hasbled_score_2_pct = as.numeric(hasbled_score_2_n / nrow(split_df))
      hasbled_score_2_clean = clean_table_text(hasbled_score_2_n, hasbled_score_2_pct)
      
      hasbled_score_3_n = as.numeric(split_hasbled_scores[4,2])
      hasbled_score_3_pct = as.numeric(hasbled_score_3_n / nrow(split_df))
      hasbled_score_3_clean = clean_table_text(hasbled_score_3_n, hasbled_score_3_pct)
      
      hasbled_score_4_n = as.numeric(split_hasbled_scores[5,2])
      hasbled_score_4_pct = as.numeric(hasbled_score_4_n / nrow(split_df))
      hasbled_score_4_clean = clean_table_text(hasbled_score_4_n, hasbled_score_4_pct)
      
      hasbled_score_5_n = as.numeric(split_hasbled_scores[6,2])
      hasbled_score_5_pct = as.numeric(hasbled_score_5_n / nrow(split_df))
      hasbled_score_5_clean = clean_table_text(hasbled_score_5_n, hasbled_score_5_pct)
      
      hasbled_score_6_n = as.numeric(split_hasbled_scores[7,2])
      hasbled_score_6_pct = as.numeric(hasbled_score_6_n / nrow(split_df))
      hasbled_score_6_clean = clean_table_text(hasbled_score_6_n, hasbled_score_6_pct)
      
      hasbled_score_7_n = as.numeric(split_hasbled_scores[8,2])
      hasbled_score_7_pct = as.numeric(hasbled_score_7_n / nrow(split_df))
      hasbled_score_7_clean = clean_table_text(hasbled_score_7_n, hasbled_score_7_pct)
      
      hasbled_score_8_n = as.numeric(split_hasbled_scores[9,2])
      hasbled_score_8_pct = as.numeric(hasbled_score_8_n / nrow(split_df))
      hasbled_score_8_clean = clean_table_text(hasbled_score_8_n, hasbled_score_8_pct)
      
      hasbled_score_9_n = as.numeric(split_hasbled_scores[10,2])
      hasbled_score_9_pct = as.numeric(hasbled_score_9_n / nrow(split_df))
      hasbled_score_9_clean = clean_table_text(hasbled_score_9_n, hasbled_score_9_pct)
      
      hasbled_4_plus_n = nrow(split_df) - (hasbled_score_0_n + hasbled_score_1_n + hasbled_score_2_n + hasbled_score_3_n)
      hasbled_4_plus_pct = 1 - (hasbled_score_0_pct + hasbled_score_1_pct + hasbled_score_2_pct + hasbled_score_3_pct)
      hasbled_4_plus_clean = clean_table_text(hasbled_4_plus_n, hasbled_4_plus_pct)
      
      #yrs since af diagnosis
      af_yrs_avg = mean(split_df$yrs_since_af_diagnosis)
      af_yrs_sd = sd(split_df$yrs_since_af_diagnosis)
      af_yrs_clean = clean_cont_text(af_yrs_avg, af_yrs_sd)
      
      af_lt2yrs_n = sum(split_df$af_lt2yrs)
      af_lt2yrs_pct = af_lt2yrs_n / nrow(split_df)
      af_lt2yrs_clean = clean_table_text(af_lt2yrs_n, af_lt2yrs_pct)
      
      af_2_to_5yrs_n = sum(split_df$af_2_to_5yrs)
      af_2_to_5yrs_pct = af_2_to_5yrs_n / nrow(split_df)
      af_2_to_5yrs_clean = clean_table_text(af_2_to_5yrs_n, af_2_to_5yrs_pct)
      
      af_gte5yrs_n = sum(split_df$af_gte5yrs)
      af_gte5yrs_pct = af_gte5yrs_n / nrow(split_df)
      af_gte5yrs_clean = clean_table_text(af_gte5yrs_n, af_gte5yrs_pct)
      
      #fall
      fall_n = sum(split_df$fall)
      fall_pct = fall_n / nrow(split_df)
      fall_clean = clean_table_text(fall_n, fall_pct)
      
      #covid event
      covid_event_n = sum(split_df$covid_infection)
      covid_event_pct = covid_event_n / nrow(split_df)
      covid_event_clean = clean_table_text(covid_event_n, covid_event_pct)
      
      #covid hospitalisation
      covid_hospitalisation_n = sum(split_df$covid_hospitalisation)
      covid_hospitalisation_pct = covid_hospitalisation_n / nrow(split_df)
      covid_hospitalisation_clean = clean_table_text(covid_hospitalisation_n, covid_hospitalisation_pct)
      
      #covid hospitalisation primary dx
      covid_hospitalisation_primary_dx_n = sum(split_df$covid_hospitalisation_primary_dx)
      covid_hospitalisation_primary_dx_pct = covid_hospitalisation_primary_dx_n / nrow(split_df)
      covid_hospitalisation_primary_dx_clean = clean_table_text(covid_hospitalisation_primary_dx_n, covid_hospitalisation_primary_dx_pct)
      
      #covid death
      covid_death_n = sum(split_df$covid_death)
      covid_death_pct = covid_death_n / nrow(split_df)
      covid_death_clean = clean_table_text(covid_death_n, covid_death_pct)
      
      #covid death primary dx
      covid_death_primary_dx_n = sum(split_df$covid_death_primary_dx)
      covid_death_primary_dx_pct = covid_death_primary_dx_n / nrow(split_df)
      covid_death_primary_dx_clean = clean_table_text(covid_death_primary_dx_n, covid_death_primary_dx_pct)
      
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
      bmi_sd = sd(split_df$bmi_impute)
      bmi_clean = clean_cont_text(split_mean_bmi, bmi_sd)
      
      #smoking status
      smoking_status_n = sum(split_df$smoking_status)
      smoking_status_pct = smoking_status_n / nrow(split_df)
      smoking_status_clean = clean_table_text(smoking_status_n, smoking_status_pct)
      
      #vaccine status
      vaccine_status_n = sum(split_df$vaccine_status)
      vaccine_status_pct = vaccine_status_n / nrow(split_df)
      vaccine_status_clean = clean_table_text(vaccine_status_n, vaccine_status_pct)
      
      new_entry = data.frame(time_index = cohort_start_date, 
                             drug_category = split, 
                             individuals_n = n_split, 
                             individuals_pct = n_split_pct, 
                             individuals_clean = n_split_clean,
                             mean_age = split_mean_age,
                             age_sd = age_sd,
                             age_clean = age_clean,
                             age65_74_n = age65_74_n,
                             age65_74_pct = age65_74_pct, 
                             age65_74_clean = age65_74_clean,
                             agegte75_n = agegte75_n, 
                             agegte75_pct = agegte75_pct,
                             agegte75_clean = agegte75_clean,
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
                             chadsvasc_sd = chadsvasc_sd, 
                             chadsvasc_clean = chadsvasc_clean,
                             chadsvasc2 = chadsvasc_2_n,
                             chadsvasc2pct = chadsvasc_2_pct, 
                             chadsvasc_2_clean  = chadsvasc_2_clean,
                             chadsvasc3n = chadsvasc_3_n,
                             chadsvasc3pct = chadsvasc_3_pct,
                             chadsvasc_3_clean = chadsvasc_3_clean,
                             chadsvasc4n = chadsvasc_4_n,
                             chadsvasc4pct = chadsvasc_4_pct,
                             chadsvasc_4_clean = chadsvasc_4_clean,
                             chadsvasc5n = chadsvasc_5_n,
                             chadsvasc5pct = chadsvasc_5_pct,
                             chadsvasc_5_clean = chadsvasc_5_clean,
                             chadsvasc6n = chadsvasc_6_n,
                             chadsvasc6pct = chadsvasc_6_pct,
                             chadsvasc_6_clean = chadsvasc_6_clean,
                             chadsvasc7n = chadsvasc_7_n, 
                             chadsvasc7pct = chadsvasc_7_pct,
                             chadsvasc_7_clean = chadsvasc_7_clean,
                             chadsvasc8n = chadsvasc_8_n,
                             chadsvasc8pct = chadsvasc_8_pct,
                             chadsvasc_8_clean = chadsvasc_8_clean,
                             chadsvasc9n = chadsvasc_9_n,
                             chadsvasc9pct = chadsvasc_9_pct,
                             chadsvasc_9_clean = chadsvasc_9_clean,
                             chadsvasc_6_plus_n = chadsvasc_6_plus_n, 
                             chadsvasc_6_plus_pct = chadsvasc_6_plus_pct, 
                             chadsvasc_6_plus_clean = chadsvasc_6_plus_clean,
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
                             hasbled_sd = hasbled_sd, 
                             hasbled_clean = hasbled_clean,
                             hasbled_score_0_n = hasbled_score_0_n,
                             hasbled_score_0_pct = hasbled_score_0_pct, 
                             hasbled_score_0_clean = hasbled_score_0_clean,
                             hasbled_score_1_n = hasbled_score_1_n,
                             hasbled_score_1_pct = hasbled_score_1_pct, 
                             hasbled_score_1_clean = hasbled_score_1_clean,
                             hasbled_score_2_n = hasbled_score_2_n,
                             hasbled_score_2_pct = hasbled_score_2_pct,
                             hasbled_score_2_clean = hasbled_score_2_clean,
                             hasbled_score_3_n = hasbled_score_3_n,
                             hasbled_score_3_pct = hasbled_score_3_pct,
                             hasbled_score_3_clean = hasbled_score_3_clean,
                             hasbled_score_4_n = hasbled_score_4_n, 
                             hasbled_score_4_pct = hasbled_score_4_pct,
                             hasbled_score_4_clean = hasbled_score_4_clean,
                             hasbled_score_5_n = hasbled_score_5_n, 
                             hasbled_score_5_pct = hasbled_score_5_pct,
                             hasbled_score_5_clean = hasbled_score_5_clean,
                             hasbled_score_6_n = hasbled_score_6_n,
                             hasbled_score_6_pct = hasbled_score_6_pct,
                             hasbled_score_6_clean = hasbled_score_6_clean,
                             hasbled_score_7_n = hasbled_score_7_n,
                             hasbled_score_7_pct = hasbled_score_7_pct,
                             hasbled_score_7_clean = hasbled_score_7_clean,
                             hasbled_score_8_n = hasbled_score_8_n,
                             hasbled_score_8_pct = hasbled_score_8_pct,
                             hasbled_score_8_clean = hasbled_score_8_clean,
                             hasbled_score_9_n = hasbled_score_9_n,
                             hasbled_score_9_pct = hasbled_score_9_pct,
                             hasbled_score_9_clean = hasbled_score_9_clean,
                             hasbled_4_plus_n = hasbled_4_plus_n,
                             hasbled_4_plus_pct = hasbled_4_plus_pct,
                             hasbled_4_plus_clean = hasbled_4_plus_clean,
                             af_yrs_since_diagnosis_avg = af_yrs_avg, 
                             af_yrs_sd = af_yrs_sd, 
                             af_yrs_clean = af_yrs_clean,
                             af_lt2yrs_n = af_lt2yrs_n, 
                             af_lt2yrs_pct = af_lt2yrs_pct, 
                             af_lt2yrs_clean = af_lt2yrs_clean,
                             af_2_to_5yrs_n = af_2_to_5yrs_n, 
                             af_2_to_5yrs_pct = af_2_to_5yrs_pct,
                             af_2_to_5yrs_clean = af_2_to_5yrs_clean,
                             af_gte5yrs_n = af_gte5yrs_n, 
                             af_gte5yrs_pct = af_gte5yrs_pct,
                             af_gte5yrs_clean = af_gte5yrs_clean,
                             fall_n = fall_n,
                             fall_pct = fall_pct,
                             fall_clean = fall_clean,
                             covid_event_n = covid_event_n, 
                             covid_event_pct = covid_event_pct,
                             covid_event_clean = covid_event_clean,
                             covid_hospitalisation_n = covid_hospitalisation_n, 
                             covid_hospitalisation_pct = covid_hospitalisation_pct,
                             covid_hospitalisation_clean = covid_hospitalisation_clean,
                             covid_hospitalisation_primary_dx_n = covid_hospitalisation_primary_dx_n, 
                             covid_hospitalisation_primary_dx_pct = covid_hospitalisation_primary_dx_pct,
                             covid_hospitalisation_primary_dx_clean = covid_hospitalisation_primary_dx_clean,
                             covid_death_n = covid_death_n, 
                             covid_death_pct = covid_death_pct,
                             covid_death_clean = covid_death_clean,
                             covid_death_primary_dx_n = covid_death_primary_dx_n, 
                             covid_death_primary_dx_pct = covid_death_primary_dx_pct,
                             covid_death_primary_dx_clean = covid_death_primary_dx_clean,
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
                             bmi_sd = bmi_sd, 
                             bmi_clean = bmi_clean,
                             smoking_status_n = smoking_status_n,
                             smoking_status_pct = smoking_status_pct,
                             smoking_status_clean = smoking_status_clean,
                             vaccine_status_n = vaccine_status_n, 
                             vaccine_status_pct = vaccine_status_pct,
                             vaccine_status_clean = vaccine_status_clean
      )
      
      table1_summary = rbind(table1_summary, new_entry)
    }
      
    
    #add table summary to all years table for prescribing trends over time
    table1_all_time_indices = rbind(table1_all_time_indices, table1_summary)
    
    #export raw summary table for each index
    table1_index_filename = paste("output/summary_characteristics_by_medication_raw_",cat_flag,"_",index,"_", today_date, ".csv", sep="")
    write.csv(table1_summary, table1_index_filename, row.names=F, quote=F)
    
    #export polished summary table for manuscript
    table1_summary_t = setNames(data.frame(t(table1_summary[,-2])), table1_summary[,2])
  
    target_rows = c("individuals_clean", "age_clean", "age65_74_clean", "agegte75_clean",
                    "female_clean", "eth_white_clean", "eth_asian_clean", "eth_black_clean",
                    "eth_mixed_clean", "eth_other_clean", "reg_se_clean", "reg_nw_clean", "reg_ee_clean",
                    "reg_sw_clean", "reg_yh_clean", "reg_wm_clean", "reg_em_clean", "reg_ln_clean", "reg_ne_clean",
                    "imd_1_clean", "imd_2_clean", "imd_3_clean","imd_4_clean", "imd_5_clean", "imd_6_clean", 
                    "imd_7_clean", "imd_8_clean", "imd_9_clean", "imd_10_clean",
                    "vascular_disease_chads_clean", "stroke_chads_clean", "chf_chads_clean", "diabetes_chads_clean",
                    "hypertension_chads_clean", "chadsvasc_clean", "chadsvasc_2_clean", "chadsvasc_3_clean", "chadsvasc_4_clean", 
                    "chadsvasc_5_clean", "chadsvasc_6_clean", "chadsvasc_7_clean", "chadsvasc_8_clean", "chadsvasc_9_clean","chadsvasc_6_plus_clean",
                    "renal_disease_hasbled_clean", "liver_disease_hasbled_clean", "stroke_hasbled_clean", "bleeding_hasbled_clean", 
                    "alcohol_hasbled_clean", "bleeding_medications_hasbled_clean", "uncontrolled_hypertension_hasbled_clean",
                    "hasbled_clean", "hasbled_score_0_clean", "hasbled_score_1_clean","hasbled_score_2_clean",
                    "hasbled_score_3_clean","hasbled_score_4_clean","hasbled_score_5_clean","hasbled_score_6_clean",
                    "hasbled_score_7_clean","hasbled_score_8_clean","hasbled_score_9_clean", "hasbled_4_plus_clean","af_yrs_clean",
                    "af_lt2yrs_clean", "af_2_to_5yrs_clean", "af_gte5yrs_clean", "fall_clean", 
                    "covid_event_clean", "covid_hospitalisation_clean", "covid_hospitalisation_primary_dx_clean","covid_death_clean","covid_death_primary_dx_clean",
                    "antihypertensives_clean", "lipid_regulating_drugs_clean", "insulin_clean", "sulphonylurea_clean",
                    "metformin_clean", "other_diabetic_drugs_clean", "proton_pump_inhibitors_clean", "nsaids_clean", "corticosteroids_clean",
                    "other_immunosuppressants_clean", "bmi_clean", "smoking_status_clean", "vaccine_status_clean")
    
    table1_clean = table1_summary_t[c(target_rows), ]
    
    print(table1_clean)
    table1_clean_index_filename = paste("output/summary_characteristics_by_medication_clean_",cat_flag,"_",index,"_", today_date, ".csv", sep="")
    write.csv(table1_clean, table1_clean_index_filename, row.names=T, quote=F)
    
    
    #RUN CHI SQUARED TESTS FOR TARGET VARIABLES ACROSS DRUG CATEGORIES--------------------
    #SLOW TO RUN - SO ONLY UNCOPY IF / WHEN NEED
    
    #only run for mutually exclusive drug categories
    
    # if (cat_flag == "cat"){
    #   chi_summary = data.frame()
    #   
    #   #for categorical variables - build contingency tables and apply chi squared test
    #   print("Starting chi squared calculations for categorical variables")
    #   
    #   for (var in cat_vars){
    #     drug_cat_txt = "drug_cat"
    #     var_data = get(df_txt_name)[var]
    #     cont_tab = table(unlist(var_data), data$drug_cat)
    #     print(cont_tab)
    #     chisq <- chisq.test(cont_tab)
    #     
    #     #add var name and p-value to an output table
    #     chi_entry = data.frame(var_name = var, p_value = chisq$p.value)
    #     print(chi_entry)
    #     
    #     chi_summary = rbind(chi_summary, chi_entry)
    #     
    #   }
    #   
    #   #for continuous variables - create data frame and apply kruskal and wallis test
    #   print("Starting chi squared calculations for continuous variables")
    #   
    #   for (var in cont_vars){
    #     chi_formula = as.formula(paste(var, "~ drug_cat"))
    #     print(chi_formula)
    #     
    #     #apply kruskal and wallis test
    #     kt = kruskal.test(chi_formula, data = data)
    #     
    #     #add var name and p-value to an output table
    #     chi_entry = data.frame(var_name = var, p_value = kt$p.value)
    #     
    #     chi_summary = rbind(chi_summary, chi_entry)
    #     
    #   }
    #   
    #   #export chi summary table for each index
    #   chi_summary_index_filename = paste("output/chi_summary_table_by_medication_",cat_flag,"_",index,"_", today_date, ".csv", sep="")
    #   write.csv(chi_summary, chi_summary_index_filename, row.names=F, quote=F)
    #   
    # }
    
  }
  #end of time indices loop
  
  #BUILD PRESCRIBING TRENDS BY MEDICATION CATEGORY CHART --------------------
  
  #check output of all time indices table
  print(table1_all_time_indices)
  
  #setup table for chart
  table_for_chart = table1_all_time_indices %>% select(time_index, drug_category, individuals_pct, individuals_n) %>% filter( drug_category != "any_at") %>% filter ( drug_category != "total")
  table_for_chart$individuals_pct = round(table_for_chart$individuals_pct, 3) * 100
  
  #filter out categories with < 1 pct of total prescriptions 
  table_for_chart = table_for_chart %>% filter(individuals_pct > 1)
  
  #tidy drug category labels
  table_for_chart = table_for_chart %>% mutate(drug_category = 
                                                 case_when(drug_category == "ac_only" ~ "AC only",
                                                           drug_category == "ap_only" ~ "AP only", 
                                                           drug_category == "ac_and_ap" ~ "AC and AP",
                                                           drug_category == "no_at" ~ "No AT",
                                                           drug_category == "aspirin" ~ "Aspirin",
                                                           drug_category == "apixaban" ~ "Apixaban",
                                                           drug_category == "rivaroxaban" ~ "Rivaroxaban",
                                                           drug_category == "edoxaban" ~ "Edoxaban",
                                                           drug_category == "warfarin" ~ "Warfarin",
                                                           drug_category == "clopidogrel" ~ "Clopidogrel",
                                                           drug_category == "dabigatran" ~ "Dabigatran"
                                                           
                                                 ))
  
  #order by largest drug category first
  table_for_chart$drug_category <- reorder(table_for_chart$drug_category, table_for_chart$individuals_pct)
  table_for_chart$drug_category <- factor(table_for_chart$drug_category, levels=levels(table_for_chart$drug_category))
  
  #treat output dates as factors so evenly spaced in chart
  table_for_chart$time_index = as.factor(table_for_chart$time_index)
  
  #tidy drug category title
  colnames(table_for_chart)[2] = "Drug category"
  
  #save table so chart can be replotted in R shiny app
  table_for_chart_filename = paste("output/table_for_all_time_indices_chart_",cat_flag,"_", today_date, ".csv", sep="")
  write.csv(table_for_chart, table_for_chart_filename, row.names=T, quote=F)
  
  #plot and save figures
  
  plot_filename = paste("output/prescribing_trends_", cat_flag,"_",today_date,".pdf", sep="")
  fig1 = ggplot(table_for_chart, aes(y = individuals_pct, x = time_index, fill = `Drug category`, label = paste(individuals_pct, "%", sep=""))) + geom_bar(stat="identity") + geom_text(size = 3, position = position_stack(vjust = 0.5)) + scale_fill_brewer(palette="Blues") + labs(title=title_text,x ="Time index", y = "Individuals %") + theme_bw() + theme(axis.text.y=element_blank(), axis.ticks.y=element_blank(), panel.grid.major = element_blank(), panel.grid.minor = element_blank(),panel.background = element_blank(), axis.line = element_line(colour = "black"))
  ggsave(plot_filename, fig1, device = "pdf", width = 10, height = 8)
  
}







