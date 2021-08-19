#clear environment
rm(list = ls())

#Load packages
library(dplyr)
library(data.table)
library(corrplot)
library(ggplot2)

# set target folder
setwd("/mnt/efs/a.handy/dars_nic_391419_j3w9t_collab/CCU020")

#load functions
source("code/CCU020_fn_run_regression.R", echo = TRUE)
source("code/CCU020_fn_build_forest_plot.R", echo = TRUE)
source("code/CCU020_fn_correlation_check.R", echo = TRUE)

# setup time indices that require processing
#time_indices = c("2020_01_01", "2020_07_01", "2021_01_01", "2021_05_01")
time_indices = c("2020_01_01")

#set today's date for file versioning
today_date = format(Sys.time(), "%d_%m_%Y")

#run regression analyses for each time index and save as a csv
for (index in time_indices){
  
  #load the saved data
  input_filename = paste("data/CCU020_base_cohort_", index, "_16_08_2021.rds", sep="")
  data = readRDS(input_filename)
  
  
  #define target variables to include in the univariable regression
  target_variables = c("age_z", "female", "ethnicity_cat", "region_cat", "imd_decile_cat",
                       "congestive_heart_failure_chads", "hypertension_chads", "vascular_disease_chads", "diabetes_chads",
                       "renal_disease_hasbled", "liver_disease_hasbled", "alcohol_hasbled", "stroke_hasbled", "bleeding_hasbled", "uncontrolled_hypertension_hasbled", "fall",
                       "antihypertensives", "lipid_regulating_drugs", "proton_pump_inhibitors", "nsaids", "corticosteroids", "other_immunosuppressants", "bmi_z", "smoking_status")
  
  #for correlation test
  target_variables_numeric = c("age_z", "female", "eth_white", "eth_asian", "eth_black", "eth_mixed", "eth_other", "reg_se","reg_nw", "reg_ee", "reg_sw", "reg_yh", "reg_wm", "reg_em", "reg_ln", "reg_ne",
                               "imd_decile","congestive_heart_failure_chads", "hypertension_chads", "vascular_disease_chads", "diabetes_chads",
                               "renal_disease_hasbled", "liver_disease_hasbled", "alcohol_hasbled","stroke_hasbled", "bleeding_hasbled", "uncontrolled_hypertension_hasbled", "fall", 
                               "antihypertensives", "lipid_regulating_drugs", "proton_pump_inhibitors", "nsaids", "corticosteroids", "other_immunosuppressants", "bmi_z", "smoking_status")
  
  
  #run correlation test
  
  cor_data = prep_correlation_check(data, target_variables_numeric)
  corrplot_filename = paste("output/factor_corr_check_", index, "_", today_date, ".png", sep="")
  png(height=1200, width=1500, pointsize=15, file=corrplot_filename)
  corrplot(cor_data, method="number", type="upper", tl.col="black")
  dev.off()
  
  #NOTE: logic relies on this order due to data subsetting logic below 
  outcomes = c("any_at", "ac_only", "doacs")
  
  for (outcome in outcomes){
    
    #subset data for relevant outcome
    
    #check rows before filter
    print(paste("Rows prior to filter for ", outcome, " :", nrow(data)))
    
    if (outcome == "ac_only"){
      
      data = data %>% filter(any_at == 1) %>% filter(ac_and_ap == 0)
      
      
    } else if (outcome == "doacs"){
      
      data = data %>% filter(ac_only == 1) %>% filter(ac_and_ap == 0) %>% filter( !((warfarin == 1) & (doacs == 1)) )
      
    } else {
      
      data = data
    }
    
    #check that data filtering has worked for the exposure
    print(paste("Rows post filter for ", outcome, " :", nrow(data)))
    
    #RUN univariable REGRESSION ANALYSIS --------------------
    #VERY TIME CONSUMING - ONLY RUN IF NEED
    # univariable_res = data.frame()
    # 
    # for (var in target_variables){
    #   formula = paste(outcome, " ~ ", var, sep="")
    #   print(formula)
    #   md = glm(formula=formula, data = data, family = "binomial")
    # 
    #   var_names = names(exp(md$coefficients)[2:length(md$coefficients)])
    #   or = unname(exp(md$coefficients)[2:length(md$coefficients)])
    #   ci_or_lower = unname(exp(confint.default(md))[2:length(md$coefficients), 1])
    #   ci_or_upper = unname(exp(confint.default(md))[2:length(md$coefficients), 2])
    # 
    #   p_val = unname(coef(summary(md))[,4][2:length(md$coefficients)])
    #   entry = data.frame(var=var_names, or=or, ci_or_lower=ci_or_lower, ci_or_upper=ci_or_upper, p=p_val)
    #   univariable_res = rbind(univariable_res, entry)
    # }
    # 
    # univariable_res$p_sig = univariable_res$p < 0.05
    # 
    # #forest plot for univariable results
    # univariable_outputs = build_forest_plot(univariable_res, exposure = "n/a", outcome = outcome, analysis = "factor", method = "basic", univariable = TRUE)
    # 
    # univariable_forest_plot_table = univariable_outputs[[1]]
    # univariable_forest_plot_graphic = univariable_outputs[[2]]
    # 
    # #export univariable results table for each index
    # univariable_table_filename = paste("output/factor_univariable_res_table_", outcome, "_", index, "_", today_date, ".csv", sep="")
    # write.csv(univariable_forest_plot_table, univariable_table_filename, row.names=F, quote=F)
    # 
    # #export forest plot for each index
    # univariable_forest_plot_filename = paste("output/factor_univariable_forest_plot_", outcome, "_", index, "_", today_date, ".png", sep="")
    # ggsave(univariable_forest_plot_filename, univariable_forest_plot_graphic)
    
    # pdf(file = univariable_forest_plot_filename,width = 10,height = 8)
    # print(forest_plot_univariable)
    # dev.off()
    
    #RUN multivariable REGRESSION ANALYSIS --------------------
    multivariable_res = run_regression(exposure = "n/a", outcome = outcome, target_variables = target_variables, data = data, method = "basic")
    
    multivariable_outputs = build_forest_plot(multivariable_res, exposure = "n/a", outcome = outcome, analysis = "factor", method = "basic", univariable = FALSE)
    
    multivariable_forest_plot_table = multivariable_outputs[[1]]
    multivariable_forest_plot_graphic = multivariable_outputs[[2]]
    
    #export multivariable results table for each index
    multivariable_table_filename = paste("output/factor_multivariable_res_table_", outcome, "_", index, "_", today_date, ".csv", sep="")
    write.csv(multivariable_forest_plot_table, multivariable_table_filename, row.names=F, quote=F)
    
    #export forest plot for each index
    multivariable_forest_plot_filename = paste("output/factor_multivariable_forest_plot_", outcome, "_", index, "_", today_date, ".png", sep="")
    ggsave(multivariable_forest_plot_filename, multivariable_forest_plot_graphic, device = "png", width = 7, height = 8)
    
  }
  
}








