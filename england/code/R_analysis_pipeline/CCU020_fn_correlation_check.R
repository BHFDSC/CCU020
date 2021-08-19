library(corrplot)
prep_correlation_check <- function(data, target_variables) {
  print("Building correlation check plot")
  num_data = data %>% select(target_variables) 
  original_headers = colnames(num_data)
  headers = data.frame(original_headers)
  
  headers = headers %>% mutate(
    clean_headers = case_when(
      original_headers == "age_z" ~ "Age (SD)",
      original_headers == "bmi_z" ~ "BMI (SD)",
      original_headers == "female" ~ "Female",
      original_headers == "smoking_status" ~ "Smoking status", 
      original_headers == "af_lt2yrs" ~ "<2 years since AF",
      original_headers == "af_2_to_5yrs" ~ "2-5 years since AF",
      original_headers == "af_gte5yrs" ~ ">=5 years since AF",
      original_headers == "eth_white" ~ "White or white british",
      original_headers == "eth_black" ~ "Black or black british", 
      original_headers == "eth_asian" ~ "Asian or asian british",
      original_headers == "eth_mixed" ~ "Mixed",
      original_headers == "eth_other" ~ "Other",
      original_headers == "reg_se" ~ "South East",
      original_headers == "reg_nw" ~ "North West",
      original_headers == "reg_ee" ~ "East of England",
      original_headers == "reg_sw" ~ "South West",
      original_headers == "reg_yh" ~ "The Humber",
      original_headers == "reg_wm" ~ "West Midlands",
      original_headers == "reg_em" ~ "East Midlands",
      original_headers == "reg_ln" ~ "London",
      original_headers == "reg_ne" ~ "North East",
      original_headers == "imd_decile" ~ "IMD decile",
      original_headers == "congestive_heart_failure_chads" ~ "Congestive heart failure",
      original_headers == "hypertension_chads" ~ "Hypertension",
      original_headers == "stroke_chads" ~ "Stroke",
      original_headers == "vascular_disease_chads" ~ "Vascular disease",
      original_headers == "diabetes_chads" ~ "Diabetes",
      original_headers == "renal_disease_hasbled" ~ "Renal disease",
      original_headers == "liver_disease_hasbled" ~ "Liver disease",
      original_headers == "alcohol_hasbled" ~ "Hazardous alcohol use",
      original_headers == "stroke_hasbled" ~ "Stroke",
      original_headers == "bleeding_hasbled" ~ "Major bleeding event",
      original_headers == "uncontrolled_hypertension_hasbled" ~ "Uncontrolled hypertension",
      original_headers == "fall" ~ "Fall",
      original_headers == "antihypertensives" ~ "Antihypertensives", 
      original_headers == "corticosteroids" ~ "Corticosteroids",
      original_headers == "any_at" ~ "Any AT vs no AT", 
      original_headers == "ac_only" ~ "AC vs AP", 
      original_headers == "doacs" ~ "DOACs vs warfarin", 
      original_headers == "lipid_regulating_drugs" ~ "Lipid regulating drugs",
      original_headers == "nsaids" ~ "NSAIDs",
      original_headers == "other_immunosuppressants" ~ "Other immunosuppressants", 
      original_headers == "proton_pump_inhibitors" ~ "Proton pump inhibitors",
      original_headers == "vaccine_status" ~ "Vaccine status"
    )
  )
  
  colnames(num_data) = headers$clean_headers
  
  cor_data = cor(num_data, method = "pearson", use = "complete.obs")
  
  return(cor_data)
  
}