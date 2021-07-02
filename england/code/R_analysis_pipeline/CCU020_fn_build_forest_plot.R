#add documentation

build_forest_plot <- function(multivariable_res, exposure, outcome, analysis="factor", method = "basic", univariable = FALSE) {
  print(paste("Creating forest plot for ", exposure, " + " , outcome, " + ", method, sep=""))
    
  multivariable_res_plot = multivariable_res %>% mutate(
    var_group = case_when(
    (var == "age_z") | (var == "bmi_z") | (var == "female") | (var == "smoking_status") | (substr(var, (nchar(var)-2),nchar(var)) == "yrs") ~ "dem*",
    substr(var, 1,9) == "ethnicity" ~ "eth**",
    substr(var, 1,6) == "region" ~ "reg****",
    substr(var, 1,3) == "imd" ~ "imd***",
    var == "fall" | (substr(var, (nchar(var)-4),nchar(var)) == "chads") | (substr(var, (nchar(var)-6),nchar(var)) == "hasbled") ~ "comorbidity",
    (var == "antihypertensives") | (var == "corticosteroids") | (var == exposure) | (var == "lipid_regulating_drugs") | (var == "nsaids") | (var == "other_immunosuppressants") | (var == "proton_pump_inhibitors") | (var == "vaccine_status") | (var == "prop_score_z") ~ "meds"
  ),
    clean_var = case_when(
      var == "age_z" ~ "Age (SD)",
      var == "bmi_z" ~ "BMI (SD)",
      var == "female" ~ "Female",
      var == "smoking_status" ~ "Smoking status", 
      var == "yrs_since_af_diagnosis_cat2-5yrs" ~ "2-5 years since AF",
      var == "yrs_since_af_diagnosis_cat>=5yrs" ~ ">=5 years since AF",
      var == "ethnicity_catBlack or Black British" ~ "Black or black british", 
      var == "ethnicity_catAsian or Asian British" ~ "Asian or asian british",
      var == "ethnicity_catMixed" ~ "Mixed",
      var == "ethnicity_catOther Ethnic Groups" ~ "Other",
      var == "region_catNorth West" ~ "North West",
      var == "region_catEast of England" ~ "East of England",
      var == "region_catSouth West" ~ "South West",
      var == "region_catYorkshire and The Humber" ~ "The Humber",
      var == "region_catWest Midlands" ~ "West Midlands",
      var == "region_catEast Midlands" ~ "East Midlands",
      var == "region_catLondon" ~ "London",
      var == "region_catNorth East" ~ "North East",
      var == "imd_decile_cat2" ~ "IMD dec 2",
      var == "imd_decile_cat3" ~ "IMD dec 3",
      var == "imd_decile_cat4" ~ "IMD dec 4",
      var == "imd_decile_cat5" ~ "IMD dec 5",
      var == "imd_decile_cat6" ~ "IMD dec 6",
      var == "imd_decile_cat7" ~ "IMD dec 7",
      var == "imd_decile_cat8" ~ "IMD dec 8",
      var == "imd_decile_cat9" ~ "IMD dec 9",
      var == "imd_decile_cat10" ~ "IMD dec 10",
      var == "congestive_heart_failure_chads" ~ "Congestive heart failure",
      var == "hypertension_chads" ~ "Hypertension",
      var == "stroke_chads" ~ "Stroke",
      var == "vascular_disease_chads" ~ "Vascular disease",
      var == "diabetes_chads" ~ "Diabetes",
      var == "renal_disease_hasbled" ~ "Renal disease",
      var == "liver_disease_hasbled" ~ "Liver disease",
      var == "stroke_hasbled" ~ "Stroke",
      var == "alcohol_hasbled" ~ "Hazardous alcohol use",
      var == "bleeding_hasbled" ~ "Major bleeding event",
      var == "uncontrolled_hypertension_hasbled" ~ "Uncontrolled hypertension",
      var == "fall" ~ "Fall",
      var == "antihypertensives" ~ "Antihypertensives", 
      var == "corticosteroids" ~ "Corticosteroids",
      var == "any_at" ~ "Any AT vs no AT", 
      var == "ac_only" ~ "AC vs AP", 
      var == "doacs" ~ "DOACs vs warfarin", 
      var == "lipid_regulating_drugs" ~ "Lipid regulating drugs",
      var == "nsaids" ~ "NSAIDs",
      var == "other_immunosuppressants" ~ "Other immunosuppressants", 
      var == "proton_pump_inhibitors" ~ "Proton pump inhibitors",
      var == "vaccine_status" ~ "Vaccine status", 
      var == "prop_score_z" ~ "AT propensity score (sd)"
    )
  )
  
  #sort title and labels
  
  if (analysis == "factor"){
    
    if ( (outcome == "doacs") ){
      outcome_text = "DOACS vs warfarin"
      exposure_text = "DOACS vs warfarin"
    } else if ( (outcome == "ac_only") ) {
      outcome_text = "AC vs AP"
      exposure_text = "AC vs AP"
    } else if ( (outcome == "any_at") ) {
      outcome_text = "Any AT vs no AT"
      exposure_text = "Any AT vs no AT"
    } else {
      outcome_text = ""
      exposure_text = ""
    }
    
  } else {
    
    if ( (outcome == "covid_death") ){
      outcome_text = "COVID-19 death"
    } else if ( (outcome == "covid_hospitalisation") ){
      outcome_text = "COVID-19 hospitalisation"
    } else {
      outcome_text = ""
    }
    
    if ( (exposure == "doacs") ){
      exposure_text = "DOACS vs warfarin"
    } else if ( (exposure == "ac_only") ) {
      exposure_text = "AC vs AP"
    } else if ( (exposure == "any_at") ) {
      exposure_text = "Any AT vs no AT"
    } else {
      exposure_text = ""
    }
  }
  
    
  #forest plot for results
  if (univariable) {
    title_text = paste("Univariable results for ", outcome_text, sep="")
  } else {
    if (exposure == "n/a"){
      title_text = paste("Multivariable results for ", outcome_text, sep="")
    } else {
      title_text = paste("Multivariable results for ", exposure_text, " on ", outcome_text, sep="")
    }
  }
  
  if (method == "cox"){
    y_label = "Hazard Ratio (95% CI)"
  } else {
    y_label = "Odds Ratio (95% CI)"
  }
  
  forest_plot = ggplot(data=multivariable_res_plot, aes(x=clean_var, y=or, ymin=ci_or_lower, ymax=ci_or_upper)) +
    geom_pointrange() +
    facet_grid(var_group~., scales= "free", space="free") +
    geom_hline(yintercept=1, lty=2) + 
    coord_flip() +  
    xlab("Factor") + ylab(y_label) + labs(title = title_text, caption = "Reference categories , *<2 years since AF, **White ***IMD dec 1 ****South East") + theme_bw()
  
  return_objs = list(multivariable_res_plot, forest_plot)
  
  return(return_objs)
  
}

    