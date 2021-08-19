run_regression <- function(exposure, outcome, target_variables, data, method = "basic") {
  print(paste("Running regression for ", exposure, " + " , outcome, " + ", method, sep=""))
  
  #setup regression formula
  covariates = paste(target_variables, collapse = ' + ')
  
  if (method == "basic"){
    
    formula = paste(outcome, " ~ ", covariates, sep="")
    
  } else if (method == "propensity") {
    
    formula = paste(outcome, " ~ ", covariates, " + prop_score_z",  sep="")
    
  } else {
    
    if (outcome == "covid_death"){
      formula = as.formula(paste("Surv(fu_time_death,", outcome, ") ~ ",covariates, " + prop_score_z", sep="")) 
    } else if (outcome == "covid_hospitalisation"){
      formula = as.formula(paste("Surv(fu_time_hosp,", outcome, ") ~ ",covariates, " + prop_score_z", sep="")) 
    } else if (outcome == "covid_death_primary_dx") {
      formula = as.formula(paste("Surv(fu_date_death_primary_dx,", outcome, ") ~ ",covariates, " + prop_score_z", sep=""))
    } else {
      formula = as.formula(paste("Surv(fu_date_hosp_primary_dx,", outcome, ") ~ ",covariates, " + prop_score_z", sep=""))
    }
    
  }
  
  #fit model
  if ( method == "cox" ) {
    
    multi_md = coxph(formula, data = data)
    
  } else {
    
    multi_md = glm(formula = formula,data = data, family = "binomial")
  }
  

  
  #setup output
  if ( method == "cox") { 
    
    var_names = names(exp(multi_md$coefficients)[1:length(multi_md$coefficients)])
    or = unname(exp(multi_md$coefficients)[1:length(multi_md$coefficients)])
    ci_or_lower = unname(exp(confint.default(multi_md))[1:length(multi_md$coefficients), 1])
    ci_or_upper = unname(exp(confint.default(multi_md))[1:length(multi_md$coefficients), 2])
    p_val = unname(coef(summary(multi_md))[,5][1:length(multi_md$coefficients)])
    multivariate_res = data.frame(var=var_names, or=or, ci_or_lower=ci_or_lower, ci_or_upper=ci_or_upper, p=p_val)
    
    
  } else {
    
    var_names = names(exp(multi_md$coefficients)[2:length(multi_md$coefficients)])
    or = unname(exp(multi_md$coefficients)[2:length(multi_md$coefficients)])
    ci_or_lower = unname(exp(confint.default(multi_md))[2:length(multi_md$coefficients), 1])
    ci_or_upper = unname(exp(confint.default(multi_md))[2:length(multi_md$coefficients), 2])
    p_val = unname(coef(summary(multi_md))[,4][2:length(multi_md$coefficients)])
    multivariate_res = data.frame(var=var_names, or=or, ci_or_lower=ci_or_lower, ci_or_upper=ci_or_upper, p=p_val)
    
  }
  
  return(multivariate_res)
}