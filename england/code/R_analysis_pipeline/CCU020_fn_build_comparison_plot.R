build_comparison_plot <- function(exp_comp_data, method = "basic"){
  print(paste("Creating comparison forest plot for ", method, " multivariable regression", sep=""))
  
  exposures_ordered = c("DOACs vs warfarin", "AC vs AP", "Any AT vs no AT")
  exp_comp_tmp = exp_comp_data
  exp_comp_tmp$clean_var <-factor(exp_comp_tmp$clean_var, levels=exposures_ordered)
  
  if (method == "basic"){
    title_text = "Comparison of exposures on COVID-19 outcomes (Multivariable regression)"
    forest_plot_exp_comp_tmp = ggplot(data=exp_comp_tmp, aes(x=clean_var, y=or, ymin=ci_or_lower, ymax=ci_or_upper)) +
      geom_pointrange() +
      facet_grid(outcome~., scales= "free", space="free") +
      geom_hline(yintercept=1, lty=2) + 
      coord_flip() + 
      xlab("Exposure") + ylab("Odds Ratio (95% CI)") + labs(title = title_text) + geom_text(label=round(exp_comp_tmp$or, 2), nudge_x=0.2) + theme_bw()
  } else if (method == "propensity") {
    title_text = "Comparison of exposures on COVID-19 outcomes (Multivariable regression with propensity score)"
    forest_plot_exp_comp_tmp = ggplot(data=exp_comp_tmp, aes(x=clean_var, y=or, ymin=ci_or_lower, ymax=ci_or_upper)) +
      geom_pointrange() +
      facet_grid(outcome~., scales= "free", space="free") +
      geom_hline(yintercept=1, lty=2) + 
      coord_flip() + 
      xlab("Exposure") + ylab("Odds Ratio (95% CI)") + labs(title = title_text) + geom_text(label=round(exp_comp_tmp$or, 2), nudge_x=0.2) + theme_bw()
  } else {
    title_text = "Comparison of exposures on COVID-19 outcomes (Cox regression)"
    forest_plot_exp_comp_tmp = ggplot(data=exp_comp_tmp, aes(x=clean_var, y=or, ymin=ci_or_lower, ymax=ci_or_upper)) +
      geom_pointrange() +
      facet_grid(outcome~., scales= "free", space="free") +
      geom_hline(yintercept=1, lty=2) + 
      coord_flip() + 
      xlab("Exposure") + ylab("Hazard Ratio (95% CI)") + labs(title = title_text) + geom_text(label=round(exp_comp_tmp$or, 2), nudge_x=0.2) + theme_bw()
  }
  
  
  return_objs = list(exp_comp_tmp, forest_plot_exp_comp_tmp)
  
  return(return_objs)
}