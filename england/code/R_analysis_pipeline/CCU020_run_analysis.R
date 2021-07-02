#add any documentation...
#TO-DO: restructure question order and underlying functions

rm(list = ls())

#ingest data from databricks and process for analysis
source("code/CCU020_process_data.R", echo = TRUE)

#run descriptive analysis and create exhibits
source("code/CCU020_descriptive_analysis.R", echo = TRUE)

#run factor analysis and create exhibits
source("code/CCU020_factor_analysis.R", echo = TRUE)

#run covid analysis and create exhibits
source("code/CCU020_covid_analysis.R", echo = TRUE)

