# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook runs each step of the pipeline (across multiple notebooks) to build the study population for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - Create a set of study parameters that can be fed into the pipeline as variables (e.g. cohort start date)  
# MAGIC 
# MAGIC - Pipeline runs sequentially with each cell triggering a notebook that conducts a specific processing step in the pipeline
# MAGIC 
# MAGIC **Reviewer(s)** UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 19-08-2021
# MAGIC  
# MAGIC **Date last reviewed** UNREVIEWED
# MAGIC  
# MAGIC **Date last run** 19-08-2021
# MAGIC  
# MAGIC **Data input** Notebooks in CCU020
# MAGIC 
# MAGIC **Data output** ccu020_[cohort_start_date]_study_population_cov on collab data storage

# COMMAND ----------

#Step 1: Create codelist table
dbutils.notebook.run("CCU020-[1]-create-codelist-table", 36000)

# COMMAND ----------

#Step 2: Build unassembled and individual presence tables
dbutils.notebook.run("CCU020-[2]-build-unassembled-and-presence-tables", 36000)

# COMMAND ----------

#Step 3: Build study population with most recent basic demographic data
dbutils.notebook.run("CCU020-[3]-build-study-population", 36000)

# COMMAND ----------

#Step 4: Add chadsvasc and hasbled risk scores and AF phenotypes
dbutils.notebook.run("CCU020-[4]-add-risk-scores-and-af", 36000)

# COMMAND ----------

#Step 5: Add medications phenotypes
dbutils.notebook.run("CCU020-[5]-add-medications", 36000)

# COMMAND ----------

#Step 6: Add COVID specific phenotypes and covariates
dbutils.notebook.run("CCU020-[6]-add-covid-phenotypes", 36000)

# COMMAND ----------


