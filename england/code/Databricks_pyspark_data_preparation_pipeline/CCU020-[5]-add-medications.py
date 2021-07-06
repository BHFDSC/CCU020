# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook adds medication data to the study population for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy 
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - For each medication, create a table of individuals who have had a prescription in the last 6 months (flag==1)
# MAGIC 
# MAGIC - Left join the tables into the input study population 
# MAGIC 
# MAGIC **Reviewer(s)** UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 24-05-2021
# MAGIC  
# MAGIC **Date last reviewed** UNREVIEWED
# MAGIC  
# MAGIC **Date last run** 06-05-2021
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC - ccu020_[cohort_start_date]_study_population_af
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC - ccu020_[cohort_start_date]_study_population_af_meds
# MAGIC 
# MAGIC **Software and versions** Python, PySpark
# MAGIC  
# MAGIC **Packages and versions** See notebooks
# MAGIC 
# MAGIC **Next steps**   
# MAGIC 
# MAGIC - Test run new codelist implementation

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU020/CCU020-helper-functions-and-project-parameters

# COMMAND ----------

#add flags to primary meds dataset for list of target drugs for target date range

drugs = ["antiplatelets", "aspirin", "clopidogrel", "dipyridamole", "ticagrelor", "prasugrel", "doacs", "apixaban", "rivaroxaban", "dabigatran", "edoxaban",  "warfarin"]

for drug in drugs:
  spark.sql(f"""
  CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_{drug}_ids AS
  SELECT DISTINCT Person_ID_DEID, (CASE WHEN ProcessingPeriodDate >= '{prescription_history_start_date}' AND ProcessingPeriodDate < '{cohort_start_date}' THEN 1 ELSE 0 END) AS {drug} 
  FROM dars_nic_391419_j3w9t_collab.primary_care_meds_dars_nic_391419_j3w9t_archive 
  WHERE BatchId = '{batch_id}' 
  AND PrescribedBNFCode IN (SELECT code FROM dars_nic_391419_j3w9t_collab.{project_prefix}_codelists WHERE codelist = '{drug}' AND system = 'BNF')
  AND ProcessingPeriodDate >= '{prescription_history_start_date}' AND ProcessingPeriodDate < '{cohort_start_date}'
  """)

# COMMAND ----------

#join to input study population

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMPORARY VIEW {project_prefix}_study_population_af_meds AS
SELECT pop.*, 
ap.antiplatelets AS antiplatelets, 
asp.aspirin AS aspirin,
cl.clopidogrel as clopidogrel,
di.dipyridamole AS dipyridamole,
ti.ticagrelor AS ticagrelor,
pr.prasugrel AS prasugrel,
do.doacs AS doacs,
apx.apixaban AS apixaban,
ri.rivaroxaban AS rivaroxaban,
da.dabigatran AS dabigatran,
ed.edoxaban AS edoxaban,
wf.warfarin AS warfarin
FROM {input_study_population_table2} AS pop
LEFT JOIN global_temp.{project_prefix}_antiplatelets_ids AS ap ON pop.NHS_NUMBER_DEID = ap.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_aspirin_ids AS asp ON pop.NHS_NUMBER_DEID = asp.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_clopidogrel_ids AS cl ON pop.NHS_NUMBER_DEID = cl.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_dipyridamole_ids AS di ON pop.NHS_NUMBER_DEID = di.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_ticagrelor_ids AS ti ON pop.NHS_NUMBER_DEID = ti.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_prasugrel_ids AS pr ON pop.NHS_NUMBER_DEID = pr.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_doacs_ids AS do ON pop.NHS_NUMBER_DEID = do.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_apixaban_ids AS apx ON pop.NHS_NUMBER_DEID = apx.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_rivaroxaban_ids AS ri ON pop.NHS_NUMBER_DEID = ri.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_dabigatran_ids AS da ON pop.NHS_NUMBER_DEID = da.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_edoxaban_ids AS ed ON pop.NHS_NUMBER_DEID = ed.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_warfarin_ids AS wf ON pop.NHS_NUMBER_DEID = wf.Person_ID_DEID
""")

# COMMAND ----------

#load tables for export into pyspark
output_table_name = "global_temp." + project_prefix + "_study_population_af_meds"
outable_table_df = spark.table(output_table_name)

# COMMAND ----------

#create and export tables
export_table_name = project_prefix + "_study_population_af_meds"
create_table_pyspark(outable_table_df, export_table_name)
