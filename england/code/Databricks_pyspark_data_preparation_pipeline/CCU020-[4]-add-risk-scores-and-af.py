# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook adds risk scores (Cha2ds2-vasc and HAS-BLED) and atrial fibrillation diagnosis to the study population for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy 
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - For each component of the risk score and for atrial firbrillation, create a table of individuals who have the component (flag==1)
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
# MAGIC - ccu020_[cohort_start_date]_study_population  
# MAGIC - dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive (for checking presence of diagnoses / phenotypes)
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC - ccu020_[cohort_start_date]_study_population_af
# MAGIC 
# MAGIC **Software and versions** Python, PySpark
# MAGIC  
# MAGIC **Packages and versions** See notebooks
# MAGIC 
# MAGIC **Next steps**  
# MAGIC 
# MAGIC - Finalise codelists for risk scores  
# MAGIC 
# MAGIC - Add bespoke implementations for has bled risk scores and fix has bled loop (add in uncontrolled hypertension)
# MAGIC 
# MAGIC - Review whether to extend risk score component lookup to HES

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU020/CCU020-helper-functions-and-project-parameters

# COMMAND ----------

#for each chadsvasc component create global temp view of ids with binary flag if have code from GDPPR
chads_components = ["vascular_disease", "stroke", "congestive_heart_failure", "diabetes", "hypertension"]

for component in chads_components:
  spark.sql(f"""
  CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_{component}_chads_ids AS
  SELECT DISTINCT NHS_NUMBER_DEID, 1 AS {component}_chads
  FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
  WHERE BatchId = '{batch_id}' 
  AND CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.{project_prefix}_codelists WHERE codelist = '{component}_chads' AND system = 'SNOMED')
  AND DATE < '{cohort_start_date}'
  """)

# COMMAND ----------

#for each hasbled component create global temp view of ids with binary flag if have code from GDPPR 
hasbled_components = ["renal_disease", "liver_disease", "stroke", "alcohol", "bleeding_medications"]

for component in hasbled_components:
  spark.sql(f"""
  CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_{component}_hasbled_ids AS
  SELECT DISTINCT NHS_NUMBER_DEID, 1 AS {component}_hasbled
  FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
  WHERE BatchId = '{batch_id}' 
  AND CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.{project_prefix}_codelists WHERE codelist = '{component}_hasbled' AND system = 'SNOMED')
  AND DATE < '{cohort_start_date}'
  """)

# COMMAND ----------

#add in bleeding using HES

hes = spark.sql(f"""
SELECT * FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
WHERE BatchId = '{batch_id}'
AND ADMIDATE < '{cohort_start_date}'
""")

bleeding_codelist_table = spark.sql(f""" 
SELECT * 
FROM dars_nic_391419_j3w9t_collab.{project_prefix}_codelists 
WHERE codelist = 'bleeding_hasbled' AND system = 'ICD'
""")

# bleeding_codelist_table = spark.table("global_temp.ccu020_20210628_2020_01_01_bleeding_hasbled_codelist")

bleeding_codelist = list_medcodes(bleeding_codelist_table)

bleeding_hasbled_ids = mk_hes_covariate_flag(
      hes, "bleeding_hasbled", bleeding_codelist
  )

bleeding_temp_table = project_prefix + "_bleeding_hasbled_ids"  
bleeding_hasbled_ids.createOrReplaceGlobalTempView(bleeding_temp_table)

# COMMAND ----------

#uncontrolled hypertension >160mmHG systolic blood pressure (average of two most recent readings if within last 3 years)

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_uncontrolled_hypertension_hasbled_ids AS
SELECT
  NHS_NUMBER_DEID,
  AVG(VALUE1_CONDITION) AS systolic_blood_pressure, 
  1 AS uncontrolled_hypertension_hasbled
FROM
  (
    SELECT
      NHS_NUMBER_DEID,
      VALUE1_CONDITION
    FROM
      (
        SELECT
          *,
          row_number() OVER (
            PARTITION BY NHS_NUMBER_DEID
            ORDER BY
              DATE desc
          ) AS sbp_rank
        FROM
          (
            SELECT
              DISTINCT NHS_NUMBER_DEID,
              DATE,
              VALUE1_CONDITION
            FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
            WHERE
              CODE IN (
                SELECT
                  code
                FROM
                  dars_nic_391419_j3w9t_collab.{project_prefix}_codelists
                WHERE
                  codelist = 'systolic_blood_pressure'
              )
              AND BatchId = '{batch_id}'
              AND DATE < '{cohort_start_date}'
              AND (DATE >= '{sys_bp_start_date}')
              AND VALUE1_CONDITION IS NOT NULL
              AND VALUE1_CONDITION > 160
              AND VALUE1_CONDITION < 270
          )
      )
    WHERE
      sbp_rank < 3
  )
GROUP BY
  NHS_NUMBER_DEID
""")


# COMMAND ----------

#for AF create global temp view of ids with binary flag if have code from GDPPR
#NOTE - adding distinct to address possible issue of duplicate values from using MIN(DATE)

spark.sql(f"""
  CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_af_ids AS
  SELECT DISTINCT NHS_NUMBER_DEID, 1 AS af, MIN(DATE) AS af_first_diagnosis
  FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
  WHERE BatchId = '{batch_id}' 
  AND CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.{project_prefix}_codelists WHERE codelist = 'atrial_fibrillation' AND system = 'SNOMED')
  AND DATE < '{cohort_start_date}'
  GROUP BY NHS_NUMBER_DEID
  """)

# COMMAND ----------

#for history of fall create global temp view of ids with binary flag if have code from GDPPR 

spark.sql(f"""
  CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_fall_ids AS
  SELECT DISTINCT NHS_NUMBER_DEID, 1 AS fall 
  FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
  WHERE BatchId = '{batch_id}' 
  AND CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.{project_prefix}_codelists WHERE codelist = 'fall' AND system = 'SNOMED')
  AND DATE < '{cohort_start_date}'
  """)

# COMMAND ----------

#join to study population 

spark.sql(f"""
  CREATE OR REPLACE GLOBAL TEMPORARY VIEW {project_prefix}_study_population_af AS
  SELECT pop.*, 
  vd.vascular_disease_chads AS vascular_disease_chads, 
  st.stroke_chads AS stroke_chads,
  chf.congestive_heart_failure_chads AS congestive_heart_failure_chads,
  db.diabetes_chads AS diabetes_chads,
  hp.hypertension_chads AS hypertension_chads,
  rd.renal_disease_hasbled AS renal_disease_hasbled,
  ld.liver_disease_hasbled AS liver_disease_hasbled,
  strhb.stroke_hasbled AS stroke_hasbled,
  al.alcohol_hasbled AS alcohol_hasbled,
  bm.bleeding_medications_hasbled AS bleeding_medications_hasbled,
  uh.uncontrolled_hypertension_hasbled AS uncontrolled_hypertension_hasbled,
  bl.bleeding_hasbled AS bleeding_hasbled,
  af.af AS af,
  af.af_first_diagnosis AS af_first_diagnosis, 
  f.fall AS fall
  FROM {input_study_population_table1} AS pop
  LEFT JOIN global_temp.{project_prefix}_vascular_disease_chads_ids AS vd ON pop.NHS_NUMBER_DEID = vd.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_stroke_chads_ids AS st ON pop.NHS_NUMBER_DEID = st.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_congestive_heart_failure_chads_ids AS chf ON pop.NHS_NUMBER_DEID = chf.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_diabetes_chads_ids AS db ON pop.NHS_NUMBER_DEID = db.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_hypertension_chads_ids AS hp ON pop.NHS_NUMBER_DEID = hp.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_renal_disease_hasbled_ids AS rd ON pop.NHS_NUMBER_DEID = rd.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_liver_disease_hasbled_ids AS ld ON pop.NHS_NUMBER_DEID = ld.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_stroke_hasbled_ids AS strhb ON pop.NHS_NUMBER_DEID = strhb.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_alcohol_hasbled_ids AS al ON pop.NHS_NUMBER_DEID = al.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_bleeding_medications_hasbled_ids AS bm ON pop.NHS_NUMBER_DEID = bm.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_uncontrolled_hypertension_hasbled_ids AS uh ON pop.NHS_NUMBER_DEID = uh.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_bleeding_hasbled_ids AS bl ON pop.NHS_NUMBER_DEID = bl.PERSON_ID_DEID
  LEFT JOIN global_temp.{project_prefix}_af_ids AS af ON pop.NHS_NUMBER_DEID = af.NHS_NUMBER_DEID
  LEFT JOIN global_temp.{project_prefix}_fall_ids AS f ON pop.NHS_NUMBER_DEID = f.NHS_NUMBER_DEID
  """)


# COMMAND ----------

#keep only individuals with AF
output_table_name = "global_temp." + project_prefix + "_study_population_af"
study_pop_all_df = spark.table(output_table_name)

study_pop_af = study_pop_all_df.filter(study_pop_all_df["af"].isNotNull())

# COMMAND ----------

#export table
export_table_name = project_prefix + "_study_population_af"
create_table_pyspark(study_pop_af, export_table_name)
