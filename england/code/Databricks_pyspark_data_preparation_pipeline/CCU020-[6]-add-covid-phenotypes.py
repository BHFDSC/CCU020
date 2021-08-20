# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook adds COVID-19 outcomes and other covariates to the study population for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy 
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - For COVID-19 outcomes, assembles phenotypes from CCU013 input tables or for primary diagnosis definitions builds directly from hospital and deaths datasets  
# MAGIC - For other covariates, builds directly from prescribing medications or primary care datasets
# MAGIC - Left join the tables into the input study population 
# MAGIC 
# MAGIC **Reviewer(s)** UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 19-08-2021
# MAGIC  
# MAGIC **Date last reviewed** UNREVIEWED
# MAGIC  
# MAGIC **Date last run** 19-08-2021
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC - dars_nic_391419_j3w9t_collab.primary_care_meds_dars_nic_391419_j3w9t_archive  
# MAGIC - dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
# MAGIC - dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory
# MAGIC - dars_nic_391419_j3w9t_collab.ccu013_covid_events
# MAGIC - dars_nic_391419_j3w9t_collab.sus_dars_nic_391419_j3w9t_archive
# MAGIC - dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
# MAGIC - dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
# MAGIC - ccu020_[cohort_start_date]_study_population_af_meds
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC - - ccu020_[cohort_start_date]_study_population_cov
# MAGIC 
# MAGIC **Other notes**
# MAGIC 
# MAGIC - For any ongoing evaluations where using start date other than Jan 1st 2020 for COVID-19 outcome analysis, review implementation of cohort start for assembling covid-19 events

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU020/CCU020-helper-functions-and-project-parameters

# COMMAND ----------

#add other medication covariates flags

drugs = ["antihypertensives", "lipid_regulating_drugs", "insulin", "sulphonylurea", "metformin", "other_diabetic_drugs", "proton_pump_inhibitors", "nsaids", "corticosteroids", "other_immunosuppressants"]

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

#add most recent bmi

spark.sql(f""" 
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_bmi AS
SELECT
  NHS_NUMBER_DEID,
  VALUE1_CONDITION AS bmi
FROM
  (
    SELECT
      *,
      row_number() OVER (
        PARTITION BY NHS_NUMBER_DEID
        ORDER BY
          DATE desc
      ) AS bmi_rank
    FROM
      (
        SELECT
          DISTINCT NHS_NUMBER_DEID,
          DATE,
          VALUE1_CONDITION
        FROM
          dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
        WHERE
          CODE IN (
            SELECT
              code
            FROM
              dars_nic_391419_j3w9t_collab.{project_prefix}_codelists
            WHERE
              codelist = 'bmi'
          )
          AND BatchId = '{batch_id}'
          AND DATE IS NOT NULL
          AND DATE >= '2009-07-01'
          AND DATE < '{cohort_start_date}'
          AND VALUE1_CONDITION IS NOT NULL
          AND VALUE1_CONDITION >= 10
          AND VALUE1_CONDITION < 80
      )
  )
WHERE
  bmi_rank == 1
""")

# COMMAND ----------

#add smoking status flag

spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_smoking 
AS SELECT NHS_NUMBER_DEID, 1 AS smoking 
FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
WHERE (CODE IN (SELECT code FROM dars_nic_391419_j3w9t_collab.{project_prefix}_codelists WHERE codelist = 'smoking')) 
AND BatchId = '{batch_id}'
AND DATE<'{cohort_start_date}' 
GROUP BY NHS_NUMBER_DEID
""")

# COMMAND ----------

#add most recent alcohol consumption

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_alcohol_consumption AS
SELECT
  NHS_NUMBER_DEID,
  alcohol_consumption
FROM (SELECT *,
      row_number() OVER (
        PARTITION BY NHS_NUMBER_DEID
        ORDER BY
          DATE desc
      ) AS alcohol_consumption_rank
    FROM
      (
        SELECT
          NHS_NUMBER_DEID,
          (VALUE1_CONDITION * 7) AS alcohol_consumption,
          CODE,
          DATE
        FROM
          dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
        WHERE
          CODE IN (
            SELECT
              code
            FROM
              dars_nic_391419_j3w9t_collab.{project_prefix}_codelists
            WHERE
              codelist = 'alcohol_consumption'
              AND term RLIKE 'day'
          ) 
          AND VALUE1_CONDITION IS NOT NULL
          AND BatchId = '{batch_id}'
          AND DATE<'{cohort_start_date}'
        UNION ALL
        SELECT
          NHS_NUMBER_DEID,
          VALUE1_CONDITION AS alcohol_consumption,
          CODE,
          DATE
        FROM
          dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
        WHERE
          CODE IN (
            SELECT
              code
            FROM
              dars_nic_391419_j3w9t_collab.{project_prefix}_codelists
            WHERE
              codelist = 'alcohol_consumption'
              AND term RLIKE 'week'
          )
          AND BatchId = '{batch_id}'
          AND DATE<'{cohort_start_date}'
          AND VALUE1_CONDITION IS NOT NULL
      )
  )
WHERE alcohol_consumption_rank == 1
""")

# COMMAND ----------

# add Systolic blood pressure (average of two most recent systolic blood pressure (if both recorded within past 3 years))

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_systolic_blood_pressure AS
SELECT
  NHS_NUMBER_DEID,
  AVG(VALUE1_CONDITION) AS systolic_blood_pressure
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
              AND VALUE1_CONDITION >= 55
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

#get covid hospitalisation dates from trajectory table
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_cov_hospitalisation_dates AS
SELECT DISTINCT person_id_deid, MIN(date) as covid_hospitalisation_date
FROM {datawrang_database_name}.ccu013_covid_trajectory
WHERE covid_phenotype = '02_Covid_admission'
AND date IS NOT NULL
AND date >= '{cohort_start_date}'
AND date <= '{cohort_end_date}'
GROUP BY person_id_deid
""")

# COMMAND ----------

#get covid death dates from trajectory table
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_cov_death_dates AS
SELECT DISTINCT person_id_deid, MIN(date) as covid_death_date
FROM {datawrang_database_name}.ccu013_covid_trajectory
WHERE covid_phenotype = '04_Fatal_with_covid_diagnosis'
AND date IS NOT NULL
AND date >= '{cohort_start_date}'
AND date <= '{cohort_end_date}'
GROUP BY person_id_deid
""")

# COMMAND ----------

#add vaccination events
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_cov_vacc_dates AS
SELECT DISTINCT PERSON_ID_DEID, MIN(TO_DATE(DATE_AND_TIME, 'yyyyMMdd')) as covid_vacc_date
FROM {datawrang_database_name}.vaccine_status_dars_nic_391419_j3w9t_archive
WHERE BatchId = '{batch_id}'
AND TO_DATE(DATE_AND_TIME, 'yyyyMMdd') IS NOT NULL
AND TO_DATE(DATE_AND_TIME, 'yyyyMMdd') <= '2021-05-01'
GROUP BY PERSON_ID_DEID
""")

# COMMAND ----------

#add covid hospitalisation outcome with covid as primary diagnosis

# HES APC with suspected or confirmed COVID-19 in POSITION 1
apc_covid_dx1 = spark.sql(f"""
SELECT DISTINCT PERSON_ID_DEID as person_id_deid
FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
WHERE DIAG_4_01 == "U071" OR DIAG_4_01 == "U072"
AND BatchId = '{batch_id}'
AND ADMIDATE >= '{cohort_start_date}'
AND ADMIDATE <= '{cohort_end_date}'
GROUP BY person_id_deid
""")

sus_covid_dx1 = spark.sql(f"""
SELECT DISTINCT NHS_NUMBER_DEID as person_id_deid
FROM dars_nic_391419_j3w9t_collab.sus_dars_nic_391419_j3w9t_archive
WHERE PRIMARY_DIAGNOSIS_CODE == "U071" OR PRIMARY_DIAGNOSIS_CODE == "U072"
AND BatchId = '{batch_id}'
AND START_DATE_HOSPITAL_PROVIDER_SPELL >= '{cohort_start_date}'
AND START_DATE_HOSPITAL_PROVIDER_SPELL <= '{cohort_end_date}'
GROUP BY person_id_deid
""")

covid_dx1 = apc_covid_dx1.join(sus_covid_dx1, 'person_id_deid', 'full')

covid_dx1.createOrReplaceGlobalTempView("covid_hospitalisations_primary_dx")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_covid_hospitalisation_primary_dx AS
SELECT DISTINCT person_id_deid, 1 as covid_hospitalisation_primary_dx
FROM global_temp.covid_hospitalisations_primary_dx
GROUP BY person_id_deid
""")

# COMMAND ----------

#add covid death outcome with covid as primary diagnosis
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_covid_death_primary_dx AS
SELECT DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID as person_id_deid, 1 as covid_death_primary_dx
FROM dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive
WHERE S_COD_CODE_1 == "U071" OR S_COD_CODE_1 == "U072"
AND BatchId = '{batch_id}'
AND TO_DATE(REG_DATE_OF_DEATH, 'yyyyMMdd') >= '{cohort_start_date}'
AND TO_DATE(REG_DATE_OF_DEATH, 'yyyyMMdd') <= '{cohort_end_date}'
GROUP BY person_id_deid
""")

# COMMAND ----------

#select target columns from ccu013 events table and join to existing population

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_study_population_cov AS
SELECT pop.*, 
cov.date_first AS covid_infection_date,
cov.0_Covid_infection AS covid_infection, 
cov.02_Covid_admission AS covid_hospitalisation,
cov_hosp.covid_hospitalisation_date AS covid_hospitalisation_date,
cov_hosp_p.covid_hospitalisation_primary_dx as covid_hospitalisation_primary_dx,
cov.04_Fatal_with_covid_diagnosis AS covid_death, 
cov_death_p.covid_death_primary_dx as covid_death_primary_dx,
cov_d.covid_death_date AS covid_death_date,
cov_v.covid_vacc_date AS covid_first_vaccine_date,
ah.antihypertensives AS antihypertensives, 
lp.lipid_regulating_drugs AS lipid_regulating_drugs, 
in.insulin AS insulin, 
sp.sulphonylurea AS sulphonylurea,
mn.metformin AS metformin, 
od.other_diabetic_drugs AS other_diabetic_drugs,
pp.proton_pump_inhibitors AS proton_pump_inhibitors,
ns.nsaids AS nsaids,
cs.corticosteroids AS corticosteroids,
oi.other_immunosuppressants AS other_immunosuppressants, 
bmi.bmi AS bmi,
sm.smoking as smoking_status, 
ac.alcohol_consumption as alcohol_consumption,
sbp.systolic_blood_pressure as systolic_blood_pressure
FROM {input_study_population_table3} AS pop
LEFT JOIN {datawrang_database_name}.ccu013_covid_events AS cov ON pop.NHS_NUMBER_DEID = cov.person_id_deid AND date_first <= '{cohort_end_date}'
LEFT JOIN global_temp.{project_prefix}_covid_hospitalisation_primary_dx AS cov_hosp_p ON pop.NHS_NUMBER_DEID = cov_hosp_p.person_id_deid
LEFT JOIN global_temp.{project_prefix}_covid_death_primary_dx AS cov_death_p ON pop.NHS_NUMBER_DEID = cov_death_p.person_id_deid
LEFT JOIN global_temp.{project_prefix}_cov_hospitalisation_dates AS cov_hosp ON pop.NHS_NUMBER_DEID = cov_hosp.person_id_deid
LEFT JOIN global_temp.{project_prefix}_cov_death_dates AS cov_d ON pop.NHS_NUMBER_DEID = cov_d.person_id_deid
LEFT JOIN global_temp.{project_prefix}_cov_vacc_dates AS cov_v ON pop.NHS_NUMBER_DEID = cov_v.PERSON_ID_DEID
LEFT JOIN global_temp.{project_prefix}_antihypertensives_ids AS ah ON pop.NHS_NUMBER_DEID = ah.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_lipid_regulating_drugs_ids AS lp ON pop.NHS_NUMBER_DEID = lp.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_insulin_ids AS in ON pop.NHS_NUMBER_DEID = in.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_sulphonylurea_ids AS sp ON pop.NHS_NUMBER_DEID = sp.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_metformin_ids AS mn ON pop.NHS_NUMBER_DEID = mn.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_other_diabetic_drugs_ids AS od ON pop.NHS_NUMBER_DEID = od.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_proton_pump_inhibitors_ids AS pp ON pop.NHS_NUMBER_DEID = pp.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_nsaids_ids AS ns ON pop.NHS_NUMBER_DEID = ns.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_corticosteroids_ids AS cs ON pop.NHS_NUMBER_DEID = cs.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_other_immunosuppressants_ids AS oi ON pop.NHS_NUMBER_DEID = oi.Person_ID_DEID
LEFT JOIN global_temp.{project_prefix}_bmi AS bmi ON pop.NHS_NUMBER_DEID = bmi.NHS_NUMBER_DEID
LEFT JOIN global_temp.{project_prefix}_smoking AS sm ON pop.NHS_NUMBER_DEID = sm.NHS_NUMBER_DEID
LEFT JOIN global_temp.{project_prefix}_alcohol_consumption AS ac ON pop.NHS_NUMBER_DEID = ac.NHS_NUMBER_DEID
LEFT JOIN global_temp.{project_prefix}_systolic_blood_pressure AS sbp ON pop.NHS_NUMBER_DEID = sbp.NHS_NUMBER_DEID
""")

# COMMAND ----------

#load tables for export into pyspark
output_table_name = "global_temp." + project_prefix + "_study_population_cov"
outable_table_df = spark.table(output_table_name)

# COMMAND ----------

#create and export tables
export_table_name = project_prefix + "_study_population_cov"
create_table_pyspark(outable_table_df, export_table_name)
