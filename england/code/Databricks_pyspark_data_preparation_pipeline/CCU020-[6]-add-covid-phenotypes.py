# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook adds COVID-19 outcomes and covariates to the study population for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy 
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - Work in progress
# MAGIC 
# MAGIC **Reviewer(s)** UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 26-04-2021
# MAGIC  
# MAGIC **Date last reviewed** UNREVIEWED
# MAGIC  
# MAGIC **Date last run** 26-04-2021
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC - TBD
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC - TBD
# MAGIC 
# MAGIC **Software and versions** Python, PySpark
# MAGIC  
# MAGIC **Packages and versions** See notebooks
# MAGIC 
# MAGIC **Next steps**  
# MAGIC 
# MAGIC - Clarify approach and required variables  
# MAGIC 
# MAGIC - Build a test cohort

# COMMAND ----------

# MAGIC %md
# MAGIC # Implementation from pre-assembled COVID tables (CCU013)

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
#TO-DO: review implementation of cohort_end_date (e.g. consider replacing with batches when available)
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_cov_hospitalisation_dates AS
SELECT DISTINCT person_id_deid, MIN(date) as covid_hospitalisation_date
FROM {datawrang_database_name}.ccu013_covid_trajectory
WHERE covid_phenotype = '02_Covid_admission'
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

#select target columns from ccu013 events table and join to existing population
#TO-DO: add max date control to covid infection date (check worked)

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_study_population_cov AS
SELECT pop.*, 
cov.date_first AS covid_infection_date,
cov.0_Covid_infection AS covid_infection, 
cov.02_Covid_admission AS covid_hospitalisation,
cov_hosp.covid_hospitalisation_date AS covid_hospitalisation_date,
cov.04_Fatal_with_covid_diagnosis AS covid_death, 
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

# COMMAND ----------

# MAGIC %md
# MAGIC # Archive implementation

# COMMAND ----------

#check counts of benchmark ccu001 tables

# from pyspark.sql.functions import countDistinct

# ccu001_cov = spark.table('dars_nic_391419_j3w9t_collab.ccu001_covid19')
# print("Total rows", ccu001_cov.count() )

# #check distinct patients
# print("Distinct ids", ccu001_cov.select(countDistinct("NHS_NUMBER_DEID")).collect()[0][0])

# COMMAND ----------

#check counts of benchmark ccu013 tables
# ccu013_cov = spark.table('dars_nic_391419_j3w9t_collab.ccu013_covid_severity')
# print("Total rows", ccu013_cov.count() )

# #check distinct patients
# print("Distinct ids", ccu013_cov.select(countDistinct("person_id_deid")).collect()[0][0])

# COMMAND ----------

# #check counts of benchmark ccu013 tables
# ccu013_cov_tr = spark.table('dars_nic_391419_j3w9t_collab.ccu013_covid_trajectory')
# print("Total rows", ccu013_cov_tr.count() )

# #check distinct patients
# print("Distinct ids", ccu013_cov_tr.select(countDistinct("person_id_deid")).collect()[0][0])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## get covid events from pillar2 (lateral flow / community tests)

# COMMAND ----------

#setup raw pillar2 table from snapshot - this may be replaced with a project specific snapshot but currently using batch ID

# pillar2 = spark.sql("""
# SELECT Person_ID_DEID as person_id_deid, 
# AppointmentDate as date, 
# Country,
# 'AppointmentDate' as date_is,
# 'pillar_2' as source,
# TestType, TestLocation, AdministrationMethod
# FROM dars_nic_391419_j3w9t_collab.pillar_2_dars_nic_391419_j3w9t_archive
# WHERE BatchId = '37ccf7c6-5553-4002-9f1b-d1865a04d27a' """)

# # reformat date as currently string
# pillar2 = pillar2.withColumn('date', substring('date', 0, 10)) # NB pillar2 dates in 2019-01-01T00:00:0000. format, therefore subset first
# asDate = udf(lambda x: datetime.strptime(x, '%Y-%m-%d'), DateType())
# pillar2 = pillar2.withColumn('date', asDate(col('date')))

# # Trim dates (not applied as applying cohort cutoffs elsewhere and will likely remove)
# # pillar2 = pillar2.filter((pillar2['date'] >= start_date) & (pillar2['date'] <= end_date))
# new_table = project_prefix + "_pillar2"
# pillar2.createOrReplaceGlobalTempView(new_table)

# COMMAND ----------

#setup new global pillar2 table filtered by GB tests - could probably condense this into previous command
#There is also a lot of repetition in code so could refactor into function

# spark.sql(f""" 
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_pillar2_covid
# AS
# SELECT person_id_deid, date, 
# "Pillar 2 positive test" as covid_phenotype, 
# "" as clinical_code, 
# "pillar_2" as description,
# "" as code,
# source, date_is
# FROM global_temp.{project_prefix}_pillar2
# WHERE Country = 'GB-ENG'
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## get covid events from sgss (PCR tests)

# COMMAND ----------

#setup raw sgss table from snapshot - this may be replaced with a project specific snapshot but currently using batch ID

# sgss = spark.sql(f"""
# SELECT *
# FROM dars_nic_391419_j3w9t_collab.sgss_dars_nic_391419_j3w9t_archive
# WHERE BatchId = '24bac3b5-8986-43b4-b801-6480d5e66e31' """)

# #consider refactoring so have one style of code (switching between pyspark pure and pyspark as SQL wrapper)
# sgss = sgss.withColumnRenamed('specimen_date', 'date')
# sgss = sgss.withColumn('date_is', lit('specimen_date'))

# # Trim dates (not applied as applying cohort cutoffs elsewhere and will likely remove)
# #sgss = sgss.filter((sgss['date'] >= start_date) & (sgss['date'] <= end_date))
# sgss = sgss.filter(sgss['person_id_deid'].isNotNull())
# new_table = project_prefix + "_sgss"
# sgss.createOrReplaceGlobalTempView(new_table)

# COMMAND ----------

#setup new sgss global table with all events
#clarify what is the LSOA cut off for?

# spark.sql(f""" 
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_sgss_covid
# AS
# SELECT person_id_deid, date, 
# "SGSS positive PCR test" as covid_phenotype, 
# "" as clinical_code, 
# CASE WHEN REPORTING_LAB_ID = '840' THEN "pillar_2" ELSE "pillar_1" END as description,
# "" as code,
# "SGSS" as source, date_is
# FROM global_temp.{project_prefix}_sgss
# WHERE LEFT(Lower_Super_Output_Area_Code, 1) = 'E'
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## get covid events from gdppr

# COMMAND ----------

#setup SNOMED-CT codes table (this will likely be abstracted to a disctinct codelists table)

# spark.sql(f"""
# -- SNOMED COVID-19 codes
# -- Create Temporary View with of COVID-19 codes and their grouping - to be used whilst waiting for them to be uploaded onto the TR
# -- Covid-1 Status groups:
# -- - Lab confirmed incidence
# -- - Lab confirmed historic
# -- - Clinically confirmed
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_snomed_codes_covid19 AS
# SELECT *
# FROM VALUES
# ("1008541000000105","Coronavirus ribonucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# ("1029481000000103","Coronavirus nucleic acid detection assay (observable entity)","0","1","Lab confirmed incidence"),
# ("120814005","Coronavirus antibody (substance)","0","1","Lab confirmed historic"),
# ("121973000","Measurement of coronavirus antibody (procedure)","0","1","Lab confirmed historic"),
# ("1240381000000105","Severe acute respiratory syndrome coronavirus 2 (organism)","0","1","Clinically confirmed"),
# ("1240391000000107","Antigen of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# ("1240401000000105","Antibody to severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed historic"),
# ("1240411000000107","Ribonucleic acid of severe acute respiratory syndrome coronavirus 2 (substance)","0","1","Lab confirmed incidence"),
# ("1240421000000101","Serotype severe acute respiratory syndrome coronavirus 2 (qualifier value)","0","1","Lab confirmed historic"),
# ("1240511000000106","Detection of severe acute respiratory syndrome coronavirus 2 using polymerase chain reaction technique (procedure)","0","1","Lab confirmed incidence"),
# ("1240521000000100","Otitis media caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1240531000000103","Myocarditis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1240541000000107","Infection of upper respiratory tract caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1240551000000105","Pneumonia caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1240561000000108","Encephalopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1240571000000101","Gastroenteritis caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1240581000000104","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid detected (finding)","0","1","Lab confirmed incidence"),
# ("1240741000000103","Severe acute respiratory syndrome coronavirus 2 serology (observable entity)","0","1","Lab confirmed historic"),
# ("1240751000000100","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1300631000000101","Coronavirus disease 19 severity score (observable entity)","0","1","Clinically confirmed"),
# ("1300671000000104","Coronavirus disease 19 severity scale (assessment scale)","0","1","Clinically confirmed"),
# ("1300681000000102","Assessment using coronavirus disease 19 severity scale (procedure)","0","1","Clinically confirmed"),
# ("1300721000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed by laboratory test (situation)","0","1","Lab confirmed historic"),
# ("1300731000000106","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 confirmed using clinical diagnostic criteria (situation)","0","1","Clinically confirmed"),
# ("1321181000000108","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 record extraction simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# ("1321191000000105","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 procedures simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# ("1321201000000107","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 health issues simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# ("1321211000000109","Coronavirus disease 19 caused by severe acute respiratory syndrome coronavirus 2 presenting complaints simple reference set (foundation metadata concept)","0","1","Clinically confirmed"),
# ("1321241000000105","Cardiomyopathy caused by severe acute respiratory syndrome coronavirus 2 (disorder)","0","1","Clinically confirmed"),
# ("1321301000000101","Severe acute respiratory syndrome coronavirus 2 ribonucleic acid qualitative existence in specimen (observable entity)","0","1","Lab confirmed incidence"),
# ("1321311000000104","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# ("1321321000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# ("1321331000000107","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 total immunoglobulin in serum (observable entity)","0","1","Lab confirmed historic"),
# ("1321341000000103","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin G in serum (observable entity)","0","1","Lab confirmed historic"),
# ("1321351000000100","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin M in serum (observable entity)","0","1","Lab confirmed historic"),
# ("1321541000000108","Severe acute respiratory syndrome coronavirus 2 immunoglobulin G detected (finding)","0","1","Lab confirmed historic"),
# ("1321551000000106","Severe acute respiratory syndrome coronavirus 2 immunoglobulin M detected (finding)","0","1","Lab confirmed historic"),
# ("1321761000000103","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A detected (finding)","0","1","Lab confirmed historic"),
# ("1321801000000108","Arbitrary concentration of severe acute respiratory syndrome coronavirus 2 immunoglobulin A in serum (observable entity)","0","1","Lab confirmed historic"),
# ("1321811000000105","Severe acute respiratory syndrome coronavirus 2 immunoglobulin A qualitative existence in specimen (observable entity)","0","1","Lab confirmed historic"),
# ("1322781000000102","Severe acute respiratory syndrome coronavirus 2 antigen detection result positive (finding)","0","1","Lab confirmed incidence"),
# ("1322871000000109","Severe acute respiratory syndrome coronavirus 2 antibody detection result positive (finding)","0","1","Lab confirmed historic"),
# ("186747009","Coronavirus infection (disorder)","0","1","Clinically confirmed")

# AS tab(clinical_code, description, sensitive_status, include_binary, covid_phenotype)
# """)

# COMMAND ----------

#setup raw gp table from snapshot - this may be replaced with a project specific snapshot but currently using batch ID

# gdppr = spark.sql(f"""
# SELECT *
# FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
# WHERE BatchId = '24bac3b5-8986-43b4-b801-6480d5e66e31' """)

# gdppr = gdppr.withColumnRenamed('DATE', 'date').withColumnRenamed('NHS_NUMBER_DEID', 'person_id_deid')
# gdppr = gdppr.withColumn('date_is', lit('DATE'))

# # Trim dates (not applied as applying cohort cutoffs elsewhere and will likely remove)
# #gdppr = gdppr.filter((gdppr['date'] >= start_date) & (gdppr['date'] <= end_date))
# new_table = project_prefix + "_gdppr"
# gdppr.createOrReplaceGlobalTempView(new_table)

# COMMAND ----------

#setup new gdppr global table with all events that have matching SNOMED-CT code
#again clarify what LSOA cut off is for
# spark.sql(f""" 
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_gdppr_covid as
# with cte_gdppr as (
# SELECT tab2.person_id_deid, tab2.date, tab2.code, tab2.date_is, tab2.LSOA, tab1.clinical_code, tab1.description
# FROM  global_temp.{project_prefix}_snomed_codes_covid19 tab1
# inner join global_temp.{project_prefix}_gdppr tab2 on tab1.clinical_code = tab2.code
# )
# SELECT person_id_deid, date, 
# "GDPPR Covid diagnosis" as covid_phenotype,
# clinical_code, description,
# "SNOMED" as code, 
# "GDPPR" as source, date_is 
# from cte_gdppr
# WHERE LEFT(LSOA,1) = 'E'
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ## get covid events from hes apc

# COMMAND ----------

#setup raw hes apc table from snapshot - this may be replaced with a project specific snapshot but currently using batch ID
# apc = spark.sql(f"""
# SELECT PERSON_ID_DEID, EPISTART, DIAG_4_CONCAT, OPERTN_4_CONCAT, SUSRECID 
# FROM dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive
# WHERE BatchId = '24bac3b5-8986-43b4-b801-6480d5e66e31'
# """)
# apc = apc.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('EPISTART', 'date')
# apc = apc.withColumn('date_is', lit('EPISTART'))

# # Trim dates (not applied as applying cohort cutoffs elsewhere and will likely remove)
# #apc = apc.filter((apc['date'] >= start_date) & (apc['date'] <= end_date))
# new_table = project_prefix + "_apc"
# apc.createOrReplaceGlobalTempView(new_table)

# COMMAND ----------

#Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
# spark.sql(f"""
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_apc_covid as
# SELECT person_id_deid, date, 
# "HES APC Covid diagnosis" as covid_phenotype,
# (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
# when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# "HES APC" as source, 
# "ICD10" as code, date_is, SUSRECID
# FROM global_temp.{project_prefix}_apc
# WHERE DIAG_4_CONCAT LIKE "%U071%"
#    OR DIAG_4_CONCAT LIKE "%U072%"
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##get covid events from hes op

# COMMAND ----------

#setup raw hes op table from snapshot - this may be replaced with a project specific snapshot but currently using batch ID

# op = spark.sql(f"""
# SELECT PERSON_ID_DEID, APPTDATE, DIAG_4_CONCAT 
# FROM dars_nic_391419_j3w9t_collab.hes_op_all_years_archive
# WHERE BatchId = '24bac3b5-8986-43b4-b801-6480d5e66e31'
# """)

# op = op.withColumnRenamed('PERSON_ID_DEID', 'person_id_deid').withColumnRenamed('APPTDATE', 'date')
# op = op.withColumn('date_is', lit('APPTDATE'))
# # Trim dates (not applied as applying cohort cutoffs elsewhere and will likely remove)
# #op = op.filter((op['date'] >= start_date) & (op['date'] <= end_date))
# new_table = project_prefix + "_op"
# op.createOrReplaceGlobalTempView(new_table)

# COMMAND ----------

# Get all patients hospitalised with a covid diagnosis U07.1 or U07.2
#LOOKS LIKE HES APC MIGHT BE AN ERROR - CONFIRM WITH CHRIS 
# spark.sql(f"""
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_op_covid as
# SELECT person_id_deid, date, 
# "HES OP Covid diagnosis" as covid_phenotype,
# (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'U07.1'
# when DIAG_4_CONCAT LIKE "%U072%" THEN 'U07.2' Else '0' End) as clinical_code,
# (case when DIAG_4_CONCAT LIKE "%U071%" THEN 'Confirmed_COVID19'
# when DIAG_4_CONCAT LIKE "%U072%" THEN 'Suspected_COVID19' Else '0' End) as description,
# "HES APC" as source, 
# "ICD10" as code, date_is
# FROM global_temp.{project_prefix}_op
# WHERE DIAG_4_CONCAT LIKE "%U071%"
#    OR DIAG_4_CONCAT LIKE "%U072%"
# """)

# COMMAND ----------

# MAGIC %md
# MAGIC ##get covid events from hes cc

# COMMAND ----------

#setup raw hes cc table from snapshot - this may be replaced with a project specific snapshot but currently using batch ID

# cc = spark.sql(f"""
# SELECT * FROM dars_nic_391419_j3w9t_collab.hes_cc_all_years_archive
# WHERE BatchId = '24bac3b5-8986-43b4-b801-6480d5e66e31'
# """)
# cc = cc.withColumnRenamed('CCSTARTDATE', 'date').withColumnRenamed('PERSON_ID_DEID', 'person_id_deid')
# cc = cc.withColumn('date_is', lit('CCSTARTDATE'))

# # reformat dates for hes_cc as currently strings
# asDate = udf(lambda x: datetime.strptime(x, '%Y%m%d'), DateType())
# cc = cc.withColumn('date', asDate(col('date')))

# # Trim dates (not applied as applying cohort cutoffs elsewhere and will likely remove)
# #cc = cc.filter((cc['date'] >= start_date) & (cc['date'] <= end_date))
# new_table = project_prefix + "_cc"
# cc.createOrReplaceGlobalTempView(new_table)

# COMMAND ----------

#get ID if is in HES_CC AND has U071 or U072 from HES_APC 
#MAY NOT ACTUALLY NEED THIS - JUST CREATES MORE DUPLICATES
# spark.sql(f"""
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_cc_covid as
# SELECT apc.person_id_deid, cc.date,
# 'ICU treatment' as covid_phenotype,
# "" as clinical_code,
# "id is in hes_cc table" as description,
# "" as code,
# 'hes_cc' as source, cc.date_is, BRESSUPDAYS, ARESSUPDAYS
# FROM global_temp.{project_prefix}_apc as apc
# INNER JOIN global_temp.{project_prefix}_cc AS cc
# ON cc.SUSRECID = apc.SUSRECID
# WHERE cc.BESTMATCH = 1
# AND (DIAG_4_CONCAT LIKE '%U071%' OR DIAG_4_CONCAT LIKE '%U072%')
# """)

# COMMAND ----------

#get covid events from hes apc niv (SKIP FOR FIRST TEST - TOO GRANULAR FOR MY REQUIREMENTS)

# COMMAND ----------

#get covid events from hes apc imv (SKIP FOR FIRST TEST - TOO GRANULAR FOR MY REQUIREMENTS)

# COMMAND ----------

#get covid events from hes apc ecmo (SKIP FOR FIRST TEST - TOO GRANULAR FOR MY REQUIREMENTS)

# COMMAND ----------

#get covid events from hes cc niv (SKIP FOR FIRST TEST - TOO GRANULAR FOR MY REQUIREMENTS)

# COMMAND ----------

#get covid events from hes cc imv (SKIP FOR FIRST TEST - TOO GRANULAR FOR MY REQUIREMENTS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## join tables and select distinct IDs by earliest recorded date of diagnosis

# COMMAND ----------

#Join tables with all covid events
#THEY LEFT OUT OP - WHY?
# spark.sql(f"""
# CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_covid_all_events
# AS
# SELECT * FROM global_temp.{project_prefix}_pillar2_covid
# UNION ALL
# SELECT * FROM global_temp.{project_prefix}_sgss_covid
# UNION ALL
# SELECT * FROM global_temp.{project_prefix}_gdppr_covid
# UNION ALL
# SELECT person_id_deid, date, covid_phenotype, clinical_code, description, code, source, date_is FROM global_temp.{project_prefix}_apc_covid
# UNION ALL
# SELECT person_id_deid, date, covid_phenotype, clinical_code, description, code, source, date_is FROM global_temp.{project_prefix}_cc_covid
# """)

# COMMAND ----------

#check count
# cov_all_temp_table = "global_temp." + project_prefix + "_covid_all_events" 

# cov_all = spark.table(cov_all_temp_table)
# print("Total rows", cov_all.count() )

# #check distinct patients
# print("Distinct ids", cov_all.select(countDistinct("person_id_deid")).collect()[0][0])

# COMMAND ----------

#Select distinct ids based on earliest recorded date of diagnosis

# spark.sql(f"""
# CREATE OR REPLACE GLOBAL TEMPORARY VIEW {project_prefix}_covid19_infection AS
# SELECT
#   person_id_deid,
#   min(date) AS covid19_infection_date
# FROM
#   global_temp.{project_prefix}_covid_all_events
# GROUP BY
#   person_id_deid
# """)

# COMMAND ----------

#check count
#CONFIRM WHY NUMBERS DIFFER TO CHRIS / JOHAN (LIKELY CAUSE IS DEMOGRAPHIC FILTERING AND TIME PERIOD DIFFERENCES GIVEN NUMBERS SIMILAR AFTER JOIN WITH STUDY POPULATION BUT HELPFUL TO CONFIRM)
# cov_uniq_temp_table = "global_temp." + project_prefix + "_covid19_infection" 
# cov_all_uniq = spark.table(cov_uniq_temp_table)
# print("Total rows", cov_all_uniq .count() )

# #check distinct patients
# print("Distinct ids", cov_all_uniq.select(countDistinct("person_id_deid")).collect()[0][0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join table to study population with covid diagnosis ids and select diagnosis date

# COMMAND ----------

#will replace study population join with project prefix / repeatable name
# spark.sql(f"""
# CREATE OR REPLACE GLOBAL TEMPORARY VIEW {project_prefix}_study_population_covid19 AS
# SELECT pop.*, cov.covid19_infection_date
# FROM dars_nic_391419_j3w9t_collab.ah_test_190421_study_population2 AS pop
# LEFT JOIN global_temp.{project_prefix}_covid19_infection AS cov
# ON pop.NHS_NUMBER_DEID = cov.person_id_deid
# """)

# COMMAND ----------

# spark.sql(f"""
# SELECT * FROM global_temp.{project_prefix}_study_population_covid19 LIMIT 10;
# """).show()

#visually check output

# covid_output_table = "global_temp." + project_prefix + "_study_population_covid19"
# study_pop_cov = spark.table(covid_output_table)
# study_pop_cov.show()

# COMMAND ----------

#count covid infections
# study_pop_cov.filter(study_pop_cov["covid19_infection_date"].isNotNull()).count()

# COMMAND ----------

#save and export table with only individuals who have had covid infections
# study_pop_cov_only = study_pop_cov.filter(study_pop_cov["covid19_infection_date"].isNotNull())

# study_pop_cov_only_table_name = project_prefix + "_study_population_covid19_only"

# create_table_pyspark(study_pop_cov_only, study_pop_cov_only_table_name)

# COMMAND ----------

#save and export table with all individuals
# study_pop_cov_all_table_name = project_prefix + "_study_population_covid19_all"

# create_table_pyspark(study_pop_cov, study_pop_cov_all_table_name)
