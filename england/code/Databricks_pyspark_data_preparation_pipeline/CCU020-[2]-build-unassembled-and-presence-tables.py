# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook creates a long "unassembled" table (all patient events across sources) with target demographic variables and a presence table (what sources patient is in) for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy (building on descriptive paper's standard methodology)
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - Create global temp tables of distinct patient IDs for each target data source (e.g. HES, GDPPR) with target demographic variables (e.g. age, ethnicity)  
# MAGIC 
# MAGIC - Join these tables with "UNION ALL" to create one long table of patient events
# MAGIC 
# MAGIC - Create a separate table with binary flags for presence in target data sources (e.g. HES, GDPPR)
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
# MAGIC - dars_nic_391419_j3w9t_collab.hes_apc_all_years_archive 
# MAGIC - dars_nic_391419_j3w9t_collab.hes_ae_all_years_archive 
# MAGIC - dars_nic_391419_j3w9t_collab.hes_op_all_years_archive  
# MAGIC - dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
# MAGIC - dars_nic_391419_j3w9t_collab.deaths_dars_nic_391419_j3w9t_archive  
# MAGIC - dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t
# MAGIC - dars_nic_391419_j3w9t.sgss_dars_nic_391419_j3w9t
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC - ccu020_[cohort_start_date]_patient_skinny_unassembled
# MAGIC - ccu020_[cohort_start_date]_patient_dataset_presence_lookups
# MAGIC 
# MAGIC **Other notes**
# MAGIC - For any ongoing evaluations, review using archive versions of deaths and sgss data tables (maintained these as this was the implementation from the original descriptive paper)

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU020/CCU020-helper-functions-and-project-parameters

# COMMAND ----------

#get secondary care data for HES APC all years

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_all_hes_apc AS
 SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      to_date(MYDOB,'MMyyyy') as DATE_OF_BIRTH , 
      NULL as DATE_OF_DEATH, 
      EPISTART as RECORD_DATE, 
      epikey as record_id,
      "hes_apc" as dataset,
      0 as primary,
      FYEAR
  FROM {datawrang_database_name}.hes_apc_all_years_archive
  WHERE BatchId = '{batch_id}'"""
)

# COMMAND ----------

#get secondary care data for HES AE all years

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_all_hes_ae AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      date_format(date_trunc("MM", date_add(ARRIVALDATE, -ARRIVALAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH, 
      ARRIVALDATE as RECORD_DATE, 
      COALESCE(epikey, aekey) as record_id,
      "hes_ae" as dataset,
      0 as primary,
      FYEAR
  FROM {datawrang_database_name}.hes_ae_all_years_archive
  WHERE BatchId = '{batch_id}'"""
         )

# COMMAND ----------

#get secondary care data for HES OP all years

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_all_hes_op AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX,
      date_format(date_trunc("MM", date_add(APPTDATE, -APPTAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH,
      APPTDATE  as RECORD_DATE,
      ATTENDKEY as record_id,
      'hes_op' as dataset,
      0 as primary,
      FYEAR
  FROM {datawrang_database_name}.hes_op_all_years_archive
  WHERE BatchId = '{batch_id}'"""
         )

# COMMAND ----------

#join the secondary care tables
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_all_hes as
SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.{project_prefix}_patient_skinny_all_hes_apc
UNION ALL
SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.{project_prefix}_patient_skinny_all_hes_ae
UNION ALL
SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.{project_prefix}_patient_skinny_all_hes_op
""")

# COMMAND ----------

#get gp data

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_gdppr_patients AS
                SELECT NHS_NUMBER_DEID, 
                      gdppr.ETHNIC, 
                      gdppr.SEX,
                      to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
                      to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
                      REPORTING_PERIOD_END_DATE as RECORD_DATE,
                      NULL as record_id,
                      'GDPPR' as dataset,
                      1 as primary
                FROM {datawrang_database_name}.gdppr_dars_nic_391419_j3w9t_archive as gdppr
                WHERE BatchId = '{batch_id}' """)

# COMMAND ----------

#get the ethnicity SNOMED-CT codes from gp data

spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_gdppr_patients_SNOMED AS
                SELECT NHS_NUMBER_DEID, 
                      eth.PrimaryCode as ETHNIC, 
                      gdppr.SEX,
                      to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
                      to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
                      DATE as RECORD_DATE,
                      NULL as record_id,
                      'GDPPR_snomed' as dataset,
                      1 as primary
                FROM {datawrang_database_name}.gdppr_dars_nic_391419_j3w9t_archive as gdppr
                      INNER JOIN dss_corporate.gdppr_ethnicity_mappings eth on gdppr.CODE = eth.ConceptId
                WHERE gdppr.BatchId = '{batch_id}'
                """)

# COMMAND ----------

#get deaths data and consolidate to a single death for each individual 

spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_single_patient_death AS

SELECT * 
FROM 
  (SELECT *, 
          to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') as REG_DATE_OF_DEATH_FORMATTED, 
          to_date(REG_DATE, 'yyyyMMdd') as REG_DATE_FORMATTED, 
          row_number() OVER (PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID 
                             ORDER BY REG_DATE desc, REG_DATE_OF_DEATH desc,
                             S_UNDERLYING_COD_ICD10 desc -- this is because you can have a situation where there are death records twice with the same dates, but only one has diagnoses... so we pick the one with the diagnoses
                             ) as death_rank
    FROM {datawrang_database_name}.deaths_dars_nic_391419_j3w9t_archive
    WHERE BatchId = '{batch_id}'
    ) cte
WHERE death_rank = 1
AND DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
and REG_DATE_OF_DEATH_formatted > '1900-01-01'
AND REG_DATE_OF_DEATH_formatted <= current_date()
""")

# COMMAND ----------

#combine gp and secondary tables and flag null values

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_unassembled AS
SELECT *,      
      CASE WHEN ETHNIC IS NULL or TRIM(ETHNIC) IN ("","9", "99", "X" , "Z") THEN 1 ELSE 0 END as ethnic_null,
      CASE WHEN SEX IS NULL or TRIM(SEX) IN ("", "9", "0" ) THEN 1 ELSE 0 END as sex_null,
      CASE WHEN DATE_OF_BIRTH IS NULL OR TRIM(DATE_OF_BIRTH) = "" OR DATE_OF_BIRTH < '1900-01-01' or DATE_OF_BIRTH > current_date() OR DATE_OF_BIRTH > RECORD_DATE THEN 1 ELSE 0 END as date_of_birth_null,
      CASE WHEN DATE_OF_DEATH IS NULL OR TRIM(DATE_OF_DEATH) = "" OR DATE_OF_DEATH < '1900-01-01' OR DATE_OF_DEATH > current_date() OR DATE_OF_DEATH > RECORD_DATE THEN 1 ELSE 0 END as date_of_death_null,
      CASE WHEN dataset = 'death' THEN 1 ELSE 0 END as death_table
FROM (
      SELECT  NHS_NUMBER_DEID,
              ETHNIC,
              SEX,
              DATE_OF_BIRTH,
              DATE_OF_DEATH,
              RECORD_DATE,
              record_id,
              dataset,
              primary,
              care_domain        
      FROM (
            SELECT NHS_NUMBER_DEID,
                ETHNIC,
                SEX,
                DATE_OF_BIRTH,
                DATE_OF_DEATH,
                RECORD_DATE,
                record_id,
                dataset,
                primary, 'primary' as care_domain
              FROM global_temp.{project_prefix}_patient_skinny_gdppr_patients 
            UNION ALL
            SELECT NHS_NUMBER_DEID,
                ETHNIC,
                SEX,
                DATE_OF_BIRTH,
                DATE_OF_DEATH,
                RECORD_DATE,
                record_id,
                dataset,
                primary, 'primary_SNOMED' as care_domain
              FROM global_temp.{project_prefix}_patient_skinny_gdppr_patients_SNOMED
            UNION ALL
            SELECT NHS_NUMBER_DEID,
                ETHNIC,
                SEX,
                DATE_OF_BIRTH,
                DATE_OF_DEATH,
                RECORD_DATE,
                record_id,
                dataset,
                primary, 'secondary' as care_domain
              FROM global_temp.{project_prefix}_patient_skinny_all_hes
            UNION ALL
            SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID as NHS_NUMBER_DEID,
                Null as ETHNIC,
                Null as SEX,
                Null as DATE_OF_BIRTH,
                REG_DATE_OF_DEATH_formatted as DATE_OF_DEATH,
                REG_DATE_formatted as RECORD_DATE,
                Null as record_id,
                'death' as dataset,
                0 as primary, 'death' as care_domain
              FROM global_temp.{project_prefix}_patient_skinny_single_patient_death
          ) all_patients 
    )""")

# COMMAND ----------

#create unassembled table
output_table_name = project_prefix + "_patient_skinny_unassembled"
sql_script = """SELECT """ + "'" + batch_id + "'" + """ as BatchId, """ + "'" + str(production_date) + "'" + """ as ProductionDate, 
NHS_NUMBER_DEID
,ETHNIC
,SEX
,DATE_OF_BIRTH
,DATE_OF_DEATH
,RECORD_DATE
,record_id
,dataset
,primary
,care_domain
,ethnic_null
,sex_null
,date_of_birth_null
,date_of_death_null
,death_table
FROM global_temp.""" + project_prefix + """_patient_skinny_unassembled"""

print(sql_script)

create_table(table_name=output_table_name, database_name=datawrang_database_name,select_sql_script=sql_script)

# COMMAND ----------

#create presence table
#for deaths and sgss leave as in original - but may want to replace with project specific snapshots - also need to clarify why not using tidied deaths table

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_dataset_presence_lookup AS
SELECT NHS_NUMBER_DEID,
      COALESCE(deaths, 0) as deaths,
      COALESCE(sgss, 0) as sgss,
      COALESCE(gdppr,0) as gdppr,
      COALESCE(hes_apc, 0) as hes_apc,
      COALESCE(hes_op, 0) as hes_op,
      COALESCE(hes_ae, 0) as hes_ae,
      CASE WHEN hes_ae = 1 or hes_apc=1 or hes_op = 1 THEN 1 ELSE 0 END as hes
FROM (
SELECT DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID as NHS_NUMBER_DEID, "deaths" as data_table, 1 as presence FROM dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t
union all
SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, "sgss" as data_table, 1 as presence FROM dars_nic_391419_j3w9t.sgss_dars_nic_391419_j3w9t
union all
SELECT DISTINCT NHS_NUMBER_DEID, "gdppr" as data_table, 1 as presence FROM global_temp.{project_prefix}_patient_skinny_gdppr_patients
union all
SELECT DISTINCT NHS_NUMBER_DEID, "hes_apc" as data_table, 1 as presence FROM global_temp.{project_prefix}_patient_skinny_all_hes_apc
union all
SELECT DISTINCT NHS_NUMBER_DEID, "hes_ae" as data_table, 1 as presence FROM global_temp.{project_prefix}_patient_skinny_all_hes_ae
union all
SELECT DISTINCT NHS_NUMBER_DEID, "hes_op" as data_table, 1 as presence FROM global_temp.{project_prefix}_patient_skinny_all_hes_op
)
PIVOT (MAX(presence) FOR data_table in ("deaths", "sgss", "gdppr", "hes_apc", "hes_op", "hes_ae"))
""")

# COMMAND ----------

output_table_name = project_prefix + "_patient_dataset_presence_lookup"
create_table(table_name=output_table_name)
