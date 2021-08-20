# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook applies the demopgraphic inclusion criteria of the study population for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy (building on descriptive paper's standard methodology)
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - Create a table that ranks the event that is most likely to provide the most up to date data for each demographic variable, with events after the cohort start date removed (excluding death) 
# MAGIC 
# MAGIC - Create a joined table of distinct patients based on this ranking
# MAGIC 
# MAGIC - Apply demographic inclusion criteria: 
# MAGIC   - gte 18 at cohort start date and 
# MAGIC   - Alive  
# MAGIC   - Age, sex, ethnicity and location not null 
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
# MAGIC - ccu020_[cohort_start_date]_patient_skinny_unassembled 
# MAGIC - curr901a_lsoa_region_lookup (for geographic data) 
# MAGIC - dss_corporate.english_indices_of_dep_v02 (for imd data)
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC - ccu020_[cohort_start_date]_study_population_dem

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU020/CCU020-helper-functions-and-project-parameters

# COMMAND ----------

#add a flag for any data after cohort start date

spark.sql(
f"""CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_skinny_unassembled_beyond_{cohort_start_date_sql} as
SELECT *, 
CASE WHEN RECORD_DATE >= '{cohort_start_date}' THEN True ELSE False END as Beyond_{cohort_start_date_sql}
FROM dars_nic_391419_j3w9t_collab.{project_prefix}_patient_skinny_unassembled""")

# COMMAND ----------

#keep records which are before cohort start date (with the exception of deaths data - for use in covid outcomes analysis question)

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_patient_fields_ranked_pre_cutoff AS
SELECT *
FROM (
      SELECT *, row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY death_table desc,  RECORD_DATE DESC) as death_recency_rank,
                row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY date_of_birth_null asc, primary desc, RECORD_DATE DESC) as birth_recency_rank,
                row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY sex_null asc, primary desc, RECORD_DATE DESC) as sex_recency_rank,
                row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
                                    ORDER BY ethnic_null asc, primary desc, RECORD_DATE DESC) as ethnic_recency_rank
                              
      FROM global_temp.{project_prefix}_patient_skinny_unassembled_beyond_{cohort_start_date_sql}
      WHERE Beyond_{cohort_start_date_sql} = False
            Or Death_table = 1
      )""")

# COMMAND ----------

#create ranked cutoff interim table
output_table_name = project_prefix + "_patient_fields_ranked_pre_cutoff"
create_table(output_table_name)

# COMMAND ----------

#make ethnicity group lookup
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW ethnicity_lookup AS
SELECT *, 
      CASE WHEN ETHNICITY_CODE IN ('1','2','3','N','M','P') THEN "Black or Black British"
           WHEN ETHNICITY_CODE IN ('0','A','B','C') THEN "White"
           WHEN ETHNICITY_CODE IN ('4','5','6','L','K','J','H') THEN "Asian or Asian British"
           WHEN ETHNICITY_CODE IN ('7','8','W','T','S','R') THEN "Other Ethnic Groups"
           WHEN ETHNICITY_CODE IN ('D','E','F','G') THEN "Mixed"
           WHEN ETHNICITY_CODE IN ('9','Z','X') THEN "Unknown"
           ELSE 'Unknown' END as ETHNIC_GROUP  
FROM (
  SELECT ETHNICITY_CODE, ETHNICITY_DESCRIPTION FROM dss_corporate.hesf_ethnicity
  UNION ALL
  SELECT Value as ETHNICITY_CODE, Label as ETHNICITY_DESCRIPTION FROM dss_corporate.gdppr_ethnicity WHERE Value not in (SELECT ETHNICITY_CODE FROM FROM dss_corporate.hesf_ethnicity))""")

# COMMAND ----------

#assemble table of distinct ids ("skinny table")

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_study_population_{cohort_start_date_sql} AS
SELECT pat.NHS_NUMBER_DEID, pat.BatchId AS batch_id, pat.ProductionDate AS production_date,
      eth_group.ETHNIC_GROUP AS ethnicity,
      sex.SEX AS sex,
      dob.DATE_OF_BIRTH AS date_of_birth,
      dod.DATE_OF_DEATH AS date_of_death,
      pres.gdppr as primary
FROM (SELECT DISTINCT NHS_NUMBER_DEID, BatchId, ProductionDate FROM dars_nic_391419_j3w9t_collab.{project_prefix}_patient_fields_ranked_pre_cutoff) pat 
        LEFT JOIN (SELECT NHS_NUMBER_DEID, ETHNIC FROM dars_nic_391419_j3w9t_collab.{project_prefix}_patient_fields_ranked_pre_cutoff WHERE ethnic_recency_rank = 1) eth ON pat.NHS_NUMBER_DEID = eth.NHS_NUMBER_DEID
        LEFT JOIN (SELECT NHS_NUMBER_DEID, SEX FROM dars_nic_391419_j3w9t_collab.{project_prefix}_patient_fields_ranked_pre_cutoff WHERE sex_recency_rank = 1) sex ON pat.NHS_NUMBER_DEID = sex.NHS_NUMBER_DEID
        LEFT JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_BIRTH FROM dars_nic_391419_j3w9t_collab.{project_prefix}_patient_fields_ranked_pre_cutoff WHERE birth_recency_rank = 1) dob ON pat.NHS_NUMBER_DEID = dob.NHS_NUMBER_DEID
        LEFT JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_DEATH FROM dars_nic_391419_j3w9t_collab.{project_prefix}_patient_fields_ranked_pre_cutoff WHERE death_recency_rank = 1) dod ON pat.NHS_NUMBER_DEID = dod.NHS_NUMBER_DEID
        LEFT JOIN dars_nic_391419_j3w9t_collab.{project_prefix}_patient_dataset_presence_lookup pres ON pat.NHS_NUMBER_DEID = pres.NHS_NUMBER_DEID
        LEFT JOIN global_temp.ethnicity_lookup eth_group ON eth.ETHNIC = eth_group.ETHNICITY_CODE
WHERE pres.gdppr = 1""")

# COMMAND ----------

#save study population base table -  basis for "Registered with a GP pratice in the UK" criteria
study_pop_base_name = "global_temp." + project_prefix + "_study_population_" + cohort_start_date_sql
study_pop_base_table = spark.table(study_pop_base_name)
study_pop_base_table_output_name = project_prefix + "_study_population_base"
create_table_pyspark(study_pop_base_table, study_pop_base_table_output_name)

# COMMAND ----------

#add age at start of cohort
import pyspark.sql.functions as f
input_table_name = "dars_nic_391419_j3w9t_collab." + study_pop_base_table_output_name
age_table_name = project_prefix + "_study_population_" + cohort_start_date_sql + "_age"
(spark.table(input_table_name)
      .selectExpr("*", 
                  f"floor(float(months_between('{cohort_start_date}', date_of_birth))/12.0) as age_at_cohort_start")
      .createOrReplaceGlobalTempView(age_table_name))

# COMMAND ----------

#filter out IDs not alive on cohort start date and create global temp view
spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_study_population_{cohort_start_date_sql}_alive AS
SELECT * 
FROM global_temp.{project_prefix}_study_population_{cohort_start_date_sql}_age
WHERE COALESCE(date_of_death, '2199-01-01') > '{cohort_start_date}'""")

# COMMAND ----------

#save study population alive table - basis for >= 18 years old and alive on cohort start date criteria
study_pop_alive_name = "global_temp." + project_prefix + "_study_population_" + cohort_start_date_sql + "_alive"
study_pop_alive_table = spark.table(study_pop_alive_name)
study_pop_alive_table_output_name = project_prefix + "_study_population_alive"
create_table_pyspark(study_pop_alive_table, study_pop_alive_table_output_name)

# COMMAND ----------

#filter out IDs with age <18
study_pop_alive_input_name = "dars_nic_391419_j3w9t_collab." + study_pop_alive_table_output_name
study_pop_alive_table = spark.table(study_pop_alive_input_name)
study_pop_gte18 = study_pop_alive_table.filter(study_pop_alive_table["age_at_cohort_start"] > age_gt_cut_off)

# COMMAND ----------

#save study population age_gte18 table - basis for >= 18 years old and alive on cohort start date criteria
study_pop_gte18_output_name = project_prefix + "_study_population_alive_age_gte18"
create_table_pyspark(study_pop_gte18, study_pop_gte18_output_name)

# COMMAND ----------

#add lsoa ids

spark.sql(f""" 
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_lsoa_ids AS
SELECT DISTINCT NHS_NUMBER_DEID, LSOA AS lsoa_code
FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
WHERE NHS_NUMBER_DEID IN (
    SELECT
      NHS_NUMBER_DEID
    FROM
      (
        SELECT
          count(NHS_NUMBER_DEID) AS Records_per_Patient,
          NHS_NUMBER_DEID
        FROM
          (
            SELECT
              DISTINCT NHS_NUMBER_DEID,
              LSOA
            FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive 
            WHERE BatchId = '{batch_id}'
            AND DATE < '{cohort_start_date}'
          )
        GROUP BY
          NHS_NUMBER_DEID
      )
    WHERE
      Records_per_Patient = 1
  )
AND BatchId = '{batch_id}'
AND DATE < '{cohort_start_date}'
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_imd AS
SELECT
  DISTINCT LSOA_CODE_2011,
  DECI_IMD
FROM
  dss_corporate.english_indices_of_dep_v02
WHERE
  LSOA_CODE_2011 IN (
    SELECT
      LSOA
    FROM dars_nic_391419_j3w9t_collab.gdppr_dars_nic_391419_j3w9t_archive
    WHERE BatchId = '{batch_id}'
    AND DATE < '{cohort_start_date}'
  )
  AND LSOA_CODE_2011 IS NOT NULL
  AND IMD IS NOT NULL
  AND IMD_YEAR = '2019'
""")

# COMMAND ----------

#add lsoa name, county and region from asset created by NHS digital to ids and IMD decile from dss corporate data

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_geography AS
SELECT lsoa.NHS_NUMBER_DEID AS NHS_NUMBER_DEID, 
lsoa.lsoa_code AS lsoa_code,
geog.lsoa_name AS lsoa_name,
geog.county_name AS county_name,
geog.region_name as region_name,
dep.DECI_IMD as imd_decile
FROM global_temp.{project_prefix}_lsoa_ids AS lsoa
LEFT JOIN dars_nic_391419_j3w9t_collab.curr901a_lsoa_region_lookup AS geog ON lsoa.lsoa_code = geog.lsoa_code
LEFT JOIN global_temp.{project_prefix}_imd AS dep ON lsoa.lsoa_code = dep.LSOA_CODE_2011
""")

# COMMAND ----------

#join table to study population with age at start of cohort data added

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW {project_prefix}_study_population_{cohort_start_date_sql}_geography AS
SELECT pop.*, 
geog.lsoa_code AS lsoa_code,
geog.lsoa_name AS lsoa_name,
geog.county_name AS county_name,
geog.region_name AS region_name, 
geog.imd_decile as imd_decile
FROM dars_nic_391419_j3w9t_collab.{project_prefix}_study_population_alive_age_gte18 as pop
LEFT JOIN global_temp.{project_prefix}_geography AS geog ON pop.NHS_NUMBER_DEID = geog.NHS_NUMBER_DEID
""")


# COMMAND ----------

#save study population geography table
study_pop_geog_name = "global_temp." + project_prefix + "_study_population_" + cohort_start_date_sql + "_geography"
study_pop_geog_table = spark.table(study_pop_geog_name)
study_pop_geog_table_output_name = project_prefix + "_study_population_geography"
create_table_pyspark(study_pop_geog_table, study_pop_geog_table_output_name)

# COMMAND ----------

#load table for additional filters
study_pop_geog_table_pre_qc = spark.table("dars_nic_391419_j3w9t_collab." + study_pop_geog_table_output_name)

# COMMAND ----------

#filter out IDs with date of birth null
study_pop_dob_nn = study_pop_geog_table_pre_qc.filter(study_pop_geog_table_pre_qc["date_of_birth"].isNotNull())

# COMMAND ----------

#filter out date of birth before 1908 (older than oldest living UK person)
study_pop_dob_gt_1908 = study_pop_dob_nn.filter(study_pop_dob_nn["date_of_birth"] > date_of_birth_cut_off)

# COMMAND ----------

#filter out IDs with ethnicity null and "Unknown"
study_pop_eth_nn = study_pop_dob_gt_1908.filter(study_pop_dob_gt_1908["ethnicity"].isNotNull())

study_pop_eth_nu = study_pop_eth_nn.filter(study_pop_eth_nn["ethnicity"] != "Unknown")

# COMMAND ----------

#filter out IDs with sex null or 0 or 9 
study_pop_sex_nn = study_pop_eth_nu.filter(study_pop_eth_nu["sex"].isNotNull())

#remove 9
study_pop_sex_n9 = study_pop_sex_nn.filter( study_pop_sex_nn["sex"] != "9" ) 

#remove 0
study_pop_sex_n0 = study_pop_sex_n9.filter( study_pop_sex_n9["sex"] != "0" )

# COMMAND ----------

#filter out IDs with null regions
study_pop_export = study_pop_sex_n0.filter(study_pop_sex_n0["region_name"].isNotNull())

# COMMAND ----------

output_table_name = project_prefix + "_study_population_demo"
create_table_pyspark(study_pop_export, output_table_name)
