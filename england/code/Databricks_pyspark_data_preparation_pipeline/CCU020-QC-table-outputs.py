# Databricks notebook source
# MAGIC %md
# MAGIC **Description** This notebook applies QC on the study population (after applying demographic inclusion criteria) for CCU020
# MAGIC  
# MAGIC **Project(s)** CCU020 - Evaluation of antithrombotic use and COVID-19 outcomes
# MAGIC  
# MAGIC **Author(s)** Alex Handy
# MAGIC 
# MAGIC **Approach**
# MAGIC 
# MAGIC - Apply a series of basic checks (e.g. row counts, category counts) to ensure key foundation variables have been processed correctly
# MAGIC 
# MAGIC **Reviewer(s)** UNREVIEWED
# MAGIC  
# MAGIC **Date last updated** 06-05-2021
# MAGIC  
# MAGIC **Date last reviewed** UNREVIEWED
# MAGIC  
# MAGIC **Date last run** 06-05-2021
# MAGIC  
# MAGIC **Data input** 
# MAGIC 
# MAGIC - ccu020_[cohort_start_date]_study_population
# MAGIC 
# MAGIC **Data output** 
# MAGIC 
# MAGIC - n/a - study population table should pass all these tests
# MAGIC 
# MAGIC **Software and versions** Python, PySpark
# MAGIC  
# MAGIC **Packages and versions** See notebooks
# MAGIC 
# MAGIC **Next steps**

# COMMAND ----------

# MAGIC %run /Workspaces/dars_nic_391419_j3w9t_collab/CCU020/CCU020-helper-functions-and-project-parameters

# COMMAND ----------

for output_table in output_tables:
  spark.sql(f"""REFRESH TABLE {output_table} """)

# COMMAND ----------

#if want to customise QC
#project_prefix = "ccu020_20210617_2020_01_01"
# project_prefix = "ccu020_20210628_2020_01_01"
# datawrang_database_name = "dars_nic_391419_j3w9t_collab"
# base = datawrang_database_name + "." + project_prefix + "_study_population_base"
# input_study_population_table1 = datawrang_database_name + "." + project_prefix + "_study_population_demo"
# input_study_population_table2 = datawrang_database_name + "." + project_prefix + "_study_population_af"
# input_study_population_table3 = datawrang_database_name + "." + project_prefix + "_study_population_af_meds"
# final_output_table = datawrang_database_name + "." + project_prefix + "_study_population_cov"
# output_tables = [base, input_study_population_table1, input_study_population_table2, input_study_population_table3, final_output_table]

# COMMAND ----------

#check for duplicates in output tables
from pyspark.sql.functions import countDistinct, col


for table in output_tables:
  print("Study table: ", table)

  study_pop = spark.table(table)
  
  #visually check available columns
  print(study_pop.columns)

  #check total rows
  total_rows = study_pop.count()
  print("Total rows", total_rows)

  #check distinct patients
  distinct_ids = study_pop.select(countDistinct("NHS_NUMBER_DEID")).collect()[0][0]
  print("Distinct ids", distinct_ids)

  #distinct IDs should equal total rows
  print("Total rows == Distinct ids?", total_rows == distinct_ids)

# COMMAND ----------

#conduct remaining QC on only the final output table
study_pop = spark.table(final_output_table)

total_rows = study_pop.count()
print("Total rows", total_rows)

# COMMAND ----------

#check for null values in columns which should have no null values

#column names will require updating
target_null_cols = ['NHS_NUMBER_DEID', 'ethnicity', 'sex', 'date_of_birth', 'age_at_cohort_start', 'region_name']

def check_null_values(table, col_name, total_rows):
  non_null_rows = table.filter(table[col_name].isNotNull()).count()
  null_rows = total_rows - non_null_rows
  print("Null rows in " + col_name + " = " + str(null_rows))
  print("Total rows == non null rows?", non_null_rows == total_rows)
  
for col_name in target_null_cols:
  check_null_values(study_pop, col_name, total_rows)

# COMMAND ----------

#check all ids are present in GP data

gp_rows = study_pop.filter(study_pop["primary"] == 1).count()
print("GP rows", gp_rows)
print("Total rows == GP rows?", gp_rows == total_rows)

# COMMAND ----------

#check range of dates for date of birth

from pyspark.sql.functions import to_date, lit

def check_date_range(table, col_name):
  max_date = table.agg({col_name: "max"}).collect()[0][0]
  print("Max date", max_date)
  min_date = table.agg({col_name: "min"}).collect()[0][0]
  print("Min date", min_date)
  return max_date, min_date
  
max_date, min_date = check_date_range(study_pop, "date_of_birth")
#print(type(max_date))
print("Max date < 2001-12-31", max_date < "2002-01-02")
print("Min date > 1908-01-01", min_date > "1908-01-01")

# COMMAND ----------

#check ranges of dates for date of death

max_date, min_date = check_date_range(study_pop, "date_of_death")
#print(type(max_date)) -> noticed that this is a different data type (datetime) than "DATE OF BIRTH" which is a string
#update max date check to agreed cohort end point
print("Max date < 2021-05-01", str(max_date) < "2021-05-01")
print("Min date > 2020-01-01", str(min_date) >= "2020-01-01")

# COMMAND ----------

#check values and distributions for categorical variables

def check_cat(table, col_name):
  table.groupby(col_name).count().show()
  
for col_name in ['ethnicity', 'sex', 'region_name']:
  check_cat(study_pop, col_name)

# COMMAND ----------

#check all ids have af in final output table

af_rows = study_pop.filter(study_pop["af"] == 1).count()
print("Af_rows", af_rows)
print("Total rows == AF rows?", af_rows == total_rows)

# COMMAND ----------

#check ranges of covid infection, hospitalisation and death
#covid vaccine data not in date format so may need to manipulate further to get an accurate check (e.g. to_date)


covid_outcomes = ["covid_infection_date", "covid_hospitalisation_date", "covid_death_date", "covid_first_vaccine_date"]

for cov_date in covid_outcomes:
  print("Cov date", cov_date)
  max_date, min_date = check_date_range(study_pop, cov_date)
  
  print("Max date < 2021-05-01", str(max_date) <= "2021-05-01")
  print("Min date > 2020-01-01", str(min_date) >= "2020-01-01")

# COMMAND ----------

#check covid death date == death date
cov_deaths = study_pop.filter(study_pop["covid_death"] == 1)
cov_deaths_count = cov_deaths.count()
print("cov deaths", cov_deaths_count)
matching_dates = cov_deaths.filter(cov_deaths["covid_death_date"] == cov_deaths["date_of_death"]).count()

print("Matching dates: ", matching_dates)

print("Mis matching dates: ", cov_deaths_count - matching_dates)


# COMMAND ----------

#check infection date < hospitalisation date < death date
#NOTE - output should all be zero
def compare_date_range(table, col_name_pre, col_name_post):
  test_rows = table.filter(study_pop[col_name_pre] > study_pop[col_name_post]).count()
  print("Rows where :", col_name_pre, " > ", col_name_post, " = ", test_rows)

compare_date_range(study_pop, "covid_infection_date", "covid_hospitalisation_date")
compare_date_range(study_pop, "covid_infection_date", "covid_death_date")
compare_date_range(study_pop, "covid_hospitalisation_date", "covid_death_date")

# COMMAND ----------

# MAGIC %md
# MAGIC # Exploration

# COMMAND ----------

#check if number of null dates has changed 

#April
# spark.sql(f"""
# SELECT DISTINCT PERSON_ID_DEID, MIN(TO_DATE(DATE_AND_TIME, 'yyyyMMdd')) as covid_vacc_date
# FROM dars_nic_391419_j3w9t_collab.vaccine_status_dars_nic_391419_j3w9t_archive
# WHERE BatchId = '37ccf7c6-5553-4002-9f1b-d1865a04d27a' 
# AND TO_DATE(DATE_AND_TIME, 'yyyyMMdd') IS NOT NULL
# AND TO_DATE(DATE_AND_TIME, 'yyyyMMdd') < '2021-05-01'
# GROUP BY PERSON_ID_DEID
# """).count()

# COMMAND ----------

#May
# spark.sql(f"""
# SELECT DISTINCT PERSON_ID_DEID, MIN(TO_DATE(DATE_AND_TIME, 'yyyyMMdd')) as covid_vacc_date
# FROM dars_nic_391419_j3w9t_collab.vaccine_status_dars_nic_391419_j3w9t_archive
# WHERE BatchId = '5ceee019-18ec-44cc-8d1d-1aac4b4ec273' 
# AND TO_DATE(DATE_AND_TIME, 'yyyyMMdd') IS NOT NULL
# AND TO_DATE(DATE_AND_TIME, 'yyyyMMdd') < '2021-05-01'
# GROUP BY PERSON_ID_DEID
# """).count()

# COMMAND ----------

#TEMPORARY REVIEW OF VACCINE DATA
# from pyspark.sql.functions import countDistinct, col
# vac = spark.table("dars_nic_391419_j3w9t.vaccine_status_dars_nic_391419_j3w9t")

# #check total rows
# total_rows = vac.count()
# print("Total rows", total_rows)

# #check distinct patients
# distinct_ids = vac.select(countDistinct("PERSON_ID_DEID")).collect()[0][0]
# print("Distinct ids", distinct_ids)

# Total rows 45973355
# Distinct ids 29960990

# COMMAND ----------

#check for duplicates in output tables

# from pyspark.sql.functions import countDistinct

# study_tables = ["dars_nic_391419_j3w9t_collab.ccu020_20210524_2020_01_01_study_population_demo", "dars_nic_391419_j3w9t_collab.ccu020_20210524_2020_01_01_study_population_af", "dars_nic_391419_j3w9t_collab.ccu020_20210524_2020_01_01_study_population_af_meds", "dars_nic_391419_j3w9t_collab.ccu020_20210524_2020_01_01_study_population_cov2"]

# for study_table in study_tables:
#   print("Study table: ", study_table)

#   study_pop = spark.table(study_table)

#   #check total rows
#   total_rows = study_pop.count()
#   print("Total rows", total_rows)

#   #check distinct patients
#   distinct_ids = study_pop.select(countDistinct("NHS_NUMBER_DEID")).collect()[0][0]
#   print("Distinct ids", distinct_ids)

#   #distinct IDs should equal total rows
#   print("Total rows == Distinct ids?", total_rows == distinct_ids)
