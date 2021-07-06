# Databricks notebook source
# MAGIC %md
# MAGIC add documentation...

# COMMAND ----------

#setup project parameters
import datetime
from dateutil.relativedelta import relativedelta

#LAST FULL RUN: cohort start date 1st July 2020 (1st Jan 2021 next)

project_prefix = "ccu020_20210701_2021_01_01"
#This batch id led implementation may be replaced by study specific snapshots - however, all versioning is handled through this batch id anyway so no clear advantage to creating study specific snapshots (e.g. can only capture month by month batch variation - CONFIRM WITH SAM)
# batch_id_feb = "24bac3b5-8986-43b4-b801-6480d5e66e31" #February Update
# batch_id_apr = "37ccf7c6-5553-4002-9f1b-d1865a04d27a" #April Update
batch_id = "5ceee019-18ec-44cc-8d1d-1aac4b4ec273" #May update

#CCU020-[2]-build-unassembled-and-presence-tables
database_name = "dars_nic_391419_j3w9t"
datawrang_database_name = "dars_nic_391419_j3w9t_collab"
production_date = datetime.datetime.now()

#CCU020-[3]-build-study-population
cohort_start_date = '2021-01-01'
cohort_end_date = '2021-05-01'
#for use in sql table names, not date logic
cohort_start_date_sql = cohort_start_date.replace("-", "_")
age_gt_cut_off = 17 #Adults -> 18 years and older
date_of_birth_cut_off = '1908-01-01' #below this implies incorrect data as would be older than oldest UK living person

#CCU020-[4]-add-risk-scores-and-af
input_study_population_table1 = datawrang_database_name + "." + project_prefix + "_study_population_demo"
cohort_start_date_datetime = datetime.datetime.strptime(cohort_start_date, '%Y-%m-%d')
sys_bp_start_date_datetime = cohort_start_date_datetime - relativedelta(months=36)
sys_bp_start_date = sys_bp_start_date_datetime.strftime('%Y-%m-%d')

#CCU020-[5]-add-medications
input_study_population_table2 = datawrang_database_name + "." + project_prefix + "_study_population_af"
prescription_history_start_date_datetime = cohort_start_date_datetime - relativedelta(months=6)
prescription_history_start_date = prescription_history_start_date_datetime.strftime('%Y-%m-%d')

#CCU020-[6]-add-covid-phenotypes
input_study_population_table3 = datawrang_database_name + "." + project_prefix + "_study_population_af_meds"

#CCU020-QC-table-outputs
final_output_table = datawrang_database_name + "." + project_prefix + "_study_population_cov"

#other interim tables
base_table = datawrang_database_name + "." + project_prefix + "_study_population_base"
alive_table = datawrang_database_name + "." + project_prefix + "_study_population_alive"
age_table = datawrang_database_name + "." + project_prefix + "_study_population_alive_age_gte18"
geog_table = datawrang_database_name + "." + project_prefix + "_study_population_geography"

output_tables = [base_table, alive_table, age_table, geog_table, input_study_population_table1, input_study_population_table2, input_study_population_table3, final_output_table]

# COMMAND ----------

import pyspark.sql.functions as f
from functools import reduce

#helper function to create table (if stored as SQL global view) -> consider abstracting into a helper functions notebook

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None, if_not_exists=True) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  if if_not_exists is True:
    if_not_exists_script=' IF NOT EXISTS'
  else:
    if_not_exists_script=''
  
  spark.sql(f"""CREATE TABLE {if_not_exists_script} {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")
  
#helper function to create table (if stored as spark dataframe)
def create_table_pyspark(df, table_name:str, database_name:str="dars_nic_391419_j3w9t_collab", select_sql_script:str=None) -> None:
#   adapted from sam h 's save function
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  spark.sql(f"""DROP TABLE IF EXISTS {database_name}.{table_name}""")
  df.createOrReplaceGlobalTempView(table_name)
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}""")
  spark.sql(f"""
                ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}
             """)

#helper function to extract IDs from HES for a given codelist
def mk_hes_covariate_flag(hes, CONDITION_HES_string, codelist):
  hes_tmp = hes.select(['PERSON_ID_DEID', "EPISTART", "DIAG_4_CONCAT"])
  hes_tmp = hes_tmp.withColumn("DIAG_4_CONCAT",f.regexp_replace(f.col("DIAG_4_CONCAT"), "\\.", ""))
  hes_cov = hes_tmp.where(
      reduce(lambda a, b: a|b, (hes_tmp['DIAG_4_CONCAT'].like('%'+code+"%") for code in codelist))
  ).select(["PERSON_ID_DEID"]).withColumn(CONDITION_HES_string, f.lit(1))
  
  #ensure distinct IDs
  df = hes_cov.filter(hes_cov[CONDITION_HES_string] == 1).groupBy("PERSON_ID_DEID").agg(f.max(CONDITION_HES_string).alias(CONDITION_HES_string))
  
  return df

#helper function to convert codelists into python list
def list_medcodes(codelist_column_df):
  codelist = [item.code for item in codelist_column_df.select('code').collect()]
  return codelist
