# Databricks notebook source
# MAGIC %md
# MAGIC #add documentation...

# COMMAND ----------

#collect data across time periods (review whether do this in R for ease of manipulation / output and to link up with Chadsvasc score)
import pandas as pd
pd.set_option('display.max_columns', 10)

project_prefix = "ccu020_20210701"
datawrang_database_name = "dars_nic_391419_j3w9t_collab"

time_indices = ["2020_01_01", "2020_07_01", "2021_01_01", "2021_05_01"]
#time_indices = ["2020_01_01", "2020_07_01", "2021_05_01"]

summary_data = pd.DataFrame()

for index in time_indices:
  index_table_name = datawrang_database_name + "." + project_prefix + "_" + index
  base_table_name = index_table_name + "_study_population_base"
  alive_table_name = index_table_name + "_study_population_alive"
  age_table_name = index_table_name + "_study_population_alive_age_gte18"
  demo_table_name = index_table_name + "_study_population_demo"
  af_table_name = index_table_name + "_study_population_af"
  final_table_name = index_table_name + "_study_population_cov"

  base_table = spark.table(base_table_name)
  base_table_rows = base_table.count()
  #print("base table", base_table_rows)
  
  alive_table = spark.table(alive_table_name)
  alive_table_rows = alive_table.count()
  
  age_table = spark.table(age_table_name)
  age_table_rows = age_table.count()
  
  demo_table = spark.table(demo_table_name)
  demo_table_rows = demo_table.count()
  
  af_table = spark.table(af_table_name)
  af_table_rows = af_table.count()
  
  final_table = spark.table(final_table_name)
  final_table_rows = final_table.count()
  
  index_data = {"time_index": [index], "base (reg GP)": [base_table_rows], "+ alive": [alive_table_rows], "+ >=18": [age_table_rows], "+ have demo": [demo_table_rows], "+ have AF": [af_table_rows], "final table": [final_table_rows]}
  index_entry = pd.DataFrame(index_data)
  #print(index_entry)
  summary_data = summary_data.append(index_entry)

print(summary_data)

# COMMAND ----------

37727552 - 1134155

# COMMAND ----------


