rm(list = ls())

#Load packages
library(odbc)
library(dplyr)
library(data.table)
library(DBI)

# set target folder
setwd("/mnt/efs/a.handy/dars_nic_391419_j3w9t_collab/CCU020")


#set today's date for file versioning
today_date = format(Sys.time(), "%d_%m_%Y")

# connect to databricks instance
con = dbConnect( odbc::odbc(), "Databricks", timeout = 60, 
                 PWD=rstudioapi::askForPassword("Please enter your Databricks personal access token"))

query = paste('SELECT * FROM dars_nic_391419_j3w9t_collab.ccu020_20210611_2020_01_01_codelists', sep="")


# load data
data = dbGetQuery(con,query)

print(data)

#save as rds object
codelists_file_rds = paste("data/CCU020_codelists_", today_date, ".rds", sep="")
saveRDS(data,codelists_file_rds)

#replace commas to prevent corrupted csv output
data = data %>% mutate(term = gsub(",", "", term))

#save as csv
#originally saved as codelists2 to fix "," issue (ref for AWS)
codelists_file_csv = paste("data/CCU020_codelists_", today_date, ".csv", sep="")
write.csv(data, codelists_file_csv, row.names=F, quote=F)
