# chooseCRANmirror()
# install.packages("readr")
library(readr)
library(dplyr)
library(tidyverse)
library(ggplot2)
print('r succesfull')
summary_input <- read.csv("./Oil_Example_bounds_qi_summary.csv")
print('summary file imported succesfully')

# summary_in <- summary_input[c(
#   "api",
#   "operator",
#   "interval",
#   "lateral_length_ft",
#   "true_vertical_depth_ft",
#   "latitude",
#   "longitude",
#   "latitude_bh",
#   "longitude_bh",
#   "frac_proppant_lbs",
#   "frac_fluid_bbl",

# )]
# for (i in 1:33){

# forecasts_input_name <- paste("./test_data/test_data", 9, ".csv", sep="")

forecasts_input_name <- "gulfcosteast.csv"
forecasts_input <- read.csv(forecasts_input_name)
# print(head(forecasts_input, n=4))
colnames(forecasts_input)[which(names(forecasts_input) == "index")] <- "producing_month"
colnames(forecasts_input)[which(names(forecasts_input) == "oil_combined_monthly")] <- "oil"
colnames(forecasts_input)[which(names(forecasts_input) == "gas_combined_monthly")] <- "gas"
colnames(forecasts_input)[which(names(forecasts_input) == "water_combined_monthly")] <- "water"

# forecasts_in<- forecasts_input[c(
#   "api",
#   "date",
#   "days_on",
#   "producing_month",
#   "oil",
#   "gas",
#   "water",
#   "well_name",
# )]

# print(paste(nrow(forecasts_in)))
# forecasts <- merge(forecasts_in, summary_in, by = 'api')
forecasts <- forecasts_input
print(paste(nrow(forecasts_input)))
print('merging is successful')

forecasts_input$well_name <- 'test'

source("./variable.initialization.tc.r")
tcInitialRateMin      <- 0.95                                 # Minimum initial rate % from peak
tcInitialRateMax      <- 1.05                                # Maximum initial rate % from peak
tcPeakMonth           <- 0.1
source("./lp_relaxation.r")
# source("./quantum_dca_final_databricks_cumulative_ORIGINAL.r")
# source("./quantum_dca_final_databricks_cumulative_ORIGINAL_100_justcopy.r")

# print('R scipt is executed succesfully')
forcast1_1 <- read.csv("forcast_new2.csv")
eur12_diff <- (sum(forcast1_1$gas_actual_monthly[1:12]) - sum(forcast1_1$gas_pred_monthly[1:12])) * 100 / sum(forcast1_1$gas_actual_monthly[1:12])
eur_diff <- (sum(forcast1_1$gas_actual_monthly) - sum(forcast1_1$gas_pred_monthly)) * 100 / sum(forcast1_1$gas_actual_monthly)
print(paste("test case", eur12_diff, eur_diff))
write.csv(tcForecast, "./forcast_new2.csv", row.names=FALSE)
write.csv(tcSummary, "./forcast_new_summary.csv", row.names=FALSE)
# print(head(tcForecast, n=4))

# }
# qi = 178508.802823257, Di = 1.71251561295533, b = 0.783853771016214, Df = 0.0512203900436695