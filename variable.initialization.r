# Copyright (c) 2022 Enertel Holdings, LLC. All Rights Reserved.

# NOTICE:  All information contained herein is, and remains
# the exclusive property of Enertel Holdings, LLC.  The intellectual
# and technical concepts contained herein are proprietary
# to Enertel Holdings, LLC and may be covered by U.S. and Foreign Patents,
# patents in process, and are protected by trade secret or copyright law.
# Dissemination, modification, or reproduction of this material
# is strictly forbidden unless prior written permission is
# obtained from Enertel Holdings, LLC.

#        _______   ____________  ______________
#       / ____/ | / / ____/ __ \/_  __/ ____/ /
#      / __/ /  |/ / __/ / /_/ / / / / __/ / /
#     / /___/ /|  / /___/ _, _/ / / / /___/ /___
#    /_____/_/ |_/_____/_/ |_| /_/ /_____/_____/

# Jonathan Henderson; Last modified: 11 MAY 2022 09:50 PM MST

# Dependencies: 

##########################################################
# Variable Initialization ################################
##########################################################

# production table input
# REQUIRES the following columns in the following ORDER:
# "api"                     --- API_UWI_14 column from Enverus Production
# "date"                    --- ProducingMonth column from Enverus Production
# "days_on"                 --- ProducingDays column from Enverus Production
# "producing_month"         --- TotalProdMonths column from Enverus Production
# "oil"                     --- LiquidsProd_BBL column from Enverus Production
# "gas"                     --- GasProd_MCF column from Enverus Production
# "water"                   --- WaterProd_BBL column from Enverus Production
# "well_name"               --- WellName column from Enverus Production / Well
# "operator"                --- ENVOperator column from Enverus Production / Well
# "interval"                --- ENVInterval column from Enverus Production / Well
# "lateral_length_ft"       --- LateralLength_FT column from Enverus Production / Well
# "true_vertical_depth_ft"  --- TVD_FT column from Enverus Production / Well
# "latitude"                --- Latitude column from Enverus Production / Well
# "longitude"               --- Longitude column from Enverus Production / Well
# "latitude_bh"             --- Latitude_BH column from Enverus Production / Well
# "longitude_bh"            --- Longitude_BH column from Enverus Production / Well
# "frac_proppant_lbs"       --- Proppant_LBS column from Enverus Production / Well
# "frac_fluid_bbl"          --- TotalFluidPumped_BBL column from Enverus Production / Well

#data <- as.data.frame(production, stringsAsFactors = FALSE)
#colnames(data) <- c("api", "date", "days_on", "producing_month", "oil", "gas", "water", "well_name", "operator", 
#                    "interval", "lateral_length_ft", "true_vertical_depth_ft", "latitude", "longitude", "latitude_bh",
#                    "longitude_bh", "frac_proppant_lbs", "frac_fluid_bbl", "working_interest", "net_revenue_interest")

# forecasts table input
# REQUIRES the following columns in the following ORDER:
# "api"                     --- API_UWI_14 column from batch forecasts
# "date"                    --- ProducingMonth column from batch forecasts
# "days_on"                 --- ProducingDays column from batch forecasts
# "producing_month"         --- TotalProdMonths column from batch forecasts
# "oil"                     --- LiquidsProd_BBL column from batch forecasts
# "gas"                     --- GasProd_MCF column from batch forecasts
# "water"                   --- WaterProd_BBL column from batch forecasts
# "well_name"               --- Type Curve Area name from API list
# "operator"                --- ENVOperator column from batch summary
# "interval"                --- ENVInterval column from batch summary
# "lateral_length_ft"       --- LateralLength_FT column from batch summary
# "true_vertical_depth_ft"  --- TVD_FT column from Enverus batch summary
# "latitude"                --- Latitude column from Enverus batch summary
# "longitude"               --- Longitude column from Enverus batch summary
# "latitude_bh"             --- Latitude_BH column from Enverus batch summary
# "longitude_bh"            --- Longitude_BH column from Enverus batch summary
# "frac_proppant_lbs"       --- Proppant_LBS column from Enverus batch summary
# "frac_fluid_bbl"          --- TotalFluidPumped_BBL column from batch summary

# data_m <- as.data.frame(forecasts, stringsAsFactors = FALSE)
# colnames(data_m) <- c("api", "date", "days_on", "producing_month", "oil", "gas", "water", "well_name", "operator", 
#                       "interval", "lateral_length_ft", "true_vertical_depth_ft", "latitude", "longitude", "latitude_bh",
#                       "longitude_bh", "frac_proppant_lbs", "frac_fluid_bbl")

# Initialization Parameters
tcInitialRate <- 0                                   # Overrides initial rate for oil
tcInitialRate2 <- 0                                  # Overrides initial rate for gas
tcInitialRateMin <- 0.3                              # Minimum initial rate % from peak
tcInitialRateMax <- 2                                # Maximum initial rate % from peak
tcInitialDecline <- 0                                # Overrides initial decline for oil
tcInitialDecline2 <- 0                               # Overrides initial decline for gas
tcInitialDeclineMin <- 0.2                           # Minimum initial decline % secant eff
tcInitialDeclineMax <- 0.95                          # Maximum initial decline % secant eff
tcBFactor <- 0                                       # Overrides b factor for oil
tcBFactor2 <- 0                                      # Overrides b factor for gas
tcBFactorMin <- 0.2                                  # Minimum b factor 
tcBFactorMax <- 1.6                                  # Maximum b factor  
tcFinalDecline <- 0.07                               # Overrides terminal decline % for oil
tcFinalDecline2 <- 0.07                              # Overrides terminal decline % for gas
tcFinalDeclineMin <- 0.01                            # Minimum terminal decline % in tangent eff
tcFinalDeclineMax <- 0.2                             # Maximum terminal decline % in tangent eff
tcForecastYears <- 50                                # Forecast length in years
tcDowntimeCutoff <- 0.01                             # Downtime exclusion limit for oil
tcDowntimeCutoffSecondary <- 0.001                   # Downtime exclusion limit for gas
tcRateLimit <- 0.0001                                # Rate limit for oil forecast
tcRateLimitSecondary <- 0.0001                       # Rate limit for gas forecast
tcWellCutoff <- 0.5                                  # Type curve minimum well count % rem needed
tcCurveName <- "BATCH"                               # Type curve name
tcPrimaryPhase <- "OIL"                              # Primary phase - DON'T CHANGE THIS IS CALC'D
tcProbability <- "Avg"                               # Probability for type curve override
tcDeclineMethod <- "batch"                           # Controls whether batch and type curve or type curve
tcBatchName <- "BATCH RUN"                           # Batch process name
tcNglForecast <- FALSE                               # Controls whether NGLs are forecast
tcInitialNgl <- 0                                    # Initial NGL yield for NGL forecast
tcFinalNgl <- 0                                      # Final NGL yield for NGL forecast
tcWaterForecast <- TRUE                              # Controls whether water is forecast
tcSegmentUpper <- 1.05                               # Segmentation sensitivity upper threshold
tcSegmentLower <- 0.95                               # Segmentation sensitivity lower threshold
tcOutlierUpper <- 2.7                                # Outlier sensitivity upper threshold
tcOutlierLower <- 0.6                                # Outlier sensitivity upper threshold
tcPeakMonth <- 0.90                                  # Peak Month detection - as % of production it can skip
tcOverrideMin <- 6                                   # Overrides using Type Curve if fewer than this months
tcFailSafeDeclineMin <- 0.2                          # Minimum decline threshold to set lower b factor max
tcFailSafeBFactorMax <- 1.1                          # Max b factor for wells within minimum decline
quantum_multi_core <- TRUE                           # Controls whether multicore processing is used
batch_max_segs <- 1                                  # Maximum number of segment breaks that can be found
tcForceSegments <- FALSE                             # Forces 1 segment break minimum for all wells if true
gor_check <- FALSE                                   # Controls whether GOR overrides are active
wor_check <- FALSE                                   # Controls whether GOR overrides are active
tcForecastAll <- FALSE                               # Controls whether all segments are forecast or only final
daily_prod <- FALSE                                  # Controls whether monthly or daily production is used
tcProdWeight <- 1.5                                  # Exponential weighting for tail production
bDistCurt <- TRUE                                    # Distribution curtailment
tcOverrideCoef <- FALSE                              # Default for type curve coeficient override
java_home <- NA_character_                           # JAVA directory for multi-core - not neccesary on Databricks
tcNormalizeValue <- 0                                # Normalization value for type curve                  
tcProdGrossNet <- 'GROSS'                            # Gross or net production
tcYears <- 3                                         # Type curve vintage years to include in filtering
tcExternal <- FALSE                                  # Control for smart TC split factor

# Output Tables
# tcForecast
# tcSummary
# tcForecastBatch
# tcSummaryBatch
  
