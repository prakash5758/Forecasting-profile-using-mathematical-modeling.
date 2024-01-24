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

# Jonathan Henderson; Last modified: 29 MAY 2022 2:10 AM MST
# Databricks notebook source
# DBTITLE 1,Import of libraries
library(parallel)
library(data.table)

# COMMAND ----------

copy_cleaned_env <- function(var_env){

  lst <- ls(var_env)

# return everything needed:
df_objects <- lst[sapply(lst,function(var) {any(class(var_env[[var]]) %in% c('numeric',"logical","character","list","data.frame"))})]
# list_objects <- lst[sapply(lst,function(var) any(class(get(var))=='list'))]

# Exclude DATABRICKS variables that start with "D"
df_objects <- df_objects[stringr::str_detect(negate=T,pattern="^D",string=df_objects)]

# # Copying variables to global as needed
for (i in df_objects){
 assign(x=i,value=var_env[[i]],envir=.GlobalEnv)
}
return(NULL)
}

# COMMAND ----------

# DBTITLE 1,Model defaults & constants
##########################################################
# Model defaults & constants #############################
##########################################################

# Set JAVA Home
#Sys.setenv(JAVA_HOME = as.character(javaHome))

# Length of forecast converted from years to monthly
forecastLength <- (tcForecastYears * 12) - 1

# Temporary constants for when functionality is added
tcOverrideCoef <- FALSE
tcExternal <- FALSE

bDistCurt <- TRUE
bDistCurt <-
  ifelse(abs(tcBFactorMax - tcBFactorMin) <= 0.5, FALSE, TRUE)

# Error handling for bad inputs on global b factor
tcBFactorMax <- abs(max(tcBFactorMin + 0.001, tcBFactorMax))
tcBFactorMin <- abs(min(tcBFactorMax - 0.001, tcBFactorMin))

# Error handling and initialization of phase-specific B factors
oil_min_b_pri <-
  as.numeric(abs(ifelse(
    exists("oil_min_b_pri") &
      max(0, get0("oil_min_b_pri"), na.rm = TRUE) != 0,
    as.numeric(oil_min_b_pri),
    max(0.001, as.numeric(tcBFactorMin))
  )))
oil_min_b_sec <-
  as.numeric(abs(ifelse(
    exists("oil_min_b_sec") &
      max(0, get0("oil_min_b_sec"), na.rm = TRUE) != 0,
    as.numeric(oil_min_b_sec),
    max(0.001, as.numeric(tcBFactorMin))
  )))
oil_max_b_pri <-
  as.numeric(abs(ifelse(
    exists("oil_max_b_pri") &
      max(0, get0("oil_max_b_pri"), na.rm = TRUE) != 0,
    as.numeric(oil_max_b_pri),
    max(0.001, as.numeric(tcBFactorMax))
  )))
oil_max_b_sec <-
  as.numeric(abs(ifelse(
    exists("oil_max_b_sec") &
      max(0, get0("oil_max_b_sec"), na.rm = TRUE) != 0,
    as.numeric(oil_max_b_sec),
    max(0.001, as.numeric(tcBFactorMax))
  )))

gas_min_b_pri <-
  as.numeric(abs(ifelse(
    exists("gas_min_b_pri") &
      max(0, get0("gas_min_b_pri"), na.rm = TRUE) != 0,
    as.numeric(gas_min_b_pri),
    max(0.001, as.numeric(tcBFactorMin))
  )))
gas_min_b_sec <-
  as.numeric(abs(ifelse(
    exists("gas_min_b_sec") &
      max(0, get0("gas_min_b_sec"), na.rm = TRUE) != 0,
    as.numeric(gas_min_b_sec),
    max(0.001, as.numeric(tcBFactorMin))
  )))
gas_max_b_pri <-
  as.numeric(abs(ifelse(
    exists("gas_max_b_pri") &
      max(0, get0("gas_max_b_pri"), na.rm = TRUE) != 0,
    as.numeric(gas_max_b_pri),
    max(0.001, as.numeric(tcBFactorMax))
  )))
gas_max_b_sec <-
  as.numeric(abs(ifelse(
    exists("gas_max_b_sec") &
      max(0, get0("gas_max_b_sec"), na.rm = TRUE) != 0,
    as.numeric(gas_max_b_sec),
    max(0.001, as.numeric(tcBFactorMax))
  )))

# Error handling for phase specific b factor bad inputs
oil_min_b_pri <- abs(min(oil_max_b_pri - 0.001, oil_min_b_pri))
oil_min_b_sec <- abs(min(oil_max_b_sec - 0.001, oil_min_b_sec))
oil_max_b_pri <- abs(max(oil_min_b_pri + 0.001, oil_max_b_pri))
oil_max_b_sec <- abs(max(oil_min_b_sec + 0.001, oil_max_b_sec))

gas_min_b_pri <- abs(min(gas_max_b_pri - 0.001, gas_min_b_pri))
gas_min_b_sec <- abs(min(gas_max_b_sec - 0.001, gas_min_b_sec))
gas_max_b_pri <- abs(max(gas_min_b_pri + 0.001, gas_max_b_pri))
gas_max_b_sec <- abs(max(gas_min_b_sec + 0.001, gas_max_b_sec))

default_NRI <- 1
default_WI <- 1

# Rate limit of forecast
rate_limit <- tcRateLimit
rate_limit_secondary <- tcRateLimitSecondary

# Production value below which is flagged as outlier
prod_limit <- tcDowntimeCutoff
prod_limit_secondary <- tcDowntimeCutoffSecondary

tc_override_min <- tcOverrideMin

# Equivalents conversions ratio:1
GAS.BOE <- 6
ECON.BOE <- 20

# Time scalars for unit conversions
MONTH.DAYS <- 30.4375
YEAR.DAYS <- 365.25
YEAR.MONTHS <- 12

# Harmonic & Exponential cutoffs
HARMONIC.EPS <- 2e-3
EXPONENTIAL.EPS <- 2e-3

tcInitialDeclineMax <-
  abs(max(tcInitialDeclineMin + 0.001, tcInitialDeclineMax))
tcInitialDeclineMin <-
  abs(min(tcInitialDeclineMax - 0.001, tcInitialDeclineMin))
tcFinalDeclineMin <-
  abs(min(
    tcFinalDeclineMax - 0.001,
    tcFinalDeclineMin,
    tcInitialDeclineMin
  ))
tcFinalDeclineMax <-
  abs(max(tcFinalDeclineMin + 0.001, tcFinalDeclineMax))
tcInitialDeclineMin <-
  abs(
    max(
      tcInitialDeclineMin,
      tcFinalDeclineMin + 0.001,
      tcFinalDecline + 0.001,
      tcFinalDecline2++0.001
    )
  )

# Default Data Frames
prod_col_vals <-
  c(
    rep('NONE', 2),
    c(format(Sys.time(), '%Y-%m-%d')),
    rep(0, 3),
    rep('FALSE', 3),
    rep(0, 42),
    'NONE',
    c(format(Sys.time(), '%Y-%m-%d')),
    'NONE'
  )

prod_cols <-
  c(
    'api',
    'well_name',
    'date',
    'days_on',
    'index',
    'peak_index',
    'included_oil',
    'included_gas',
    'included_water',
    'oil_actual_daily',
    'oil_actual_monthly',
    'oil_actual_cumulative',
    'oil_pred_daily',
    'oil_pred_monthly',
    'oil_pred_cumulative',
    'oil_combined_monthly',
    'gas_actual_daily',
    'gas_actual_monthly',
    'gas_actual_cumulative',
    'gas_pred_daily',
    'gas_pred_monthly',
    'gas_pred_cumulative',
    'gas_combined_monthly',
    'water_actual_daily',
    'water_actual_monthly',
    'water_actual_cumulative',
    'water_pred_daily',
    'water_pred_monthly',
    'water_pred_cumulative',
    'water_combined_monthly',
    'ngl_actual_daily',
    'ngl_actual_monthly',
    'ngl_actual_cumulative',
    'ngl_pred_daily',
    'ngl_pred_monthly',
    'ngl_pred_cumulative',
    'ngl_combined_monthly',
    'boe6_actual_daily',
    'boe6_actual_monthly',
    'boe6_actual_cumulative',
    'boe6_pred_daily',
    'boe6_pred_monthly',
    'boe6_pred_cumulative',
    'boe6_combined_monthly',
    'boe20_actual_daily',
    'boe20_actual_monthly',
    'boe20_actual_cumulative',
    'boe20_pred_daily',
    'boe20_pred_monthly',
    'boe20_pred_cumulative',
    'boe20_combined_monthly',
    'batch_name',
    'timestamp',
    'unique_id'
  )

summary_col_vals <-
  (
    c(
      rep('NONE', 4),
      rep(0, 10),
      c(0),
      rep('NONE', 7),
      c(0),
      c(format(Sys.time(), '%Y-%m-%d')),
      rep(0, 2),
      c(format(Sys.time(), '%Y-%m-%d')),
      'NONE',
      rep(c(format(
        Sys.time(), '%Y-%m-%d'
      )), 3),
      rep(0, 55),
      rep('NONE', 3),
      rep(0, 5),
      'NONE',
      rep(0, 5),
      'NONE',
      rep(0, 5),
      c(format(Sys.time(), '%Y-%m-%d')),
      rep('NONE', 2),
      rep(0, 6),
      rep(0, 6),
      rep(0, 6)
    )
  )

summary_cols <-
  c(
    'api',
    'well_name',
    'operator',
    'interval',
    'latitude',
    'longitude',
    'latitude_bh',
    'longitude_bh',
    'lateral_length_ft',
    'true_vertical_depth_ft',
    'frac_proppant_lbs',
    'frac_fluid_bbl',
    'net_revenue_interest',
    'working_interest',
    'ip_year',
    'segment',
    'segment_final',
    'single_segment_flag',
    'gor_override',
    'wor_override',
    'forecast_override',
    'forecast_type',
    't_start',
    't_start_date',
    't_seg_months',
    't_end',
    't_end_date',
    'primary_phase',
    'peak_month_oil',
    'peak_month_gas',
    'peak_month_water',
    'oil_pre_forecast',
    'gas_pre_forecast',
    'water_pre_forecast',
    'ngl_pre_forecast',
    'oil_actual_cumulative',
    'oil_eur',
    'oil_remaining',
    'qi_oil',
    'b_oil',
    'di_nom_oil',
    'di_sec_oil',
    'di_tan_oil',
    'dlim_nom_oil',
    'dlim_sec_oil',
    'dlim_tan_oil',
    'q_lim_oil',
    'q_final_oil',
    'gas_actual_cumulative',
    'gas_eur',
    'gas_remaining',
    'qi_gas',
    'b_gas',
    'di_nom_gas',
    'di_sec_gas',
    'di_tan_gas',
    'dlim_nom_gas',
    'dlim_sec_gas',
    'dlim_tan_gas',
    'q_lim_gas',
    'q_final_gas',
    'water_actual_cumulative',
    'water_eur',
    'water_remaining',
    'qi_water',
    'b_water',
    'di_nom_water',
    'di_sec_water',
    'di_tan_water',
    'dlim_nom_water',
    'dlim_sec_water',
    'dlim_tan_water',
    'q_lim_water',
    'q_final_water',
    'ngl_actual_cumulative',
    'ngl_eur',
    'ngl_remaining',
    'q_lim_ngl',
    'q_final_ngl',
    'boe6_actual_cumulative',
    'boe6_eur',
    'boe6_remaining',
    'boe20_actual_cumulative',
    'boe20_eur',
    'boe20_remaining',
    'volatility_score',
    'volatility_flag',
    'error_flag',
    'convergence_oil',
    'iterations_oil',
    'error_oil',
    'error_perc_oil',
    'error_abs_oil',
    'error_abs_perc_oil',
    'convergence_gas',
    'iterations_gas',
    'error_gas',
    'error_perc_gas',
    'error_abs_gas',
    'error_abs_perc_gas',
    'convergence_water',
    'iterations_water',
    'error_water',
    'error_perc_water',
    'error_abs_water',
    'error_abs_perc_water',
    'timestamp',
    'batch_name',
    'unique_id',
    'oil_buildRamp',
    'oil_buildfirstMonth' ,
    'oil_buildAcPeak',
    'oil_buildAlpha',
    'oil_AcPeakIdx' ,
    'oil_ForecastPeakIdx' ,
    'gas_buildRamp',
    'gas_buildfirstMonth' ,
    'gas_buildAcPeak',
    'gas_buildAlpha' ,
    'gas_AcPeakIdx' ,
    'gas_ForecastPeakIdx' ,
    'water_buildRamp',
    'water_buildfirstMonth' ,
    'water_buildAcPeak',
    'water_buildAlpha',
    'water_AcPeakIdx' ,
    'water_ForecastPeakIdx'
  )

default_df_prod <- data.frame(api = 'NONE')
default_df_prod[, c(prod_cols)] <- c(prod_col_vals)
default_df_summary <- data.frame(api = 'NONE')
default_df_summary[, c(summary_cols)] <- c(summary_col_vals)

# COMMAND ----------

# MAGIC %md
# MAGIC # Helper functions

# COMMAND ----------

# DBTITLE 1,Segmentation methods
##########################################################
# Segmentation methods ###################################
##########################################################

find.segments <-
  function(prod,
           # numeric vector of production
           prod_index,
           # integer vector of producing months 1:n
           return_all = FALSE,
           # return all segments
           log_x = TRUE,
           # log x axis for segment detection
           segment_size = 0.06,
           # fraction of production to look for segments
           segment_max = 10,
           # max number of segments
           min_months_start = 6,
           # minimum months segments can be from first prod mo
           min_months_end = 12,
           # minimum months segments can be to last prod mo
           min_months_between = 12,
           # minimum mo between segments
           force_segments = tcForceSegments,
           # boolean flag to force segments
           force_segment_min = 17,
           # min number of months required for forced segment
           force_segment_frac = 0.35,
           # fraction of producing months for forced segment
           outlier_upper = 2.0,
           # threshold for exceeding upper outlier band
           outlier_lower = 0.8,
           # threshold for exceeding lower outlier band
           segment_upper = 1.1,
           # threshold for statistic significance on upper segment
           segment_lower = 0.8,
           # threshold for statistic significance on upper segment
           include_lower = TRUE,
           # include lower bound for statistic significance
           prod_limit = 0.01) {
    # production limit

    # segment length is <= 1 or if production length is less than the min months, return zero segment breaks
    if (segment_max <= 1 | length(prod) <= min_months_start) {
      change_points <- 0
      return(change_points)
    } else {
      # run outliers to impute points for segmentation
      prod_outliers <- tryCatch(
        find.outliers(
          prod,
          prod_index,
          upper.bound = outlier_upper,
          lower.bound = outlier_lower,
          qlimit = prod_limit
        ),
        error = function(e) {
          prod_outliers <- rep("TRUE", length(prod))
        }
      )

      prod_loess <-
        suppressWarnings(tryCatch(
          loess(prod ~ prod_index)$fitted,
          error = function(e) {
            prod_loess <- suppressWarnings(smooth.spline(prod, df = 4)$y)
          }
        ))

      # compute moving average
      prod_ewma <- ewa.avg(prod)

      # calculate upper and lower bounds for segmentation heuristics
      loess_upper <- prod_loess * segment_upper
      ewma_lower <- prod_ewma * segment_lower

      prod_alt <- replace.na(prod)
      prod_alt[prod_outliers == FALSE] <-
        prod_ewma[prod_outliers == FALSE]
      prod_alt[prod_alt == 0] <- 0.0001

      change_points_initial <- 0

      if (log_x) {
        change_points_initial <-
          tryCatch(
            breakpoints(
              log(prod_alt) ~ log(prod_index),
              h = round(length(prod_alt) /
                          10, 0),
              breaks = segment_max
            ),
            error = function(e) {
              change_points_initial =
                tryCatch(
                  breakpoints(
                    log(prod_alt) ~ log(prod_index),
                    h = round(length(prod_alt) /
                                10, 0),
                    breaks = segment_max
                  ),
                  error = function(e) {
                    change_points_initial = list(breakpoints = as.numeric(0))
                  }
                )
            }
          )
      } else {
        change_points_initial <-
          tryCatch(
            breakpoints(
              log(prod_alt) ~ prod_index,
              h = round(length(prod_alt) /
                          10, 0),
              breaks = segment_max
            ),
            error = function(e) {
              change_points_initial =
                tryCatch(
                  breakpoints(
                    log(prod_alt) ~ prod_index,
                    h = round(length(prod_alt) /
                                10, 0),
                    breaks = segment_max
                  ),
                  error = function(e) {
                    change_points_initial = list(breakpoints = as.numeric(0))
                  }
                )
            }
          )
      }

      # ensure change points are not null and continue with heuristics
      if (length(change_points_initial$breakpoints) > 1) {
        change_points <- c(change_points_initial$breakpoints + 1)
      } else {
        change_points <- 0
      }
      # if return all is set to true, passes heuristics tests
      if (return_all == FALSE) {
        # heuristic 1
        # test for significance via deviation from loess and ewma fits
        change_points <-
          change_points[prod[change_points] > loess_upper[change_points] |
                          # uses actual production
                          prod_alt[change_points] < ewma_lower[change_points]]   # uses imputed production

        # use lower thresholds to trigger segments as well
        if (include_lower) {
          change_points <-
            change_points[prod[change_points] > loess_upper[change_points] |
                            # uses actual production
                            prod_alt[change_points] < ewma_lower[change_points]] # uses imputed production
        } else {
          change_points <-
            change_points[prod[change_points] > prod_loess[change_points]]     # uses actual production
        }

        # heuristic 2
        # eliminate points near final producing months and closer than min seg start
        if (max(0, change_points, na.rm = TRUE) >= 1) {
          rem_prod <-
            prod_outliers[max(change_points, na.rm = TRUE):length(prod_outliers)]
          rem_prod <- rem_prod[rem_prod == TRUE]
          if ((max(change_points, na.rm = TRUE) + min_months_end > length(prod)) == TRUE |
              (length(rem_prod) < min_months_end) == TRUE) {
            change_points <- head(change_points, -1)
          }
        }

        if (any(change_points > length(prod) - min_months_end)) {
          change_points <-
            change_points[change_points <= length(prod) - min_months_end]
        }

        if (any(change_points < min_months_start)) {
          change_points <- change_points[change_points >= min_months_start]
        }

        # heuristic 3
        # remove change least significant change point when closer than n months apart
        if (length(change_points) >= 2) {
          dist_between <- diff(change_points)
          if ((min(dist_between) < min_months_between &
               length(change_points) > 2) == TRUE) {
            no_pts <- length(change_points)
            bad_pts <- rep(FALSE, no_pts)

            # eliminate peaks that are close to bigger peaks
            for (i in 1:no_pts) {
              pos_i <- change_points[i]
              if (!bad_pts[i]) {
                pos_f <- abs(pos_i - change_points)
                bad_pts <-
                  bad_pts | (pos_f > 0 & pos_f < min_months_between)
              }
            }
            # select the good peaks
            change_points <- change_points[!bad_pts, drop = FALSE]
          }
        }

        if (length(change_points) >= 2) {
          tail_prod <- prod[tail(change_points, 1):length(prod)]
          if (mean(tail_prod, na.rm = TRUE) < prod_limit |
              length(tail_prod[tail_prod > prod_limit]) < min_months_end) {
            change_points <- head(change_points, -1)
          }
        }
      }
      # force segments if production length exceeds threshold and option true
      if (force_segments) {
        if (length(prod[prod_outliers == TRUE]) >= force_segment_min &
            sum(change_points, na.rm = TRUE) == 0) {
          change_points <-
            max(round(max(prod_index, na.rm = TRUE) * force_segment_frac, 0), min_months_start)
        }
      }
      return(change_points)
    }
  }

# recursive residuals
recresid <- function(x, ...)
{
  UseMethod("recresid")
}

recresid.formula <- function(formula, data = list(), ...)
{
  mf <- model.frame(formula, data = data)
  y <- model.response(mf)
  modelterms <- terms(formula, data = data)
  X <- model.matrix(modelterms, data = data)
  rr <- recresid(X, y, ...)
  return(rr)
}

recresid.lm <- function(x, data = list(), ...)
{
  X <-
    if (is.matrix(x$x))
      x$x
  else
    model.matrix(terms(x), model.frame(x))
  y <- if (is.vector(x$y))
    x$y
  else
    model.response(model.frame(x))
  rr <- recresid(X, y, ...)
  return(rr)
}

# R version of recresid.
recresid_r <- function(x,
                       y,
                       start = ncol(x) + 1,
                       end = nrow(x),
                       tol = sqrt(.Machine$double.eps) / ncol(x),
                       qr.tol = 1e-7,
                       ...)
{
  n <- end
  q <- start - 1
  k <- ncol(x)
  rval <- rep(0, n - q)

  # convenience function to replace NAs with 0s in coefs
  coef0 <- function(obj) {
    cf <- obj$coefficients
    ifelse(is.na(cf), 0, cf)
  }
  Xinv0 <- function(obj) {
    qr <- obj$qr
    rval <- matrix(0, ncol = k, nrow = k)
    wi <- qr$pivot[1:qr$rank]
    rval[wi, wi] <-
      chol2inv(qr$qr[1:qr$rank, 1:qr$rank, drop = FALSE])
    rval
  }

  # initialize recursion
  y1 <- y[1:q]
  fm <- lm.fit(x[1:q, , drop = FALSE], y1, tol = qr.tol, ...)
  X1 <- Xinv0(fm)
  betar <- coef0(fm)
  xr <- as.vector(x[q + 1, ])
  fr <- as.vector((1 + (t(xr) %*% X1 %*% xr)))
  rval[1] <- (y[q + 1] - t(xr) %*% betar) / sqrt(fr)

  # check recursion agains full QR decomposition?
  check <- TRUE

  if ((q + 1) < n)
  {
    for (r in ((q + 2):n))
    {
      # check for NAs in coefficients
      nona <- all(!is.na(fm$coefficients))

      # recursion formula
      X1 <- X1 - (X1 %*% outer(xr, xr) %*% X1) / fr
      betar <- betar + X1 %*% xr * rval[r - q - 1] * sqrt(fr)

      # full QR decomposition
      if (check) {
        y1 <- y[1:(r - 1)]
        fm <-
          lm.fit(x[1:(r - 1), , drop = FALSE], y1, tol = qr.tol, ...)
        nona <-
          nona & all(!is.na(betar)) & all(!is.na(fm$coefficients))
        # keep checking?
        if (nona &&
            isTRUE(all.equal(as.vector(fm$coefficients), as.vector(betar), tol = tol)))
          check <- FALSE
        X1 <- Xinv0(fm)
        betar <- coef0(fm)
      }

      # residual
      xr <- as.vector(x[r, ])
      fr <- as.vector((1 + (t(xr) %*% X1 %*% xr)))
      rval[r - q] <- (y[r] - sum(xr * betar, na.rm = TRUE)) / sqrt(fr)
    }
  }
  return(rval)
}


# C version of recresid.
recresid_c <- function(x,
                       y,
                       start = ncol(x) + 1,
                       end = nrow(x),
                       tol = sqrt(.Machine$double.eps) / ncol(x),
                       ...)
{
  n <- end
  q <- start - 1
  k <- ncol(x)
  rval <- rep(0, n - q)

  # convenience function to replace NAs with 0s in coefs
  coef0 <- function(obj) {
    cf <- obj$coefficients
    ifelse(is.na(cf), 0, cf)
  }
  Xinv0 <- function(obj) {
    qr <- obj$qr
    rval <- matrix(0, ncol = k, nrow = k)
    wi <- qr$pivot[1:qr$rank]
    rval[wi, wi] <-
      chol2inv(qr$qr[1:qr$rank, 1:qr$rank, drop = FALSE])
    rval
  }

  # initialize recursion
  y1 <- y[1:q]
  fm <- lm.fit(x[1:q, , drop = FALSE], y1)
  X1 <- Xinv0(fm)
  betar <- coef0(fm)
  xr <- as.vector(x[q + 1,])
  fr <- as.vector((1 + (t(xr) %*% X1 %*% xr)))
  rval[1] <- (y[q + 1] - t(xr) %*% betar) / sqrt(fr)
  check <- TRUE

  # fallback function to be called from C
  fallback <- function(r, fm, betar, check) {
    nona <- all(!is.na(fm$coefficients))
    y1 <- y[1:(r - 1)]
    fm <- lm.fit(x[1:(r - 1), , drop = FALSE], y1)
    nona <- nona & all(!is.na(betar)) & all(!is.na(fm$coefficients))
    if (nona &&
        isTRUE(all.equal(as.vector(fm$coefficients), as.vector(betar), tol = tol)))
      check <- FALSE
    return(list(
      "fm" = fm,
      "X1" = Xinv0(fm),
      "betar" = coef0(fm),
      "check" = check
    ))
  }

  rho <- new.env()

  ## run the C version of recresid
  if ((q + 1) < n) {
    rval <-
      .Call(
        "recresid",
        as.integer(q + 2),
        as.integer(n),
        X1,
        xr,
        fr,
        betar,
        rval,
        x,
        y,
        check,
        fallback,
        fm,
        rho,
        PACKAGE = "strucchange"
      )
  }

  return(rval)
}

# default wrapper for recresid.
recresid.default <-
  function(x,
           y,
           start = ncol(x) + 1,
           end = nrow(x),
           tol = sqrt(.Machine$double.eps) / ncol(x),
           qr.tol = 1e-7,
           engine = c("R", "C"),
           ...)
  {
    # checks and data dimensions
    stopifnot(start > ncol(x) & start <= nrow(x))
    stopifnot(end >= start & end <= nrow(x))

    engine <- match.arg(engine, c("R", "C"))

    recresid_fun <- switch(engine,
                           "R" = recresid_r,
                           "C" = recresid_c)

    return(recresid_fun(
      x = x,
      y = y,
      start = start,
      end = end,
      tol = tol,
      qr.tol = qr.tol,
      ...
    ))
  }

breakpoints <- function(obj, ...)
{
  UseMethod("breakpoints")
}

breakpoints.Fstats <- function(obj, ...)
{
  RVAL <- list(
    breakpoints = obj$breakpoint,
    RSS = obj$RSS,
    nobs = obj$nobs,
    nreg = obj$nreg,
    call = match.call(),
    datatsp = obj$datatsp
  )
  class(RVAL) <- "breakpoints"
  return(RVAL)
}

breakpoints.formula <- function(formula,
                                h = 0.15,
                                breaks = NULL,
                                data = list(),
                                ...)
{
  mf <- model.frame(formula, data = data)
  y <- model.response(mf)
  modelterms <- terms(formula, data = data)
  X <- model.matrix(modelterms, data = data)

  n <- nrow(X)
  k <- ncol(X)
  intercept_only <- isTRUE(all.equal(as.vector(X), rep(1L, n)))
  if (is.null(h))
    h <- k + 1
  if (h < 1)
    h <- floor(n * h)
  if (h <= k)
    stop("minimum segment size must be greater than the number of regressors")
  if (h > floor(n / 2))
    stop("minimum segment size must be smaller than half the number of observations")
  if (is.null(breaks)) {
    breaks <- ceiling(n / h) - 2
  } else {
    if (breaks < 1) {
      breaks <- 1
      warning("number of breaks must be at least 1")
    }
    if (breaks > ceiling(n / h) - 2) {
      breaks0 <- breaks
      breaks <- ceiling(n / h) - 2
      warning(sprintf(
        "requested number of breaks = %i too large, changed to %i",
        breaks0,
        breaks
      ))
    }
  }

  RSSi <- function(i)
  {
    ssr <- if (intercept_only) {
      (y[i:n] - cumsum(y[i:n]) / (1L:(n - i + 1L)))[-1L] * sqrt(1L + 1L / (1L:(n -
                                                                                 i)))
    } else {
      recresid(X[i:n, , drop = FALSE], y[i:n], ...)
    }
    c(rep(NA, k), cumsum(ssr ^ 2))
  }

  RSS.triang <- sapply(1:(n - h + 1), RSSi)

  # function to extract the RSS(i,j) from RSS.triang
  RSS <- function(i, j)
    RSS.triang[[i]][j - i + 1]

  # compute optimal previous partner if observation i is the mth break
  # store results together with RSSs in RSS.table; breaks = 1
  index <- h:(n - h)
  break.RSS <- sapply(index, function(i)
    RSS(1, i))

  RSS.table <- cbind(index, break.RSS)
  rownames(RSS.table) <- as.character(index)

  # breaks >= 2
  extend.RSS.table <- function(RSS.table, breaks)
  {
    if ((breaks * 2) > ncol(RSS.table)) {
      for (m in (ncol(RSS.table) / 2 + 1):breaks)
      {
        my.index <- (m * h):(n - h)
        my.RSS.table <- RSS.table[, c((m - 1) * 2 - 1, (m - 1) * 2)]
        my.RSS.table <- cbind(my.RSS.table, NA, NA)
        for (i in my.index)
        {
          pot.index <- ((m - 1) * h):(i - h)
          break.RSS <-
            sapply(pot.index, function(j)
              my.RSS.table[as.character(j), 2] + RSS(j + 1, i))
          opt <- which.min(break.RSS)
          my.RSS.table[as.character(i), 3:4] <-
            c(pot.index[opt], break.RSS[opt])
        }
        RSS.table <- cbind(RSS.table, my.RSS.table[, 3:4])
      }
      colnames(RSS.table) <-
        as.vector(rbind(
          paste("break", 1:breaks, sep = ""),
          paste("RSS", 1:breaks, sep = "")
        ))
    }
    return(RSS.table)
  }

  RSS.table <- extend.RSS.table(RSS.table, breaks)

  # extract optimal breaks
  extract.breaks <- function(RSS.table, breaks)
  {
    if ((breaks * 2) > ncol(RSS.table))
      stop("compute RSS.table with enough breaks before")
    index <- RSS.table[, 1, drop = TRUE]
    break.RSS <-
      sapply(index, function(i)
        RSS.table[as.character(i), breaks * 2] + RSS(i + 1, n))
    opt <- index[which.min(break.RSS)]
    if (breaks > 1) {
      for (i in ((breaks:2) * 2 - 1))
        opt <- c(RSS.table[as.character(opt[1]), i], opt)
    }
    names(opt) <- NULL
    return(opt)
  }

  opt <- extract.breaks(RSS.table, breaks)

  if (is.ts(data)) {
    if (NROW(data) == n)
      datatsp <- tsp(data)
    else
      datatsp <- c(1 / n, 1, n)
  } else {
    env <- environment(formula)
    if (missing(data))
      data <- env
    orig.y <- eval(attr(modelterms, "variables")[[2]], data, env)
    if (is.ts(orig.y) & (NROW(orig.y) == n))
      datatsp <- tsp(orig.y)
    else
      datatsp <- c(1 / n, 1, n)
  }

  RVAL <- list(
    breakpoints = opt,
    RSS.table = RSS.table,
    RSS.triang = RSS.triang,
    RSS = RSS,
    extract.breaks = extract.breaks,
    extend.RSS.table = extend.RSS.table,
    nobs = n,
    nreg = k,
    y = y,
    X = X,
    call = match.call(),
    datatsp = datatsp
  )
  class(RVAL) <- c("breakpointsfull", "breakpoints")
  RVAL$breakpoints <- breakpoints(RVAL)$breakpoints
  return(RVAL)
}

breakpoints.breakpointsfull <- function(obj, breaks = NULL, ...)
{
  if (is.null(breaks))
  {
    sbp <- summary(obj)
    breaks <- which.min(sbp$RSS["BIC", ]) - 1
  }
  if (breaks < 1)
  {
    breakpoints <- NA
    RSS <- obj$RSS(1, obj$nobs)
  } else {
    RSS.tab <- obj$extend.RSS.table(obj$RSS.table, breaks)
    breakpoints <- obj$extract.breaks(RSS.tab, breaks)
    bp <- c(0, breakpoints, obj$nobs)
    RSS <- sum(apply(cbind(bp[-length(bp)] + 1, bp[-1]), 1,
                     function(x)
                       obj$RSS(x[1], x[2])))
  }
  RVAL <- list(
    breakpoints = breakpoints,
    RSS = RSS,
    nobs = obj$nobs,
    nreg = obj$nreg,
    call = match.call(),
    datatsp = obj$datatsp
  )
  class(RVAL) <- "breakpoints"
  return(RVAL)
}

summary.breakpointsfull <- function(object,
                                    breaks = NULL,
                                    sort = TRUE,
                                    format.times = NULL,
                                    ...)
{
  if (is.null(format.times))
    format.times <-
      ((object$datatsp[3] > 1) & (object$datatsp[3] < object$nobs))
  if (is.null(breaks))
    breaks <- ncol(object$RSS.table) / 2
  n <- object$nobs
  RSS <- c(object$RSS(1, n), rep(NA, breaks))
  BIC <-
    c(n * (log(RSS[1]) + 1 - log(n) + log(2 * pi)) + log(n) * (object$nreg + 1),
      rep(NA, breaks))
  names(RSS) <- as.character(0:breaks)
  bp <- breakpoints(object, breaks = breaks)
  RSS[breaks + 1] <- bp$RSS
  BIC[breaks + 1] <- AIC(bp, k = log(n))
  bp <- bp$breakpoints
  if (breaks > 1) {
    for (m in (breaks - 1):1)
    {
      bp <- rbind(NA, bp)
      bpm <- breakpoints(object, breaks = m)
      if (sort) {
        pos <- apply(outer(
          bpm$breakpoints,
          bp[nrow(bp), ],
          FUN = function(x, y)
            abs(x - y)
        ),
        1,
        which.min)
        if (length(pos) > unique(length(pos))) {
          warning("sorting not possible", call. = FALSE)
          sort <- FALSE
        }
      }
      if (!sort)
        pos <- 1:m
      bp[1, pos] <- bpm$breakpoints
      RSS[m + 1] <- bpm$RSS
      BIC[m + 1] <- AIC(bpm, k = log(n))
    }
  } else {
    bp <- as.matrix(bp)
  }
  rownames(bp) <- as.character(1:breaks)
  colnames(bp) <- rep("", breaks)
  RSS <- rbind(RSS, BIC)
  rownames(RSS) <- c("RSS", "BIC")
  RVAL <- list(breakpoints = bp,
               RSS = RSS,
               call = object$call)
  class(RVAL) <- "summary.breakpointsfull"
  return(RVAL)
}

logLik.breakpoints <- function(object, ...)
{
  n <- object$nobs
  df <-
    (object$nreg + 1) * (length(object$breakpoints[!is.na(object$breakpoints)]) + 1)
  logL <- -0.5 * n * (log(object$RSS) + 1 - log(n) + log(2 * pi))
  attr(logL, "df") <- df
  class(logL) <- "logLik"
  return(logL)
}


# COMMAND ----------

# DBTITLE 1,Outlier function
##########################################################
# Outlier function #######################################
##########################################################

# outlier detection
find.outliers <-
  function(prod,
           prod_index,
           upper.bound,
           lower.bound,
           qlimit = 0.01) {
    prod_len <- length(prod)

    if (prod_len == forecastLength + 1) {
      outs <- rep('TRUE', length(prod))
      outs[prod < qlimit] <- FALSE
      outs[round(tcWellCutoff * prod_len, 0):prod_len] <- FALSE
      return(outs)
    }

    if (length(prod) >= 6) {
      loess_outs <- suppressWarnings(tryCatch(
        loess(prod ~ prod_index, span = 0.3),
        error = function(e) {
          loess_outs <- loess(prod ~ prod_index)
        }
      ))
    } else {
      outs <- rep('TRUE', length(prod))
      outs[prod < qlimit] <- FALSE
      return(outs)
    }

    ewma <- ewa.avg(prod)

    lower_lowess <- loess_outs$fitted * lower.bound
    upper_lowess <- loess_outs$fitted * upper.bound
    upper_ewma <- ewma * upper.bound
    upper_limit <- pmax(upper_lowess, upper_ewma)

    # Lowess model outliers using lower and upper band - assign TRUE/FALSE for inclusion
    outs <- rep('TRUE', length(prod))
    outs[prod <= lower_lowess | (prod > upper_limit)] <- FALSE
    outs[prod < qlimit] <- FALSE

    peak <- peak.production.t(prod, tcPeakMonth)
    post_peak_primary <- prod_index >= prod_index[peak]
    outs[!post_peak_primary] <- FALSE
    outs[peak] <- TRUE
    return(outs)
  }


# COMMAND ----------

# DBTITLE 1,Cost functions
##########################################################
# Cost functions #########################################
##########################################################

# sum-of-squared-errors cost function
sse <- function (q, forecast) {
  sum((q - forecast) ^ 2)
}

# sum-of-absolute-errors cost function
sae <- function (q, forecast) {
  sum(abs(q - forecast))
}

# mean-absolute-errors cost function
mae <- function (q, forecast) {
  mean(abs(q - forecast))
}

# mean-of-squared-errors cost function
mse <- function (q, forecast) {
  mean((q - forecast) ^ 2)
}

# sum of squared-log-errors cost function
ssle <- function (q, forecast) {
  if (length(q) <= 24 |
      tcProdWeight == 0 | length(q) == forecastLength + 1) {
    prod_weight <- 1
  } else {
    wghts <- tcProdWeight
    prod_weight <-
      exp(wghts * seq(1, length.out = length(q)) / length(q))
  }
  err <- ((log(q) - log(forecast)) ^ 2)
  sum(err * prod_weight)
}

ssle_cum <- function (q, forecast) {
  if (length(q) <= 24 |
      tcProdWeight == 0 | length(q) == forecastLength + 1) {
    prod_weight <- 1
  } else {
    wghts <- tcProdWeight
    prod_weight <-
      exp(wghts * seq(1, length.out = length(q)) / length(q))
  }
  err <- ((log(cumsum.na(q)) - log(cumsum.na(forecast))) ^ 2)
  sum(err * prod_weight)
}

# root-mean-square-error cost function
rmse <- function (q, forecast) {
  sqrt(mean((q - forecast) ^ 2))
}

# root-mean-square-log-error cost function
rmsle <- function (q, forecast) {
  sqrt(mean((log(1 + q) - log(1 + forecast)) ^ 2))
}

# mean average percentage error
mape <- function(q, forecast) {
  mean(abs((q - forecast) / q))
}

# median absoute percentage error
medape <- function(q, forecast) {
  median(abs((q - forecast) / q))
}

# relative absolute error loss
rae <- function(q, forecast) {
  sum(abs(q - forecast)) / sum(abs(q - mean(q)))
}

# root relative squared error loss
rrse <- function(q, forecast) {
  sqrt(sum((q - forecast) ^ 2) / sum((q - mean(q)) ^ 2))
}

# pseudo-Huber loss function
pseudo.huber <- function (q, forecast, delta = 1.345) {
  if (length(q) <= 12 |
      tcProdWeight == 0 | length(q) == forecastLength + 1) {
    prod_weight <- 1
  } else {
    wghts <- tcProdWeight
    prod_weight <-
      exp(wghts * seq(1, length.out = length(q)) / length(q))
  }
  err <- abs(log(q) - log(forecast))
  subsets = ifelse(err <= delta, err, 2 * err ^ 0.5 - 1)
  sum(subsets * prod_weight)
}

pseudo.huber_cum <- function (q, forecast, delta = 1.345) {
  if (length(q) <= 12 |
      tcProdWeight == 0 | length(q) == forecastLength + 1) {
    prod_weight <- 1
  } else {
    wghts <- tcProdWeight
    prod_weight <-
      exp(wghts * seq(1, length.out = length(q)) / length(q))
  }
  err <- abs(log(cumsum.na(q)) - log(cumsum.na(forecast)))
  subsets = ifelse(err <= delta, err, 2 * err ^ 0.5 - 1)
  sum(subsets * prod_weight)
}

# huber loss function
huber <- function(q, forecast, delta = 1.345) {
  if (length(q) <= 12 | tcProdWeight == 0) {
    prod_weight <- 1
  } else {
    wghts <- tcProdWeight
    prod_weight <-
      exp(wghts * seq(1, length.out = length(q)) / length(q))
  }
  a <- q - forecast
  subsets <- (delta ^ 2 * (sqrt(1 + (a / delta) ^ 2) - 1))
  sum(subsets * prod_weight)
}

huber_cum <- function(q, forecast, delta = 1.345) {
  if (length(q) <= 12 | tcProdWeight == 0) {
    prod_weight <- 1
  } else {
    wghts <- tcProdWeight
    prod_weight <-
      exp(wghts * seq(1, length.out = length(q)) / length(q))
  }
  a <- cumsum.na(q) - cumsum.na(forecast)
  subsets <- (delta ^ 2 * (sqrt(1 + (a / delta) ^ 2) - 1))
  sum(subsets * prod_weight)
}

# log.cosh loss
log.cosh <- function(q, forecast) {
  err <- abs(q - forecast)
  loss <- log(cosh(err))
  sum(loss)
}

log_eur_weight<- function(q, forecast) {
  eur12_diff <- sum(log(q[1:12]))-sum(log(forecast[1:12]))
  eur_diff <- sum(log(q))-sum(log(forecast))

  weight <- sum(log(q[1:12]))/(sum(log(q[1:12]))+sum(log(q)))
  weighted_error <- abs(eur12_diff) * (1 - weight) + abs(eur_diff) * weight

  return(weighted_error)
}

log_eur_<- function(q, forecast) {
  eur12_diff <- sum(log(q[1:12]))-sum(log(forecast[1:12]))
  eur_diff <- sum(log(q))-sum(log(forecast))

  weighted_error <- abs(eur12_diff) + abs(eur_diff)

  return(weighted_error)
}
eur_weight<- function(q, forecast) {
  eur12_diff <- sum(q[1:12])-sum(forecast[1:12])
  eur_diff <- sum(q)-sum(forecast)
  if (sum(q[1:12]) + sum(q)==0){
    weight <- 0.5
  } else {
    weight <- sum(q[1:12])/(sum(q[1:12])+sum(q))
  }
  weighted_error <- abs(eur12_diff) * (1 - weight) + abs(eur_diff) * weight

  return(weighted_error)
}
just_eur<- function(q, forecast) {
  eur12_diff <- sum(q[1:12])-sum(forecast[1:12])
  eur_diff <- sum(q)-sum(forecast)

  weighted_error <- abs(eur12_diff) + abs(eur_diff)

  return(weighted_error)
}


# COMMAND ----------

# DBTITLE 1,Decline conversions
##########################################################
# Decline conversions ####################################
##########################################################

# Nominal decline from tangent
nominal.from.tangent <- function(Deff) {
  (-log(1 - Deff))
}

# Secant effective decline from nominal
secant.from.nominal <- function(Dnom, b) {
  if (b < EXPONENTIAL.EPS)
    tangent.from.nominal(Dnom)
  else
    1 - (1 + b * Dnom) ^ (-1 / b)
}

# Tangent effective decline from nominal
tangent.from.nominal <- function(Dnom) {
  1 - exp(-Dnom)
}

nominal.from.secant <- function(Dsec, b) {
  if (b < EXPONENTIAL.EPS)
    nominal.from.tangent(Dsec)
  else
    (1/b)*(((1-Dsec)^(-b))-1)
}


# COMMAND ----------

# DBTITLE 1,Decline Methods
##########################################################
# Decline Methods ########################################
##########################################################

# Exponential decline
exponential.q <- function (qi, Di, t) {
  qi * exp(-Di * t)
}

# Harmonic decline
harmonic.q <- function (qi, Di, t) {
  qi / (1 + Di * t)
}

# Hyperbolic decline
hyperbolic.q <- function (qi, Di, b, t) {
  if (abs(b - 1) < HARMONIC.EPS)
    harmonic.q(qi, Di, t)

  else if (abs(b) < EXPONENTIAL.EPS)
    exponential.q(qi, Di, t)

  else
    qi / (1 + b * Di * t) ^ (1 / b)
}


# COMMAND ----------

# DBTITLE 1,Cumulative Production
##########################################################
# Cumulative Production ##################################
##########################################################

# Exponential cumulative production
exponential.np <- function (qi, Di, t) {
  if (Di == 0)
    qi * t
  else
    qi / Di * (1 - exp(-Di * t))
}

# Harmonic cumulative production
harmonic.np <- function (qi, Di, t) {
  if (Di == 0)
    qi * t
  else
    qi / Di * log(1 + Di * t)
}

# Hyperbolic cumulative production
hyperbolic.np <- function(qi, Di, b, t) {
  if (abs(b - 1) < HARMONIC.EPS) {
    harmonic.np(qi, Di, t)
  } else if (abs(b) < EXPONENTIAL.EPS) {
    exponential.np(qi, Di, t)
  } else {
    qi / ((1 - b) * Di) * (1 - (1 + b * Di * t) ^ (1 - 1 / (b)))
  }
}


# COMMAND ----------

# DBTITLE 1,Other decline functions
##########################################################
# Other decline functions ################################
##########################################################

# Hyperbolic flow rate when rate limit is reached
hyp.q.lim <- function (qi, b, Di, Df) {
  q.lim = qi * (Df / Di) ^ (1 / b)
  q.lim
}

# Time limit when flow rate is reached
hyp.t.lim <- function (qi, qlim, b, Di) {
  t.lim = ((qi / qlim) ^ b - 1) / (b * Di)
  t.lim
}


# COMMAND ----------

# DBTITLE 1,Rate and Time Calculations
##########################################################
# Rate and Time Calculations #############################
##########################################################

forecast.exp.cum <- function(qi, qf, di) {
  return((qi - qf) / di)
}

forecast.hyp.cum <- function(qi, qf, b, di) {
  return(((qi ^ b) / (di * (1 - b))) * (qi ^ (1 - b) - qf ^ (1 - b)))
}

forecast.eur <- function(qi, b, di, dlim, qf) {
  if (b <= EXPONENTIAL.EPS) {
    return (forecast.exp.cum(qi, qf, di))
  } else {
    qlim = hyp.q.lim(qi, dlim, b, di)
  }
  if (qlim > qf) {
    return(forecast.hyp.cum(qi, qf, b, di) + forecast.exp.cum(qi, qf, di))
  } else {
    return(forecast.hyp.cum(qi, qf, b, di))
  }
}


# COMMAND ----------

# DBTITLE 1,Hyperbolic to exponential functions
##########################################################
# Hyperbolic to exponential functions ####################
##########################################################

# Hyp to exponential transition
hyp2exp.transition <- function (Di, b, Df) {
  if (Di < EXPONENTIAL.EPS ||
      Df < EXPONENTIAL.EPS || b < HARMONIC.EPS)
    Inf
  else if (abs(Df - Di) < EXPONENTIAL.EPS)
    0
  else
    (Di/Df-1)/(b*Di)
}

# Hyperbolic to exponential instantaneous flow rate calculation
hyp2exp.q <- function (qi, Di, b, Df, t) {
  t.trans <- hyp2exp.transition(Di, b, Df)
  q.trans <- hyperbolic.q(qi, Di, b, t.trans)

  q <- hyperbolic.q(qi, Di, b, t)
  q[t > t.trans] <-
    exponential.q(q.trans, Df, t[t > t.trans] - t.trans)
  q
}


# Hyperbolic to exponential cumulative production calculation
hyp2exp.np <- function (qi, Di, b, Df, t) {
  t.trans <- hyp2exp.transition(Di, b, Df)
  q.trans <- hyperbolic.q(qi, Di, b, t.trans)
  np.trans <- hyperbolic.np(qi, Di, b, t.trans)

  np <- hyperbolic.np(qi, Di, b, t)
  np[t > t.trans] <- np.trans +
    exponential.np(q.trans, Df, t[t > t.trans] - t.trans)
  np
}

multi_segment2.q <- function( qi, Di, b, Df, alpha, texp_idx, t) {
  thyp_idx <- length(t)
  if (texp_idx == 1) {
    q_hyp <- hyp2exp.np(qi, Di, b, Df, t)
    return(q_hyp)
  } else {
    # convert texp_idx into integer
    integer_part <- floor(texp_idx)
    decimal_part <- texp_idx - integer_part
    t.trans <- t[integer_part]+(t[integer_part+1]-t[integer_part])*decimal_part
    q.trans <- exponential.q(qi, alpha, t.trans)
    q <-exponential.q(qi, alpha, t)
    q[t > t.trans] <- hyp2exp.q(q.trans, Di, b, Df, t[t > t.trans] - t.trans)
    q
  }
}

multi_segment2.np <- function(qi, Di, b, Df, alpha, texp_idx, t) {
  # expotential and hyperbolic cumulative flow calculation
  thyp_idx <- length(t)
  if (texp_idx == 1) {
    np_hyp <- hyp2exp.np(qi, Di, b, Df, t)
    return(np_hyp)
  } else {
    t.trans <- t[texp_idx]
    q.trans <- exponential.q(qi, alpha, t.trans)
    np.trans <- exponential.np(qi, alpha, t.trans)

    np <- exponential.np(qi, alpha, t)
    np[t > t.trans] <- np.trans +
    hyp2exp.np(q.trans, Di, b, Df, t[t > t.trans] - t.trans)
  np
  }
}

# COMMAND ----------

# DBTITLE 1,Decline fitting functions
##########################################################
# Decline fitting functions ##############################
##########################################################

fit.hyp2exp <-
  function (q,
            t,
            phase.active,
            phase.major,
            min.b = tcBFactorMin,
            max.b = tcBFactorMax,
            gor.override = FALSE) {
    if (length(q) != length(t)) {
      stop("Invalid lengths for q, t vectors.")
    } else if (length(q) <= 2) {
      # Default coefs for 2 or fewer points
      list(
        decline = c(
          qi = max(q, na.rm = TRUE),
          Di = NA,
          b = NA,
          Df = NA
        ),
        convergence = c(
          conv = 52,
          iter = 0,
          message = NA_character_
        )
      )
    } else {
      if (gor.override == FALSE) {
        b_max <- max.b
        b_min <- min.b

        b_max <- switch(
          paste(phase.active, phase.major),
          "GAS GAS" = gas_max_b_pri,
          "GAS OIL" = gas_max_b_sec,
          "OIL OIL" = oil_max_b_pri,
          "OIL GAS" = oil_max_b_sec,
          "WATER OIL" = oil_max_b_pri,
          "WATER GAS" = gas_max_b_pri
        )

        b_min <- switch(
          paste(phase.active, phase.major),
          "GAS GAS" = gas_min_b_pri,
          "GAS OIL" = gas_min_b_sec,
          "OIL OIL" = oil_min_b_pri,
          "OIL GAS" = oil_min_b_sec,
          "WATER OIL" = oil_min_b_pri,
          "WATER GAS" = gas_min_b_pri
        )
      } else {
        b_max <- max.b
        b_min <- min.b
      }

      b_max <- ifelse(length(q) < 12, min(1.3, b_max), b_max)
      b_min <- ifelse(length(q) < 24, min(0.5, b_min), b_min)

      b_guess <- 1.1
      calc_decl <- 0.75
      di_guess <- 3.286893

      # Compute initial guesses
      if (length(q) >= 6) {
        avg_start <- mean(head(q, 3), na.rm = TRUE)
        avg_end <- mean(tail(q, 3), na.rm = TRUE)
        calc_decl <- 1 - avg_end / (avg_start+0.000001)

        set.seed(round(head(q, 1), 0))
        b_guess <-
          min(base::sample(rlnorm(
            10000, mean = 0.35, sd = 0.375
          ) / 2, 1), b_max)
        di_guess <-
          ifelse(
            calc_decl < 0,
            min(
              3.286893,
              nominal.from.secant(tcInitialDeclineMax, b_max)
            ),
            min(
              nominal.from.secant(calc_decl, b_guess),
              nominal.from.secant(tcInitialDeclineMax, b_max)
            )
          )
      }
      # Initial guess for solver
      initial.guess <- c(
        qi = q[1] * 1.2,
        Di = di_guess,
        b = b_guess,
        Df = nominal.from.tangent(0.06),
        alpha = nominal.from.tangent(0.06),
        texp_idx = 5

      )
      # Minimum constraints for solver
      min.guess <- c(
        qi = max(1, (q[1] * (
          as.numeric(tcInitialRateMin)
        ))),
        Di = max(0.01, as.numeric(
          nominal.from.secant(tcInitialDeclineMin, b_min)
        )),
        b = max(0.01, as.numeric(b_min)),
        Df = as.numeric(nominal.from.tangent(tcFinalDeclineMin)),
        alpha = 0,
        texp_idx = 1

      )
      # Max constraints for solver
      max.guess <- c(
        qi = (q[1] * (as.numeric(
          tcInitialRateMax
        ))),
        Di = min(
          nominal.from.tangent(0.999999999),
          as.numeric(nominal.from.secant(tcInitialDeclineMax, b_max))
        ),
        b = as.numeric(b_max),

        Df = as.numeric(nominal.from.tangent(tcFinalDeclineMax)),
        alpha = 20,
        texp_idx = 18
      )


      if (tcBFactor > 0 & phase.active == 'OIL') {
        max.guess["b"] <- tcBFactor
        min.guess["b"] <- tcBFactor - 0.0000001
        initial.guess["b"] <- tcBFactor
      }

      if (tcBFactor2 > 0 & phase.active == 'GAS') {
        max.guess["b"] <- tcBFactor2
        min.guess["b"] <- tcBFactor2 - 0.0000001
        initial.guess["b"] <- tcBFactor2
      }

      if (tcInitialDecline > 0 & phase.active == 'OIL') {
        max.guess["Di"] <- tcInitialDecline
        min.guess["Di"] <- tcInitialDecline - 0.0000001
        initial.guess["Di"] <- tcInitialDecline
      }

      if (tcInitialDecline2 > 0 & phase.active == 'GAS') {
        max.guess["Di"] <- tcInitialDecline2
        min.guess["Di"] <- tcInitialDecline2 - 0.0000001
        initial.guess["Di"] <- tcInitialDecline2
      }

      goal.function.huber <- function (guess) {
        q.forecast <- multi_segment2.q(
          qi = guess[1],
          Di = guess[2],
          b =  guess[3],
          Df = guess[4],
          alpha = guess[5],
          texp_idx = guess[6],
          t
        )
        pseudo.huber_cum(q, q.forecast)
      }

      goal.function.ssle <- function (guess) {
        q.forecast <- multi_segment2.q(
          qi = guess[1],
          Di = guess[2],
          b =  guess[3],
          Df = guess[4],
          alpha = guess[5],
          texp_idx = guess[6],
          t
        )
        ssle_cum(q, q.forecast)
      }

      scaling_hl <- c(1000, 10, 5, 1, 10, 10)
      scaling_ssle <- c(1000, 4.176091, 5.045323, 1, 4.176091, 5)

      res.huber <- try(optim(
        par = initial.guess,
        fn = goal.function.huber,
        lower = min.guess,
        upper = max.guess,
        method = 'L-BFGS-B',
        control = list(maxit = 5000,
                      parscale = scaling_ssle)
      ),
      silent = TRUE)

      res.ssle <- try(
        optim(
          par = initial.guess,
          fn = goal.function.ssle,
          lower = min.guess,
          upper = max.guess,
          method = 'L-BFGS-B',
          control = list(maxit = 5000, parscale = scaling_ssle)
        ),
        silent = TRUE
      )

      loss_lim2 <- length(q)
      loss_lim <-
        max(min(loss_lim2 - 1, 5), round(loss_lim2 * 1, 0))

      huber.predict <-
        multi_segment2.q(
          qi = res.huber$par[1],
          Di = res.huber$par[2],
          b =  res.huber$par[3],
          Df = res.huber$par[4],
          alpha = res.huber$par[5],
          texp_idx = res.huber$par[6],
          t
      )

      huber.loss.q <- eur_weight(q, huber.predict)

      ssle.predict <-
        multi_segment2.q(
          qi = res.ssle$par[1],
          Di = res.ssle$par[2],
          b =  res.ssle$par[3],
          Df = res.ssle$par[4],
          alpha = res.ssle$par[5],
          texp_idx = res.ssle$par[6],
          t
        )
      ssle.loss.q <- eur_weight(q, ssle.predict)

      if (abs(huber.loss.q) < abs(ssle.loss.q)) {
        res <- res.huber
        scaling <- scaling_hl
        goal_fn <- goal.function.huber
      } else {
        res <- res.ssle
        scaling <- scaling_ssle
        goal_fn <- goal.function.ssle
      }

      list(
        decline = c(
          qi = unname(res$par[1]),
          Di = unname(res$par[2]),
          b = unname(res$par[3]),
          Df = unname(res$par[4]),
          alpha = unname(res$par[5]),
          texp_idx = unname(res$par[6])
        ),
        convergence = c(
          conv = unname(res$convergence),
          iter = res$counts,
          message = res$message
        )
      )
    }
  }


# COMMAND ----------

# DBTITLE 1,Utility functions
##########################################################
# Utility functions ######################################
##########################################################

# replace all NA in a vector with last non na value
replace.na.prior <- function(x) {
  stopifnot(is.vector(x))
  which.na <- c(which(!is.na(x)), length(x) + 1)
  values <- na.omit(x)

  if (which.na[1] != 1) {
    which.na <- c(1, which.na)
    values <- c(values[1], values)
  }

  diffs <- diff(which.na)
  return(rep(values, times = diffs))
}

# replace NA with default value of 0
replace.na <- function (xs, default = 0) {
  xs[is.infinite(xs)] <- default
  xs[is.na(xs)] <- default
  xs
}

# exponential weighted moving average
ewa.avg <- function(x) {
  x <- replace.na(x)
  if (length(x) == 1) {
    y <- x
  } else {
    nx <- length(x)
    y <- numeric(nx)
    a <- 0.2
    y[1] <- x[1]
    for (k in 2:nx)
      y[k] <- a * x[k] + (1 - a) * y[k - 1]
  }
  return(y)
}

# split object at x position
split.at <- function(x, position) {
  if (!is.na(position[1])) {
    out <- list()
    position_2 <- c(1, position, length(x) + 1)
    for (i in seq_along(position_2[-1])) {
      out[[i]] <- x[position_2[i]:(position_2[i + 1] - 1)]
    }
    return(out)
  } else {
    return(x)
  }
}

# return lagged row of input vector by n length
row.shift <- function(x, shiftLen = 1L) {
  r <- (1L + shiftLen):(length(x) + shiftLen)
  r[r < 1] <- NA
  return(x[r])
}

# add months to a date input
add.months <- function(date, n = 1) {
  date = as.Date(date, format = "%Y-%m-%d")
  out <- seq(date, by = paste (n, "months"), length = 2)[2]
  out
}

# Spotfire specific date rounding for conversions from date
round.date <- function(date_col, period = "days") {
  rounded_date_col <-  round(as.POSIXlt(date_col), period)
  rounded_date_col <- as.POSIXct(rounded_date_col,
                                 format = "%Y-%m-%d",
                                 origin = "1970-01-01")
  return(rounded_date_col)
}

# calculate days in month
# Borrowed from HMISC package
n.days <- function(time) {
  time <- as.POSIXlt(time)
  time$mday[] <- time$sec[] <- time$min <- time$hour <- 0
  time$mon <- time$mon + 1
  return(as.POSIXlt(as.POSIXct(time))$mday)
}

# do.call function with slight speed optimization over base do.call
fast.do.call <-
  function(what,
           args,
           quote = FALSE,
           envir = parent.frame()) {
    if (quote)
      args <- lapply(args, enquote)

    if (is.null(names(args)) ||
        is.data.frame(args)) {
      argn <- args
      args <- list()
    } else{
      # Add all the named arguments
      argn <- lapply(names(args)[names(args) != ""], as.name)
      names(argn) <- names(args)[names(args) != ""]
      # Add the unnamed arguments
      argn <- c(argn, args[names(args) == ""])
      args <- args[names(args) != ""]
    }

    if (class(what) == "character") {
      if (is.character(what)) {
        fn <- strsplit(what, "[:]{2,3}")[[1]]
        what <- if (length(fn) == 1) {
          get(fn[[1]], envir = envir, mode = "function")
        } else {
          get(fn[[2]], envir = asNamespace(fn[[1]]), mode = "function")
        }
      }
      call <- as.call(c(list(what), argn))
    } else if (class(what) == "function") {
      f_name <- deparse(substitute(what))
      call <- as.call(c(list(as.name(f_name)), argn))
      args[[f_name]] <- what
    } else if (class(what) == "name") {
      call <- as.call(c(list(what, argn)))
    }

    eval(call,
         envir = args,
         enclos = envir)
  }

# cumsum treating NA as zero
cumsum.na <- function(xs) {
  c <- cumsum(replace.na(xs))
  is.na(c) <- is.na(xs)
  c
}

# max ignoring NAs, but returning NA rather than -Inf
max.na <- function(xs) {
  xs <- na.omit(xs)
  if (length(xs) == 0)
    NA
  else
    max(xs)
}

# Peak pattern detection
find.peaks <-
  function(x,
           nups = 1,
           ndowns = 3,
           zero = "0",
           peakpat = NULL,
           # peakpat = "[+]{2,}[0]*[-]{2,}",
           minpeakheight = -Inf,
           minpeakdistance = 1,
           threshold = 0,
           npeaks = 0,
           sortstr = FALSE)
  {
    stopifnot(is.vector(x, mode = "numeric") || length(is.na(x)) == 0)
    if (!zero %in% c('0', '+', '-'))
      stop("Argument 'zero' can only be '0', '+', or '-'.")

    # transform x into a "+-+...-+-" character string
    xc <- paste(as.character(sign(diff(x))), collapse = "")
    xc <- gsub("1", "+", gsub("-1", "-", xc))
    # transform '0' to zero
    if (zero != '0')
      xc <- gsub("0", zero, xc)

    # generate the peak pattern with no of ups and downs
    if (is.null(peakpat)) {
      peakpat <- sprintf("[+]{%d,}[-]{%d,}", nups, ndowns)
    }

    # generate and apply the peak pattern
    rc <- gregexpr(peakpat, xc)[[1]]
    if (rc[1] < 0)
      return(NULL)

    # get indices from regular expression parser
    x1 <- rc
    x2 <- rc + attr(rc, "match.length")
    attributes(x1) <- NULL
    attributes(x2) <- NULL

    # find index positions and maximum values
    n <- length(x1)
    xv <- xp <- numeric(n)
    for (i in 1:n) {
      xp[i] <- which.max(x[x1[i]:x2[i]]) + x1[i] - 1
      xv[i] <- x[xp[i]]
    }

    # eliminate peaks that are too low
    inds <-
      which(xv >= minpeakheight & xv - pmax(x[x1], x[x2]) >= threshold)

    # combine into a matrix format
    X <- cbind(xv[inds], xp[inds], x1[inds], x2[inds])

    # eliminate peaks that are near by
    if (minpeakdistance < 1)
      warning("Handling 'minpeakdistance < 1' is logically not possible.")

    # sort according to peak height
    if (sortstr || minpeakdistance > 1) {
      sl <- sort.list(X[, 1], na.last = NA, decreasing = TRUE)
      X <- X[sl, , drop = FALSE]
    }

    # return NULL if no peaks
    if (length(X) == 0)
      return(c())

    # find peaks sufficiently distant
    if (minpeakdistance > 1) {
      no_peaks <- nrow(X)
      badpeaks <- rep(FALSE, no_peaks)

      # eliminate peaks that are close to bigger peaks
      for (i in 1:no_peaks) {
        ipos <- X[i, 2]
        if (!badpeaks[i]) {
          dpos <- abs(ipos - X[, 2])
          badpeaks <- badpeaks | (dpos > 0 & dpos < minpeakdistance)
        }
      }
      # select the good peaks
      X <- X[!badpeaks, , drop = FALSE]
    }

    # Return only the first 'npeaks' peaks
    if (npeaks > 0 && npeaks < nrow(X)) {
      X <- X[1:npeaks, , drop = FALSE]
    }

    return(X)
  }

# find when peak rate occurs
peak.production.t <- function (q, min_life = 0.95) {
  q2 <- q
  q <- q[q > prod_limit]
  lrq <- round(length(q) * as.numeric(min_life), 0)

  if (lrq > length(q) - 3 & batch_max_segs > 1) {
    lrq <- min(round(length(q) * 0.75, 0), abs(length(q) - 3))
  }

  if (length(q) == forecastLength + 1 & length(q) >= 50) {
    vals.index <- which(row.shift(q2[1:50]) / q2[1:50] < 0.95)
    vals.index <- max(1, vals.index, na.rm = TRUE)
    seq.index <-
      seq(min(vals.index),
          length.out = length(vals.index),
          by = 1)

    if (sum(vals.index[1:4] < 7, na.rm = TRUE) >= 4) {
      peak.t <- which.max(q2)
    } else {
      peak.t <- which(row.shift(q2[1:50]) / q2[1:50] < 0.95)[3]
      if (is.na(peak.t)) {
        peak.t <- which(row.shift(q2[1:50]) / q2[1:50] < 0.95)[2]
        if (is.na(peak.t)) {
          peak.t <- max(1, which.max(q), vals.index, na.rm = TRUE)
        }
      }
      peak.t <- peak.t
    }
  }
  if (batch_max_segs < 6 & length(q) != forecastLength + 1) {
    peaks <- find.peaks(q[1:lrq], nups = 1, ndowns = 4)

    if (is.null(peaks)) {
      peaks <- which.max(q2[1:lrq])
    }

    limit_q2 <- ifelse(max(q2, na.rm = TRUE) >= 2500, 105, 36)

    peaks <-
      ifelse(length(q2) > limit_q2, head(tail(sort(
        peaks, decreasing = TRUE
      ), -1), 1),
      head(sort(peaks, decreasing = TRUE), 1))
    peaks <- match(peaks, q2, nomatch = 1)
    peak.t <- max(peaks, which.max(q2[1:lrq]), na.rm = TRUE)
    mult_coef <- max(2, length(q) / 22)
    peak.t <-
      ifelse(max(q2[1:lrq], na.rm = TRUE) > (q2[peak.t] * mult_coef),
             which.max(q2[1:lrq]),
             peak.t)
  }
  peak.t <- which.max(q2[1:lrq])
  if (is.null(get0("peak.t")) | is.na(get0("peak.t"))) {
    peak.t <- which.max(q2[1:lrq])
  } else {
    return(peak.t)
  }
}

# Compute volatility ranking
vol.score <- function(q, max_index = length(prod)) {
  prod_len <- length(q)
  if (max_index == prod_len & prod_len >= 48) {
    avg_prod <- mean(q, na.rm = TRUE)
    med_point <- round(length(q) / 2, 0)
    vol_subset <- q[med_point:length(q)]
    med_count <- length(vol_subset)
    vol_count <- length(vol_subset[vol_subset > avg_prod])
    vol_score <- vol_count / med_count
  } else {
    vol_score <- 0
  }
  return(vol_score)
}

# Compute buildup phase
calc_buildup <- function(actual, predicted, buildup_time) {
    actual_max_idx <- which.max(actual)
    pred_max_idx <- which.max(actual)
    # pred_max_idx <- min(which(!is.na(predicted)))
    replace_forecast <- c(NA)

    if (actual_max_idx != tail(buildup_time, 1)) {
      start_idx <- 1
      start_rate <- actual[start_idx]
      peak_idx <- pred_max_idx
      peak_rate <- max(actual, na.rm = TRUE)
      forecast_idx <- pred_max_idx
      forecast_rate <- max(predicted, na.rm = TRUE)

      m <- (peak_rate - start_rate) / (peak_idx - start_idx)
      xvec <- c(start_idx:peak_idx)
      xvec_for_calc <- c(0:(peak_idx - start_idx))
      yvec <- xvec_for_calc * m + start_rate

      m2 <- (peak_rate - forecast_rate) / (peak_idx - forecast_idx)
      xvec2 <- c(peak_idx:forecast_idx)
      xvec_for_calc2 <- c(0:(forecast_idx - peak_idx))
      yvec2 <- xvec_for_calc2 * m2 + peak_rate

      # fit an exponential between actual peak and forecast peak
      ao <- peak_rate
      alpha <- (log(peak_rate/forecast_rate))/(peak_idx - forecast_idx)
      yvec_exp <- ao*exp(alpha*(xvec_for_calc2))

      replace_forecast <- which(is.na(predicted))
      replace_forecast[xvec] <- yvec
      replace_forecast[xvec2] <- yvec_exp

    } else {
      start_idx <- 1
      start_rate <- actual[start_idx]
      peak_idx <- pred_max_idx
      peak_rate <- max(actual, na.rm = TRUE)
      forecast_idx <- pred_max_idx
      forecast_rate <- max(predicted, na.rm = TRUE)
      m <- (peak_rate - start_rate) / (peak_idx - start_idx)
      xvec <- c(start_idx:peak_idx)
      xvec_for_calc <- c(0:(peak_idx - start_idx))
      yvec <- xvec_for_calc * m + start_rate

      ao <- peak_rate
      alpha <- 0
      replace_forecast[xvec] <- yvec

    }
    predicted[seq_along(replace_forecast)] <- replace_forecast

  return(list(predicted, m, start_rate, ao, alpha, peak_idx, forecast_idx))

}

# Compute error rates
calc_error <-
  function(actual,
           predicted,
           included,
           days,
           absolute = FALSE,
           percentage = FALSE) {
    if (absolute) {
      error <-
        sum(abs((actual[included] * days[included]) - (predicted[included] * days[included])), na.rm = TRUE)
    } else {
      vols_actual <- sum(actual[included] * days[included], na.rm = TRUE)
      vols_pred <-
        sum(predicted[included] * days[included], na.rm = TRUE)
      error <- vols_actual - vols_pred
    }
    if (percentage) {
      if (error == 0) {
        error <- 0
      } else {
        error <-
          error / (sum(actual[included] * days[included], na.rm = TRUE))
      }
    }
    return(error)
  }

# Calculate NGL yield based on predicted gas
calc_ngl <-
  function(gas,
           initial_ratio = tcInitialNgl,
           final_ratio = tcFinalNgl) {
    if (tcNglForecast == FALSE) {
      ngl_daily <- 0
    } else {
      gas_cum <- cumsum.na(gas)
      ratio <-  gas_cum / max.na(gas_cum)
      ngl_yield <-
        ratio * (final_ratio - initial_ratio) + initial_ratio
      ngl_daily <- ngl_yield * (gas / 1000)
    }
    return(ngl_daily)
  }

# Calculate equivalent production
calc_equiv <- function(oil, gas, ngl, conv_factor = 6) {
  equiv = replace.na(oil) + replace.na(ngl) + (replace.na(gas) / conv_factor)
  equiv[is.na(oil) & is.na(gas)] <- NA
  return(equiv)
}

# Calculate EUR rem and cum values
calc_eurs <- function(actual_monthly, predicted_monthly) {
  cum_val <- max(cumsum.na(actual_monthly), 0, na.rm = TRUE)
  eur_val <-
    sum(max(cumsum.na(predicted_monthly[is.na(actual_monthly)]), 0, na.rm = TRUE),
        cum_val, na.rm = TRUE)
  if (length(actual_monthly[actual_monthly == 0]) == length(actual_monthly) &
      sum(predicted_monthly > 0, na.rm = TRUE)) {
    eur_val <- sum(predicted_monthly, na.rm = TRUE)
  }
  rem_val <- eur_val - cum_val

  if (length(actual_monthly[actual_monthly > 0 &
                            !is.na(actual_monthly)]) == length(predicted_monthly[predicted_monthly > 0])) {
    eur_val <- max(cumsum.na(predicted_monthly), na.rm = TRUE)
    cum_val <- max(cumsum.na(actual_monthly), 0, na.rm = TRUE)
    rem_val <- 0
  }
  return(list(cum_val, eur_val, rem_val))
}

# Add summary columns to output dataframe
calc_summary_col <- function(col) {
  output <- ifelse(is.null(col[1]), NA, col[1])
  return(output)
}

# Add combined columns to forecast
calc_combined <- function(actual, predicted, forecast) {
  combined_out  <-  ifelse(forecast == 'FORECAST', predicted, actual)
  return(combined_out)
}

# Aggregate columns from singular column inputs
bind_data_col <- function(column, df, na_type) {
  if (length(column) == nrow(df)) {
    df[, c(deparse(substitute(column)))] <- column
  } else {
    df[, c(deparse(substitute(column)))] <- na_type
  }
  return(df)
}

#Sort list for more efficient parallel computing
well_list_sort <- function(df, split_col, n_cores = 4) {
  # ensure df argument is a data frame
  split_col <- deparse(substitute(split_col))
  if (class(df) != "data.frame")
    stop("Data frame input must be a data frame")

  if (split_col %in% colnames(df)) {
    # split function requires factor type to split a df
    y <- df[[split_col]]
    # split data frame by column
    x <- split(df, y)
    # order list by decreasing producing months (row count)
    x <- x[c(order(sapply(x, nrow), decreasing = TRUE))]
    # re-index sorted list by decreasing row count across cores used
    sortvec <-
      rep(c(seq(1, n_cores), seq(n_cores, 1)), length = length(x))
    sortvec <- order(sortvec)
    x <- x[c(sortvec)]
    return(x)
  } else {
    # ensure column argument is in the data frame
    stop("Column not in data frame")
  }
}

# Normalize production
normalize_prod <-
  function(prod_col,
           norm_col,
           norm_val,
           norm_factor = 0.8) {
    prod_col <- as.vector(as.double(prod_col))
    norm_col <- as.vector(as.double(norm_col))
    norm_val <- as.vector(as.double(norm_val))
    norm_col <- ifelse(is.na(norm_col), norm_val, norm_col)
    out = prod_col * (norm_col + (norm_val - norm_col) * norm_factor) / norm_col
    return(out)
  }

# Aggregate adjusted mean for type curve
aggs.mean <- function(x, obs) {
  outs <- sum(x, na.rm = TRUE) / obs
  return(outs)
}

# Aggregate data frame into type curve
agg_type_curve <- function(df, prob) {
  oil = as.vector(replace.na(df$oil) / df$days_on)
  gas = as.vector(replace.na(df$gas) / df$days_on)
  water = as.vector(replace.na(df$water) / df$days_on)
  producing_month = as.vector(df$producing_month)
  obs_count <- length(unique(df$api))
  active_prob = switch(
    as.character(prob),
    P50 = 0.5,
    P10 = 0.1,
    P25 = 0.25,
    P75 = 0.75,
    P90 = 0.9,
    Avg = 'Avg'
  )

  if (active_prob != 'Avg') {
    oil_agg <-
      tapply(oil, producing_month, quantile, probs = active_prob)
    gas_agg <-
      tapply(gas, producing_month, quantile, probs = active_prob)
    water_agg <-
      tapply(water, producing_month, quantile, probs = active_prob)
    df_out <-
      data.frame(
        producing_month = names(oil_agg),
        oil = oil_agg,
        gas = gas_agg,
        water = water_agg
      )
  } else {
    oil_agg <- tapply(oil, producing_month, aggs.mean, obs_count)
    gas_agg <- tapply(gas, producing_month, aggs.mean, obs_count)
    water_agg <-
      tapply(water, producing_month, aggs.mean, obs_count)
    df_out <-
      data.frame(
        producing_month = names(oil_agg),
        oil = oil_agg,
        gas = gas_agg,
        water = water_agg
      )
  }

  min_date <- min(df$date, na.rm = TRUE)
  df_out$well_count <-
    tapply(df$well_count, producing_month, sum, na.rm = TRUE)
  df_out$date <-
    sapply(df_out$producing_month, add.months, date = min_date)
  df_out$date <-
    as.POSIXct(as.Date(df_out$date, origin = "1970-01-01") + 0.33)
  df_out$days_on <- MONTH.DAYS
  df_out$api <-
    c(paste(unique(df$api), collapse = " | "), rep(NA, nrow(df_out) - 1))
  df_out$interval <-
    c(paste(unique(df$interval), collapse = " | "), rep(NA, nrow(df_out) -
                                                          1))
  df_out$well_name <-
    ifelse(tcExternal == FALSE,
           head(df$well_name, 1),
           head(df$interval, 1))
  df_out$operator <-
    c(paste(unique(df$operator), collapse = " | "), rep(NA, nrow(df_out) -
                                                          1))
  df_out$lateral_length_ft <-
    mean(tapply(df$lateral_length_ft, df$api, mean, na.rm = TRUE),
         na.rm = TRUE)
  df_out$true_vertical_depth_ft <-
    mean(tapply(df$true_vertical_depth_ft, df$api, mean, na.rm = TRUE),
         na.rm = TRUE)
  df_out$latitude <-
    mean(tapply(df$latitude, df$api, mean, na.rm = TRUE), na.rm = TRUE)
  df_out$longitude <-
    mean(tapply(df$longitude, df$api, mean, na.rm = TRUE), na.rm = TRUE)
  df_out$latitude_bh <-
    mean(tapply(df$latitude_bh, df$api, mean, na.rm = TRUE), na.rm = TRUE)
  df_out$longitude_bh <-
    mean(tapply(df$longitude_bh, df$api, mean, na.rm = TRUE), na.rm = TRUE)
  df_out$frac_proppant_lbs <-
    mean(tapply(df$frac_proppant_lbs, df$api, mean, na.rm = TRUE),
         na.rm = TRUE)
  df_out$frac_fluid_bbl <-
    mean(tapply(df$frac_fluid_bbl, df$api, mean, na.rm = TRUE), na.rm = TRUE)
  df_out$working_interest <- c(NA_real_)
  df_out$net_revenue_interest <- c(NA_real_)
  df_out$play <- paste(unique(df$play), collapse = " | ")
  df_out$county <- paste(unique(df$county), collapse = " | ")
  df_out$ip_year <- c(NA_integer_)
  return(df_out)
}

# Aggregate data frame into type curve
agg_batch_curve <- function(df, prob) {
  oil = as.vector(replace.na(df$oil_pred_monthly) / df$days_on)
  gas = as.vector(replace.na(df$gas_pred_monthly) / df$days_on)
  water = as.vector(replace.na(df$water_pred_monthly) / df$days_on)
  producing_month = as.vector(df$index)
  obs_count <- length(unique(df$api))
  active_prob = switch(
    as.character(prob),
    P50 = 0.5,
    P10 = 0.1,
    P25 = 0.25,
    P75 = 0.75,
    P90 = 0.9,
    Avg = 'Avg'
  )

  if (active_prob != 'Avg') {
    oil_agg <-
      tapply(oil, producing_month, quantile, probs = active_prob)
    gas_agg <-
      tapply(gas, producing_month, quantile, probs = active_prob)
    water_agg <-
      tapply(water, producing_month, quantile, probs = active_prob)
    df_out <-
      data.frame(
        producing_month = names(oil_agg),
        oil = oil_agg,
        gas = gas_agg,
        water = water_agg
      )
  } else {
    oil_agg <- tapply(oil, producing_month, aggs.mean, obs_count)
    gas_agg <- tapply(gas, producing_month, aggs.mean, obs_count)
    water_agg <-
      tapply(water, producing_month, aggs.mean, obs_count)
    df_out <-
      data.frame(
        producing_month = names(oil_agg),
        oil = oil_agg,
        gas = gas_agg,
        water = water_agg
      )
  }

  min_date <- min(df$date, na.rm = TRUE)
  df_out$well_count <-
    tapply(df$well_count, producing_month, sum, na.rm = TRUE)
  df_out$date <-
    sapply(df_out$producing_month, add.months, date = min_date)
  df_out$date <-
    as.POSIXct(as.Date(df_out$date, origin = "1970-01-01") + 0.33)
  df_out$days_on <- MONTH.DAYS
  df_out$api <-
    c(paste(unique(df$api), collapse = " | "), rep(NA, nrow(df_out) - 1))
  df_out$interval <- paste(toupper(prob), head(df$interval, 1))
  df_out$well_name <- head(df$interval, 1)
  df_out$operator <-
    c(paste(unique(df$operator), collapse = " | "), rep(NA, nrow(df_out) -
                                                          1))
  df_out$lateral_length_ft <- c(NA_real_)
  df_out$true_vertical_depth_ft <- c(NA_real_)
  df_out$latitude <- c(NA_real_)
  df_out$longitude <- c(NA_real_)
  df_out$latitude_bh <- c(NA_real_)
  df_out$longitude_bh <- c(NA_real_)
  df_out$frac_proppant_lbs <- c(NA_real_)
  df_out$frac_fluid_bbl <- c(NA_real_)
  df_out$working_interest <- c(NA_real_)
  df_out$net_revenue_interest <- c(NA_real_)
  df_out$play <- c(NA_character_)
  df_out$county <- c(NA_character_)
  df_out$ip_year <- c(NA_integer_)
  return(df_out)
}


# COMMAND ----------

# DBTITLE 1,Main fitting function
##########################################################
# Main fitting function ##################################
##########################################################

fit.prod <-
  function(q1,
           q2,
           q3,
           index,
           dates,
           days,
           identifier = "single_segment_1",
           tmax,
           primary_phase = tcPrimaryPhase,
           forecast_all = FALSE,
           activeInterval = active_interval) {
    seg <-
      as.integer(substr(identifier, nchar(identifier), nchar(identifier)))
    final_seg <- c(max(index, na.rm = TRUE) == tmax)
    oil <- as.vector(q1)
    gas <- as.vector(q2)
    water <- as.vector(q3)
    index <- as.vector(index)
    days <- as.vector(days)

    oil_limit <- tcDowntimeCutoff           # oil downtime rate
    oil_rate_min <- tcRateLimit             # oil forecast min
    gas_limit <- tcDowntimeCutoffSecondary  # gas downtime rate
    gas_rate_min <- tcRateLimitSecondary    # gas forecast min
    oil_qi_or <- tcInitialRate              # oil qi override
    oil_b_or <- tcBFactor                   # oil b override
    oil_de_or <- tcInitialDecline           # oil de override
    oil_df_or <- tcFinalDecline             # oil df override
    gas_qi_or <- tcInitialRate2             # gas qi override
    gas_b_or <- tcBFactor2                  # gas b override
    gas_de_or <- tcInitialDecline2          # gas de override
    gas_df_or <- tcFinalDecline2            # gas df override

    primary_product <- primary_phase

    peak_index_oil <-
      index - index[peak.production.t(oil, tcPeakMonth)] + 1
    peak_index_gas <-
      index - index[peak.production.t(gas, tcPeakMonth)] + 1
    peak_index_water <-
      index - index[peak.production.t(water, tcPeakMonth)] + 1

    if (forecast_all == FALSE & final_seg != TRUE) {
      df_prod <- data.frame(
        date = dates,
        index = index,
        days_on = days,
        oil_actual_daily = oil,
        oil_actual_monthly = oil * days,
        oil_pred_daily = c(NA),
        gas_actual_daily = gas,
        gas_actual_monthly = gas * days,
        gas_pred_daily = c(NA),
        water_actual_daily = water,
        water_actual_monthly =  water * days,
        water_pred_daily = c(NA),
        included_oil = c(TRUE),
        included_gas = c(TRUE),
        included_water = c(TRUE),
        peak_index = ifelse(primary_product == 'OIL', peak_index_oil, peak_index_gas),
        oil_pred_monthly = c(NA),
        gas_pred_monthly = c(NA),
        water_pred_monthly = c(NA),
        stringsAsFactors = FALSE,
        row.names = NULL
      )

      # Summary table
      df_summary <- data.frame(
        segment = c(identifier),
        segment_final = final_seg,
        single_segment_flag = ifelse(length(index) == tmax, TRUE, FALSE),
        forecast_override = NA_character_,
        forecast_type = NA_character_,
        gor_override = FALSE,
        wor_override = FALSE,
        t_start = index[1],
        t_start_date = head(dates, 1),
        t_seg_months = tail(index, 1) - index[1],
        t_end = tail(index, 1),
        t_end_date = tail(dates, 1),
        primary_phase = primary_product,
        peak_month_oil = dates[peak_index_oil == 1],
        peak_month_gas = dates[peak_index_gas == 1],
        peak_month_water = dates[peak_index_water == 1],
        oil_pre_forecast = NA_real_,
        gas_pre_forecast = NA_real_,
        water_pre_forecast = NA_real_,
        ngl_pre_forecast = NA_real_,
        qi_oil = NA_real_,
        b_oil = NA_real_,
        di_nom_oil = NA_real_,
        di_sec_oil = NA_real_,
        di_tan_oil = NA_real_,
        dlim_nom_oil = NA_real_,
        dlim_sec_oil = NA_real_,
        dlim_tan_oil = NA_real_,
        qi_gas = NA_real_,
        b_gas = NA_real_,
        di_nom_gas = NA_real_,
        di_sec_gas = NA_real_,
        di_tan_gas = NA_real_,
        dlim_nom_gas = NA_real_,
        dlim_sec_gas = NA_real_,
        dlim_tan_gas = NA_real_,
        qi_water = NA_real_,
        b_water = NA_real_,
        di_nom_water = NA_real_,
        di_sec_water = NA_real_,
        di_tan_water = NA_real_,
        dlim_nom_water = NA_real_,
        dlim_sec_water = NA_real_,
        dlim_tan_water = NA_real_,
        volatility_score = NA_real_,
        volatility_flag = NA_real_,
        convergence_oil = NA_character_,
        iterations_oil = NA_real_,
        error_oil = NA_real_,
        error_perc_oil = NA_real_,
        error_abs_oil = NA_real_,
        error_abs_perc_oil = NA_real_,
        convergence_gas = NA_character_,
        iterations_gas = NA_real_,
        error_gas = NA_real_,
        error_perc_gas = NA_real_,
        error_abs_gas = NA_real_,
        error_abs_perc_gas = NA_real_,
        convergence_water = NA_character_,
        iterations_water = NA_real_,
        error_water = NA_real_,
        error_perc_water = NA_real_,
        error_abs_water = NA_real_,
        error_abs_perc_water = NA_real_,
        stringsAsFactors = FALSE,
        row.names = NULL
      )

    } else {
      max_length_stream <- max(length(oil), length(gas), na.rm = TRUE)
      padding <- 24
      len_tmp <-
        ifelse(final_seg, forecastLength + padding, max_length_stream)
      len_cutoff <-
        ifelse(final_seg,
               (forecastLength + 1) - (head(index) - 1),
               max_length_stream)

      post_peak_oil <-
        index >= index[peak.production.t(oil, tcPeakMonth)]
      post_peak_gas <-
        index >= index[peak.production.t(gas, tcPeakMonth)]
      post_peak_water <-
        index >= index[peak.production.t(water, tcPeakMonth)]

      # Outliers for oil phase
      oil_outliers <-
        find.outliers(oil, index, tcOutlierUpper, tcOutlierLower, qlimit = oil_limit)
      oil_outliers <- rep('True', length(oil))

      oil_outliers[post_peak_oil < 1] <- FALSE
      # oil_outliers[oil < oil_limit] <- FALSE
      oil_outliers <- as.logical(oil_outliers)

      # Outliers for gas phase

      gas_outliers <-
        find.outliers(gas, index, tcOutlierUpper, tcOutlierLower, qlimit = gas_limit)

      gas_outliers <- rep('True', length(gas))
      gas_outliers[post_peak_gas < 1] <- FALSE
      # gas_outliers[gas < gas_limit] <- FALSE
      gas_outliers <- as.logical(gas_outliers)

      # If water forecast is set to true, run outlier analysis on water otherwise set no outliers
      if (tcWaterForecast) {
        water_outliers <-
          find.outliers(water, index, tcOutlierUpper, tcOutlierLower, qlimit = 0.1)
        water_outliers[post_peak_water < 1] <- FALSE
        water_outliers <- as.logical(water_outliers)
      } else {
        water_outliers <- rep(TRUE, length(water))
        water_outliers[post_peak_water < 1] <- FALSE
        water_outliers <- as.logical(water_outliers)
      }

      # Calculate GOR yields for overrides
      gor_initial <-
        mean(head(na.omit(gas[gas_outliers]), 1), na.rm = TRUE) /
        (max(0.00001, mean(head(
          na.omit(oil[oil_outliers]), 1
        ), na.rm = TRUE))) * 1000
      gor_final <-
        mean(tail(na.omit(gas[gas_outliers]), 6), na.rm = TRUE) /
        (max(0.00001, mean(tail(
          na.omit(oil[oil_outliers]), 6
        ), na.rm = TRUE))) * 1000
      cum_gor <- sum(gas[gas_outliers], na.rm = TRUE) /
        max(0.00001, sum(oil[oil_outliers], na.rm = TRUE)) * 1000

      # Compute rolling GOR from monthly volumes - reduce impact of downtime or bad reporting
      rolling_gor <-
        pmax(0, cumsum(gas * days)) / pmax(0, cumsum(oil * days)) * 1000
      max_cum_gor <- max(0, rolling_gor, na.rm = TRUE)
      curr_cum_gor <- max(0, tail(rolling_gor, 1), na.rm = TRUE)

      if (primary_product == 'OIL') {
        wor_final <-
          mean(tail(na.omit(water[water_outliers]), 6), na.rm = TRUE) /
          (max(0.00001, mean(tail(
            na.omit(oil[oil_outliers]), 6
          ), na.rm = TRUE)))
      } else {
        wor_final <-
          mean(tail(na.omit(water[water_outliers]), 6), na.rm = TRUE) /
          (max(0.00001, mean(tail(
            na.omit(gas[gas_outliers]), 6
          ), na.rm = TRUE)))
      }

      if (tcOverrideCoef == TRUE) {
        # Type curve overrides
        analogue_curve <-
          subset(
            batch_tc_summary,
            well_name == activeInterval,
            select = c(
              b_oil,
              di_nom_oil,
              dlim_nom_oil,
              b_gas,
              di_nom_gas,
              dlim_nom_gas,
              b_water,
              di_nom_water,
              dlim_nom_water
            )
          )

        if (nrow(analogue_curve) == 0) {
          analogue_curve <-
            data.frame(
              b_oil = NA,
              di_nom_oil = NA,
              dlim_nom_oil = NA,
              b_gas = NA,
              di_nom_gas = NA,
              dlim_nom_gas = NA,
              b_water = NA,
              di_nom_water = NA,
              dlim_nom_water = NA
            )
        }

        fit_oil <- list(
          decline = c(
            qi = max.na(oil) * YEAR.DAYS,
            Di = head(analogue_curve$di_nom_oil, 1),
            b = head(analogue_curve$b_oil, 1),
            Df = head(analogue_curve$dlim_nom_oil, 1)
          ),
          convergence = c(
            conv = NA_character_,
            iter = 0,
            message = NA_character_
          )
        )
        fit_gas <- list(
          decline = c(
            qi = max.na(gas) * YEAR.DAYS,
            Di = head(analogue_curve$di_nom_gas, 1),
            b = head(analogue_curve$b_gas, 1),
            Df = head(analogue_curve$dlim_nom_gas, 1)
          ),
          convergence = c(
            conv = NA_character_,
            iter = 0,
            message = NA_character_
          )
        )
        fit_water <- list(
          decline = c(
            qi = max.na(water) * YEAR.DAYS,
            Di = head(analogue_curve$di_nom_water, 1),
            b = head(analogue_curve$b_water, 1),
            Df = head(analogue_curve$dlim_nom_water, 1)
          ),
          convergence = c(
            conv = NA_character_,
            iter = 0,
            message = NA_character_
          )
        )
      } else {
        # Fit phases
        if (tcExternal == TRUE) {
          fit_oil <-
            fit.hyp2exp(
              oil[oil_outliers] * YEAR.DAYS,
              (peak_index_oil[oil_outliers] - 0.5) / YEAR.MONTHS,
              phase.active = "OIL",
              phase.major = primary_product,
              min.b = tcBFactorMin,
              max.b = tcBFactorMax
            )
          fit_gas <-
            fit.hyp2exp(
              gas[gas_outliers] * YEAR.DAYS,
              (peak_index_gas[gas_outliers] - 0.5) / YEAR.MONTHS,
              phase.active = "GAS",
              phase.major = primary_product,
              min.b = tcBFactorMin,
              max.b = tcBFactorMax
            )

        } else {
          if (primary_product == 'OIL') {
            fit_oil <-
              fit.hyp2exp(
                oil[oil_outliers] * YEAR.DAYS,
                (peak_index_oil[oil_outliers] - 0.5) / YEAR.MONTHS,
                phase.active = "OIL",
                phase.major = primary_product,
                min.b = tcBFactorMin,
                max.b = tcBFactorMax
              )

            if (curr_cum_gor >= max_cum_gor &
                gor_final > gor_initial &
                sum(gas[gas_outliers], na.rm = TRUE) > 1 &
                is.na(fit_oil$decline["b"]) == FALSE) {
              fit_gas <-
                fit.hyp2exp(
                  gas[gas_outliers] * YEAR.DAYS,
                  (peak_index_gas[gas_outliers] - 0.5) / YEAR.MONTHS,
                  phase.active = "GAS",
                  phase.major = primary_product,
                  min.b = min(fit_oil$decline["b"] * 1.025, tcBFactorMax - 0.001),
                  max.b = tcBFactorMax
                )
            } else {
              fit_gas <-
                fit.hyp2exp(
                  gas[gas_outliers] * YEAR.DAYS,
                  (peak_index_gas[gas_outliers] - 0.5) / YEAR.MONTHS,
                  phase.active = "GAS",
                  phase.major = primary_product,
                  min.b = tcBFactorMin,
                  max.b = tcBFactorMax
                )
            }
          }

          if (primary_product == 'GAS') {
            fit_gas <-
              fit.hyp2exp(
                gas[gas_outliers] * YEAR.DAYS,
                (peak_index_gas[gas_outliers] - 0.5) / YEAR.MONTHS,
                phase.active = "GAS",
                phase.major = primary_product,
                min.b = tcBFactorMin,
                max.b = tcBFactorMax
              )

            if (curr_cum_gor >= max_cum_gor  &
                gor_final > gor_initial &
                sum(oil[oil_outliers], na.rm = TRUE) > 1 &
                is.na(fit_gas$decline["b"]) == FALSE)   {
              fit_oil <-
                fit.hyp2exp(
                  oil[oil_outliers] * YEAR.DAYS,
                  (peak_index_oil[oil_outliers] - 0.5) / YEAR.MONTHS,
                  phase.active = "OIL",
                  phase.major = primary_product,
                  min.b = tcBFactorMin,
                  max.b = max(fit_gas$decline["b"] * 0.975, tcBFactorMin + 0.001)
                )

            } else {
              fit_oil <-
                fit.hyp2exp(
                  oil[oil_outliers] * YEAR.DAYS,
                  (peak_index_oil[oil_outliers] - 0.5) / YEAR.MONTHS,
                  phase.active = "OIL",
                  phase.major = primary_product,
                  min.b = tcBFactorMin,
                  max.b = tcBFactorMax
                )
            }
          }
        }

        # Fit water if forecast flag set to true
        if (tcWaterForecast) {
          fit_water <-
            fit.hyp2exp(
              water[water_outliers] * YEAR.DAYS,
              (peak_index_water[water_outliers] - 0.5) / YEAR.MONTHS,
              phase.active = "WATER",
              phase.major = primary_product,
              min.b = tcBFactorMin
            )
        } else {
          fit_water <-
            list(
              decline = c(
                qi = NA_real_,
                Di = NA_real_,
                b = NA_real_,
                Df = NA_real_
              ),
              convergence = c(
                conv = NA_character_,
                iter = NA_character_,
                message = NA_character_
              )
            )
        }
      }

      # Oil phase overrides
      if (oil_b_or > 0)
        fit_oil$decline["b"] <- oil_b_or
      if (oil_qi_or > 0)
        fit_oil$decline["qi"] <- oil_qi_or * YEAR.DAYS
      if (oil_de_or > 0)
        fit_oil$decline["Di"] <-
        nominal.from.secant(oil_de_or, max(fit_oil$decline["b"], 0, na.rm = TRUE))
      if (oil_df_or > 0)
        fit_oil$decline["Df"] <- nominal.from.tangent(oil_df_or)

      # Gas phase overrides
      if (gas_b_or > 0)
        fit_gas$decline["b"] <- gas_b_or
      if (gas_qi_or > 0)
        fit_gas$decline["qi"] <- gas_qi_or * YEAR.DAYS
      if (gas_de_or > 0)
        fit_gas$decline["Di"] <-
        nominal.from.secant(gas_de_or, max(fit_gas$decline["b"], 0, na.rm = TRUE))
      if (gas_df_or > 0)
        fit_gas$decline["Df"] <- nominal.from.tangent(gas_df_or)

      # Calculate predictions from fit coefficients and overrides

      if (sum(is.na(fit_oil$decline)) < 1) {
        fit_volumes_oil <- multi_segment2.q(
          fit_oil$decline["qi"]/12,
          fit_oil$decline["Di"],
          fit_oil$decline["b"],
          fit_oil$decline["Df"],
          fit_oil$decline["alpha"],
          fit_oil$decline["texp_idx"],
          t = seq(
            from = (1-0.5) / YEAR.MONTHS,
            length.out = len_tmp,
            by = 1 / YEAR.MONTHS
          )
        )

        if (forecastLength > 0)
          fit_volumes_oil <- fit_volumes_oil
          #   c(fit_volumes_oil[1], diff(fit_volumes_oil))
      } else {
        fit_volumes_oil <- rep(0, len_tmp)
      }

      if (sum(is.na(fit_gas$decline)) < 1) {
        fit_volumes_gas <- multi_segment2.q(
          fit_gas$decline["qi"]/12,
          fit_gas$decline["Di"],
          fit_gas$decline["b"],
          fit_gas$decline["Df"],
          fit_gas$decline["alpha"],
          fit_gas$decline["texp_idx"],
          t = seq(
            from = (1-0.5) / YEAR.MONTHS,
            length.out = len_tmp,
            by = 1 / YEAR.MONTHS
          )
        )

        if (forecastLength > 0)
          fit_volumes_gas <- fit_volumes_gas
            # c(fit_volumes_gas[1], diff(fit_volumes_gas))
      } else {
        fit_volumes_gas <- rep(0, len_tmp)
      }

      # Forecast water if forecast flag set to true
      if (sum(is.na(fit_water$decline)) < 1 &
          tcWaterForecast == TRUE) {
        fit_volumes_water <- multi_segment2.np(
          fit_water$decline["qi"]/12,
          fit_water$decline["Di"],
          fit_water$decline["b"],
          fit_water$decline["Df"],
          fit_water$decline["alpha"],
          fit_water$decline["texp_idx"],
          t = seq(
            from = 1 / YEAR.MONTHS,
            length.out = len_tmp,
            by = 1 / YEAR.MONTHS
          )
        )

        if (forecastLength > 0)
          fit_volumes_water <-
            c(fit_volumes_water[1], diff(fit_volumes_water))

      } else {
        fit_volumes_water <- rep(0, len_tmp)
      }

      forecast_oil <-
        data.frame(
          oil_pred_daily = fit_volumes_oil / MONTH.DAYS,
          peak_index_oil = seq_along(fit_volumes_oil),
          stringsAsFactors = FALSE
        )

      forecast_gas <-
        data.frame(
          gas_pred_daily = fit_volumes_gas / MONTH.DAYS,
          peak_index_gas = seq_along(fit_volumes_gas),
          stringsAsFactors = FALSE
        )

      forecast_water <-
        data.frame(
          water_pred_daily = fit_volumes_water / MONTH.DAYS,
          peak_index_water = seq_along(fit_volumes_water),
          stringsAsFactors = FALSE
        )

      # Set WOR GOR override flags to default false
      gor_override <- FALSE
      wor_override <- FALSE

      forecasts <-
        data.frame(
          oil_pred_daily = c(rep(NA, which(
            peak_index_oil == 1
          ) - 1),
          fit_volumes_oil / MONTH.DAYS)[1:len_cutoff],
          gas_pred_daily = c(rep(NA, which(
            peak_index_gas == 1
          ) - 1),
          fit_volumes_gas / MONTH.DAYS)[1:len_cutoff],
          water_pred_daily = c(rep(NA, which(
            peak_index_water == 1
          ) - 1),
          fit_volumes_water / MONTH.DAYS)[1:len_cutoff],
          index = seq(from = 1, to = len_cutoff, by = 1)
        )

      pred_gor <-
        sum(forecasts$gas_pred_daily[max_length_stream:len_cutoff], na.rm = TRUE) /
        max(0.000001, sum(forecasts$oil_pred_daily[max_length_stream:len_cutoff], na.rm = TRUE)) * 1000

      if (pred_gor <= cum_gor & gor_check == TRUE
          & gor_initial <= gor_final
          & pred_gor != 0
          & cum_gor != 0) {
        gor_check <- TRUE
      } else {
        gor_check <- FALSE
      }

      if (gor_check == TRUE &
          primary_product == 'OIL' & final_seg == TRUE) {
        gas_min_cum <-
          pmax(
            forecasts$oil_pred_daily * cum_gor / 1000,
            forecasts$oil_pred_daily * gor_final / 1000
          )
        gas_min_fin <-
          pmax(
            forecasts$oil_pred_daily * gor_final / 1000,
            forecasts$oil_pred_daily * gor_final / 1000
          )
        gas_min <- pmin(gas_min_cum, gas_min_fin)
        #forecasts$gas_pred_daily <- pmax(gas_min, forecasts$gas_pred_daily)
        forecasts$gas_pred_daily <- gas_min
        gor_override <- TRUE
      }

      if (gor_check == TRUE &
          primary_product == 'GAS' & final_seg == TRUE) {
        oil_min_cum <-
          pmin(
            forecasts$gas_pred_daily / cum_gor * 1000,
            forecasts$gas_pred_daily / gor_final * 1000
          )
        oil_min_fin <-
          pmin(
            forecasts$gas_pred_daily / gor_final * 1000,
            forecasts$gas_pred_daily / gor_final * 1000
          )
        oil_min <- pmin(oil_min_cum, oil_min_fin)

        #forecasts$oil_pred_daily <- pmin(oil_min, forecasts$oil_pred_daily)
        forecasts$oil_pred_daily <- oil_min
        gor_override <- TRUE
      }

      if (wor_check == TRUE & final_seg == TRUE) {
        if (primary_product == 'OIL') {
          water_min <- forecasts$oil_pred_daily * wor_final
        } else {
          water_min <- forecasts$gas_pred_daily * wor_final
        }
        forecasts$water_pred_daily <-
          pmax(water_min, forecasts$water_pred_daily)
        wor_override <- TRUE
      }

      date_length <- nrow(forecasts)
      forecast_start <- max(dates, na.rm = TRUE)
      actuals_dates <- dates
      delta_dates <- date_length - length(dates)
      forecast_date_start <- add.months(forecast_start, 1)

      if (nrow(forecasts) == length(actuals_dates)) {
        forecast_dates <- dates
      } else {
        forecast_dates <-
          c(
            actuals_dates,
            seq(
              from = forecast_date_start,
              by = "month",
              length.out = delta_dates
            )
          )
      }

      #days_on_forecast <- rep(30.4375, nrow(forecasts))
      days_on_forecast <- n.days(forecast_dates)
      days_on_forecast[1:length(days)] = days

      if (primary_phase == 'OIL') {
        volatility_score <- vol.score(oil, tmax)
      } else {
        volatility_score <- vol.score(gas, tmax)
      }

      # Forecast table
      df_prod <- data.frame(
        date = forecast_dates,
        index = forecasts$index,
        days_on = days_on_forecast,
        oil_actual_daily = c(oil, rep(NA, len_cutoff - length(oil))),
        oil_actual_monthly = c(oil, rep(NA, len_cutoff - length(oil))) * days_on_forecast,
        oil_pred_daily = forecasts$oil_pred_daily,
        gas_actual_daily = c(gas, rep(NA, len_cutoff - length(gas))),
        gas_actual_monthly = c(gas, rep(NA, len_cutoff - length(gas))) * days_on_forecast,
        gas_pred_daily = forecasts$gas_pred_daily,
        water_actual_daily = c(water, rep(NA, len_cutoff - length(water))),
        water_actual_monthly =  c(water, rep(NA, len_cutoff - length(water))) * days_on_forecast,
        water_pred_daily = forecasts$water_pred_daily,
        included_oil = c(oil_outliers, rep(NA, len_cutoff - length(oil_outliers))),
        included_gas = c(gas_outliers, rep(NA, len_cutoff - length(gas_outliers))),
        included_water = c(water_outliers, rep(
          NA, len_cutoff - length(water_outliers)
        )),
        stringsAsFactors = FALSE,
        row.names = NULL
      )

      # Summary table
      df_summary <- data.frame(
        segment = c(identifier),
        segment_final = ifelse(tmax == max(index), TRUE, FALSE),
        single_segment_flag = ifelse(length(index) == tmax, TRUE, FALSE),
        forecast_override = tcOverrideCoef,
        forecast_type = NA_character_,
        gor_override = gor_override,
        wor_override = wor_override,
        t_start = index[1],
        t_start_date = head(dates, 1),
        t_seg_months = tail(index, 1) - index[1],
        t_end = tail(index, 1),
        t_end_date = tail(dates, 1),
        primary_phase = primary_product,
        peak_month_oil = dates[peak_index_oil == 1],
        peak_month_gas = dates[peak_index_gas == 1],
        peak_month_water = dates[peak_index_water == 1],
        oil_pre_forecast = sum(df_prod$oil_actual_monthly[0:match(0, peak_index_oil, nomatch = 0)], na.rm = TRUE),
        gas_pre_forecast = sum(df_prod$gas_actual_monthly[0:match(0, peak_index_gas, nomatch = 0)], na.rm = TRUE),
        water_pre_forecast = sum(df_prod$water_actual_monthly[0:match(0, peak_index_water, nomatch = 0)], na.rm = TRUE),
        ngl_pre_forecast = 0,
        qi_oil = fit_oil$decline["qi"] / YEAR.DAYS,
        b_oil = fit_oil$decline["b"],
        di_nom_oil = fit_oil$decline["Di"],
        di_sec_oil = ifelse(
          is.na(fit_oil$decline["Di"]) |
            is.na(fit_oil$decline["b"]),
          NA_real_,
          secant.from.nominal(fit_oil$decline["Di"], fit_oil$decline["b"])
        ),
        di_tan_oil = ifelse(
          is.na(fit_oil$decline["Di"]),
          NA_real_,
          tangent.from.nominal(fit_oil$decline["Di"])
        ),
        dlim_nom_oil = ifelse(is.na(fit_oil$decline["Df"]), NA_real_, fit_oil$decline["Df"]),
        dlim_sec_oil = ifelse(
          is.na(fit_oil$decline["Df"]) |
            is.na(fit_oil$decline["b"]),
          NA_real_,
          secant.from.nominal(fit_oil$decline["Df"], fit_oil$decline["b"])
        ),
        dlim_tan_oil = ifelse(
          is.na(fit_oil$decline["Df"]),
          NA_real_,
          tangent.from.nominal(fit_oil$decline["Df"])
        ),
        qi_gas = fit_gas$decline["qi"] / YEAR.DAYS,
        b_gas = fit_gas$decline["b"],
        di_nom_gas = fit_gas$decline["Di"],
        di_sec_gas = ifelse(
          is.na(fit_gas$decline["Di"]) |
            is.na(fit_gas$decline["b"]),
          NA_real_,
          secant.from.nominal(fit_gas$decline["Di"], fit_gas$decline["b"])
        ),
        di_tan_gas = ifelse(
          is.na(fit_gas$decline["Di"]),
          NA_real_,
          tangent.from.nominal(fit_gas$decline["Di"])
        ),
        dlim_nom_gas = ifelse(is.na(fit_gas$decline["Df"]), NA_real_, fit_gas$decline["Df"]),
        dlim_sec_gas = ifelse(
          is.na(fit_gas$decline["Df"]) |
            is.na(fit_gas$decline["b"]),
          NA_real_,
          secant.from.nominal(fit_gas$decline["Df"], fit_gas$decline["b"])
        ),
        dlim_tan_gas = ifelse(
          is.na(fit_gas$decline["Df"]),
          NA_real_,
          tangent.from.nominal(fit_gas$decline["Df"])
        ),
        qi_water = fit_water$decline["qi"] / YEAR.DAYS,
        b_water = fit_water$decline["b"],
        di_nom_water = fit_water$decline["Di"],
        di_sec_water = ifelse(
          is.na(fit_water$decline["Di"]) |
            is.na(fit_water$decline["b"]),
          NA_real_,
          secant.from.nominal(fit_water$decline["Di"], fit_water$decline["b"])
        ),
        di_tan_water = ifelse(
          is.na(fit_water$decline["Di"]),
          NA_real_,
          tangent.from.nominal(fit_water$decline["Di"])
        ),
        dlim_nom_water = ifelse(is.na(fit_water$decline["Df"]), NA_real_, fit_water$decline["Df"]),
        dlim_sec_water = ifelse(
          is.na(fit_water$decline["Df"]) |
            is.na(fit_water$decline["b"]),
          NA_real_,
          secant.from.nominal(fit_water$decline["Df"], fit_water$decline["b"])
        ),
        dlim_tan_water = ifelse(
          is.na(fit_water$decline["Df"]),
          NA_real_,
          tangent.from.nominal(fit_water$decline["Df"])
        ),
        volatility_score = volatility_score,
        volatility_flag = ifelse(volatility_score > 0.2, TRUE, FALSE),
        convergence_oil = ifelse(
          fit_oil$convergence["conv"] == 0,
          "CONVERGED",
          ifelse(fit_oil$convergence["conv"] == 51, "WARNING", "NON-CONVERGENCE")
        ),
        iterations_oil = as.integer(fit_oil$convergence["iter.function"]),
        error_oil = calc_error(
          df_prod$oil_actual_daily,
          df_prod$oil_pred_daily,
          df_prod$included_oil,
          days_on_forecast,
          absolute = FALSE,
          percentage = FALSE
        ),
        error_perc_oil = calc_error(
          df_prod$oil_actual_daily,
          df_prod$oil_pred_daily,
          df_prod$included_oil,
          days_on_forecast,
          absolute = FALSE,
          percentage = TRUE
        ),
        error_abs_oil = calc_error(
          df_prod$oil_actual_daily,
          df_prod$oil_pred_daily,
          df_prod$included_oil,
          days_on_forecast,
          absolute = TRUE,
          percentage = FALSE
        ),
        error_abs_perc_oil = calc_error(
          df_prod$oil_actual_daily,
          df_prod$oil_pred_daily,
          df_prod$included_oil,
          days_on_forecast,
          absolute = TRUE,
          percentage = TRUE
        ),
        convergence_gas = ifelse(
          fit_gas$convergence["conv"] == 0,
          "CONVERGED",
          ifelse(fit_gas$convergence["conv"] == 51, "WARNING", "NON-CONVERGENCE")
        ),
        iterations_gas = as.integer(fit_gas$convergence["iter.function"]),
        error_gas = calc_error(
          df_prod$gas_actual_daily,
          df_prod$gas_pred_daily,
          df_prod$included_gas,
          days_on_forecast,
          absolute = FALSE,
          percentage = FALSE
        ),
        error_perc_gas = calc_error(
          df_prod$gas_actual_daily,
          df_prod$gas_pred_daily,
          df_prod$included_gas,
          days_on_forecast,
          absolute = FALSE,
          percentage = TRUE
        ),
        error_abs_gas = calc_error(
          df_prod$gas_actual_daily,
          df_prod$gas_pred_daily,
          df_prod$included_gas,
          days_on_forecast,
          absolute = TRUE,
          percentage = FALSE
        ),
        error_abs_perc_gas = calc_error(
          df_prod$gas_actual_daily,
          df_prod$gas_pred_daily,
          df_prod$included_gas,
          days_on_forecast,
          absolute = TRUE,
          percentage = TRUE
        ),
        convergence_water = ifelse(
          fit_water$convergence["conv"] == 0,
          "CONVERGED",
          ifelse(fit_water$convergence["conv"] ==
                   51, "WARNING", "NON-CONVERGENCE")
        ),
        iterations_water = as.integer(fit_water$convergence["iter.function"]),
        error_water = calc_error(
          df_prod$water_actual_daily,
          df_prod$water_pred_daily,
          df_prod$included_water,
          days_on_forecast,
          absolute = FALSE,
          percentage = FALSE
        ),
        error_perc_water = calc_error(
          df_prod$water_actual_daily,
          df_prod$water_pred_daily,
          df_prod$included_water,
          days_on_forecast,
          absolute = FALSE,
          percentage = TRUE
        ),
        error_abs_water = calc_error(
          df_prod$water_actual_daily,
          df_prod$water_pred_daily,
          df_prod$included_water,
          days_on_forecast,
          absolute = TRUE,
          percentage = FALSE
        ),
        error_abs_perc_water = calc_error(
          df_prod$water_actual_daily,
          df_prod$water_pred_daily,
          df_prod$included_water,
          days_on_forecast,
          absolute = TRUE,
          percentage = TRUE
        ),
        stringsAsFactors = FALSE,
        row.names = NULL
      )

      cut_length <- min(nrow(df_prod), forecastLength + 1)
      df_prod <- df_prod[1:cut_length, ]

      df_summary$oil_buildRamp <- 0
      df_summary$oil_buildfirstMonth <- 0
      df_summary$oil_buildAcPeak <- 0
      df_summary$oil_buildAlpha <- 0
      df_summary$oil_AcPeakIdx <- 0
      df_summary$oil_ForecastPeakIdx <- 0

      df_summary$gas_buildRamp <- 0
      df_summary$gas_buildfirstMonth <- 0
      df_summary$gas_buildAcPeak <- 0
      df_summary$gas_buildAlpha <- 0
      df_summary$gas_AcPeakIdx <- 0
      df_summary$gas_ForecastPeakIdx <- 0

      df_summary$water_buildRamp <- 0
      df_summary$water_buildfirstMonth <- 0
      df_summary$water_buildAcPeak <- 0
      df_summary$water_buildAlpha <- 0
      df_summary$water_AcPeakIdx <- 0
      df_summary$water_ForecastPeakIdx <- 0

      # Reindex based on primary phase
      if (primary_product == 'OIL') {
        df_prod$peak_index <-
          df_prod$index - df_prod$index[peak.production.t(df_prod$oil_actual_daily, tcPeakMonth)] + 1
      } else {
        df_prod$peak_index <-
          df_prod$index - df_prod$index[peak.production.t(df_prod$gas_actual_daily, tcPeakMonth)] + 1
      }

      if (min(peak_index_oil, na.rm = TRUE) <= 0 & seg == 1) {
        buildup_time_oil <- which(peak_index_oil <= 1)
        output <-
          calc_buildup(df_prod$oil_actual_daily,
                       df_prod$oil_pred_daily,
                       buildup_time_oil)
        df_prod$oil_pred_daily <- output[[1]]
        df_summary$oil_buildRamp <- output[[2]]
        df_summary$oil_buildfirstMonth <- output[[3]]
        df_summary$oil_buildAcPeak <- output[[4]]
        df_summary$oil_buildAlpha <- fit_oil$decline["alpha"]
        df_summary$oil_AcPeakIdx <- output[[6]]
        df_summary$oil_ForecastPeakIdx <- fit_oil$decline["texp_idx"]+output[[6]]
      }

      if (min(peak_index_gas, na.rm = TRUE) <= 0 & seg == 1) {
        buildup_time_gas <- which(peak_index_gas <= 1)
        output <-
          calc_buildup(df_prod$gas_actual_daily,
                       df_prod$gas_pred_daily,
                       buildup_time_gas)
        df_prod$gas_pred_daily <- output[[1]]
        df_summary$gas_buildRamp <- output[[2]]
        df_summary$gas_buildfirstMonth <- output[[3]]
        df_summary$gas_buildAcPeak <- output[[4]]
        df_summary$gas_buildAlpha <- fit_gas$decline["alpha"]
        df_summary$gas_AcPeakIdx <- output[[6]]
        df_summary$gas_ForecastPeakIdx <- fit_gas$decline["texp_idx"]+output[[6]]
      }

      if (min(peak_index_water, na.rm = TRUE) <= 0 & seg == 1) {
        buildup_time_water <- which(peak_index_water <= 1)
        output <-
          calc_buildup(df_prod$water_actual_daily,
                       df_prod$water_pred_daily,
                       buildup_time_water)
        df_prod$water_pred_daily <- output[[1]]
        df_summary$water_buildRamp <- output[[2]]
        df_summary$water_buildfirstMonth <- output[[3]]
        df_summary$water_buildAcPeak <- output[[4]]
        df_summary$water_buildAlpha <- fit_water$decline["alpha"]
        df_summary$water_AcPeakIdx <- output[[6]]
        df_summary$water_ForecastPeakIdx <- fit_water$decline["texp_idx"]+output[[6]]
      }

      # Calculate monthly volume
      df_prod$oil_pred_monthly  <-
        df_prod$oil_pred_daily * days_on_forecast
      df_prod$gas_pred_monthly <-
        df_prod$gas_pred_daily * days_on_forecast
      df_prod$water_pred_monthly  <-
        df_prod$water_pred_daily * days_on_forecast

      # Reduce forecasts for volumes below limits
      if (primary_product == 'OIL') {
        df_prod$oil_pred_daily[df_prod$oil_pred_daily <= rate_limit] <-
          NA_real_
        df_prod$gas_pred_daily[df_prod$gas_pred_daily <= rate_limit_secondary] <-
          NA_real_
        df_prod <-
          df_prod[!is.na(df_prod$oil_pred_daily) |
                    !is.na(df_prod$oil_actual_daily), ]

      }  else {
        df_prod$oil_pred_daily[df_prod$oil_pred_daily <= rate_limit_secondary] <-
          NA_real_
        df_prod$gas_pred_daily[df_prod$gas_pred_daily <= rate_limit] <-
          NA_real_
        df_prod <-
          df_prod[!is.na(df_prod$gas_pred_daily) |
                    !is.na(df_prod$gas_actual_daily), ]
      }
    }

    result_df <- list(df_prod, df_summary)
    names(result_df) <- c("prod", "summary")

    return(result_df)
  }


# COMMAND ----------

# DBTITLE 1,Main decline function
##########################################################
# Main decline function ##################################
##########################################################

# run.declines <- function(oil, gas, water = c(NA), days_on = 30.4375, interval = c(NA)) {
run.declines <- function(df) {

  summming_na_f <- function (xs){
    na.pass(sum(xs, na.rm = TRUE))
  }
  data.table::setDT(df)
  gdata <- df[,lapply(.SD, summming_na_f),.SDcols=c("oil", "gas", "water", "days_on"),.(date)]

  setDF(gdata)

  peak_boe_month <- which.max(gdata$oil + gdata$gas / 6)

  gor_3month_peak <-
    (max(0, sum(gdata$gas[(peak_boe_month):(peak_boe_month + 2)], na.rm = TRUE))
     / max(0.00001, sum(gdata$oil[(peak_boe_month):(peak_boe_month +
                                                      2)], na.rm = TRUE)))
  primary_prod_phase <-
    ifelse(gor_3month_peak <= 3.3 , 'OIL', 'GAS')

  active_interval <- head(df$interval, 1)

  gdata$producing_month <- seq_len(nrow(gdata))

  # Date conversion for Spotfire to better handle date sequencing
  gdata$date <- as.Date(gdata$date, format = '%Y-%m-%d')
  t_max <- max(gdata$producing_month, na.rm = TRUE)

  if (primary_prod_phase == 'OIL') {
    gdata$primary_phase <- gdata$oil
  } else {
    gdata$primary_phase <- gdata$gas
  }

  # Compute segment identification and add 1 to better approximate the peak
  change_points_initial <- 0
  if (batch_max_segs > 1) {
    minimum_mo <- max(round(length(gdata$primary_phase) * 0.30, 0), 24)

    change_points <- find.segments(
      gdata$primary_phase,
      # numeric vector of production
      gdata$producing_month,
      # integer vector of producing months 1:n
      return_all = FALSE,
      # return all segments
      log_x = TRUE,
      # log x axis for segment detection
      segment_size = 0.06,
      # fraction of production to look for segments
      segment_max = batch_max_segs,
      # max number of segments
      min_months_start = 9,
      # minimum months segments can be from first prod mo
      min_months_end = minimum_mo,
      # minimum months segments can be to last prod mo
      min_months_between = 18,
      # minimum mo between segments
      force_segments = tcForceSegments,
      # boolean flag to force segments
      force_segment_min = 18,
      # min number of months required for forced segment
      force_segment_frac = 0.35,
      # fraction of producing months for forced segment
      outlier_upper = tcOutlierUpper,
      # threshold for exceeding upper outlier band
      outlier_lower = tcOutlierLower,
      # threshold for exceeding lower outlier band
      segment_upper = tcSegmentUpper,
      # threshold for statistic significance on upper segment
      segment_lower = tcSegmentLower,
      # threshold for statistic significance on upper segment
      include_lower = TRUE,
      # include lower bound for statistic significance
      prod_limit = prod_limit
    )         # primary phase rate limit
  } else {
    change_points <- 0
  }

  # Add conditionality for no changepoints
  if (sum(change_points, na.rm = TRUE) == 0) {
    # Calculate forecasts for wells with no identified segments

    results_list <- fit.prod(
      q1 = gdata$oil,
      q2 = gdata$gas,
      q3 = gdata$water,
      index = gdata$producing_month,
      dates = gdata$date,
      days = gdata$days_on,
      identifier = "single_segment_1",
      tmax = t_max,
      primary_phase = primary_prod_phase,
      forecast_all = tcForecastAll,
      activeInterval = active_interval
    )

    result_fits <- results_list$prod
    result_summary <- results_list$summary

  } else {
    # Calculate forecasts for wells with identified segments
    oil_split <- split.at(gdata$oil, change_points)
    gas_split <- split.at(gdata$gas, change_points)
    water_split <- split.at(gdata$water, change_points)
    index_split <- split.at(gdata$producing_month, change_points)
    date_split <- split.at(gdata$date, change_points)
    days_split <- split.at(gdata$days_on, change_points)
    split_names <- c(paste0("segment_", seq_along(date_split)))

    results_list <- try(Map(
      fit.prod,
      q1 = oil_split,
      q2 = gas_split,
      q3 = water_split,
      index = index_split,
      dates = date_split,
      days = days_split,
      identifier = split_names,
      tmax = t_max,
      primary_phase = primary_prod_phase,
      forecast_all = tcForecastAll,
      activeInterval = active_interval
    ),
    silent = TRUE)

    result_fits <- data.table::rbindlist(purrr::map(results_list,~(.x[["prod"]])),use.names=T)
    result_summary <- data.table::rbindlist(purrr::map(results_list,~(.x[["summary"]])))

    row.names(result_fits) <- NULL
    row.names(result_summary) <- NULL
  }

  # Add header columns to forecast table
  result_fits$api <- df$api[1]
  result_fits$well_name <- df$well_name[1]
  result_fits$index <- seq(from = 1, to = nrow(result_fits))

  # Add header columns to summary table
  summary_input_cols <-
    c(with(
      df,
      list(
        api,
        well_name,
        operator,
        interval,
        latitude,
        longitude,
        latitude_bh,
        longitude_bh,
        lateral_length_ft,
        true_vertical_depth_ft,
        frac_proppant_lbs,
        frac_fluid_bbl,
        net_revenue_interest,
        working_interest
      )
    ))

  summary_output_cols <-
    c(
      'api',
      'well_name',
      'operator',
      'interval',
      'latitude',
      'longitude',
      'latitude_bh',
      'longitude_bh',
      'lateral_length_ft',
      'true_vertical_depth_ft',
      'frac_proppant_lbs',
      'frac_fluid_bbl',
      'net_revenue_interest',
      'working_interest'
    )

  result_summary[, c(summary_output_cols)] <-
    (Map(calc_summary_col, col = summary_input_cols))
  result_summary$ip_year <-
    as.integer(format(result_fits$date[1], "%Y"))


  # Abandonment rate and rate limit calculations
  result_summary$q_lim_oil <-
    hyp.q.lim(
      tail(result_summary$qi_oil, 1),
      # tail(result_summary$alpha_oil, 1),
      # tail(result_summary$texp_idx_oil, 1),
      tail(result_summary$b_oil, 1),
      tail(result_summary$di_nom_oil, 1),
      tail(result_summary$dlim_nom_oil, 1)
    )

  result_summary$q_lim_gas <-
    hyp.q.lim(
      tail(result_summary$qi_gas, 1),
      # tail(result_summary$alpha_gas, 1),
      # tail(result_summary$texp_idx_gas, 1),
      tail(result_summary$b_gas, 1),
      tail(result_summary$di_nom_gas, 1),
      tail(result_summary$dlim_nom_gas, 1)
    )
  result_summary$q_lim_water <-
    hyp.q.lim(
      tail(result_summary$qi_water, 1),
      # tail(result_summary$alpha_water, 1),
      # tail(result_summary$texp_idx_water, 1),
      tail(result_summary$b_water, 1),
      tail(result_summary$di_nom_water, 1),
      tail(result_summary$dlim_nom_water, 1)
    )
  result_summary[, c('q_final_oil', 'q_final_gas', 'q_final_water')] <-
    c(rep(tail(result_fits$oil_pred_daily, 1), nrow(result_summary)),
      rep(tail(result_fits$gas_pred_daily, 1), nrow(result_summary)),
      rep(
        tail(result_fits$water_pred_daily, 1),
        nrow(result_summary)
      ))

  result_summary$q_lim_ngl <- NA_real_
  result_summary$q_final_ngl <- NA_real_

  # Add Oil/Gas/Water EUR, CUM, REM to summary table
  val_list <- c("_actual_cumulative", "_eur", "_remaining")

  result_summary[, c(paste0(rep("oil", 3), val_list))] <-
    calc_eurs(result_fits$oil_actual_monthly,
              result_fits$oil_pred_monthly)
  result_summary[, c(paste0(rep("gas", 3), val_list))] <-
    calc_eurs(result_fits$gas_actual_monthly,
              result_fits$gas_pred_monthly)
  result_summary[, c(paste0(rep("water", 3), val_list))] <-
    calc_eurs(result_fits$water_actual_monthly,
              result_fits$water_pred_monthly)
  # NGL column calculations
  ngl_output_cols <-
    c(
      'ngl_actual_daily',
      'ngl_actual_monthly',
      'ngl_actual_cumulative',
      'ngl_pred_daily',
      'ngl_pred_monthly',
      'ngl_pred_cumulative'
    )
  result_fits[, c(ngl_output_cols)] <- c(0)

  if (tcNglForecast == 'TRUE') {
    result_fits$ngl_pred_daily  <- calc_ngl(result_fits$gas_pred_daily,
                                            initial_ratio = tcInitialNgl,
                                            final_ratio = tcFinalNgl)
    result_fits$ngl_pred_monthly <-
      result_fits$ngl_pred_daily * result_fits$days_on
    result_fits$ngl_pred_cumulative <-
      cumsum.na(result_fits$ngl_pred_monthly)

    result_summary$q_lim_ngl <-
      tcFinalNgl * (tail(result_summary$q_lim_gas, 1) / 1000)
    result_summary$q_final_ngl <-
      rep(tail(result_fits$ngl_pred_daily, 1),
          nrow(result_summary))
  }

  result_summary[, c(paste0(rep("ngl", 3), val_list))] <-
    calc_eurs(result_fits$ngl_actual_monthly,
              result_fits$ngl_pred_monthly)

  # Cumulative calculations
  cum_input_cols <-
    with(
      result_fits,
      list(
        oil_actual_monthly,
        oil_pred_monthly,
        gas_actual_monthly,
        gas_pred_monthly,
        water_actual_monthly,
        water_pred_monthly
      )
    )
  cum_output_cols <-
    c(
      'oil_actual_cumulative',
      'oil_pred_cumulative',
      'gas_actual_cumulative',
      'gas_pred_cumulative',
      'water_actual_cumulative',
      'water_pred_cumulative'
    )
  result_fits[, c(cum_output_cols)] <-
    Map(cumsum.na, xs = cum_input_cols)

  # Gather input columns for equivalents calculations
  oil_cols <-
    c(with(
      result_fits,
      list(
        oil_actual_daily,
        oil_actual_monthly,
        oil_actual_cumulative,
        oil_pred_daily,
        oil_pred_monthly,
        oil_pred_cumulative
      )
    ))
  gas_cols <-
    c(with(
      result_fits,
      list(
        gas_actual_daily,
        gas_actual_monthly,
        gas_actual_cumulative,
        gas_pred_daily,
        gas_pred_monthly,
        gas_pred_cumulative
      )
    ))
  ngl_cols <-
    c(with(
      result_fits,
      list(
        ngl_actual_daily,
        ngl_actual_monthly,
        ngl_actual_cumulative,
        ngl_pred_daily,
        ngl_pred_monthly,
        ngl_pred_cumulative
      )
    ))

  # Calculate equivalents columns on 6:1 and 20:1 basis
  val_list_equiv <-
    c(
      '_actual_daily',
      '_actual_monthly',
      '_actual_cumulative',
      '_pred_daily',
      '_pred_monthly',
      '_pred_cumulative'
    )
  result_fits[, c(paste0(rep("boe6", 6), val_list_equiv), paste0(rep("boe20", 6), val_list_equiv))] <-
    Map(
      calc_equiv,
      oil = oil_cols,
      gas = gas_cols,
      ngl = ngl_cols,
      conv_factor = c(rep(6, 6), rep(20, 6))
    )
  result_summary[, c(paste0(rep("boe6", 3), val_list))] <-
    calc_eurs(result_fits$boe6_actual_monthly,
              result_fits$boe6_pred_monthly)
  result_summary[, c(paste0(rep("boe20", 3), val_list))] <-
    calc_eurs(result_fits$boe20_actual_monthly,
              result_fits$boe20_pred_monthly)
  # Error flags
  result_summary$error_flag = ifelse(
    primary_prod_phase == 'OIL' &
      result_summary$error_perc_oil >= 0.15,
    TRUE,
    ifelse(
      primary_prod_phase == 'GAS' &
        result_summary$error_perc_gas >= 0.15,
      TRUE,
      FALSE
    )
  )

  # Calculate combined columns with actual and predicted
  forecast <-
    ifelse(result_fits$index > which.max(gdata$date),
           'FORECAST',
           'HISTORICAL')
  comb_output_cols <-
    c(paste0(
      c('oil', 'gas', 'water', 'ngl', 'boe6', 'boe20'),
      rep('_combined_monthly', 6)
    ))
  comb_input_actuals <-
    c(with(
      result_fits,
      list(
        oil_actual_monthly,
        gas_actual_monthly,
        water_actual_monthly,
        ngl_actual_monthly,
        boe6_actual_monthly,
        boe20_actual_monthly
      )
    ))
  comb_input_pred <-
    c(with(
      result_fits,
      list(
        oil_pred_monthly,
        gas_pred_monthly,
        water_pred_monthly,
        ngl_pred_monthly,
        boe6_pred_monthly,
        boe20_pred_monthly
      )
    ))
  result_fits[, c(comb_output_cols)] <-
    mapply(calc_combined,
           comb_input_actuals,
           comb_input_pred,
           list(forecast))

  # Add batch name to summary & forcast tables
  result_summary$batch_name <- c(tcBatchName)
  result_fits$batch_name <- c(tcBatchName)

  result_summary$forecast_type <-
    ifelse(nchar(result_summary$api[1]) > 20, "Type Curve", "Single Well")

  result_df <- list("prod"=list(result_fits), "summary"=list(result_summary))
#   names(result_df) <- c("prod", "summary")

  return(result_df)
}

run.declines.safe <- function(df) {
  return(tryCatch(
    run.declines(df),
    error = function(e)
      list("prod" = list(NA), "summary" = list(e$message))
  ))
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Additional code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark related functions

# COMMAND ----------

# Function to prepare the list as needed for spark processing
modify_list_elemnts_to_carry_needed_info <- function(total_list,env_path_value){

  adding_needed_elements <- function(list_element,env_path_value){
  y <- list()
  y[["df"]] <- list_element # 1 is the proxy for already present data df in the list element
  y[["env_path"]] <- env_path_value
  return(y)
    }
  total_list <- purrr::map(total_list,adding_needed_elements,env_path_value)
  return(total_list)
  }


# Spark function for run.declines.safe
run_spark_function <- function(list_element){
  # Setting up environment
  var_env <-readr::read_rds(file=list_element[["env_path"]])
  copy_cleaned_env(rlang::env_clone(var_env))
  library(data.table)
  # Running the decline function
#   unpacking list df
  df <- (list_element[["df"]])
  setDT(df)
  results=df[,run.declines.safe(.SD),.(api),.SDcols=colnames(df)]
  return(results)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Envrionment copy functions

# COMMAND ----------

copy_cleaned_env <- function(var_env){

 lst <- ls(var_env)

# return everything needed:
df_objects <- lst[sapply(lst,function(var) {any(class(var_env[[var]]) %in% c('numeric',"logical","character","list","data.frame"))})]

# Exclude DATABRICKS variables that start with "D"
df_objects <- df_objects[stringr::str_detect(negate=T,pattern="^D",string=df_objects)]

# # Copying variables to global as needed
for (i in df_objects){
 assign(x=i,value=var_env[[i]],envir=.GlobalEnv)
}
return(NULL)
}


copy_needed_env_variables_to_rds_file <- function(env_path){

 lst <- ls(envir=.GlobalEnv)
# Ignoring list & data.frame objects
  df_objects <- lst[sapply(lst,function(var) any(class(get(var))=='data.frame'))]
  list_objects <- lst[sapply(lst,function(var) any(class(get(var))=='list'))]
  env_replica <- rlang::env_clone(.GlobalEnv)
  rm(list=c(df_objects,list_objects),envir=env_replica)
  readr::write_rds(x=env_replica,file=env_path)
}


# COMMAND ----------

# MAGIC %md
# MAGIC ## Faster well sort

# COMMAND ----------

# DBTITLE 1,Faster well sorting function
well_list_sort_new <- function(df, split_col
                               ,n_cores = 4 # dont require this as we use spark for running
                               ) {
  # ensure df argument is a data frame
  split_col <- deparse(substitute(split_col))
  if (!("data.frame" %in% class(df)))
    stop("Data frame input must be a data frame or data table")
  # changing to data.table for faster processing and below syntax only works for data.table
    setDT(df)
  if (split_col %in% colnames(df)) {

    # split data frame by column
    y <- df[,list(list(.SD)),.("split_cols"=as.factor(get(split_col)))]

    # Combine some api dfs in a single dataframe to do faster spark processing
    y[,combine_index:=as.integer((1:.N)/n_cores)]
    x <- y[,list(list(rbindlist(V1))),.(combine_index)]
    return(x[["V1"]])
  } else {
    # ensure column argument is in the data frame
    stop("Column not in data frame")
  }
}

# COMMAND ----------

# MAGIC %md
# MAGIC # Env copy path

# COMMAND ----------

# Copying env variables to this path
env_path <- "/dbfs/FileStore/spark_R_variables_testing.rds"

# COMMAND ----------

# MAGIC %md
# MAGIC # Primary Directive

# COMMAND ----------

##########################################################
# Primary Directive ######################################
##########################################################

## Implemented spark logic, so commented the Parallel logic cluster code
#browser()
if (tcDeclineMethod != 'type') {
  # Import data and rename columns
  data <- as.data.frame(production, stringsAsFactors = FALSE)
  colnames(data) <-
    c(
      "api",
      "date",
      "days_on",
      "producing_month",
      "oil",
      "gas",
      "water",
      "well_name",
      "operator",
      "interval",
      "lateral_length_ft",
      "true_vertical_depth_ft",
      "latitude",
      "longitude",
      "latitude_bh",
      "longitude_bh",
      "frac_proppant_lbs",
      "frac_fluid_bbl",
      "working_interest",
      "net_revenue_interest"
    )

  # Ensure additional columns exist and if not, add NA reps for data frame length
  # well_name <- get0('well_name', ifnotfound = rep(NA_character_, nrow(data)))
  # operator <- get0('operator', ifnotfound = rep(NA_character_, nrow(data)))
  # play <- get0('play', ifnotfound = rep(NA_character_, nrow(data)))
  # county <- get0('county', ifnotfound = rep(NA_character_, nrow(data)))
  # interval <- get0('interval', ifnotfound = rep(NA_character_, nrow(data)))
  # days_on <- get0('days_on', ifnotfound = rep(NA_real_, nrow(data)))
  # working_interest <- get0('working_interest', ifnotfound = rep(default_WI, nrow(data)))
  # net_revenue_interest <- get0('net_revenue_interest', ifnotfound = rep(default_NRI, nrow(data)))
  # latitude <- get0('latitude', ifnotfound = rep(NA_real_, nrow(data)))
  # longitude <- get0('longitude', ifnotfound = rep(NA_real_, nrow(data)))
  # latitude_bh <- get0('latitude_bh', ifnotfound = rep(NA_real_, nrow(data)))
  # longitude_bh <- get0('longitude_bh', ifnotfound = rep(NA_real_, nrow(data)))
  # lateral_length_ft <- get0('lateral_length_ft', ifnotfound = rep(NA_real_, nrow(data)))
  # true_vertical_depth_ft <- get0('true_vertical_depth_ft', ifnotfound = rep(NA_real_, nrow(data)))
  # frac_proppant_lbs <- get0('frac_proppant_lbs', ifnotfound = rep(NA_real_, nrow(data)))
  # frac_fluid_bbl <- get0('frac_fluid_bbl', ifnotfound = rep(NA_real_, nrow(data)))

  # Bind columns to data frame
  data_m <- data[!(is.na(data$producing_month)), ]
  # data_m <- bind_data_col(well_name, data_m, NA_character_)
  # data_m <- bind_data_col(operator, data_m, NA_character_)
  # data_m <- bind_data_col(play, data_m, NA_character_)
  # data_m <- bind_data_col(county, data_m, NA_character_)
  # data_m <- bind_data_col(interval, data_m, NA_character_)
  # data_m <- bind_data_col(days_on, data_m, NA_real_)
  # data_m <- bind_data_col(working_interest, data_m, default_WI)
  # data_m <- bind_data_col(net_revenue_interest, data_m, default_NRI)
  # data_m <- bind_data_col(latitude, data_m, NA_real_)
  # data_m <- bind_data_col(longitude, data_m, NA_real_)
  # data_m <- bind_data_col(latitude_bh, data_m, NA_real_)
  # data_m <- bind_data_col(longitude_bh, data_m, NA_real_)
  # data_m <- bind_data_col(lateral_length_ft, data_m, NA_real_)
  # data_m <- bind_data_col(true_vertical_depth_ft, data_m, NA_real_)
  # data_m <- bind_data_col(frac_proppant_lbs, data_m, NA_real_)
  # data_m <- bind_data_col(frac_fluid_bbl, data_m, NA_real_)
  #
} else {
  data_m <- as.data.frame(forecasts, stringsAsFactors = FALSE)
  colnames(data_m) <-
    c(
      "api",
      "date",
      "days_on",
      "producing_month",
      "oil",
      "gas",
      "water",
      "well_name",
      "operator",
      "interval",
      "lateral_length_ft",
      "true_vertical_depth_ft",
      "latitude",
      "longitude",
      "latitude_bh",
      "longitude_bh",
      "frac_proppant_lbs",
      "frac_fluid_bbl"
    )
  quantum_multi_core <- FALSE
}

# Prevent scientific notation for API designation
data_m$api <-
  as.character(trimws(format(data_m$api, scientific = FALSE)))

prod_rows <- nrow(data_m)

if (prod_rows < 1000) {
  quantum_multi_core <- FALSE
}

# if (dir.exists(as.character(javaHome)) == FALSE) {
#   quantum_multi_core <- FALSE
# }

if (quantum_multi_core) {
#   suppressWarnings(suppressPackageStartupMessages(library(parallel)))
    num_cores <- try(data.table::getDTthreads(), silent = TRUE)
#   num_cores <- try(parallel::detectCores(), silent = TRUE)
  if (class(num_cores) !=  "try-error") {
    nw <- num_cores - 1
  } else {
    nw <- 1
  }

#   sock_type <- parallel:::getClusterOption("type")

#   if (sock_type %in% c("TERR", "PSOCK")) {
#     cl <- makeCluster(nw, JAVA_OPTIONS = rep("-Xmx1024m", nw))
#   } else {
#     cl <- makeCluster(nw, useXDR = FALSE)
#   }

  passed_vars <- c(
    "ECON.BOE",
    "EXPONENTIAL.EPS",
    "GAS.BOE",
    "HARMONIC.EPS",
    "MONTH.DAYS",
    "YEAR.DAYS",
    "YEAR.MONTHS",
    "add.months",
    "breakpoints",
    "cumsum.na",
    "exponential.np",
    "exponential.q",
    "find.outliers",
    "fit.hyp2exp",
    "run.declines",
    "fit.prod",
    "forecast.exp.cum",
    "forecast.hyp.cum",
    "forecastLength",
    "harmonic.np",
    "harmonic.q",
    "hyp.q.lim",
    "hyp.t.lim",
    "hyp2exp.np",
    "hyp2exp.q",
    "hyp2exp.transition",
    "hyperbolic.np",
    "hyperbolic.q",
    "max.na",
    "n.days",
    "nominal.from.secant",
    "nominal.from.tangent",
    "peak.production.t",
    "prod_limit",
    "prod_limit_secondary",
    "pseudo.huber",
    "mae",
    "sse",
    "mape",
    "rmse",
    "rmsle",
    "medape",
    "rrse",
    "ssle",
    "mse",
    "sae",
    "rae",
    "rate_limit",
    "rate_limit_secondary",
    "find.segments",
    "ewa.avg",
    "replace.na",
    "row.shift",
    "secant.from.nominal",
    "split.at",
    "tangent.from.nominal",
    "tcBFactor",
    "tcBFactor2",
    "tcBFactorMax",
    "tcBFactorMin",
    "tcBatchName",
    "tcDowntimeCutoff",
    "tcDowntimeCutoffSecondary",
    "tcFinalDecline",
    "tcFinalDecline2",
    "tcFinalDeclineMax",
    "tcFinalDeclineMin",
    "tcFinalNgl",
    "tcForecastYears",
    "tcInitialDecline",
    "tcInitialDecline2",
    "tcInitialDeclineMax",
    "tcInitialDeclineMin",
    "tcInitialNgl",
    "tcInitialRate",
    "tcInitialRate2",
    "tcInitialRateMax",
    "tcInitialRateMin",
    "tcNglForecast",
    "tcOutlierLower",
    "tcOutlierUpper",
    "tcOverrideMin",
    "tcPeakMonth",
    "breakpoints",
    "tcProbability",
    "tcProdGrossNet",
    "tcRateLimit",
    "tcSegmentLower",
    "tcSegmentUpper",
    "tcWaterForecast",
    "tcWellCutoff",
    "tc_override_min",
    "breakpoints.Fstats",
    "breakpoints.breakpointsfull",
    "breakpoints.formula",
    "logLik.breakpoints",
    "recresid",
    "recresid.default",
    "recresid.formula",
    "recresid.lm",
    "recresid_c",
    "summary.breakpointsfull",
    "recresid_r",
    "tcFailSafeDeclineMin",
    "tcFailSafeBFactorMax",
    "log.cosh",
    "huber",
    "gor_check",
    "wor_check",
    "tcForecastAll",
    "daily_prod",
    "agg_batch_curve",
    "agg_type_curve",
    "bind_data_col",
    "calc_combined",
    "calc_summary_col",
    "calc_eurs",
    "calc_equiv",
    "calc_ngl",
    "calc_error",
    "calc_buildup",
    "vol.score",
    "replace.na.prior",
    "tcOverrideCoef",
    "batch_max_segs",
    "tcOverrideCoef",
    "tcForceSegments",
    "find.peaks",
    "tcRateLimitSecondary",
    "tcProdWeight",
    "bDistCurt",
    "tcNormalizeValue",
    "run.declines.safe",
    "tcExternal",
    "aggs.mean",
    "oil_min_b_pri",
    "oil_min_b_sec",
    "oil_max_b_pri",
    "oil_max_b_sec",
    "gas_min_b_pri",
    "gas_min_b_sec",
    "gas_max_b_pri",
    "gas_max_b_sec"
  )

#   clusterExport(cl, passed_vars,envir=.GlobalEnv)
}

# COMMAND ----------

# Changing data_m to R data.table for faster processing
setDT(data_m)

if (nrow(data_m) > 1) {
  # Calculate IP Year for each well
  data_m[, ip_year := rep(as.integer(format(min(as.Date(date)), '%Y')), length(date)), by =
           .(as.factor(api))]

  # Well counts for type curve
  data_m$well_count <- 1

  if (tcProdGrossNet == 'NET') {
    data_m[, c('oil', 'gas', 'water') := list(
      oil * net_revenue_interest,
      gas * net_revenue_interest,
      water * net_revenue_interest
    )]
  }

  #   data_m[,days_on:=n.days(date)]
  if (tcDeclineMethod == 'type') {

    if (tcNormalizeValue > 0) {
      normalization_factor <-
        as.numeric(get0("tcNormalizeFactor", ifnotfound = 0.8))
      setDF(data_m)
      data_m[, c('oil', 'gas', 'water')] <- mapply(
        normalize_prod,
       # prod_col = df[, c('oil', 'gas', 'water')],
        prod_col = list(data_m$oil,data_m$gas,data_m$water),
        norm_col = list(data_m$lateral_length_ft),
        norm_val = tcNormalizeValue,
        norm_factor = normalization_factor
      )
    }
    if (tcExternal == FALSE) {
      tc_subset <- base::split(data_m, as.factor(data_m$well_name))
    } else {
      tc_subset <- base::split(data_m, as.factor(data_m$interval))
    }

    if (quantum_multi_core) {
      actual_tc <-
        parLapply(cl, tc_subset, agg_type_curve, prob = tcProbability)
    } else {
      actual_tc <- lapply(tc_subset, agg_type_curve, prob = tcProbability)
    }
    # Temporarily change the batch max segments to 1 for type curve
    tmp_batch_max_segs <- batch_max_segs
    batch_max_segs <- 1

    # Run declines on aggregated curves and export production and summary tables
    if (quantum_multi_core) {
      tc_run <-  parLapply(cl, actual_tc, run.declines.safe)
    } else {
      #tc_run <-  lapply(actual_tc, run.declines.safe)
      tc_run <-  lapply(actual_tc, run.declines)
    }
    # New way of unpacking results
    batch_run <- data.table::rbindlist(tc_run, use.names = T)

    # Check if any wells errored out in the fitting routine
    bad_fits <- batch_run[is.na(prod)]

    if (nrow(bad_fits) > 0) {
      bad_well_errors <- as.vector(bad_fits[["summary"]])
      bad_well_names <-
        paste0("API: ", as.vector(bad_fits[["api"]]))
      # Remove bad fits from well set
      batch_run <- batch_run[!is.na(prod)]
      fit_warns <-
        data.frame(API = bad_well_names, ERROR = bad_well_errors)
      warning(
        "THE FOLLOWING WELLS WERE UNABLE TO BE FIT: \n",
        paste0("API:", fit_warns$API, " ", fit_warns$ERROR, "\n")
      )
    }

    tc_fits <-
      data.table::rbindlist(batch_run[["prod"]], use.names = T)
    tc_summary <-
      data.table::rbindlist(batch_run[["summary"]], use.names = T)

    # Reset max segments
    batch_max_segs <- tmp_batch_max_segs

    tcForecastBatch <- default_df_prod
    tcSummaryBatch <- default_df_summary
    tcForecast <- tc_fits
    tcSummary <- tc_summary
  }
  if (tcDeclineMethod %in% c('batch', 'batchtc')) {
    data_m[, days_on := n.days(date)]

    # Convert to daily production data
    if (daily_prod != 'TRUE') {
      data_m[, c('oil', 'gas', 'water') := list(oil / days_on,
                                                gas / days_on,
                                                water / days_on)]

    }

    # Split well table into lists by API
    y <- data_m[, list(list(.SD)), .("api" = as.factor(api))]
    well_lists <- y[["V1"]]
    names(well_lists) <- y$api

    # Count observations in input data frame & subset by min months
    obs_count <- sapply(well_lists, nrow)
    initial_pass <-
      names(obs_count[obs_count > tc_override_min &
                        obs_count < (tcForecastYears * YEAR.MONTHS)])
    final_pass <- names(obs_count[obs_count <= tc_override_min])

    # Create lists of APIs with and without enough production data
    data_complete <- data_m[api %in% initial_pass, ]
    data_limited <- data_m[api %in% final_pass, ]

    # Create list of wells by API, sorted according to multi core compute
    data_full <-
      well_list_sort_new(data_complete,
                         api,
                         ifelse(quantum_multi_core, 3*nw, 1))

    # Parallel function to run declines on list of wells
    if (quantum_multi_core) {
      copy_needed_env_variables_to_rds_file(env_path)
      s1 <-
        modify_list_elemnts_to_carry_needed_info(total_list = data_full, env_path_value =
                                                   env_path)
      cat(length(s1), "\n")
      batch_run <- SparkR::spark.lapply(s1, run_spark_function)
    } else {
      batch_run <-  lapply(data_full, run.declines.safe)
    }

    batch_run <- data.table::rbindlist(batch_run, use.names = T)

    # Check if any wells errored out in the fitting routine
    bad_fits <- batch_run[is.na(prod)]

    if (nrow(bad_fits) > 0) {
      bad_well_errors <- as.vector(bad_fits[["summary"]])
      bad_well_names <-
        paste0("API: ", as.vector(bad_fits[["api"]]))
      # Remove bad fits from well set
      batch_run <- batch_run[!is.na(prod)]
      fit_warns <-
        data.frame(API = bad_well_names, ERROR = bad_well_errors)
      warning(
        "THE FOLLOWING WELLS WERE UNABLE TO BE FIT: \n",
        paste0("API:", fit_warns$API, " ", fit_warns$ERROR, "\n")
      )
    }

    batch_fits <-
      data.table::rbindlist(batch_run[["prod"]], use.names = T)
    batch_summary <-
      data.table::rbindlist(batch_run[["summary"]], use.names = T)

    # Run type curve in addition to batch decline
    if (tcDeclineMethod == 'batchtc') {
      ##### Type curve from batch fits ####
      # Create list of APIs that are within last n years and intervals needed for type curves
      n_years  <- abs(tcYears) - 1

      if (nrow(data_limited) == 0) {
        tc_apis <-
          unique(
            subset(
              batch_summary,
              interval %in% data_complete$interval
              &
                ip_year >= max(data_m$ip_year, na.rm = TRUE) - n_years,
              select = c(api, interval)
            )
          )
      } else {
        tc_apis <-
          unique(
            subset(
              batch_summary,
              interval %in% data_limited$interval
              &
                ip_year >= max(data_m$ip_year, na.rm = TRUE) - n_years,
              select = c(api, interval)
            )
          )
      }

      # Filter to forecast wells within relevant interval and time horizon
      tc_batch_subset <- subset(batch_fits, api %in% tc_apis$api)

      # Error handling for cases when not enough wells exist
      if (nrow(tc_batch_subset) == 0) {
        tc_batch_subset <- batch_fits
        tc_batch_subset$interval <-
          batch_summary$interval[match(tc_batch_subset$api, batch_summary$api)]
        tc_batch_subset$well_count <- 1
        tc_batch_subset <- well_list_sort(tc_batch_subset, interval)
      } else {
        tc_batch_subset$interval <-
          tc_apis$interval[match(tc_batch_subset$api, tc_apis$api)]
        tc_batch_subset$well_count <- 1
        tc_batch_subset <- well_list_sort(tc_batch_subset, interval)
      }

      # Aggregate batch fits into single stream for given probability per interval
      batch_tc <-
        lapply(tc_batch_subset, agg_batch_curve, prob = list(tcProbability))

      # Temporarily change the batch max segments to 1 for type curve
      tmp_batch_max_segs <- batch_max_segs
      batch_max_segs <- 1

      if (quantum_multi_core) {
        clusterExport(cl, passed_vars)
        batch_tc_run <- parLapply(cl, batch_tc, run.declines.safe)
      } else {
        batch_tc_run <-  lapply(batch_tc, run.declines.safe)
      }

      # Check if any wells errored out in the fitting routine
      bad_fits <-
        pmax(0, ceiling(which(sapply(
          batch_tc_run, is.na
        )) / 2))

      if (sum(bad_fits, na.rm = TRUE) > 0) {
        list_bad <- batch_tc_run[c(bad_fits)]
        bad_well_errors <-
          paste0("Error:", unname(do.call(
            "rbind", lapply(list_bad, function (x)
              return(x$summary))
          )))
        bad_well_names <- paste0("API: ", names(list_bad))

        # Remove bad fits from well set
        batch_tc_run[c(bad_fits)] <- NULL

        fit_warns <-
          data.frame(API = bad_well_names, ERROR = bad_well_errors)
        warning(
          "THE FOLLOWING WELLS WERE UNABLE TO BE FIT: \n",
          paste0("API:", fit_warns$API, " ", fit_warns$ERROR, "\n")
        )
      }

      # Reset max segments
      batch_max_segs <- tmp_batch_max_segs

      batch_tc_fits <-
        do.call("rbind", lapply(batch_tc_run, function (x)
          return(x$prod)))
      batch_tc_summary <-
        do.call("rbind", lapply(batch_tc_run, function (x)
          return(x$summary)))

      tcForecastBatch <- batch_fits
      tcSummaryBatch <- batch_summary

      # Batch fits on wells with override
      if (nrow(data_limited) > 0) {
        data_limited <-
          well_list_sort(data_limited, api, ifelse(quantum_multi_core, nw, 1))

        tcOverrideCoef <- TRUE

        if (quantum_multi_core) {
          #           clusterExport(cl, c(passed_vars, "batch_tc_summary"))
          copy_needed_env_variables_to_rds_file(env_path)
          s1 <-
            modify_list_elemnts_to_carry_needed_info(total_list = data_limited, env_path_value =
                                                       env_path)
          batch_run_or <-
            SparkR::spark.lapply(s1, run_spark_function)
          #           batch_run_or <-
          #             parLapply(cl, data_limited, run.declines.safe)
        } else {
          batch_run_or <- lapply(data_limited, run.declines.safe)
        }

        tcOverrideCoef <- FALSE

        # Check if any wells errored out in the fitting routine
        bad_fits <-
          pmax(0, ceiling(which(sapply(
            batch_run_or, is.na
          )) / 2))

        if (sum(bad_fits, na.rm = TRUE) > 0) {
          list_bad <- batch_run_or[c(bad_fits)]
          bad_well_errors <-
            paste0("Error:", unname(do.call(
              "rbind", lapply(list_bad, function (x)
                return(x$summary))
            )))
          bad_well_names <- paste0("API: ", names(list_bad))

          # Remove bad fits from well set
          batch_run_or[c(bad_fits)] <- NULL

          fit_warns <-
            data.frame(API = bad_well_names, ERROR = bad_well_errors)
          warning(
            "THE FOLLOWING WELLS WERE UNABLE TO BE FIT: \n",
            paste0("API:", fit_warns$API, " ", fit_warns$ERROR, "\n")
          )
        }

        batch_fits_or <-
          do.call("rbind", lapply(batch_run_or, function (x)
            return(x$prod)))
        batch_summary_or <-
          do.call("rbind", lapply(batch_run_or, function (x)
            return(x$summary)))

        # Combine initial batch run with secondary run
        tcForecastBatch <- rbind(batch_fits, batch_fits_or)
        tcSummaryBatch <- rbind(batch_summary, batch_summary_or)
      }

      tcForecast <- batch_tc_fits
      tcSummary <- batch_tc_summary

    } else {
      tcForecastBatch <- batch_fits
      tcSummaryBatch <- batch_summary

      tcForecast <- default_df_prod
      tcSummary <- default_df_summary
    }
  }
}

# COMMAND ----------

if (is.null(get0("tcForecastBatch")) |
    is.null(get0("tcForecast")) |
    is.null(get0("tcSummaryBatch")) | is.null(get0("tcSummary"))) {
  tcForecastBatch <- default_df_prod
  tcSummaryBatch <- default_df_summary
  tcForecast <- default_df_prod
  tcSummary <- default_df_summary
}

timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

# Calculate unique id and append to output data frames
unique_id <- paste(tcBatchName,
                   tryCatch(
                     Sys.getenv("USERNAME"),
                     warning = 'user',
                     error = 'user'
                   ),
                   format(Sys.time(), "%Y-%m-%d-T%H%M%S%Z"),
                   sep = "_")
unique_id <- gsub(" ", "_", unique_id)

tcForecastBatch$unique_id <- unique_id
tcSummaryBatch$unique_id <- unique_id
tcForecast$unique_id <- unique_id
tcSummary$unique_id <- unique_id

tcForecastBatch$timestamp <- timestamp
tcSummaryBatch$timestamp <- timestamp
tcForecast$timestamp <- timestamp
tcSummary$timestamp <- timestamp
