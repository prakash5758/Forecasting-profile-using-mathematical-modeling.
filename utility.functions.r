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

# Jonathan Henderson; Last modified: 02 JAN 2022 10:00 PM MST

# Dependencies: 

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
replace.na <- function (xs, default=0) {
  xs[is.infinite(xs)] <- default
  xs[is.na(xs)] <- default
  xs
}

# exponential weighted moving average
ewa.avg <- function(x) {
  x <- replace.na(x)
  if(length(x) == 1) {
    y <- x
  } else {
    nx <- length(x)
    y <- numeric(nx)
    a <- 0.2
    y[1] <- x[1]
    for (k in 2:nx) y[k] <- a*x[k] + (1-a)*y[k-1]
  }
  return(y)
}

# split object at x position 
split.at <- function(x, position) {
  if (!is.na(position[1])) {
    out <- list()
    position_2 <- c(1, position, length(x)+1)
    for (i in seq_along(position_2[-1])) {
      out[[i]] <- x[position_2[i]:(position_2[i+1]-1)]
    }
    return(out)
  } else {
    return(x)
  }
}

# return lagged row of input vector by n length
row.shift <- function(x, shiftLen = 1L) {
  r <- (1L + shiftLen):(length(x) + shiftLen)
  r[r<1] <- NA
  return(x[r])
}

# add months to a date input
add.months <- function(date, n=1) {
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
fast.do.call <- function(what, args, quote = FALSE, envir = parent.frame()) {
  if (quote)
    args <- lapply(args, enquote)
  
  if (is.null(names(args)) ||
      is.data.frame(args)){
    argn <- args
    args <- list()
  }else{
    # Add all the named arguments
    argn <- lapply(names(args)[names(args) != ""], as.name)
    names(argn) <- names(args)[names(args) != ""]
    # Add the unnamed arguments
    argn <- c(argn, args[names(args) == ""])
    args <- args[names(args) != ""]
  }
  
  if (class(what) == "character"){
    if(is.character(what)){
      fn <- strsplit(what, "[:]{2,3}")[[1]]
      what <- if(length(fn)==1) {
        get(fn[[1]], envir=envir, mode="function")
      } else {
        get(fn[[2]], envir=asNamespace(fn[[1]]), mode="function")
      }
    }
    call <- as.call(c(list(what), argn))
  }else if (class(what) == "function"){
    f_name <- deparse(substitute(what))
    call <- as.call(c(list(as.name(f_name)), argn))
    args[[f_name]] <- what
  }else if (class(what) == "name"){
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
find.peaks <- function(x, nups = 1, ndowns = 3, zero = "0", peakpat = NULL, 
                       # peakpat = "[+]{2,}[0]*[-]{2,}", 
                       minpeakheight = -Inf, minpeakdistance = 1,
                       threshold = 0, npeaks = 0, sortstr = FALSE)
{
  stopifnot(is.vector(x, mode="numeric") || length(is.na(x)) == 0)
  if (! zero %in% c('0', '+', '-'))
    stop("Argument 'zero' can only be '0', '+', or '-'.")
  
  # transform x into a "+-+...-+-" character string
  xc <- paste(as.character(sign(diff(x))), collapse="")
  xc <- gsub("1", "+", gsub("-1", "-", xc))
  # transform '0' to zero
  if (zero != '0') xc <- gsub("0", zero, xc)
  
  # generate the peak pattern with no of ups and downs
  if (is.null(peakpat)) {
    peakpat <- sprintf("[+]{%d,}[-]{%d,}", nups, ndowns)
  }
  
  # generate and apply the peak pattern
  rc <- gregexpr(peakpat, xc)[[1]]
  if (rc[1] < 0) return(NULL)
  
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
  inds <- which(xv >= minpeakheight & xv - pmax(x[x1], x[x2]) >= threshold)
  
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
  if (length(X) == 0) return(c())
  
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
peak.production.t <- function (q, min_life = 0.9) {
  q2 <- q
  q <- q[q > prod_limit]
  if (min_life > 1 | min_life <= 0 | class(min_life) != 'numeric')
    stop("Min_life can only be greater than zero and less than or equal to 1")
  lrq <- round(length(q) * as.numeric(min_life),0)
  
  if (length(q) <= 12) {
    peak.t <- min(length(q), 3, which.max(q[1:lrq]))
  }
  
  if (lrq > length(q)-3 & batch_max_segs > 1) {
    lrq <- min(round(length(q) * 0.75,0), abs(length(q)-3))
  }
  
  if (batch_max_segs < 6 & length(q) != forecastLength + 1 ) {
    peaks <- find.peaks(q[1:lrq], nups = 1, ndowns = 4)
    
    if (is.null(peaks)) {
      peaks <- which.max(q2[1:lrq])
    }
    
    limit_q2 <- ifelse(max(q2, na.rm = TRUE) >= 2500, 105, 36)
    
    peaks <- ifelse(length(q2) > limit_q2, head(tail(sort(peaks, decreasing = TRUE), -1),1),
                    head(sort(peaks, decreasing = TRUE),1))
    peaks <- match(peaks, q2, nomatch = 1)
    peak.t <- max(peaks, which.max(q2[1:lrq]), na.rm = TRUE)
    mult_coef <- max(2, length(q)/22)
    peak.t <- ifelse(max(q2[1:lrq], na.rm = TRUE) > (q2[peak.t] * mult_coef),
                     which.max(q2[1:lrq]), peak.t)
  } else {
    peak.t <- which.max(q2[1:lrq])
  }
  
  return(peak.t)
}

# Compute volatility ranking
#vol.score <- function(q) {
#  if(length(q) > 1) {
#    lm_decl <- lm(rank(-q, ties.method = "max") 
#                  ~ seq_along(q))
#    vol <- mean(abs(lm_decl$residuals), na.rm = TRUE)
#    return(vol)
#  } else {
#    return(NA)
#  }
#}

vol.score <- function(q, max_index = length(prod)) {
  prod_len <- length(q)
  if (max_index == prod_len & prod_len >= 48) {
    avg_prod <- mean(q)
    med_point <- round(length(q)/2,0)
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
#calc_buildup <- function(actual, predicted, buildup_time) {
#  buildup_rate <- actual[1]
#  peak_rate <- max(c(0, predicted), na.rm = TRUE)
#  
#  buildup <- (peak_rate - buildup_rate) / length(buildup_time)
#  buildup_prod <- buildup * (buildup_time - 1) + buildup_rate
#  
#  predicted[buildup_time] <- buildup_prod
#  predicted[1] <- round(buildup_rate, -1)
#  return(predicted)
#}

calc_buildup <- function(actual, predicted, buildup_time) {
  predicted[buildup_time] <- actual[buildup_time]
  return(predicted)
}

# Compute error rates
calc_error <- function(actual, predicted, included, days, absolute = FALSE, percentage = FALSE) {
  if (absolute) {
    error <- sum(abs((actual[included] * days[included]) - (predicted[included] * days[included])), na.rm = TRUE) 
  } else {
    vols_actual <- sum(actual[included] * days[included], na.rm = TRUE)
    vols_pred <- sum(predicted[included] * days[included], na.rm = TRUE)
    error <- vols_actual - vols_pred
  }
  if (percentage) {
    if (error == 0) {
      error <- 0 
    } else {
      error <- error / (sum(actual[included] * days[included], na.rm = TRUE))
    }
  }
  return(error)
}

# Calculate NGL yield based on predicted gas
calc_ngl <- function(gas, initial_ratio = tcInitialNgl, final_ratio = tcFinalNgl) {
  if (tcNglForecast == FALSE) {
    ngl_daily <- 0
  } else {
    gas_cum <- cumsum.na(gas)
    ratio <-  gas_cum / max.na(gas_cum)
    ngl_yield <- ratio * (final_ratio - initial_ratio) + initial_ratio
    ngl_daily <- ngl_yield * (gas/1000)
  }
  return(ngl_daily)
}

# Calculate equivalent production
calc_equiv <- function(oil, gas, ngl, conv_factor = 6) {
  equiv = replace.na(oil) + replace.na(ngl) + (replace.na(gas)/conv_factor)
  equiv[is.na(oil) & is.na(gas)] <- NA
  return(equiv)
}

# Calculate EUR rem and cum values
calc_eurs <- function(actual_monthly, predicted_monthly) {
  cum_val <- max(cumsum.na(actual_monthly),0, na.rm = TRUE)
  eur_val <- sum(max(cumsum.na(predicted_monthly[is.na(actual_monthly)]),0, na.rm = TRUE),
                 cum_val, na.rm = TRUE)
  if (length(actual_monthly[actual_monthly == 0]) == length(actual_monthly) & sum(predicted_monthly > 0, na.rm = TRUE)) {
    eur_val <- sum(predicted_monthly, na.rm = TRUE)
  }
  rem_val <- eur_val - cum_val
  return(list(cum_val, eur_val, rem_val))
}

# Add summary columns to output dataframe
calc_summary_col <- function(col) {
  output <- ifelse(is.null(col[1]),NA, col[1])
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
    df[,c(deparse(substitute(column)))] <- column
  } else {
    df[,c(deparse(substitute(column)))] <- na_type
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
    sortvec <- rep(c(seq(1, n_cores), seq(n_cores, 1)), length = length(x))
    sortvec <- order(sortvec)
    x <- x[c(sortvec)]
    return(x)
  } else {
    # ensure column argument is in the data frame
    stop("Column not in data frame")
  }
}

# Aggregate data frame into type curve
agg_type_curve <- function(df, prob) {
  oil = replace.na(df$oil)
  gas = replace.na(df$gas)
  water = replace.na(df$water)
  producing_month = df$producing_month
  
  active_prob = switch(as.character(prob), P50 = 0.5, P10 = 0.1, P25 = 0.25, P75 = 0.75, P90 = 0.9, Avg = 'Avg')
  if (active_prob != 'Avg') {
    df_out <- aggregate(cbind(oil, gas, water) ~ producing_month, 
                        data = cbind(oil,gas,water,producing_month), 
                        FUN = quantile, probs = active_prob)  
  } else {
    df_out <- aggregate(cbind(oil, gas, water) ~ producing_month, 
                        data = cbind(oil,gas,water,producing_month), 
                        FUN = function(x) mean(x, na.rm = TRUE))  
  }
  
  min_date <- min(df$date, na.rm = TRUE)
  df_out$well_count <- aggregate(df$well_count ~ df$producing_month, data = df, FUN = sum)[,2]
  df_out$date <- sapply(df_out$producing_month, add.months, date = min_date)
  df_out$date <- as.POSIXct(as.Date(df_out$date, origin = "1970-01-01")+ 0.33) 
  df_out$days_on <- MONTH.DAYS
  df_out$api <- c(paste(unique(df$api), collapse = " | "), rep(NA, nrow(df_out)-1))
  df_out$interval <- paste(toupper(prob), head(df$interval,1))
  df_out$well_name <- head(df$interval,1)
  df_out$operator <- c("TYPE CURVE")
  df_out$lateral_length_ft <- mean(df$lateral_length_ft, na.rm = TRUE)
  df_out$true_vertical_depth_ft <- mean(df$true_vertical_depth_ft, na.rm = TRUE)
  df_out$latitude <- c(NA_real_)
  df_out$longitude <- c(NA_real_)
  df_out$latitude_bh <- c(NA_real_)
  df_out$longitude_bh <- c(NA_real_)
  df_out$frac_proppant_lbs <- mean(df$frac_proppant_lbs, na.rm = TRUE)
  df_out$frac_fluid_bbl <- mean(df$frac_fluid_bbl, na.rm = TRUE)
  df_out$working_interest <- c(NA_real_)
  df_out$net_revenue_interest <- c(NA_real_)
  df_out$play <- paste(unique(df$play), collapse = " | ")
  df_out$county <- paste(unique(df$county), collapse = " | ")
  df_out$ip_year <- c(NA_integer_)
  
  return(df_out)
}

# Aggregate data frame into type curve
agg_batch_curve <- function(df, prob) {
  oil = replace.na(df$oil_pred_monthly / df$days_on)
  gas = replace.na(df$gas_pred_monthly / df$days_on)
  water = replace.na(df$water_pred_monthly / df$days_on)
  producing_month = df$index
  
  active_prob = switch(as.character(prob), P50 = 0.5, P10 = 0.1, P25 = 0.25, P75 = 0.75, P90 = 0.9, Avg = 'Avg')
  if (active_prob != 'Avg') {
    df_out <- aggregate(cbind(oil, gas, water) ~ producing_month, 
                        data = cbind(oil,gas,water,producing_month), 
                        FUN = quantile, probs = active_prob)  
  } else {
    df_out <- aggregate(cbind(oil, gas, water) ~ producing_month, 
                        data = cbind(oil,gas,water,producing_month), 
                        FUN = mean)  
  }
  
  min_date <- min(df$date, na.rm = TRUE)
  df_out$well_count <- aggregate(df$well_count ~ df$index, data = df, FUN = sum)[,2]
  df_out$date <- sapply(df_out$producing_month, add.months, date = min_date)
  df_out$date <- as.POSIXct(as.Date(df_out$date, origin = "1970-01-01")+ 0.33) 
  df_out$days_on <- MONTH.DAYS
  df_out$api <- c(paste(unique(df$api), collapse = " | "), rep(NA, nrow(df_out)-1))
  df_out$interval <- paste(toupper(prob), head(df$interval,1))
  df_out$well_name <- head(df$interval,1)
  df_out$operator <- c("TYPE CURVE")
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

run.declines.safe <- function(df) {
  return(tryCatch(run.declines(df), 
                  error = function(e) list(prod = NA, summary = e$message)))
}
