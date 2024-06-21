################################################################
################################################################
###################
################### Scripts for applying EWS metrics to event 
################### reporting data.
###################
################################################################
################################################################

rm(list = ls())
beepr::beep()

library(tidyverse)
library(quanteda)
# library(ggplot2)
library(EWSmethods)
library(here)
Sys.setenv(R_CONFIG_ACTIVE = 'calculon')
config <- config::get()
source('1_funcs.R')


# Build all event measure dataframes
load_from_preProcessed_files <- TRUE # if false takes a long time to re-run PV
source('1_get_and_clean_and_code_all_events.R')
beepr::beep()


##################################################
##############
############## OLD CODE
##############
##################################################




######################################
########### PSN data
######################################

psn_bert_vader_df <- read.csv('/Volumes/LaCie/event_ews/all_psn_metrics_12-28-2023.csv')

psn_bert_vader_df <- psn_bert_vader_df |>
  drop_na() |>
  mutate(t = row_number()) |> relocate(t) |> relocate(date, .after = last_col())

  # filter(date < lubridate::ymd('2023-01-01'))

key_date_to_t <- psn_bert_vader_df |>
  select(t,date) %>%
  distinct() %>%
  mutate(date = as.Date(date))

skimr::skim(psn_bert_vader_df)

psn_multi_ews_bert_vader <- multiEWS(data = psn_bert_vader_df[,1:6],
                                 metrics = c("meanAR","maxAR","meanSD","maxSD","eigenMAF","mafAR","mafSD","pcaAR","pcaSD","eigenCOV","maxCOV","mutINFO"),
                                 method = "expanding",
                                 burn_in = 50,
                                 threshold = 2)

plot(psn_multi_ews_bert_vader,  y_lab = "Density")

view(psn_multi_ews_bert_vader$EWS$raw)
skimr::skim(psn_multi_ews_bert_vader$EWS$raw)
table(psn_multi_ews_bert_vader$EWS$raw$threshold.crossed)

psn_test2 <- psn_multi_ews_bert_vader$EWS$raw |>
  # group_by(metric.code) |>
  # summarize(t_crossings = sum(threshold.crossed))
  group_by(time) %>% summarise(threshold.crossed.count = sum(threshold.crossed))
skimr::skim(psn_test2)
hist(psn_test2$threshold.crossed.count)

metrics <- c('lag_sim','mean_sim','std_sim','vader_comp_mean','vader_comp_std')
metrics_dfs <- list()
# metrics_dfs[['time']] <- bert_vader_df$t

for (metric in metrics) {
  print(metric)
  EWS_model <- EWSmethods::uniEWS(data = psn_bert_vader_df |> select(t,!!sym(metric)),
                                  metrics = c("ar1","SD","skew"),
                                  method = "expanding",
                                  burn_in = 50,
                                  threshold = 2)
  threshold_hits <- EWS_model$EWS |>
    group_by(time) |> summarise('{metric}.threshold.crossed.count' := sum(threshold.crossed))
  print(nrow(threshold_hits))
  metrics_dfs[[metric]] <- threshold_hits
}

cmbd_ind_vars <- bind_cols(metrics_dfs) |>
  rename(time = time...1) |>
  select(-contains('...'))

skimr::skim(cmbd_ind_vars)

######################################
########### PSN to outcomes... occurrence of high harm events
######################################

names(psn)
hist(psn$HARM_SCORE)

psn_harm_by_day <- psn |>
  group_by(FLR_SUBMIT_DATE) |>
  summarise(
    mean_hs = mean(HARM_SCORE),
    max_hs = max(HARM_SCORE)
  ) |>
  ungroup() |>
  rename(date = FLR_SUBMIT_DATE) |>
  full_join(key_date_to_t, by = 'date') |>
  rename(time = t) |>
  full_join(cmbd_ind_vars, by = 'time') |>
  # full_join(psn_test2, by = 'time') |>
  drop_na()

skimr::skim(psn_harm_by_week)

forecast::auto.arima(psn_harm_by_day$max_hs)
psn_base_arima <- arima(psn_harm_by_day$mean_hs,order = c(0,0,0))
psn_test_arima <- arima(psn_harm_by_day$mean_hs,order = c(0,0,0), xreg = psn_harm_by_day |> 
                          # select(ends_with('.count'))
                          select(lag_sim.threshold.crossed.count,vader_comp_mean.threshold.crossed.count)
                        )
lmtest::coeftest(psn_test_arima)

psn_test_arimaMulti <- arima(psn_harm_by_day$mean_hs,order = c(0,0,0), xreg = psn_harm_by_day |> select(ends_with('.count')))
lmtest::coeftest(psn_test_arimaMulti)
