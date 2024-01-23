################################################################
################################################################
###################
################### Scripts for applying EWS metrics to event 
################### reporting data.
###################
################################################################
################################################################

library(tidyverse)
library(quanteda)
library(ggplot2)
library(EWSmethods)

get_and_clean_ASRS <- function(asrs.files) {
  dfs <- NULL
  for (f_path in asrs.files) {
    l1 <- readxl::read_xlsx(asrs.files[1],
                            n_max = 0,
                            .name_repair = 'minimal') |> names()
    l2 <- readxl::read_xlsx(asrs.files[1],
                            skip = 1,
                            n_max = 0,
                            .name_repair = 'minimal') |> names()
    if (length(l1) == length(l2)) {
      header <- mapply(FUN = function(x,y) {
        paste0(x,y,sep = '.')}, x = l1,y = l2
      )
    }
    df <- readxl::read_xlsx(f_path,
                            skip = 3,
                            col_names = header,
                            range = readxl::cell_cols("A:DU")) |>
      janitor::clean_names()
    if (is.null(dfs)) {
      dfs <- df
    } else {dfs <- bind_rows(dfs,df)}
  }
  dfs <- dfs |>
    filter(!is.na(acn), acn != 'ACN') |>
    mutate(
      date = as.Date(paste0(substr(time_date,start = 1, stop = 4),'-',substr(time_date,start = 5, stop = 6),'-01'),format = "%Y-%m-%d")
    ) |>
    unite('cmbd_narrative',starts_with('report_'),sep = " ", na.rm = TRUE,remove = FALSE) |>
    mutate(tot_wc = stringr::str_count(cmbd_narrative, '\\w+'))
  return(dfs)
}

df <- get_and_clean_ASRS(
  asrs.files = list.files(path = '/Volumes/LaCie/ASRS_data',
                          pattern = "*.xlsx$",
                          full.names = TRUE,
                          recursive = FALSE)
) 
df |> write.csv('/Volumes/LaCie/event_ews/cmb_asrs.csv')
hist(df$tot_wc)
df2 <- df |>
  filter(aircraft_1aircraft_operator == 'Air Carrier', person_1reporter_organization == 'Air Carrier') |>
  filter(aircraft_1mission == 'Passenger') |>
  group_by(date) |>
  summarize(
    n_reports = n(),
    #cmbd_mnthly_narrative = paste0(cmbd_narrative, collapse = " ")
    cmbd_mnthly_narrative = paste0(report_1narrative, collapse = " ")
  )

# df |> writexl::write_xlsx(path = 'cmbd_ASRS.xlsx')


asrs_corpus <- quanteda::corpus(df2$cmbd_mnthly_narrative)
docvars(asrs_corpus,'date') <- df2$date 

token_info <- summary(asrs_corpus, n = nrow(df2))
ggplot(token_info, aes(x = date, y = Tokens)) + geom_line()

asrs_dfm <- dfm(asrs_corpus, 
                remove = stopwords("english"), stem = TRUE, remove_punct = TRUE)
asrs_sim <- quanteda.textstats::textstat_simil(asrs_dfm,method = 'cosine',margin = 'documents')
m <- as.matrix(asrs_sim)

test <- lapply(seq(2,nrow(m),1), function(x) {
  m[[x,x-1]]
})
x <- data.frame(
  date = df2[2:nrow(df2),'date'],
  t = seq(1,nrow(df2)-1,1),
  similarity = unlist(test))
y <- x |> filter(date < lubridate::ymd('2023-01-01'))
#y <- x
ggplot(y, aes(x = date, y = similarity)) + geom_line()

z <- EWSmethods::uniEWS(data = y[,2:3],
       metrics = c("ar1","SD","skew"),
       method = "expanding",
       burn_in = 50,
       threshold = 2)
plot(z,  y_lab = "Density")

view(z$EWS)
table(z$EWS$metric.code)

test <- z$EWS |>
#   filter(metric.code == 'ar1 + SD + skew')
table(z$EWS$threshold.crossed)

test <- z$EWS |>
  # group_by(metric.code) |>
  # summarize(t_crossings = sum(threshold.crossed))
  filter(metric.code == 'SD') |> # ar1 + SD + skew
  select(time,threshold.crossed)  %>%
  mutate(t = seq(51,420,1))

length(seq(51,420,1))
# EWSmethods::ewsnet_init(envname = "EWSNET_env", pip_ignore_installed = FALSE, auto = FALSE)

######################################
########### LIWC measures for asrs
######################################

asrs_liwc_df <- read.csv('/Volumes/LaCie/event_ews/LIWC-22 Results - cmb_asrs - LIWC Analysis.csv')
asrs_liwc_df <- asrs_liwc_df |>
  select(date,acn,
         focuspast,focuspresent,focusfuture,
         fatigue,
         prosocial,conflict,
         cogproc,insight,cause,certitude,
         power,affiliation
         ) |>
  group_by(date) |>
  summarise(across(everything(), .f = list(mean = mean, std = sd), na.rm = TRUE))

######################################
########### Agency / communality measures for asrs
######################################

ASRS_corpus <- quanteda::corpus(
  df$report_1narrative
)

ASRS_agenCom <- quanteda::tokens(ASRS_corpus) |>
  tokens_lookup(dictionary = agen_com_dic) |>
  quanteda::dfm() 

ASRS_agenCom_by_date <- df |>
  bind_cols(
    quanteda::convert(ASRS_agenCom,to = 'data.frame')
  ) |>
  select(date, agency, communion) |>
  group_by(date) |>
  summarise(across(everything(), .f = list(mean = mean, std = sd), na.rm = TRUE))

d <- spacyr::spacy_parse(df$report_1narrative, dependency = T)
ASRS_aux_pass <- d |>
  mutate(doc_id = as.numeric(str_remove_all(doc_id, "text"))) |>
  group_by(doc_id) |>
  summarise(aux_pass = sum(dep_rel=="auxpass"))
ASRS_AuxPass_by_date <- df |>
  bind_cols(
    ASRS_aux_pass
  ) |>
  select(date,aux_pass) |>
  group_by(date) |>
  summarize(across(everything(), .f = list(mean = mean, std = sd), na.rm = TRUE))

ASRS_all_agentic_lang <- ASRS_agenCom_by_date |>
  full_join(ASRS_AuxPass_by_date, by = 'date')

######################################
########### MULTI-EWS based on bert and vader data
######################################

bert_vader_df <- read_csv('/Volumes/LaCie/event_ews/all_asrs_metrics_12-28-2023.csv') |>
  left_join(ASRS_all_agentic_lang, by = 'date')

bert_vader_df <- bert_vader_df |>
  mutate(t = row_number()) |> relocate(t) |> relocate(date, .after = last_col()) |>
  filter(date < lubridate::ymd('2023-01-01'))

multi_ews_bert_vader <- multiEWS(
  data = bert_vader_df[,1:6],
  # data = bert_vader_df[,1:12],
                         metrics = c("meanAR","maxAR","meanSD","maxSD","eigenMAF","mafAR","mafSD","pcaAR","pcaSD","eigenCOV","maxCOV","mutINFO"),
                         method = "expanding",
                         burn_in = 50,
                         threshold = 2)

plot(multi_ews_bert_vader,  y_lab = "Density")

view(multi_ews_bert_vader$EWS$raw)

test2 <- multi_ews_bert_vader$EWS$raw |>
  # group_by(metric.code) |>
  # summarize(t_crossings = sum(threshold.crossed))
  group_by(time) %>% summarise(threshold.crossed.count = sum(threshold.crossed))


metrics <- c('lag_sim','mean_sim','std_sim','vader_comp_mean','vader_comp_std',
             'agency_mean', 'agency_std', 'communion_mean', 'communion_std','aux_pass_mean', 'aux_pass_std')
metrics_dfs <- list()
# metrics_dfs[['time']] <- bert_vader_df$t

for (metric in metrics) {
  print(metric)
  EWS_model <- EWSmethods::uniEWS(data = bert_vader_df |> select(t,!!sym(metric)),
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

z2 <- EWSmethods::uniEWS(data = bert_vader_df |> select(t,vader_comp_std),
                        metrics = c("ar1","SD","skew"),
                        method = "expanding",
                        burn_in = 50,
                        threshold = 2)
plot(z2,  y_lab = "Density")



######################################
########### NTSB data
######################################

ntsb_acc <- c('/Volumes/LaCie/event_ews/ntsb_av_accident_data/events_pre2008.xlsx',
              '/Volumes/LaCie/event_ews/ntsb_av_accident_data/events.xlsx') |>
  purrr::map_dfr(readxl::read_xlsx)
ntsb_acc <- ntsb_acc |>
  filter(ev_country == 'USA', ev_type == "ACC", 
         ev_date >= lubridate::ymd('1988-01-01'),
         ev_date < lubridate::ymd('2023-01-01'))

acc_by_month <- ntsb_acc %>%
  # mutate(year = lubridate::year(ev_date)) |>
  # group_by(year) |>
  mutate(month = lubridate::floor_date(ev_date, "month")) |>
  group_by(month) |>
  summarize(
    num_accidents = n()
  ) %>%
  # mutate(t = row_number()) %>%
  # left_join(test, by = 't')
  mutate(time = row_number()) |>
  # left_join(test2, by = 'time')
  left_join(cmbd_ind_vars, by = 'time')

acc_by_month |>
  # group_by(threshold.crossed) %>%
  group_by(threshold.crossed.count) |>
  summarize(sum_accidents = sum(num_accidents),
            rate = sum_accidents / n())

acc_by_month |>
  #ggplot(aes(x=month, y = num_accidents, fill = as.factor(threshold.crossed))) + geom_bar(stat = "identity")
  ggplot(aes(x=month, y = num_accidents, fill = as.factor(threshold.crossed.count))) + geom_bar(stat = "identity") +
  ggthemes::theme_tufte() #+ scale_fill_brewer(palette = 'YlOrRd')#scale_fill_gradient(low = 'black', high = 'red')

names(ntsb_acc)
table(ntsb_acc$ev_type)


psn <- read_csv('/Volumes/LaCie/event_ews/psn_vader_bert.csv') 

yy <- psn %>%
  filter(PRIMARY_LOC_NAME == 'Pediatrics: Bloom 4S PICU') |>
  group_by(FLR_SUBMIT_DATE) %>%
  summarize(compound = mean(compound)) %>%
  ungroup() %>%
  mutate(t = row_number()) %>%
  relocate(t) |>
  select(-FLR_SUBMIT_DATE)

zz <- EWSmethods::uniEWS(data = yy,
                        metrics = c("ar1","SD","skew"),
                        method = "expanding",
                        burn_in = 50,
                        threshold = 2)
plot(zz,  y_lab = "Density")

######################################
########### attempt at accident prediction
######################################

acc_by_month2 <- acc_by_month |> mutate(
  threshold.crossed.count.last.month = lag(threshold.crossed.count),
  threshold.crossed.count.next.month = lead(threshold.crossed.count),
) |> drop_na()

forecast::auto.arima(acc_by_month2$num_accidents)
base_arima <- arima(acc_by_month2$num_accidents,order = c(2,1,2))
test_arima <- arima(acc_by_month2$num_accidents,order = c(2,1,2), xreg = acc_by_month2$threshold.crossed.count)
# test_arima2 <- arima(acc_by_month2$num_accidents,order = c(2,1,2), xreg = acc_by_month2$threshold.crossed.count.next.month)
# test_arima3 <- arima(acc_by_month2$num_accidents,order = c(2,1,2), xreg = acc_by_month2$threshold.crossed.count.last.month)
# test_pred <- forecast::forecast.Arima(test_arima, h = 12)
# anova(base_arima,test_arima)
lmtest::coeftest(test_arima)
hist(acc_by_month2$num_accidents)

acc_by_month3 <- acc_by_month |> drop_na()
forecast::auto.arima(acc_by_month3$num_accidents)
base_arima2 <- arima(acc_by_month3$num_accidents,order = c(2,1,2))
test_arima2 <- arima(acc_by_month3$num_accidents,order = c(2,1,2), xreg = acc_by_month3 |>
                       # select(ends_with('.count'))
                     select(contains('sim'))
                       # select(starts_with('vader_'))
                     # select(starts_with(c('agency','communion','aux')))
                       # select(lag_sim.threshold.crossed.count,vader_comp_mean.threshold.crossed.count)
                     )
lmtest::coeftest(test_arima2)


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
