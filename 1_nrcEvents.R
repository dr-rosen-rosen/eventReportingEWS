library(tidyverse)
library(quanteda)
library(text)
library(spacyr)
library(lsa)
library(lme4)
library(here)
source('1_funcs.R')

# read in event data
# need to figure out some of the remaining low n facilities

nrc_df <- get_and_clean_NRC_events(
  f = here(config$nrc_data_path,config$nrc_events_file),
  add_liwc = here::here(config$nrc_data_path,config$nrc_liwc_file),
  add_emo_voc = here::here(config$nrc_data_path,config$nrc_emo_voc_file),
  add_butter = here::here(config$nrc_data_path,config$nrc_butter_file))
skimr::skim(nrc_df)

table(nrc_df$facility)
nrc_df <- nrc_df |> 
  group_by(facility) |> 
  filter(n() >= 100) |> # drop facilities with low n
  ungroup() |>
  filter(between(lubridate::year(event_date2),1999,2023))

# nrc_df |>
#   mutate(year = lubridate::year(event_date2)) |> 
#   filter(between(year,1999,2023)) |>
#   group_by(year) |>
#   summarize(n = n()) |>
#   ggplot(aes(x = year, y = n)) + geom_bar(stat = 'identity')


nrc_dict_df <- getMultipleDictionaryScores(
  df = nrc_df, 
  text_col = 'event_text',
  dict_file_path = config$dict_file_path, 
  dict_list = list(
    # 'ag_co' = config$agen_com_dict,
    'gi' = config$general_inquir#,
    # 'pro_so' = config$prosocial_dict,
    # 'stress' = config$stress_dict,
    # 'tms' = config$tms_streng_dict,
    # 'uncert' = config$uncertainty_dict#,
    # 'per_val' = config$personal_values_dict#,
    # 'wllbng' = config$wwpb_wellbeing
  ))
# write.csv(names(nrc_dict_df),'var_names.csv')
nrc_dict_df |>
  select(starts_with(c('liwc','emo_voc','gi','pro_so','stress','per_val'))) |>
  colMeans(na.rm = TRUE) |> write.csv('dict_colMeans.csv')

nrc_dict_df |>
  dplyr::select(agency,communion) %>%
  # dplyr::select(drives, cognition, affect, social, big_words, pcp_to_pt_wc) %>%
  careless::mahad() #%>% #+ geom_hline(yintercept = 40, color = "red")
  # cbind(df_lpa_md) %>%
  # rename('mahad' = '.') %>%
  # filter(mahad < 40)

# inter-correlations; vars are supposed to be uncorrelated
nrc_dict_df %>%
  dplyr::select(agency,communion) %>%
  modelsummary::datasummary_correlation()


# nrc_pv <- getPassiveVoice(df = nrc_df, text_col = 'event_text',ratio = TRUE)
# 
# embeddings <- text::textEmbed(
#   # texts = nrc_df$event_text,
#   texts = nrc_df |> slice(1:5) |> select(event_text),
#   model = "bert-base-uncased",
#   layers = -2,
#   aggregation_from_tokens_to_texts = "mean",
#   aggregation_from_tokens_to_word_types = "mean",
#   keep_token_embeddings = FALSE)

iccs <- list()
for (cl in colnames(nrc_dict_df[,24:ncol(nrc_dict_df)])) {
  iccs[[cl]] <- ICC::ICCbare(facility,eval(substitute(cl), nrc_dict_df), data = nrc_dict_df)
}
icc_df <- as.data.frame(iccs) |>
  pivot_longer(
    cols = everything(),
    names_to = 'variable',
    values_to = 'nrc_facility_ICC'
  )

