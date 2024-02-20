library(tidyverse)
library(quanteda)
library(text)
library(spacyr)
library(lsa)
library(lme4)
source('1_funcs.R')

# read in event data
# need to figure out some of the remaining low n facilities

nrc_df <- read.csv('/Users/michaelrosen/Documents/data_anlaysis/eventReportingEWS/event_reports_nrc.csv') |>
  mutate(
    event_date = case_match(event_date,
                            '11/20/2103' ~ '11/20/2013',
                            .default = event_date),
    event_date2 = lubridate::mdy(event_date),
    notification_date2 = lubridate::mdy(notification_date),
    event_text = tolower(event_text),
    facility = case_match(tolower(facility),
                          'columbia generating statiregion:' ~ 'columbia generating station',
                          'davis-besse'~ 'davis besse',
                          'washington nuclear (wnp-2region:' ~ 'washington nuclear',
                          'vogtle 1/2' ~ 'vogtle',
                          'vogtle 3/4' ~ 'vogtle',
                          'fort calhoun' ~ 'ft calhoun',
                          'summer construction' ~ 'summer',
                          .default = tolower(facility))  
  ) |>
  select(-X)
skimr::skim(nrc_df)

table(nrc_df$facility)

nrc_df |>
  mutate(year = lubridate::year(event_date2)) |> 
  filter(between(year,1999,2023)) |>
  group_by(year) |>
  summarize(n = n()) |>
  ggplot(aes(x = year, y = n)) + geom_bar(stat = 'identity')

# t <- getDictionaryScores(
#   df = nrc_df, 
#   text_col = 'event_text', 
#   dict_file_loc = here::here(config$dict_file_path,config$agen_com_dict)
embeddings <- text::textEmbed(
  texts = nrc_df$event_text,
  model = "bert-base-uncased",
  layers = -2,
  aggregation_from_tokens_to_texts = "mean",
  aggregation_from_tokens_to_word_types = "mean",
  keep_token_embeddings = FALSE)
