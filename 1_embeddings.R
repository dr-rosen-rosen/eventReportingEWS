### try to calculate embeddings


library(tidyverse)
library(quanteda)
library(text)
library(spacyr)
library(lsa)


# Install text required python packages in a conda environment (with defaults).
# text::textrpp_install()

# Initialize the installed conda environment.
# save_profile = TRUE saves the settings so that you don't have to run textrpp_initialize() after restarting R. 
text::textrpp_initialize(save_profile = TRUE)
pgvector.serialize <- function(v) {
  stopifnot(is.numeric(v))
  paste0("[", paste(v, collapse=","), "]")
}

# NRC
embeddings <- text::textEmbed(
  # x <- text::textEmbed(
  # texts = test_df$text,
  texts = nrc_events$event_text,
  # texts = 'Hi there',
  model = "bert-base-uncased",
  layers = -2,
  aggregation_from_tokens_to_texts = "mean",
  aggregation_from_tokens_to_word_types = "mean",
  keep_token_embeddings = FALSE)

embed_to_insert <- data.frame(
  event_num = nrc_events$event_num,
  embedding = apply(embeddings$texts$texts,1,pgvector.serialize)
)

dbExecute(conn = con, "CREATE EXTENSION IF NOT EXISTS vector")
dbExecute(conn = con, "CREATE TABLE embeddings (event_num integer PRIMARY KEY, embedding vector(768))")
dbAppendTable(conn = con, "embeddings", embed_to_insert)

# Rail
Sys.time()
embeddings <- text::textEmbed(
  texts = rail_df$Narrative,
  model = "bert-base-uncased",
  layers = -2,
  aggregation_from_tokens_to_texts = "mean",
  aggregation_from_tokens_to_word_types = "mean",
  keep_token_embeddings = FALSE)
Sys.time()

embed_to_insert <- data.frame(
  data = rail_df$dataSet_num,
  embedding = apply(embeddings$texts$texts,1,pgvector.serialize)
)
# set connection
dbExecute(conn = con, "CREATE EXTENSION IF NOT EXISTS vector")
dbExecute(conn = con, "CREATE TABLE embeddings (dataSet_num text PRIMARY KEY, embedding vector(768))")
dbAppendTable(conn = con, "embeddings", embed_to_insert)

# Draft for adding columns to existing tables

insert_new_column <- function(con, table, column_name, column_type) {
  q <- DBI::sqlInterpolate(conn = con, "ALTER TABLE ?table ADD COLUMN IF NOT EXISTS ?column_name ?column_type;", table=table, column_name=column_name, column_type=column_type)
  DBI::dbExcecute(conn = con, q)
}

for (c in columns(liwc_df)) {
  insert_new_column(
    con = con,
    table = 'liwc',
    column_name = c,
    column_type = if_else(c == 'WC', 'integer', 'double precision')
  )
}

for (c in columns(emo_voc_df)) {
  insert_new_column(
    con = con,
    table = 'emo_voc',
    column_name = c,
    column_type = if_else(str_detect(c,"Count|Unique', 'integer', 'double precision')
  )
}
