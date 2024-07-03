### calculate embeddings


library(tidyverse)
library(quanteda)
library(text)
library(spacyr)
library(lsa)

con <- DBI::dbConnect(RPostgres::Postgres(),
  user = config$dbUser,
  password = config$dbPW,
  host = config$db_host,
  dbname = config$db_name,
  port = config$dbPort
)

# Install text required python packages in a conda environment (with defaults).
# text::textrpp_install()

# Initialize the installed conda environment.
text::textrpp_initialize(save_profile = TRUE)

# create_embeddings_table(con = con)

# asrs_df <- get_eids(con = con, df = asrs_df |> mutate(sys_source = 'asrs'), sys_source = 'asrs')
rail_df2 <- get_eids(con = con, df = rail_df |> mutate(sys_source = 'rail'), sys_source = 'rail')
psn_df2 <- psn_df |> select(eid,event_text) |> drop_na()


embed_and_save(
  con = con,
  df = psn_df2 #[1,]  # nrc_df, asrs, rail, and phmsa already done
)

# embeddings <- text::textEmbed(
#   texts = asrs_df[1,'event_text'],
#   model = "bert-base-uncased",
#   layers = -2,
#   aggregation_from_tokens_to_texts = "mean",
#   aggregation_from_tokens_to_word_types = "mean",
#   keep_token_embeddings = FALSE)