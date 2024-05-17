library(tidyverse)
library(quanteda)
library(text)
library(spacyr)
library(lsa)

# Pre-processing
# spell check

# Dictionary scores

# Passive voice

# Similarity using bert embeddings

test_df <- data.frame(doc_id = c(1,2),text = c('Hi, this is the first test I will be writing today. Boats and cars','And this is the second one. Kids are still at school, and we have music class tongith. Pork and beans'))

embeddings <- text::textEmbed(
# x <- text::textEmbed(
  texts = test_df$text,
  # texts = 'Hi there',
  model = "bert-base-uncased",
  layers = -2,
  aggregation_from_tokens_to_texts = "mean",
  aggregation_from_tokens_to_word_types = "mean",
  keep_token_embeddings = FALSE)

test_df$embeddings <- embeddings$texts$texts

lsa::cosine(unlist(test_df[1,'embeddings'], use.names = FALSE),unlist(test_df[2,'embeddings'], use.names = FALSE))
