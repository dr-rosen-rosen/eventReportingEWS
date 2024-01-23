library(tidyverse)
library(quanteda)
library(spacyr)

################################################################
###################
################### General functions
################### 
###################
################################################################
################################################################

getDictionaryScores <- function(df, text_col, dict_file_loc) {
  # This should apply any correctly formatted closed-voc data set to a df,
  # and return original df with appened dictionary scores
  dic <- quanteda::dictionary(
    file = dict_file_loc,
    format = 'LIWC')
  print(df[text_col])
  corp <- quanteda::corpus(df[text_col])
  scored_df <- quanteda::tokens(corp) |>
    tokens_lookup(dictionary = dic) |>
    quanteda::dfm() 
  df <- df |>
    bind_cols(
      quanteda::convert(scored_df,to = 'data.frame')
    )
  return(df)
}

getPassiveVoice <- function(df,text_col,ratio) {
  # largely taken from here: https://osf.io/nwsx3/
  d <- spacyr::spacy_parse(df[,text_col], dependency = T)
  aux_pass <- d |>
    mutate(doc_id = as.numeric(str_remove_all(doc_id, "text"))) |>
    group_by(doc_id) |>
    summarise(aux_pass = sum(dep_rel=="auxpass"))
  if (ratio){
    total_words <- df |> 
      select(!!sym(text_col)) |>
      as_tibble() |>
      rowwise() |>
      mutate(wc = ngram::wordcount(value))
    
    return(aux_pass$aux_pass/total_words$wc)}
  else{
    return(aux_pass$aux_pass)}
}