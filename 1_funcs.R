library(tidyverse)
library(quanteda)
library(spacyr)
source('2_dict_var_defs.R')
################################################################
###################
################### General functions
################### 
###################
################################################################

getDictionaryScores <- function(df, text_col, dict_file_loc) {
  # This should apply any correctly formatted closed-voc data set to a df,
  # and return original df with appened dictionary scores
  dic <- quanteda::dictionary(
    file = dict_file_loc,
    format = 'LIWC')
  # print(df[text_col])
  corp <- quanteda::corpus(df[,text_col])
  scored_df <- quanteda::tokens(corp) |>
    tokens_lookup(dictionary = dic) |>
    quanteda::dfm() 
  df <- df |>
    bind_cols(
      quanteda::convert(scored_df,to = 'data.frame')
    )
  return(df)
}

getMultipleDictionaryScores <- function(df, text_col, dict_file_path, dict_list) {
  # This should apply any correctly formatted closed-voc data set to a df,
  # and return original df with appened dictionary scores
  df_list <- list()
  df_list[['orig_df']] <- df
  corp <- quanteda::corpus(df, text_field = text_col)
  for (dict in names(dict_list)) {
    print(dict)
    dict_file <- dict_list[[dict]]
    if (endsWith(dict_file,'.dic')) {frmt <- 'LIWC'} else {frmt <- 'yoshikoder'}
    print(glue::glue("got format: {frmt}"))
    print(here::here(dict_file_path,dict_file))
    dic <- quanteda::dictionary(
      file = here::here(dict_file_path,dict_file),
      format = frmt)
    print('got dictionariy...')
    scored_df <- quanteda::tokens(corp) |>
      tokens_lookup(dictionary = dic) |>
      quanteda::dfm()
    print('scored text...')
    df_list[[dict]] <- quanteda::convert(scored_df,to = 'data.frame') |> 
      select(-doc_id) |> 
      rename_with( ~ paste0(dict,"_", .x))
  }
  df <- bind_cols(df_list)
  return(df)
}

getPassiveVoice <- function(df,text_col,ratio) {
  # largely taken from here: https://osf.io/nwsx3/
  d <- spacyr::spacy_parse(
    quanteda::corpus(df, text_field = text_col), 
    dependency = TRUE)
  aux_pass <- d |>
    mutate(doc_id = as.numeric(str_remove_all(doc_id, "text"))) |>
    group_by(doc_id) |>
    summarise(aux_pass = sum(dep_rel=="auxpass"))
  if (ratio){
    return(aux_pass$aux_pass / unname(sapply(df[,text_col], ngram::wordcount)))
    }
  else{
    return(aux_pass$aux_pass)}
}

################################################################
###################
################### Reading and cleaning data
################### 
###################
################################################################

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

get_cmbd_ASRS <- function(f,add_liwc,add_emo_voc,add_butter) {
  df <- readxl::read_excel(f)
  if(!is.na(add_liwc)) {
    df <- df |> bind_cols(
      read.csv(add_liwc)  |> select(all_of(liwc_vars)) |>
        rename_with(~ paste0("liwc_", .x))
    )
  }
  if(!is.na(add_emo_voc)) {
    df <- df |> bind_cols(
      read.csv(add_emo_voc)|> select(all_of(emo_voc_vars)) |>
        rename_with(~ paste0("emo_voc_", .x))
    )
  }
  if(!is.na(add_butter)) {
    df <- df |> bind_cols(
      (read.csv(add_butter) |> select(-TextID,-Segment,-SegmentID,-TokenCount))
    )
  }
  return(df)
}

get_and_clean_NRC_events <- function(f,add_liwc,add_emo_voc,add_butter) {
  df <- read.csv(f) |>
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
  if(!is.na(add_liwc)) {
    df <- df |> bind_cols(
      read.csv(add_liwc)  |> select(all_of(liwc_vars)) |>
        rename_with(~ paste0("liwc_", .x))
    )
  }
  if(!is.na(add_emo_voc)) {
    df <- df |> bind_cols(
      read.csv(add_emo_voc)|> select(all_of(emo_voc_vars)) |>
        rename_with(~ paste0("emo_voc_", .x))
    )
  }
  if(!is.na(add_butter)) {
    df <- df |> bind_cols(
      (read.csv(add_butter) |> select(-TextID,-Segment,-SegmentID,-TokenCount))
                          )
  }
  return(df)
}

get_and_clean_rail_events <- function(f,add_liwc,add_emo_voc,add_butter) {
  df <- read.csv(f)
  if(!is.na(add_liwc)) {
    df <- df |> bind_cols(
      read.csv(add_liwc)  |> select(all_of(liwc_vars)) |>
        rename_with(~ paste0("liwc_", .x))
    )
  }
  if(!is.na(add_emo_voc)) {
    df <- df |> bind_cols(
      read.csv(add_emo_voc)|> select(all_of(emo_voc_vars)) |>
        rename_with(~ paste0("emo_voc_", .x))
    )
  }
  if(!is.na(add_butter)) {
    df <- df |> bind_cols(
      (read.csv(add_butter) |> select(-TextID,-Segment,-SegmentID,-TokenCount))
    )
  }
  return(df)
}

get_cmbd_phmsa <- function(f,add_liwc,add_emo_voc,add_butter) {
  df <- read.csv(f)
  if(!is.na(add_liwc)) {
    df_liwc <- read.csv(add_liwc) |>
      select(all_of(c('report_no',liwc_vars))) |>
      rename_with(.cols = all_of(liwc_vars), .fn = ~ paste0("liwc_", .x))
    df <- df |> left_join(
      df_liwc, by = 'report_no')# |> 
      # select(all_of(liwc_vars)) |>
      # rename_with(~ paste0("liwc_", .x))
  }
  if(!is.na(add_emo_voc)) {
    df <- df |> bind_cols(
      read.csv(add_emo_voc)|> select(all_of(emo_voc_vars)) |>
        rename_with(~ paste0("emo_voc_", .x))
    )
  }
  if(!is.na(add_butter)) {
    df <- df |> bind_cols(
      (read.csv(add_butter) |> select(-TextID,-Segment,-SegmentID,-TokenCount))
    )
  }
  return(df)
}
