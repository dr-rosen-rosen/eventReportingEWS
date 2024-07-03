library(tidyverse)
library(quanteda)
library(spacyr)
source('1_dict_var_defs.R')


################################################################
###################
################### Reading and cleaning data
################### 
###################
################################################################

get_and_clean_ASRS <- function(asrs.files) {
  # merges and does minimal cleaning for the raw
  # data files downloaded from ASRS (could not
  # download all files at once)
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

get_and_clean_and_code_ASRS <- function(f,add_liwc,add_emo_voc,add_butter,add_multiple_dict_scores,add_pv) {
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
  df <- df |> 
    filter(
      !is.na(cmbd_narrative), 
      cmbd_narrative != '', 
      utf8::utf8_valid(cmbd_narrative))
  if(add_multiple_dict_scores){
    df <- getMultipleDictionaryScores(
      df = df, 
      text_col = 'cmbd_narrative',
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
  }
  if(add_pv){
    df$cmbd_narrative <- as.character(df$cmbd_narrative)
    df$PV_aux_pas <- getPassiveVoice(df = df, text_col = 'cmbd_narrative',ratio = TRUE)
    spacyr::spacy_finalize()
  }
  return(df)
}

get_and_clean_and_code_NRC_events <- function(f,add_liwc,add_emo_voc,add_butter,add_multiple_dict_scores,add_pv) {
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
  df <- df |> filter(!is.na(event_text), event_text != '', utf8::utf8_valid(event_text))
  if(add_multiple_dict_scores) {
    df <- getMultipleDictionaryScores(
      df = df, 
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
  }
  if(add_pv) {

    df$PV_aux_pas <- getPassiveVoice(df = df, text_col = 'event_text',ratio = TRUE)
    spacyr::spacy_finalize()
  }
  return(df)
}

get_and_clean_and_code_rail_events <- function(f,add_liwc,add_emo_voc,add_butter,add_multiple_dict_scores,add_pv) {
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
  df <- df |>
    mutate(
      Narrative = str_replace_all(Narrative,'NoneNone',''),
      Narrative = str_remove(Narrative,'None$')
    ) |>
    filter(
      !is.na(Narrative), 
      Narrative != '',
      Narrative != 'None',
      utf8::utf8_valid(Narrative))
  
  if(add_multiple_dict_scores) {
    df <- getMultipleDictionaryScores(
      df = df, 
      text_col = 'Narrative',
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
      )) }
  if(add_pv) {
    df$PV_aux_pas <- getPassiveVoice(df = df, text_col = 'Narrative',ratio = TRUE)
    spacyr::spacy_finalize()
  }
  return(df)
}

get_and_clean_and_code_phmsa <- function(f,add_liwc,add_emo_voc,add_butter,add_multiple_dict_scores,add_pv) {
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
  df <- df |> 
    filter(
      !is.na(cmbd_narrative), 
      cmbd_narrative != '', 
      utf8::utf8_valid(cmbd_narrative))
  if(add_multiple_dict_scores){
    df <- getMultipleDictionaryScores(
      df = df, 
      text_col = 'cmbd_narrative',
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
  }
  if(add_pv){
    df$cmbd_narrative <- as.character(df$cmbd_narrative)
    df$PV_aux_pas <- getPassiveVoice(df = df, text_col = 'cmbd_narrative',ratio = TRUE)
    spacyr::spacy_finalize()
  }
  return(df)
}

get_and_clean_and_code_psn <- function(f,add_liwc,add_emo_voc,add_butter,add_multiple_dict_scores,add_pv) {
  df <- read.csv(f)
  if(!is.na(add_liwc)) {
    df_liwc <- read.csv(add_liwc) |>
      select(all_of(c('Report_ID',liwc_vars))) |>
      rename_with(.cols = all_of(liwc_vars), .fn = ~ paste0("liwc_", .x))
    df <- df |> left_join(
      df_liwc, by = 'Report_ID')# |> 
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
  df <- df |> 
    filter(
      !is.na(Narrative_merged), 
      Narrative_merged != '', 
      utf8::utf8_valid(Narrative_merged))
  if(add_multiple_dict_scores){
    df <- getMultipleDictionaryScores(
      df = df, 
      text_col = 'Narrative_merged',
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
  }
  if(add_pv){
    df$Narrative_merged <- as.character(df$Narrative_merged)
    df$PV_aux_pas <- getPassiveVoice(df = df, text_col = 'Narrative_merged',ratio = TRUE)
    spacyr::spacy_finalize()
  }
  return(df)
}


harmonize_key_vars <- function(df, source) {
  #creates 
  if (source == 'asrs') {
    key <- c(event_date = 'date',event_num = 'acn', event_text = 'cmbd_narrative')#cmbd_narrative = 'event_text')
  } else if (source == 'rail') {
    key <- c(event_date = 'Date',event_num = 'Accident.Number', event_text = 'Narrative')#Narrative = 'event_text')
  } else if (source == 'nrc') {
    key <- c(event_date = 'event_date',event_num = 'event_num')
  } else if (source == 'phmsa') {
    key <- c(event_date = 'date',event_num = 'report_no', event_text = 'cmbd_narrative')#cmbd_narrative = 'event_text')
  } else if (source == 'psn') {
    key <- c(event_date = 'FLR_SUBMIT_DATE',event_num = 'Report_ID', event_text = 'Narrative_merged')
  }
  if (exists('key')) {
    return(
      df |> rename(all_of(key)) |> mutate(event_num = as.character(event_num))
    )
  } else {
    print('No appropriate source provided...')
    return(NULL)
  }

}

################################################################
###################
################### Database mngmt
################### 
###################
################################################################

insert_new_column <- function(con, table, column_name, column_type, vec_len) {
  print(glue::glue("Attempting to insert {column_name} as {column_type}"))
  column_type <- if_else(is.na(vec_len), column_type, paste0('vector(',vec_len,')'))
  stmt <- glue::glue(
    "ALTER TABLE ?table ADD COLUMN IF NOT EXISTS ?column_name {column_type};")
  q <- DBI::sqlInterpolate(
    conn = con, 
    stmt, 
    table= DBI::dbQuoteIdentifier(con, table), 
    column_name = DBI::dbQuoteIdentifier(con, column_name)
    )
  print(q)
  DBI::dbExecute(conn = con, q)
}

create_link_table <- function(con){
  q <- DBI::sqlInterpolate(
    conn = con, 
    "CREATE TABLE IF NOT EXISTS link_table (eid uuid PRIMARY KEY DEFAULT gen_random_uuid(), system_source text, event_date date, event_num text)")
  DBI::dbExecute(conn = con, q)
}

create_raw_table <- function(con,df, table_name, update_values) {
  q <- DBI::sqlInterpolate(
    conn = con,
    "CREATE TABLE IF NOT EXISTS ?table_name (eid uuid PRIMARY KEY, CONSTRAINT fk_eid FOREIGN KEY(eid) REFERENCES link_table(eid));",
    table_name = DBI::dbQuoteIdentifier(con, table_name)
  )
  DBI::dbExecute(conn = con, q)
  for(c in names(df)) {
    if(c != 'eid') {
      print(c)
      print(class(df[,c]))
      insert_new_column(
        con = con,
        table = table_name,
        column_name = c,
        column_type = DBI::dbDataType(con, df[,c]),
        vec_len = NA
        )
    }
  }
  if(update_values) { # need to check existing entries
    DBI::dbWriteTable(
      conn = con, 
      table_name, 
      df, 
      append = TRUE)
  }
}

updateLinkTable <- function(con, df, sys_source, return_eid){
  df <- df |> 
    mutate(
      system_source = sys_source,
      event_num = as.character(event_num))
  DBI::dbWriteTable(
    conn = con, 
    'link_table', 
    df |> select(event_num,event_date, system_source), 
    append = TRUE)
  if(return_eid){
    # pull all eid's for update events
    t <- tbl(con, 'link_table')
    eids <- t  |> collect() |> # this is lazy at it pulls entire talbe, but...
      filter(system_source == !!sys_source) |>
      filter(event_num %in% unique(df$event_num)) |>
      select(eid, event_num)
    df <- df |> 
      left_join(eids, by = 'event_num') |> 
      relocate(eid) |> 
      select(-system_source)
    return(df)
  }
}

get_eids <- function(con, df, sys_source) {
  # pull all eid's for update events
  
  q <- DBI::sqlInterpolate(
    conn = con,
    glue::glue_sql(
      "SELECT * FROM link_table as lt WHERE (lt.system_source = ?sys) AND (lt.event_num IN {event_nums});"),
    event_nums = unique(df$event_num),
    sys = sys_source
  )
  eids <- DBI::dbGetQuery(conn = con, q) |>
    select(eid, event_num)
  
  # t <- tbl(con, 'link_table')
  # eids <- t  |> collect() |> # this is lazy at it pulls entire talbe, but...
  #   filter(system_source == !!sys_source) |>
  #   filter(event_num %in% unique(df$event_num)) |>
  #   select(eid, event_num)
  
  df <- df |> 
    left_join(eids, by = 'event_num') |> 
    relocate(eid)
  return(df)
}

create_embeddings_table <- function(con) {
  DBI::dbExecute(conn = con, "CREATE EXTENSION IF NOT EXISTS vector")
  DBI::dbExecute(conn = con, "CREATE TABLE embeddings (eid uuid PRIMARY KEY, embedding vector(768), CONSTRAINT fk_eid FOREIGN KEY(eid) REFERENCES link_table(eid))")
}

insert_vec <- function(r,con, col_name) {
  vec <- r[1,col_name]
  event_num = r[1,'event_num']
  sys_source = r[1,'sys_source']
  print(glue::glue('{event_num} : {col_name} : {sys_source} : {vec}'))
  s <- DBI::sqlInterpolate(
    conn = con,
    glue::glue(
      "UPDATE embeddings SET ?col_name = ?vec WHERE eid = (SELECT eid FROM link_table WHERE link_table.system_source = ?sys_source AND link_table.event_num = ?event_num);"),
    col_name = DBI::dbQuoteIdentifier(con, col_name),
    vec = DBI::dbQuoteLiteral(con, vec),
    sys_source = DBI::dbQuoteLiteral(con, sys_source),
    event_num = DBI::dbQuoteLiteral(con, event_num)
  )
  print(s)
  DBI::dbExecute(conn = con, statement = s)
  NULL
}

# fixing lack of system source in link_table
# t <- tbl(con, 'link_table')
# system_source <- 'nrc'
# for (event_num in unique(nrc_df$event_num)) {
#   q <- DBI::sqlInterpolate(con,"UPDATE link_table SET system_source = ?system_source WHERE event_num = ?event_num;",
#                            system_source = system_source, #DBI::dbQuoteIdentifier(con,system_source),
#                            event_num = event_num )#DBI::dbQuoteIdentifier(con,event_num))
#   DBI::dbExecute(conn = con, q)
# }

################################################################
###################
################### Funcs for embeddings
################### 
###################
################################################################

pgvector.serialize <- function(v) {
  stopifnot(is.numeric(v))
  return(paste0("[", paste(v, collapse=","), "]"))
}

pgvector.unserialize <- function(v) {
  return(lapply(strsplit(substring(v, 2, nchar(v) - 1), ","), as.numeric))
}

dict_to_vec <- function(df, serialize) {
  # creates a single vector from row of dataframes for storing in pgvector
  v <- apply(df,2,unlist)
  print(str(v))
  v <- unname(v)
  if(serialize) {
    v <- apply(v,1,pgvector.serialize)
  }
  return(v)
}

embed_and_save <- function(con, df) {
  print(Sys.time())
  for(i in seq(1,nrow(df),1)) {
    embeddings <- text::textEmbed(
      texts = df[i,'event_text'],
      model = "bert-base-uncased",
      layers = -2,
      aggregation_from_tokens_to_texts = "mean",
      aggregation_from_tokens_to_word_types = "mean",
      keep_token_embeddings = FALSE)
    embed_to_insert <- data.frame(
      eid = df[i,'eid'],
      embedding = apply(embeddings$texts$event_text,1,pgvector.serialize)
    )
    DBI::dbAppendTable(conn = con, "embeddings", embed_to_insert)
  }
}

get_cos_sim <- function(x,y) {
  # This works on the text string pulled from pg_vector
  if (is.na(x) | is.na(y)) {
    return(NA)
  } else {
    return(
      as.numeric(lsa::cosine(
        unlist(pgvector.unserialize(x)),
        unlist(pgvector.unserialize(y)))
      ))}
}

rolling_vec_mean <- function(x) {
  # x <- x |>
  #   mutate(embedding = pgvector.unserialize(embedding))
  # this unpacks the char so each row has a column for each embedding value
  # then takes colmeans and repacks it into a char

  df <- as.data.frame(
    do.call(
      rbind,
      pgvector.unserialize(x)))
  return(
    pgvector.serialize(
      unname(colMeans(df))
    )
  )
}

get_rw_cs <- function(df,winSize,vCol) {
  if (winSize > 1 & winSize < (nrow(df)/2)) {
    df$rw_mean <- runner::runner(
      x = df[,vCol],
      k = winSize,
      f = rolling_vec_mean,
      na_pad = FALSE
    )
    df <- df |>
      mutate(
        rw_mean_lag = lag(rw_mean)
      ) |>
      rowwise() |>
      mutate('cos_sim_rw_{vCol}_{winSize}' := get_cos_sim(!!sym(vCol),rw_mean_lag)) |>
      ungroup()
    return(df)
  } else if (winSize == 1) {
    df <- df |>
      mutate(
        lag = lag(!!sym(vCol))
      ) |>
      rowwise() |>
      mutate('cos_sim_rw_{vCol}_{winSize}' := get_cos_sim(!!sym(vCol),lag)) |> 
      ungroup()
    return(df)
  } else {
    print('not valid winSize')
    return(NA)
  }
}

################################################################
###################
################### Functions for pulling and manipulating vector
################### 
###################
################################################################


# pull, clean, and structure vectors

get_clean_vecs <- function(vCol, minFacilityReport, winSize, sys_source, org_unit, e_date,con) {
  q <- DBI::sqlInterpolate(
    conn = con,
    glue::glue(
      "SELECT et.eid, et.{vCol}, rt.{org_unit}, rt.{e_date} FROM embeddings as et LEFT JOIN link_table as lt USING (eid) LEFT JOIN ?raw_table as rt USING (eid) WHERE lt.system_source = ?sys;"),
    raw_table = DBI::dbQuoteIdentifier(con, paste0(sys_source,'_raw')),
    sys = sys_source
  )
  print(q)
  
  vec_df <- DBI::dbGetQuery(conn = con, q)
  print(head(vec_df,10))
  if (sys_source == 'nrc') {
    vec_df <- vec_df|>
      # group_by(facility) |>
      group_by(!!sym(org_unit)) |>
      filter(n() >= 100) |> # drop facilities with low n
      ungroup() |>
      # mutate(event_date2 = lubridate::ymd(event_date2)) |>
      # filter(between(lubridate::year(event_date2),1999,2023)) |>
      mutate(!!sym(e_date) := lubridate::ymd(!!sym(e_date))) |>
      filter(between(lubridate::year(!!sym(e_date)),1999,2023)) |>
      group_by(!!sym(org_unit)) |>
      arrange(!!sym(e_date), .by_group = TRUE) |>
      group_modify(
        ~ get_rw_cs(
          df = .x,
          winSize = winSize,
          vCol = vCol
        )
      ) |> 
      ungroup()
  } else if (sys_source == 'rail') {
    vec_df <- vec_df|>
      group_by(!!sym(org_unit)) |>
      filter(n() >= 100) |> # drop facilities with low n
      ungroup() |>
      mutate(!!sym(e_date) := lubridate::mdy(!!sym(e_date))) |>
      # filter(between(lubridate::year(!!sym(e_date)),1999,2023)) |>
      group_by(!!sym(org_unit)) |>
      arrange(!!sym(e_date), .by_group = TRUE) |>
      group_modify(
        ~ get_rw_cs(
          df = .x,
          winSize = winSize,
          vCol = vCol
        )
      ) |> ungroup()
  }
  return(vec_df)
}

################################################################
###################
################### General functions for closed-voc coding
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
