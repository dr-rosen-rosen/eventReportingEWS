################################################################
################################################################
###################
################### Loads event data, combines with external measures
################### and calculates other scores as indicated.
###################
################################################################
################################################################
key_vars <- c('event_date','event_num','dataSet_num')
if (load_from_preProcessed_files) {
  rail_df <- harmonize_key_vars(
    df = read.csv(here::here(config$rail_data_path,'rail_dict_df.csv')),
    source = 'rail') |> 
    mutate(
      data_set = 'rail',
      num = as.character(row_number())) |> unite('dataSet_num', data_set:num)
    
  nrc_df <- harmonize_key_vars(
    df = read.csv(here::here(config$nrc_data_path,'nrc_dict_df.csv')),
    source = 'nrc') |> 
    mutate(
      data_set = 'nrc',
      num = as.character(row_number())) |> unite('dataSet_num', data_set:num)
  
  asrs_df <- harmonize_key_vars(
    df = read.csv(here::here(config$asrs_data_path,'asrs_dict_df.csv')),
    source = 'asrs') |> 
    mutate(
      data_set = 'nrc',
      num = as.character(row_number())) |> unite('dataSet_num', data_set:num)
  
  phmsa_df <- harmonize_key_vars(
    df = read.csv(here::here(config$phmsa_data_path,'phmsa_dict_df.csv')),
    source = 'phmsa') |> 
    mutate(
      data_set = 'phmsa',
      num = as.character(row_number())) |> unite('dataSet_num', data_set:num)
} else {
  rail_df <- get_and_clean_and_code_rail_events(
    f = here::here(config$rail_data_path,config$rail_events_file),
    add_liwc = here::here(config$rail_data_path,config$rail_liwc_file),
    add_emo_voc = here::here(config$rail_data_path,config$rail_emo_voc_file),
    add_butter = here::here(config$rail_data_path,config$rail_butter_file),
    add_multiple_dict_scores = TRUE,
    add_pv = TRUE)
  write.csv(rail_df, here::here(config$rail_data_path,'rail_dict_df.csv'))
  # skimr::skim(rail_df)
  
  nrc_df <- get_and_clean_and_code_NRC_events(
    f = here(config$nrc_data_path,config$nrc_events_file),
    add_liwc = here::here(config$nrc_data_path,config$nrc_liwc_file),
    add_emo_voc = here::here(config$nrc_data_path,config$nrc_emo_voc_file),
    add_butter = here::here(config$nrc_data_path,config$nrc_butter_file),
    add_multiple_dict_scores = TRUE,
    add_pv = TRUE)
  write.csv(nrc_df, here::here(config$nrc_data_path,'nrc_dict_df.csv'))
  # skimr::skim(nrc_df)
  
  phmsa_df <- get_and_clean_and_code_phmsa(
    f = here::here(config$phmsa_data_path,config$phmsa_events_file),
    add_liwc = here::here(config$phmsa_data_path,config$phmsa_liwc_file),
    add_emo_voc = here::here(config$phmsa_data_path,config$phmsa_emo_voc_file),
    add_butter = here::here(config$phmsa_data_path,config$phmsa_butter_file),
    add_multiple_dict_scores = TRUE,
    add_pv = TRUE)
  write.csv(phmsa_df, here::here(config$phmsa_data_path,'phmsa_dict_df.csv'))
  # skimr::skim(nrc_df)
  
  asrs_df <- get_and_clean_and_code_ASRS(
    f = here::here(config$asrs_data_path,config$asrs_events_file),
    add_liwc = here::here(config$asrs_data_path,config$asrs_liwc_file),
    add_emo_voc = here::here(config$asrs_data_path,config$asrs_emo_voc_file),
    add_butter = here::here(config$asrs_data_path,config$asrs_butter_file),
    add_multiple_dict_scores = TRUE,
    add_pv = TRUE)
  write.csv(asrs_df, here::here(config$asrs_data_path,'asrs_dict_df.csv'))
  # skimr::skim(nrc_df)
}

