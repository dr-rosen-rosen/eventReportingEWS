

asrs_df <- get_cmbd_ASRS(
  f = here::here(config$asrs_data_path,config$asrs_events_file),
  add_liwc = here::here(config$asrs_data_path,config$asrs_liwc_file),
  add_emo_voc = here::here(config$asrs_data_path,config$asrs_emo_voc_file),
  add_butter = here::here(config$asrs_data_path,config$asrs_butter_file))

skimr::skim(asrs_df)
nrow(asrs_df)
# unique(asrs_df$Reporting.Railroad.Code)
nrow(asrs_df)
# asrs_df <- asrs_df |>
#   group_by(Reporting.Railroad.Code) |>
#   filter(n() >= 100) |> # drop facilities with low n
#   ungroup() #|>
# # filter(between(lubridate::year(event_date2),1999,2023))


asrs_dict_df <- getMultipleDictionaryScores(
  df = asrs_df, 
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
