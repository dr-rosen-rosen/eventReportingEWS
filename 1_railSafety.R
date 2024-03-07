

rail_df <- get_and_clean_rail_events(
  f = here::here(config$rail_data_path,config$rail_events_file),
  add_liwc = here::here(config$rail_data_path,config$rail_liwc_file),
  add_emo_voc = here::here(config$rail_data_path,config$rail_emo_voc_file),
  add_butter = here::here(config$rail_data_path,config$rail_butter_file))
skimr::skim(rail_df)

unique(rail_df$Reporting.Railroad.Code)
nrow(rail_df)
rail_df <- rail_df |> 
  group_by(Reporting.Railroad.Code) |> 
  filter(n() >= 100) |> # drop facilities with low n
  ungroup() #|>
  # filter(between(lubridate::year(event_date2),1999,2023))


rail_dict_df <- getMultipleDictionaryScores(
  df = rail_df, 
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
  ))

iccs <- list()
for (cl in colnames(rail_dict_df[,163:ncol(rail_dict_df)])) {
  iccs[[cl]] <- ICC::ICCbare(Reporting.Railroad.Code,eval(substitute(cl), rail_dict_df), data = rail_dict_df)
}
rail_icc_df <- as.data.frame(iccs) |>
  pivot_longer(
    cols = everything(),
    names_to = 'variable',
    values_to = 'rail_RRCode_ICC'
  )
