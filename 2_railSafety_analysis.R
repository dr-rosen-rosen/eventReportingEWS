


##################################################
##############
############## Using factor scores
##############
##################################################

unique(rail_df$Reporting.Railroad.Code)
nrow(rail_df)
rail_dict_df2 <- rail_dict_df3 |>
  group_by(Reporting.Railroad.Code) |>
  filter(n() >= 100) |> # drop facilities with low n
  ungroup() #|>
# filter(between(lubridate::year(event_date2),1999,2023))

iccs <- list()
for (cl in colnames(rail_dict_df3 |> select(starts_with('RC')))) {
  print(cl)
  # iccs[[cl]] <- ICC::ICCbare(Reporting.Railroad.Code,eval(substitute(cl), rail_dict_df2), data = rail_dict_df2)
}
rail_icc_df <- as.data.frame(iccs) |>
  pivot_longer(
    cols = everything(),
    names_to = 'variable',
    values_to = 'rail_RRCode_ICC'
  )
