dict_map <- readxl::read_excel('/Volumes/calculon/event_reporting/dictionary_mappiings.xlsx') |>
  filter(is.na(drop)) |>
  unite('var_name',prefix:variables, sep = '')



features <- unique(dict_map$var_name)
length(features)

nrc_dict_df2 <- nrc_dict_df |> select(all_of(features)) 
rail_dict_df2 <- rail_dict_df |> select(all_of(features)) 
asrs_dict_df2 <- asrs_dict_df |> select(all_of(features))

cmb_df <- bind_rows(nrc_dict_df2,rail_dict_df2,asrs_dict_df2) |>
  select(where(~any(sd(., na.rm = TRUE) != 0))) # drops any variable with NO variation
nearZeroVars <- caret::nearZeroVar(cmb_df, allowParallel = TRUE, foreach = TRUE, saveMetrics = FALSE, names = TRUE)
length(nearZeroVars)
cmb_df <- cmb_df |> select(-one_of(nearZeroVars))

# corrplot::corrplot(cor(cmb_df, use="pairwise.complete.obs"), order = "hclust", tl.col='black', tl.cex=.75)

EFAtools::BARTLETT(cmb_df)
kmo <- EFAtools::KMO(cmb_df)
acceptable_kmo <- enframe(kmo$KMO_i) |>
  filter(value >=.1)

kmo2 <- EFAtools::KMO(cmb_df |> select(all_of(unique(acceptable_kmo$name))))

EFAtools::N_FACTORS(cmb_df |> select(all_of(unique(acceptable_kmo$name))))
