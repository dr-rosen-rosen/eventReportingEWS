
##################################################
##############
############## Read in and combine data across all reporting systems
##############
##################################################
dict_map <- readxl::read_excel('/Volumes/calculon/event_reporting/dictionary_mappiings.xlsx') |>
  filter(is.na(drop)) |>
  unite('var_name',prefix:variables, sep = '')
features <- unique(dict_map$var_name)
length(features)

nrc_dict_df2 <- nrc_df |> select(all_of(c(features,key_vars)))
rail_dict_df2 <- rail_df |> select(all_of(c(features,key_vars)))
asrs_dict_df2 <- asrs_df |> select(all_of(c(features,key_vars)))
phmsa_dict_df2 <- phmsa_df |> select(all_of(c(features,key_vars)))

cmb_df <- bind_rows(nrc_dict_df2,rail_dict_df2,asrs_dict_df2,phmsa_dict_df2) |>
  rowwise() |>
  mutate(across(.cols = starts_with('gi_'), ~ .x / liwc_WC)) |> # change GI vars from counts to proportions
  ungroup() |> select(-liwc_WC)
skimr::skim(cmb_df)

##################################################
##############
############## Clean and EFA
##############
##################################################
nearZeroVars <- caret::nearZeroVar(
  cmb_df,
  # freqCut = 
  allowParallel = TRUE, foreach = TRUE, saveMetrics = FALSE, names = TRUE)
length(nearZeroVars)
# cmb_df |> select(one_of(nearZeroVars)) |>
#   skimr::skim()

cmb_df_cln <- cmb_df |>
  select(-one_of(nearZeroVars)) |>
  select(where(is.numeric)) |>
  select(where(~any(sd(., na.rm = TRUE) != 0))) |> # drops any variable with NO variation
  select(where(~any(mad(.,na.rm = TRUE) != 0))) |> # drops any variable with MAD of 0
  mutate(
    dataSet_num = cmb_df$dataSet_num)
ncol(cmb_df_cln)

EFAtools::BARTLETT(cmb_df_cln |> select(-dataSet_num) |> 
                     drop_na() |>
                     mutate(across(.cols = where(is.numeric), ~ datawizard::standardize(.x,
                                                                                                          robust = TRUE,
                                                                                                          two_sd = FALSE,
                                                                                                          weights = NULL,
                                                                                                          reference = NULL,
                                                                                                          center = NULL,
                                                                                                          scale = NULL,
                                                                                                          verbose = TRUE
                     )
                     )
                     ))

kmo <- EFAtools::KMO(cmb_df_cln |> select(-dataSet_num) |> 
                       drop_na() |>
                       mutate(across(.cols = where(is.numeric), ~ datawizard::standardize(.x,
                                                                                                            robust = TRUE,
                                                                                                            two_sd = FALSE,
                                                                                                            weights = NULL,
                                                                                                            reference = NULL,
                                                                                                            center = NULL,
                                                                                                            scale = NULL,
                                                                                                            verbose = TRUE
                       )
                       )
                       ))

acceptable_kmo <- enframe(kmo$KMO_i) #|>
#  filter(value >=.1)


n_fact_results <- EFAtools::N_FACTORS(
  cmb_df_cln |> 
    select(all_of(unique(acceptable_kmo$name))) |> 
    drop_na() |>
    mutate(across(.cols = all_of(unique(acceptable_kmo$name)), ~ datawizard::standardize(.x,
                                                                                         robust = TRUE,
                                                                                         two_sd = FALSE,
                                                                                         weights = NULL,
                                                                                         reference = NULL,
                                                                                         center = NULL,
                                                                                         scale = NULL,
                                                                                         verbose = TRUE
                                                                                         )
                  )
           )
  )

beepr::beep()

EFAtools::SMT(cmb_df_cln |> 
                select(all_of(unique(acceptable_kmo$name))) |> 
                drop_na() |>
                mutate(across(.cols = all_of(unique(acceptable_kmo$name)), ~ datawizard::standardize(.x,
                                                                                                     robust = TRUE,
                                                                                                     two_sd = FALSE,
                                                                                                     weights = NULL,
                                                                                                     reference = NULL,
                                                                                                     center = NULL,
                                                                                                     scale = NULL,
                                                                                                     verbose = TRUE)
                              )
                       )
)


efa <- factanal(x = cmb_df_cln |> 
                  select(all_of(unique(acceptable_kmo$name))) |> 
                  drop_na() |>
                  mutate(across(.cols = all_of(unique(acceptable_kmo$name)), ~ datawizard::standardize(.x,
                                                                                                       robust = TRUE,
                                                                                                       two_sd = FALSE,
                                                                                                       weights = NULL,
                                                                                                       reference = NULL,
                                                                                                       center = NULL,
                                                                                                       scale = NULL,
                                                                                                       verbose = TRUE)
                  )
                  ), factors = 19)

##################################################
##############
############## Final EFA and factor scores
##############
##################################################

# pca.19 <- FactoMineR::PCA(
#   cmb_df_cln |> select(all_of(unique(acceptable_kmo$name))) |> drop_na(),
#   scale.unit = TRUE,
#   ncp = 19,
#   graph = FALSE
# )
# 
# write.csv(pca.24$var$cor, here::here(config$dict_file_path,'factor_loadings24.csv'))

psych.29 <- psych::principal(
  r = cmb_df_cln |> 
    select(all_of(unique(acceptable_kmo$name))) |> 
    drop_na() |>
    mutate(across(.cols = all_of(unique(acceptable_kmo$name)), ~ datawizard::standardize(.x,
                                                                                         robust = TRUE,
                                                                                         two_sd = FALSE,
                                                                                         weights = NULL,
                                                                                         reference = NULL,
                                                                                         center = NULL,
                                                                                         scale = NULL,
                                                                                         verbose = TRUE)
    )
    ),
  nfactors = 29,
  rotate = 'varimax'
)

# write.csv(psych.29$loadings, here::here(config$dict_file_path,'psych29Loadings.csv'))

scored_df <- cmb_df |> drop_na(unique(acceptable_kmo$name)) |> bind_cols(psych.29$scores) |>
  separate_wider_delim(dataSet_num, delim = '_', names = c('dataSet','num'))

##################################################
##############
############## Clustering in data set of factor scores
##############
##################################################

iccs <- list()
n_factors <- 29
for (cl in colnames(scored_df[,(ncol(scored_df)-n_factors+1):ncol(scored_df)])) {
  iccs[[cl]] <- ICC::ICCbare(dataSet,eval(substitute(cl), scored_df), data = scored_df)
}
icc_df <- as.data.frame(iccs) |>
  pivot_longer(
    cols = everything(),
    names_to = 'variable',
    values_to = 'dataSet_ICC'
  )
names(scored_df)

##################################################
##############
############## Linking back to original data sets.
##############
##################################################

nrc_df_w_fa <- nrc_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'nrc',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')
rail_df_w_fa <- rail_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'rail',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')
asrs_df_w_fa <- asrs_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'asrs',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')
phmsa_df_w_fa <- phmsa_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'phmsa',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')