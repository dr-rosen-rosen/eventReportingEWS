
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

nrc_dict_df2 <- nrc_dict_df |> select(all_of(features)) |> 
  mutate(
    data_set = 'nrc',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num)
rail_dict_df2 <- rail_dict_df |> select(all_of(features)) |> 
  mutate(
    data_set = 'rail',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num)
asrs_dict_df2 <- asrs_dict_df |> select(all_of(features)) |> 
  mutate(
    data_set = 'asrs',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num)
phmsa_dict_df2 <- phmsa_dict_df |> select(all_of(features)) |> 
  mutate(
    data_set = 'phmsa',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num)

cmb_df <- bind_rows(nrc_dict_df2,rail_dict_df2,asrs_dict_df2,phmsa_dict_df2) |>
  write.csv('combinedReportingData_03-12-2024.csv')

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
cmb_df |> select(one_of(nearZeroVars)) |>
  skimr::skim()

cmb_df <- cmb_df |>
  # select(-one_of(nearZeroVars)) #|>
  select(where(is.numeric)) |>
  select(where(~any(sd(., na.rm = TRUE) != 0))) |> # drops any variable with NO variation
  mutate(
    dataSet_num = cmb_df$dataSet_num)


# corrplot::corrplot(cor(cmb_df, use="pairwise.complete.obs"), order = "hclust", tl.col='black', tl.cex=.75)

EFAtools::BARTLETT(cmb_df |> select(-dataSet_num))
kmo <- EFAtools::KMO(cmb_df |> select(-dataSet_num))
acceptable_kmo <- enframe(kmo$KMO_i) #|>
  filter(value >=.1)
# 
kmo3 <- EFAtools::KMO(cmb_df |> select(all_of(unique(acceptable_kmo2$name))))
acceptable_kmo3 <- enframe(kmo3$KMO_i) |>
  filter(value >=.1)
acceptable_kmo <- acceptable_kmo3
EFAtools::N_FACTORS(cmb_df |> select(all_of(unique(acceptable_kmo$name))))
beepr::beep()
#  CD = 11; EKC & PA with PCA = 24

EFAtools::SMT(cmb_df |> select(all_of(unique(acceptable_kmo$name))) |> drop_na())
# lower bound RMSEA = 30; AIC = 115

efa <- factanal(x = cmb_df |> select(all_of(unique(acceptable_kmo$name))) |> drop_na(), factors = 19)

##################################################
##############
############## Final EFA and factor scores
##############
##################################################

pca.19 <- FactoMineR::PCA(
  cmb_df |> select(all_of(unique(acceptable_kmo$name))) |> drop_na(),
  scale.unit = TRUE,
  ncp = 19,
  graph = FALSE
)

write.csv(pca.24$var$cor, here::here(config$dict_file_path,'factor_loadings24.csv'))

psych.30 <- psych::principal(
  r = cmb_df |> select(all_of(unique(acceptable_kmo$name))) |> drop_na(),
  nfactors = 30,
  rotate = 'varimax'
)
write.csv(psych.30$loadings, here::here(config$dict_file_path,'psych30Loadings.csv'))

scored_df <- cmb_df |> drop_na(unique(acceptable_kmo$name)) |> bind_cols(psych.30$scores) |>
  separate_wider_delim(dataSet_num, delim = '_', names = c('dataSet','num'))

##################################################
##############
############## Clustering in data set of factor scores
##############
##################################################

iccs <- list()
for (cl in colnames(scored_df[,46:59])) {
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

nrc_dict_df3 <- nrc_dict_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'nrc',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')
rail_dict_df3 <- rail_dict_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'rail',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')
asrs_dict_df3 <- asrs_dict_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'asrs',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')
phmsa_dict_df3 <- phmsa_dict_df |> #select(all_of(features)) |> 
  mutate(
    data_set = 'phmsa',
    num = as.character(row_number())) |> unite('dataSet_num', data_set:num) |>
  left_join(scored_df |> select(dataSet_num,starts_with('RC')), by = 'dataSet_num')