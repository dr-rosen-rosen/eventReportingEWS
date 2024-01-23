###########
### 2Qs analyses
###########

TwoQs_df <- read.csv('/Volumes/LaCie/event_ews/all_2Qs_metrics_12-29-2023.csv')
culture_df <- readxl::read_excel('/Volumes/LaCie/event_ews/old_culture_data/culture\ Data\ Set\ From\ 05-13-13.xlsx', 
                                 sheet = 'To Go In Stata') 

######################################
########### LIWC measures for asrs
######################################

TwoQs_liwc_df <- read.csv('/Volumes/LaCie/event_ews/old_culture_data/LIWC-22 Results - 2013_JHMI_Pascal_all_QualComms - LIWC Analysis.csv')
TwoQs_liwc_df <- TwoQs_liwc_df |>
  filter(hospital == 'JHH') |>
  select(work_setting,
         focuspast,focuspresent,focusfuture,
         fatigue,
         prosocial,conflict,
         cogproc,insight,cause,certitude,
         power,affiliation
  ) |>
  group_by(work_setting) |>
  summarise(across(everything(), .f = list(mean = mean, std = sd), na.rm = TRUE))


cmb_df <- TwoQs_df |>
  right_join(culture_df, by = 'work_setting') |>
  left_join(TwoQs_agenCom_by_workSet, by = 'work_setting') |>
  left_join(AuxPass_by_workSet, by = 'work_setting') |>
  left_join(TwoQs_liwc_df, by = 'work_setting')

skimr::skim(cmb_df)

cult_metrics <- c('TC','SC','PSM','PLM','HHT','TAHU','CRS')
liwc_metrics <- c('focuspast','focuspresent','focusfuture','fatigue','prosocial','conflict','cogproc','insight','cause','certitude','power','affiliation')
psn_metrics <-c("Ratio (Avg number of events per bed per month)", "Proportion of Events with Manager Follow-UP", 'Average Bed', 'Proportion of Harm','cusp')

cmb_df_trim <- cmb_df |>
  select(
    all_of(cult_metrics),
    # starts_with(liwc_metrics),
         # starts_with('compound'),
    # starts_with(c('agency','commun')),
    # starts_with('aux_pass'),
    #      starts_with('neg'),
    #      starts_with('pos'),
    #      starts_with('neu'),
         ends_with('sim')#,
         # starts_with('SAQ')
         # all_of(psn_metrics)
         )
cmb_cor <- cor(cmb_df_trim |> select(where(is.numeric)), use = "pairwise.complete.obs")

m.TC <- lm(PLM ~ 1 + mean_sim + neg_mean + pos_mean + power_mean + agency_mean + `Average Bed` + aux_pass_mean + certitude_mean + prosocial_mean, data = cmb_df)
sjPlot::tab_model(m.TC)


######################################
########### LASSO
######################################
