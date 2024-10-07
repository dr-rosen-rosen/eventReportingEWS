#########################################################
#########################################################
########## Generate rolling window measures
#########################################################
#########################################################
library(EWSmethods)
library(multispatialCCM)

#########################################################
########## Read in and merge data
#########################################################

con <- DBI::dbConnect(RPostgres::Postgres(),
                      user = config$dbUser,
                      password = config$dbPW,
                      host = config$db_host,
                      dbname = config$db_name,
                      port = config$dbPort
)

winSize <- 1
minFacilityReport <- 100
# nrc
sys_source <- 'nrc'
org_unit <- 'facility'
e_date <- 'event_date2'
# rail
sys_source <- 'rail'
org_unit <- 'reporting_railroad_code'
e_date <- 'event_date'


t <- get_clean_vecs(
  vCol = 'composite_climate_vec', 
  minFacilityReport = minFacilityReport, 
  winSize = winSize, 
  sys_source = sys_source,
  org_unit = org_unit, 
  e_date = e_date,
  con = con) |> select(eid,cos_sim_rw_composite_climate_vec_1)
u <- get_clean_vecs(
  vCol = 'embedding', 
  minFacilityReport = minFacilityReport, 
  winSize = winSize, 
  sys_source = sys_source,
  org_unit = org_unit, 
  e_date = e_date,
  con = con)

v <- full_join(t,u, by = 'eid')

table(v |> select(!!sym(org_unit)))

#########################################################
########## Get inter-event durations and burstiness
#########################################################
t_unit <- 'days'
getEventBurstiness <- function(g, e_date, t_unit, corrected) {
  data <- g |> 
    arrange(!!sym(e_date)) |>
    mutate(
      lag_time_in = lag(!!sym(e_date)),
      inter_event_dur = as.numeric(difftime(!!sym(e_date), lag_time_in, units = t_unit))
    ) |>
    dplyr::select(inter_event_dur) |>
    drop_na()
  r <- sd(data$inter_event_dur, na.rm = TRUE) / mean(data$inter_event_dur, na.rm = TRUE)
  n <- nrow(data)
  if (corrected) {
    b_param <- (sqrt(n + r) - sqrt(n - 1)) / (((sqrt(n + 1) - 2) * r) + sqrt(n - 1))
  } else {
    b_param <- (r - 1) / (r + 1)
  }
  
  return(b_param)
}

getEventBurstiness_sfly <- purrr::safely(.f = getEventBurstiness, otherwise = NA)

data_by_org <- v |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  mutate(
    lag_e_time = lag(!!sym(e_date)),
    inter_event_dur = as.numeric(difftime(!!sym(e_date), lag_e_time, units = t_unit))
  ) |>
  select(-lag_e_time) |>
  ungroup() |>
  split(v[,org_unit])

tictoc::tic()
bursty_df <- purrr::map(
  data_by_org, 
  function(x = .x) {
    b <- runner::runner(
      x = x, 
      k = 10, # time window for burstiness calculation
      na_pad = TRUE,
      f = function(x) {getEventBurstiness_sfly(g = x,e_date = e_date,
                                               t_unit = t_unit,corrected = FALSE)$result})
    x$b_param <- b
    return(x)}
) |> bind_rows(.id = org_unit) |> select(eid,inter_event_dur, b_param)
tictoc::toc()

tictoc::tic()
bursty_df2 <- purrr::map(
  data_by_org, 
  function(x = .x) {
    b <- runner::runner(
      x = x, 
      k = 10, # time window for burstiness calculation
      na_pad = TRUE,
      f = function(x) {getEventBurstiness_sfly(g = x,e_date = e_date,
                                               t_unit = t_unit,corrected = TRUE)$result})
    x$b_param2 <- b
    return(x)}
) |> bind_rows(.id = org_unit) |> select(eid,b_param2)
tictoc::toc()

bursty_df <- full_join(bursty_df,bursty_df2, by = 'eid')

#########################################################
########## ESW analyses - Multivariate
#########################################################

key <- v |>
  full_join(bursty_df, by = join_by('eid')) |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  mutate(t = row_number()) |> ungroup()

ews_vars <- c('cos_sim_rw_composite_climate_vec_1','cos_sim_rw_embedding_1', 'b_param','inter_event_dur') # 'inter_event_dur' 'b_param2',

tictoc::tic()
future::plan(multisession, workers = availableCores())
ews_df2 <- v |>
  full_join(bursty_df, by = join_by('eid')) |>
  select(!!sym(org_unit),!!sym(e_date),all_of(ews_vars)) |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  select(-!!sym(e_date)) |>
  mutate(t = row_number()) |> 
  relocate(t) |> 
  tidyr::nest() |>
  mutate(ews_results = furrr::future_map(data, ~EWSmethods::multiEWS(data = .,
                                                                     metrics = c("meanAR","maxAR","meanSD","maxSD","eigenMAF","mafAR","mafSD","pcaAR","pcaSD","eigenCOV","maxCOV","mutINFO"),
                                                                     method = "expanding",
                                                                     burn_in = 50,
                                                                     threshold = 2)))
tictoc::toc()
saveRDS(ews_df2, glue::glue("{sys_source}_ews_df2_multi.rds"))
future::plan(sequential)

ews_df3 <- purrr::pmap_dfr(ews_df2, ~ data.frame(..3$EWS$raw) |>
                             mutate(!!sym(org_unit) := ..1) |>
                             group_by(!!sym(org_unit),time) |>
                             summarise(multi.threshold.crossed.count = sum(threshold.crossed))) |>
  left_join(key,by = c(org_unit,'time' = 't'))

plot(ews_df2$ews_results[[24]],  y_lab = "Density")
# 
for (i in seq(from = 1, to = nrow(ews_df2),by=1)) {
  p<-plot(ews_df2$ews_results[[i]],  y_lab = "Density")
  ggsave(here::here('plots',glue::glue("EWS_multi_{i}.png")),p)
}

#########################################################
########## ESW analyses - Single measures
#########################################################

single_ews_df <- v |>
  full_join(bursty_df, by = join_by('eid')) |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  mutate(t = row_number()) |> ungroup()

future::plan(multisession, workers = availableCores())
tictoc::tic()
metrics <- c('cos_sim_rw_composite_climate_vec_1','cos_sim_rw_embedding_1','b_param','inter_event_dur') # 'b_param2',
for (metric in metrics) {
  print(metric)
  single_ews_df2 <- v |>
    full_join(bursty_df, by = join_by('eid')) |>
    drop_na() |>
    select(!!sym(org_unit),!!sym(e_date),!!sym(metric)) |>
    group_by(!!sym(org_unit)) |>
    arrange(!!sym(e_date), .by_group = TRUE) |>
    select(-!!sym(e_date)) |>
    mutate(t = row_number()) |> 
    relocate(t) |> 
    tidyr::nest() |>
    mutate(ews_results = furrr::future_map(data, ~EWSmethods::uniEWS(data = .,
                                                                     metrics = c("ar1","SD","skew"),
                                                                     method = "expanding",
                                                                     burn_in = 50,
                                                                     threshold = 2)))

  single_ews_df <- left_join(single_ews_df,
                             purrr::pmap_dfr(single_ews_df2, ~ data.frame(..3$EWS) |>
                                      mutate(!!sym(org_unit) := ..1) |>
                                      group_by(!!sym(org_unit),time) |>
                                      summarise("{metric}.threshold.crossed.count" := sum(threshold.crossed))),
                             by = c(org_unit,'t' = 'time'))
  
}
tictoc::toc()
saveRDS(single_ews_df2, glue::glue("{sys_source}_single_ews_df2_{metric}.rds" ))
beepr::beep()
future::plan(sequential)

single_ews_df |> select(contains('threshold.crossed')) |> drop_na() |> skimr::skim()
single_ews_df |> select(contains('threshold.crossed')) |> drop_na() |> cor()

# plot(single_ews_df2$ews_results[[1]],  y_lab = "Density")
# 
# for (i in seq(from = 1, to = nrow(single_ews_df2),by=1)) {
#   p<-plot(single_ews_df2$ews_results[[i]],  y_lab = "Density")
#   ggsave(here::here('plots',glue::glue("single_EWS_{sys_source}_{metric}_{i}.png")),p)
# }

cmb_ews <- single_ews_df |>
  select(eid, contains('threshold.crossed')) |>
  full_join(ews_df3, by = 'eid')
ews.cor <- cor(cmb_ews |> select(contains('threshold.crossed')) |> drop_na())
corrplot::corrplot(ews.cor)

#########################################################
#########################################################
########## Link to outcome data
#########################################################
#########################################################

# mutate(
#   harm_score = case_when(harm_score %in% 1:2 ~ 1,
#                          harm_score %in% 3:5 ~ 2,
#                          harm_score %in% 6:9 ~ 3)
# )

# nrc
t <- tbl(con,'nrc_raw')
nrc_raw <- t  |> collect() |>
  mutate(scram_recode = if_else(scram_code == 'N',0,1)) |>
  select(eid,scram_recode)
cmb_ews <- cmb_ews |> left_join(nrc_raw, by = 'eid') |> drop_na(all_of(org_unit))

# rail
t <- tbl(con, 'rail_raw')
rail_raw <- t |> collect() |> janitor::clean_names() |>
  select(eid, total_persons_killed, total_persons_injured, total_damage_cost)
cmb_ews <- cmb_ews |> left_join(rail_raw, by = 'eid') |> drop_na(all_of(org_unit))


#########################################################
#########################################################
########## CCM analyses for OUTCOMES
#########################################################
#########################################################

### Performed over each unit's data

metrics <- c(
  'cos_sim_rw_composite_climate_vec_1.threshold.crossed.count',
             'cos_sim_rw_embedding_1.threshold.crossed.count',
             'b_param.threshold.crossed.count',
             # 'b_param2.threshold.crossed.count',
             'inter_event_dur.threshold.crossed.count',
              'multi.threshold.crossed.count'
             )
### psn
# outcome <- 'harm_score'

### nrc
# outcome <- 'scram_recode'

### rail
outcome <- 'total_persons_killed'
outcome <- 'total_persons_injured'
outcome <- 'total_damage_cost'

min_nrow <- 50
for (unit in unlist(unique(cmb_ews[,org_unit]))) {
  print(unit)
  for (metric in metrics) {
    print(glue::glue("Starting {metric} for {unit}..."))
    r <- cmb_ews |>
      filter(!!sym(org_unit) == unit) |>
      select(!!sym(org_unit),!!sym(e_date), !!sym(outcome), !!sym(metric)) |>
      drop_na() |>
      arrange(e_date)
  
    print(nrow(r))
    if (nrow(r) >= min_nrow) {
      v_outcome <- unname(unlist(r[,outcome]))
      v_thresh <- unname(unlist(r[,metric]))
      
      #Calculate optimal E
      maxE<-15 #Maximum E to test
      #Matrix for storing output
      Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
      #Loop over potential E values and calculate predictive ability
      #of each process for its own dynamics
      for(E in 2:maxE) {
        #Uses defaults of looking forward one prediction step (predstep)
        #And using time lag intervals of one time step (tau)
        Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_outcome, E=E, predstep=1, tau=1)$rho
        Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_thresh, E=E, predstep=1, tau=1)$rho
      }
      #Look at plots to find E for each process at which
      #predictive ability rho is maximized
      # matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,main = glue::glue("Unit: {unit}"),
      #         xlab="E", ylab="rho", lwd=2)
      # legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")
      E_A <- which.max(abs(Emat[,"A"]))
      print(E_A)
      E_B <- which.max(abs(Emat[,"B"]))
      print(E_B)
      tryCatch(
        expr = {
          signal_A_out<-multispatialCCM::SSR_check_signal(A=v_outcome, E=E_A, tau=1,
                                                          predsteplist=1:10)
          print(signal_A_out)
        },
        error = function(e){
          signal_A_out <- FALSE
          print(e)
        }
      )
      # print(signal_A_out)
      tryCatch(
        expr = {
          signal_B_out<-multispatialCCM::SSR_check_signal(A=v_thresh, E=E_B, tau=1,
                                                          predsteplist=1:10)
          print(signal_B_out)
        },
        error = function(e){
          signal_B_out <- FALSE
          print(e)
        }
      )
      # print(signal_B_out)
      tryCatch(
        expr = {
          if ((signal_A_out$rho_pre_slope[["Pr(>|t|)"]] <= 0.2) & (signal_B_out$rho_pre_slope[["Pr(>|t|)"]] < 0.2)) {
            #Run the CCM test
            CCM_boot_A<-multispatialCCM::CCM_boot(A = v_outcome, B = v_thresh, E = E_A, tau=1, iterations=10)
            # Does B "cause" A?
            CCM_boot_B<-multispatialCCM::CCM_boot(A = v_thresh, B = v_outcome, E = E_B, tau=1, iterations=10)
            #Test for significant causal signal
            CCM_significance_test<-multispatialCCM::ccmtest(CCM_boot_A,
                                                            CCM_boot_B)
            print(CCM_significance_test)
          } else {
            print(glue::glue("No significant dyanmics for {unit}..."))
          }},
          error = function(e) {
            print(e)
          }
      )
      } else { print(glue::glue("Too few rows for {unit} on {metric}:{nrow(r)}"))}
    print(glue::glue("Done metric: {metric}"))
  }
  print(glue::glue("Done unit: {unit}"))
}
