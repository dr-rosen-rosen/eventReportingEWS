#########################################################
#########################################################
########## Generate rolling window measures
#########################################################
#########################################################

library(lsa)
# Pull needed data
con <- DBI::dbConnect(RPostgres::Postgres(),
                      user = config$dbUser,
                      password = config$dbPW,
                      host = config$db_host,
                      dbname = config$db_name,
                      port = config$dbPort
)

winSize <- 1
t <- get_clean_vecs(
  vCol = 'composite_climate_vec', 
  minFacilityReport = 100, 
  winSize = winSize, 
  sys_source = 'psn',
  org_unit = 'primary_loc_name', 
  e_date = 'event_date',
  con = con) |> select(eid,cos_sim_rw_composite_climate_vec_1)
u <- get_clean_vecs(
  vCol = 'embedding', 
  minFacilityReport = 100, 
  winSize = winSize, 
  sys_source = 'psn',
  org_unit = 'primary_loc_name', 
  e_date = 'event_date',
  con = con)

v <- full_join(t,u, by = 'eid') |>
  filter(!(primary_loc_name %in% c('1-Not Applicable','Community Physicians')))

primary_locations <- as.data.frame(table(v$primary_loc_name))


#########################################################
#########################################################
########## Attempt ESW analyses
#########################################################
#########################################################
library(EWSmethods)
library(future)
library(furrr)


org_unit <- 'primary_loc_name'
e_date <- 'event_date'
sys_source <- 'psn'

key <- v |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  mutate(t = row_number()) |> ungroup()

tictoc::tic()
future::plan(multisession, workers = availableCores())
ews_df2 <- v |>
  drop_na() |>
  select(!!sym(org_unit),!!sym(e_date),cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1) |>
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
saveRDS(ews_df2, "psn_ews_df2.rds")
future::plan(sequential)

ews_df3 <- purrr::pmap_dfr(ews_df2, ~ data.frame(..3$EWS$raw) |>
                             mutate(!!sym(org_unit) := ..1) |>
                             group_by(!!sym(org_unit),time) |>
                             summarise(threshold.crossed.count = sum(threshold.crossed))) |>
  left_join(key,by = c(org_unit,'time' = 't'))

plot(ews_df2$ews_results[[1]],  y_lab = "Density")

for (i in seq(from = 1, to = nrow(ews_df2),by=1)) {
  p<-plot(ews_df2$ews_results[[i]],  y_lab = "Density")
  ggsave(here::here('plots',glue::glue("EWS_{sys_source}_{i}.png")),p)
}

######### Single measures

org_unit <- 'primary_loc_name'
e_date <- 'event_date'
sys_source <- 'psn'

key <- v |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  mutate(t = row_number()) |> ungroup()

tictoc::tic()
future::plan(multisession, workers = availableCores())
metric <- 'cos_sim_rw_composite_climate_vec_1' #'cos_sim_rw_composite_climate_vec_1' #'cos_sim_rw_embedding_1'
single_ews_df2 <- v |>
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
tictoc::toc()
saveRDS(single_ews_df2, glue::glue("{sys_source}_single_ews_df2_{metric}.rds" ))
beepr::beep()
future::plan(sequential)

single_ews_df3 <- purrr::pmap_dfr(single_ews_df2, ~ data.frame(..3$EWS) |>
                             mutate(!!sym(org_unit) := ..1) |>
                             group_by(!!sym(org_unit),time) |>
                             summarise(threshold.crossed.count = sum(threshold.crossed))) |>
  left_join(key,by = c(org_unit,'time' = 't'))

plot(single_ews_df2$ews_results[[1]],  y_lab = "Density")

for (i in seq(from = 1, to = nrow(single_ews_df2),by=1)) {
  p<-plot(single_ews_df2$ews_results[[i]],  y_lab = "Density")
  ggsave(here::here('plots',glue::glue("single_EWS_{sys_source}_{metric}_{i}.png")),p)
}


data_by_org <- v |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  ungroup() |>
  split(v[,org_unit])

bursty_df <- purrr::map(
  data_by_org, 
  function(x = .x) {
    b <- runner::runner(
      x = x, 
      k = 10,
      na_pad = TRUE,
      f = function(x) {getEventBurstiness_sfly(g = x,e_date = e_date,
                                               t_unit = 'weeks')$result})
    x$b_param <- b
    return(x)}
) |> bind_rows(.id = 'eid')


#########################################################
#########################################################
########## Attempt CCM analyses
#########################################################
#########################################################
library(multispatialCCM)

######### First try
r <- v |>
  select(!!sym(org_unit),!!sym(e_date), cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1) |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  group_modify(~add_row(.x,!!sym(e_date) := NA, cos_sim_rw_composite_climate_vec_1 = NA,cos_sim_rw_embedding_1 = NA)) |> ungroup()
v_emb <- unname(unlist(r$cos_sim_rw_embedding_1))
v_cli <- unname(unlist(r$cos_sim_rw_composite_climate_vec_1))

# #Calculate optimal E
maxE<-10 #Maximum E to test
# #Matrix for storing output
Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
# #Loop over potential E values and calculate predictive ability
# #of each process for its own dynamics
for(E in 2:maxE) {
  #Uses defaults of looking forward one prediction step (predstep)
  #And using time lag intervals of one time step (tau)
  Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_emb, E=E, predstep=1, tau=1)$rho
  Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_cli, E=E, predstep=1, tau=1)$rho
}

# # ccm_data_out<-multispatialCCM::make_ccm_data()
# 
# #Look at plots to find E for each process at which
# #predictive ability rho is maximized
matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,
        xlab="E", ylab="rho", lwd=2)
legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")

E_A <- 7
E_B <- 6

# #Check data for nonlinear signal that is not dominated by noise
# #Checks whether predictive ability of processes declines with
# #increasing time distance
# #See manuscript and R code for details
signal_A_out<-multispatialCCM::SSR_check_signal(A=v_emb, E=E_A, tau=1,
                                                predsteplist=1:10)
signal_B_out<-multispatialCCM::SSR_check_signal(A=v_cli, E=E_B, tau=1,
                                                predsteplist=1:10)

# #Run the CCM test
# #E_A and E_B are the embedding dimensions for A and B.
# tau is the length of time steps used (default is 1)
# 
# #iterations is the number of bootsrap iterations (default 100)
# # Does A "cause" B?
# #Note - increase iterations to 100 for consistant results
CCM_boot_A<-multispatialCCM::CCM_boot(v_emb, v_cli, E_A, tau=1, DesiredL = 10:100, iterations=10)
# # Does B "cause" A?
CCM_boot_B<-multispatialCCM::CCM_boot(v_cli, v_emb, E_B, tau=1, DesiredL = 100:100, iterations=10)
# 
# #Test for significant causal signal
# #See R function for details
(CCM_significance_test<-ccmtest(CCM_boot_A,
                                CCM_boot_B))
# 
# #Plot results
plotxlimits<-range(c(CCM_boot_A$Lobs, CCM_boot_B$Lobs))
# #Plot "A causes B"
plot(CCM_boot_A$Lobs, CCM_boot_A$rho, type="l", col=1, lwd=2,
     xlim=c(plotxlimits[1], plotxlimits[2]), ylim=c(0,1),
     xlab="L", ylab="rho")
# #Add +/- 1 standard error
matlines(CCM_boot_A$Lobs,
         cbind(CCM_boot_A$rho-CCM_boot_A$sdevrho,
               CCM_boot_A$rho+CCM_boot_A$sdevrho),
         lty=3, col=1)
# #Plot "B causes A"
lines(CCM_boot_B$Lobs, CCM_boot_B$rho, type="l", col=2, lty=2, lwd=2)
# #Add +/- 1 standard error
matlines(CCM_boot_B$Lobs,
         cbind(CCM_boot_B$rho-CCM_boot_B$sdevrho,
               CCM_boot_B$rho+CCM_boot_B$sdevrho),
         lty=3, col=2)
legend("topleft",
       c("A causes B", "B causes A"),
       lty=c(1,2), col=c(1,2), lwd=2, bty="n")


######### Try with outcomes 

t <- tbl(con,'psn_raw')
psn_raw <- t  |> collect() |>
  # mutate(scram_recode = if_else(scram_code == 'N',0,1)) |>
  select(eid,harm_score) |>
  janitor::clean_names()
skimr::skim(psn_raw)

ews_df4 <- ews_df3 |> left_join(psn_raw, by = 'eid')

r <- ews_df4 |>
  select(!!sym(org_unit),!!sym(e_date), harm_score,threshold.crossed.count) |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  group_modify(~add_row(.x,!!sym(e_date) := NA, harm_score = NA,threshold.crossed.count = NA)) |> ungroup()

v_harm <- unname(unlist(r$harm_score))
v_thresh <- unname(unlist(r$threshold.crossed.count))

#Calculate optimal E
maxE<-30 #Maximum E to test
#Matrix for storing output
Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
#Loop over potential E values and calculate predictive ability
#of each process for its own dynamics
for(E in 2:maxE) {
  #Uses defaults of looking forward one prediction step (predstep)
  #And using time lag intervals of one time step (tau)
  Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_harm, E=E, predstep=1, tau=1)$rho
  Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_thresh, E=E, predstep=1, tau=1)$rho
}

# ccm_data_out<-multispatialCCM::make_ccm_data()

#Look at plots to find E for each process at which
#predictive ability rho is maximized
matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,
        xlab="E", ylab="rho", lwd=2)
legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")

E_A <- 7
E_B <- 5

#Check data for nonlinear signal that is not dominated by noise
#Checks whether predictive ability of processes declines with
#increasing time distance
#See manuscript and R code for details
signal_A_out<-multispatialCCM::SSR_check_signal(A=v_harm, E=E_A, tau=1,
                                                predsteplist=1:10)
signal_B_out<-multispatialCCM::SSR_check_signal(A=v_thresh, E=E_B, tau=1,
                                                predsteplist=1:10)

#Run the CCM test
#E_A and E_B are the embedding dimensions for A and B.
#tau is the length of time steps used (default is 1)

#iterations is the number of bootsrap iterations (default 100)
# Does A "cause" B?
#Note - increase iterations to 100 for consistant results
tictoc::tic()
CCM_boot_A<-multispatialCCM::CCM_boot(A = v_harm, B = v_thresh, E = E_A, tau=1, iterations=10)
tictoc::toc()
beepr::beep()
# Does B "cause" A?
tictoc::tic()
CCM_boot_B<-multispatialCCM::CCM_boot(A = v_thresh, B = v_harm, E = E_B, tau=1, iterations=10)
tictoc::toc()
beepr::beep()
#Test for significant causal signal
#See R function for details
(CCM_significance_test<-ccmtest(CCM_boot_A,
                                CCM_boot_B))

#Plot results
plotxlimits<-range(c(CCM_boot_A$Lobs, CCM_boot_B$Lobs))
#Plot "A causes B"
plot(CCM_boot_A$Lobs, CCM_boot_A$rho, type="l", col=1, lwd=2,
     xlim=c(plotxlimits[1], plotxlimits[2]), ylim=c(0,1),
     xlab="L", ylab="rho")
#Add +/- 1 standard error
matlines(CCM_boot_A$Lobs,
         cbind(CCM_boot_A$rho-CCM_boot_A$sdevrho,
               CCM_boot_A$rho+CCM_boot_A$sdevrho),
         lty=3, col=1)
#Plot "B causes A"
lines(CCM_boot_B$Lobs, CCM_boot_B$rho, type="l", col=2, lty=2, lwd=2)
#Add +/- 1 standard error
matlines(CCM_boot_B$Lobs,
         cbind(CCM_boot_B$rho-CCM_boot_B$sdevrho,
               CCM_boot_B$rho+CCM_boot_B$sdevrho),
         lty=3, col=2)
legend("topleft",
       c("A causes B", "B causes A"),
       lty=c(1,2), col=c(1,2), lwd=2, bty="n")


######### Try with outcomes SINGLE measures

t <- tbl(con,'psn_raw')
psn_raw <- t  |> collect() |>
  # mutate(scram_recode = if_else(scram_code == 'N',0,1)) |>
  select(eid,harm_score) |>
  janitor::clean_names()
skimr::skim(psn_raw)

single_ews_df4 <- single_ews_df3 |> left_join(psn_raw, by = 'eid')

### Performed over each unit's data

for (unit in unique(single_ews_df4$primary_loc_name)) {
  print(unit)
  # print(nrow(single_ews_df4))
  r <- single_ews_df4 |>
    filter(primary_loc_name == unit) |>
    select(!!sym(org_unit),!!sym(e_date), harm_score, threshold.crossed.count) |>
    drop_na() |> 
    mutate(
      harm_score = case_when(harm_score %in% 1:2 ~ 1,
                             harm_score %in% 3:5 ~ 2,
                             harm_score %in% 6:9 ~ 3)
    )
  print(nrow(r))
  if (nrow(r) >= 100) {
    v_harm <- unname(unlist(r$harm_score))
    v_thresh <- unname(unlist(r$threshold.crossed.count))
    
    #Calculate optimal E
    maxE<-15 #Maximum E to test
    #Matrix for storing output
    Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
    #Loop over potential E values and calculate predictive ability
    #of each process for its own dynamics
    for(E in 2:maxE) {
      #Uses defaults of looking forward one prediction step (predstep)
      #And using time lag intervals of one time step (tau)
      Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_harm, E=E, predstep=1, tau=1)$rho
      Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_thresh, E=E, predstep=1, tau=1)$rho
    }
    #Look at plots to find E for each process at which
    #predictive ability rho is maximized
    matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,main = glue::glue("Unit: {unit}"),
            xlab="E", ylab="rho", lwd=2)
    legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")
    E_A <- which.max(abs(Emat[,"A"]))
    print(E_A)
    E_B <- which.max(abs(Emat[,"B"]))
    print(E_B)
    tryCatch(
      expr = {
        signal_A_out<-multispatialCCM::SSR_check_signal(A=v_harm, E=E_A, tau=1,
                                                        predsteplist=1:10)
      },
      error = function(e){
        signal_A_out <- FALSE
      }
    )
    # print(signal_A_out)
    tryCatch(
      expr = {
        signal_B_out<-multispatialCCM::SSR_check_signal(A=v_thresh, E=E_B, tau=1,
                                                        predsteplist=1:10)
      },
      error = function(e){
        signal_B_out <- FALSE
      }
    )
    # print(signal_B_out)
    
    

    if ((signal_A_out$rho_pre_slope[["Pr(>|t|)"]] <= 0.2) & (signal_B_out$rho_pre_slope[["Pr(>|t|)"]] < 0.2)) {
      #Run the CCM test
      #E_A and E_B are the embedding dimensions for A and B.
      #tau is the length of time steps used (default is 1)
      
      #iterations is the number of bootsrap iterations (default 100)
      # Does A "cause" B?
      #Note - increase iterations to 100 for consistant results
      tictoc::tic()
      CCM_boot_A<-multispatialCCM::CCM_boot(A = v_harm, B = v_thresh, E = E_A, tau=1, iterations=100)
      tictoc::toc()
      beepr::beep()
      # Does B "cause" A?
      tictoc::tic()
      CCM_boot_B<-multispatialCCM::CCM_boot(A = v_thresh, B = v_harm, E = E_B, tau=1, iterations=100)
      tictoc::toc()
      beepr::beep()
      #Test for significant causal signal
      #See R function for details
      CCM_significance_test<-multispatialCCM::ccmtest(CCM_boot_A,
                                      CCM_boot_B)
      print(CCM_significance_test)
    }
    print(glue::glue("Done unit: {unit}"))
  }
}

# r <- single_ews_df4 |>
#   select(!!sym(org_unit),!!sym(e_date), harm_score,threshold.crossed.count) |>
#   drop_na() |>
#   group_by(!!sym(org_unit)) |>
#   arrange(!!sym(e_date), .by_group = TRUE) |>
#   group_modify(~add_row(.x,!!sym(e_date) := NA, harm_score = NA,threshold.crossed.count = NA)) |> ungroup()

# v_harm <- unname(unlist(r$harm_score))
# v_thresh <- unname(unlist(r$threshold.crossed.count))

# #Calculate optimal E
# maxE<-30 #Maximum E to test
# #Matrix for storing output
# Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
# #Loop over potential E values and calculate predictive ability
# #of each process for its own dynamics
# for(E in 2:maxE) {
#   #Uses defaults of looking forward one prediction step (predstep)
#   #And using time lag intervals of one time step (tau)
#   Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_harm, E=E, predstep=1, tau=1)$rho
#   Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_thresh, E=E, predstep=1, tau=1)$rho
# }
# 
# # ccm_data_out<-multispatialCCM::make_ccm_data()
# 
# #Look at plots to find E for each process at which
# #predictive ability rho is maximized
# matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,
#         xlab="E", ylab="rho", lwd=2)
# legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")

# E_A <- 7
# E_B <- 15

#Check data for nonlinear signal that is not dominated by noise
#Checks whether predictive ability of processes declines with
#increasing time distance
#See manuscript and R code for details
# signal_A_out<-multispatialCCM::SSR_check_signal(A=v_harm, E=E_A, tau=1,
#                                                 predsteplist=1:10)
# signal_B_out<-multispatialCCM::SSR_check_signal(A=v_thresh, E=E_B, tau=1,
#                                                 predsteplist=1:10)

#Run the CCM test
#E_A and E_B are the embedding dimensions for A and B.
#tau is the length of time steps used (default is 1)

#iterations is the number of bootsrap iterations (default 100)
# Does A "cause" B?
#Note - increase iterations to 100 for consistant results
tictoc::tic()
CCM_boot_A<-multispatialCCM::CCM_boot(A = v_harm, B = v_thresh, E = E_A, tau=1, iterations=10)
tictoc::toc()
beepr::beep()
# Does B "cause" A?
tictoc::tic()
CCM_boot_B<-multispatialCCM::CCM_boot(A = v_thresh, B = v_harm, E = E_B, tau=1, iterations=10)
tictoc::toc()
beepr::beep()
#Test for significant causal signal
#See R function for details
(CCM_significance_test<-ccmtest(CCM_boot_A,
                                CCM_boot_B))

#Plot results
plotxlimits<-range(c(CCM_boot_A$Lobs, CCM_boot_B$Lobs))
#Plot "A causes B"
plot(CCM_boot_A$Lobs, CCM_boot_A$rho, type="l", col=1, lwd=2,
     xlim=c(plotxlimits[1], plotxlimits[2]), ylim=c(0,1),
     xlab="L", ylab="rho")
#Add +/- 1 standard error
matlines(CCM_boot_A$Lobs,
         cbind(CCM_boot_A$rho-CCM_boot_A$sdevrho,
               CCM_boot_A$rho+CCM_boot_A$sdevrho),
         lty=3, col=1)
#Plot "B causes A"
lines(CCM_boot_B$Lobs, CCM_boot_B$rho, type="l", col=2, lty=2, lwd=2)
#Add +/- 1 standard error
matlines(CCM_boot_B$Lobs,
         cbind(CCM_boot_B$rho-CCM_boot_B$sdevrho,
               CCM_boot_B$rho+CCM_boot_B$sdevrho),
         lty=3, col=2)
legend("topleft",
       c("A causes B", "B causes A"),
       lty=c(1,2), col=c(1,2), lwd=2, bty="n")
#########################################################
#########################################################
########## Climate data
#########################################################
#########################################################

climate_survey_df <- readxl::read_excel(
  path = '/Volumes/calculon/event_reporting/old_culture_data/culture\ Data\ Set\ From\ 05-13-13\ CROSS\ MAPPED.xlsx',
  sheet = 'FINAL')

cmb_df <- left_join(ews_df4, climate_survey_df, by = c(''))