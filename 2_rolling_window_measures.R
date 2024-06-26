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

t <- get_clean_vecs(
  vCol = 'composite_climate_vec', 
  minFacilityReport = 100, 
  winSize = 1, 
  sys_source = 'nrc',
  con = con) |> select(eid,cos_sim_rw_composite_climate_vec_1)
u <- get_clean_vecs(
  vCol = 'embedding', 
  minFacilityReport = 100, 
  winSize = 1, 
  sys_source = 'nrc',
  con = con)

v <- full_join(t,u, by = 'eid')

table(nrc_df$facility)


#########################################################
#########################################################
########## Attempt ESW analyses
#########################################################
#########################################################
library(EWSmethods)

key <- v |>
  drop_na() |>
  #select(facility,event_date2,eid,cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1) |>
  group_by(facility) |>
  arrange(event_date2, .by_group = TRUE) |>
  mutate(t = row_number()) |> ungroup()
  
ews_df2 <- v |>
  drop_na() |>
  select(facility,event_date2,cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1) |>
  group_by(facility) |>
  arrange(event_date2, .by_group = TRUE) |>
  select(-event_date2) |>
  mutate(t = row_number()) |> 
  relocate(t) |> 
  tidyr::nest() |>
  mutate(ews_results = purrr::map(data, ~EWSmethods::multiEWS(data = .,
                                                           metrics = c("meanAR","maxAR","meanSD","maxSD","eigenMAF","mafAR","mafSD","pcaAR","pcaSD","eigenCOV","maxCOV","mutINFO"),
                                                           method = "expanding",
                                                           burn_in = 50,
                                                           threshold = 2)))

ews_df3 <- purrr::pmap_dfr(ews_df2, ~ data.frame(..3$EWS$raw) |>
                       mutate(facility = ..1) |>
                       group_by(facility,time) |>
                       summarise(threshold.crossed.count = sum(threshold.crossed))) |>
  left_join(key,by = c('facility','time' = 't'))

plot(ews_df2$ews_results[[1]],  y_lab = "Density")

for (i in seq(from = 1, to = length(ews_df),by=1)) {
  p<-plot(ews_df[[i]],  y_lab = "Density")
  ggsave(here::here('plots',glue::glue("EWS_{i}.png")),p)
}



# for (metric in metrics) {
#   print(metric)
#   EWS_model <- EWSmethods::uniEWS(data = psn_bert_vader_df |> select(t,!!sym(metric)),
#                                   metrics = c("ar1","SD","skew"),
#                                   method = "expanding",
#                                   burn_in = 50,
#                                   threshold = 2)
#   threshold_hits <- EWS_model$EWS |>
#     group_by(time) |> summarise('{metric}.threshold.crossed.count' := sum(threshold.crossed))
#   print(nrow(threshold_hits))
#   metrics_dfs[[metric]] <- threshold_hits
# }
# 
# cmbd_ind_vars <- bind_cols(metrics_dfs) |>
#   rename(time = time...1) |>
#   select(-contains('...'))


#########################################################
#########################################################
########## Attempt CCM analyses
#########################################################
#########################################################
library(multispatialCCM)

######### First try
# r <- v |>
#   select(facility,event_date2, cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1) |>
#   drop_na() |>
#   group_by(facility) |>
#   arrange(event_date2, .by_group = TRUE) |>
#   group_modify(~add_row(.x,event_date2 = NA, cos_sim_rw_composite_climate_vec_1 = NA,cos_sim_rw_embedding_1 = NA)) |> ungroup()
# v_emb <- unname(unlist(r$cos_sim_rw_embedding_1))
# v_cli <- unname(unlist(r$cos_sim_rw_composite_climate_vec_1))
# 
# #Calculate optimal E
# maxE<-10 #Maximum E to test
# #Matrix for storing output
# Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
# #Loop over potential E values and calculate predictive ability
# #of each process for its own dynamics
# for(E in 2:maxE) {
#   #Uses defaults of looking forward one prediction step (predstep)
#   #And using time lag intervals of one time step (tau)
#   Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_emb, E=E, predstep=1, tau=1)$rho
#   Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_cli, E=E, predstep=1, tau=1)$rho
# }
# 
# # ccm_data_out<-multispatialCCM::make_ccm_data()
# 
# #Look at plots to find E for each process at which
# #predictive ability rho is maximized
# matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,
#         xlab="E", ylab="rho", lwd=2)
# legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")
# 
# E_A <- 5
# E_B <- 5
# 
# #Check data for nonlinear signal that is not dominated by noise
# #Checks whether predictive ability of processes declines with
# #increasing time distance
# #See manuscript and R code for details
# signal_A_out<-multispatialCCM::SSR_check_signal(A=v_emb, E=E_A, tau=1,
#                                                 predsteplist=1:10)
# signal_B_out<-multispatialCCM::SSR_check_signal(A=v_cli, E=E_B, tau=1,
#                                                 predsteplist=1:10)
# 
# #Run the CCM test
# #E_A and E_B are the embedding dimensions for A and B.
# #tau is the length of time steps used (default is 1)
# 
# #iterations is the number of bootsrap iterations (default 100)
# # Does A "cause" B?
# #Note - increase iterations to 100 for consistant results
# CCM_boot_A<-multispatialCCM::CCM_boot(v_emb, v_cli, E_A, tau=1, DesiredL = 10:100, iterations=10)
# # Does B "cause" A?
# CCM_boot_B<-multispatialCCM::CCM_boot(v_cli, v_emb, E_B, tau=1, DesiredL = 100:100, iterations=10)
# 
# #Test for significant causal signal
# #See R function for details
# (CCM_significance_test<-ccmtest(CCM_boot_A,
#                                 CCM_boot_B))
# 
# #Plot results
# plotxlimits<-range(c(CCM_boot_A$Lobs, CCM_boot_B$Lobs))
# #Plot "A causes B"
# plot(CCM_boot_A$Lobs, CCM_boot_A$rho, type="l", col=1, lwd=2,
#      xlim=c(plotxlimits[1], plotxlimits[2]), ylim=c(0,1),
#      xlab="L", ylab="rho")
# #Add +/- 1 standard error
# matlines(CCM_boot_A$Lobs,
#          cbind(CCM_boot_A$rho-CCM_boot_A$sdevrho,
#                CCM_boot_A$rho+CCM_boot_A$sdevrho),
#          lty=3, col=1)
# #Plot "B causes A"
# lines(CCM_boot_B$Lobs, CCM_boot_B$rho, type="l", col=2, lty=2, lwd=2)
# #Add +/- 1 standard error
# matlines(CCM_boot_B$Lobs,
#          cbind(CCM_boot_B$rho-CCM_boot_B$sdevrho,
#                CCM_boot_B$rho+CCM_boot_B$sdevrho),
#          lty=3, col=2)
# legend("topleft",
#        c("A causes B", "B causes A"),
#        lty=c(1,2), col=c(1,2), lwd=2, bty="n")

t <- tbl(con,'nrc_raw')
nrc_raw <- t  |> collect() |>
  mutate(scram_recode = if_else(scram_code == 'N',0,1)) |>
  select(eid,scram_recode)

ews_df4 <- ews_df3 |> left_join(nrc_raw, by = 'eid')

r <- ews_df4 |>
  select(facility,event_date2, scram_recode, threshold.crossed.count) |>
  drop_na() |>
  group_by(facility) |>
  arrange(event_date2, .by_group = TRUE) |>
  group_modify(~add_row(.x,event_date2 = NA, scram_recode = NA,threshold.crossed.count = NA)) |> ungroup()
v_scram <- unname(unlist(r$scram_recode))
v_thresh <- unname(unlist(r$threshold.crossed.count))

#Calculate optimal E
maxE<-10 #Maximum E to test
#Matrix for storing output
Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
#Loop over potential E values and calculate predictive ability
#of each process for its own dynamics
for(E in 2:maxE) {
  #Uses defaults of looking forward one prediction step (predstep)
  #And using time lag intervals of one time step (tau)
  Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_scram, E=E, predstep=1, tau=1)$rho
  Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_thresh, E=E, predstep=1, tau=1)$rho
}

# ccm_data_out<-multispatialCCM::make_ccm_data()

#Look at plots to find E for each process at which
#predictive ability rho is maximized
matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,
        xlab="E", ylab="rho", lwd=2)
legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")

E_A <- 3
E_B <- 4

#Check data for nonlinear signal that is not dominated by noise
#Checks whether predictive ability of processes declines with
#increasing time distance
#See manuscript and R code for details
signal_A_out<-multispatialCCM::SSR_check_signal(A=v_scram, E=E_A, tau=1,
                               predsteplist=1:10)
signal_B_out<-multispatialCCM::SSR_check_signal(A=v_thresh, E=E_B, tau=1,
                               predsteplist=1:10)

#Run the CCM test
#E_A and E_B are the embedding dimensions for A and B.
#tau is the length of time steps used (default is 1)

#iterations is the number of bootsrap iterations (default 100)
# Does A "cause" B?
#Note - increase iterations to 100 for consistant results
CCM_boot_A<-multispatialCCM::CCM_boot(v_scram, E_A, tau=1, DesiredL = 10:100, iterations=10)
# Does B "cause" A?
CCM_boot_B<-multispatialCCM::CCM_boot(v_thresh, E_B, tau=1, DesiredL = 10:100, iterations=10)

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
