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

# for (f in unique(v$facility)) {
#   p <- v |>
#     filter(facility == f) |>
#     ggplot(aes(x = event_date2)) + 
#     geom_line(aes(y = cos_sim_rw_composite_climate_vec_1), color = 'red') +
#     geom_line(aes(y = cos_sim_rw_embedding_1), color = 'black') +
#     ggthemes::theme_clean() + labs(title = facility)
#   ggsave(here::here('plots',glue::glue("nrc_faciility_sim_{f}.png")))
# }
# 
# v |> filter(facility == 'susquehanna') |>
#   ggplot(aes(x = event_date2)) + 
#   geom_line(aes(y = cos_sim_rw_composite_climate_vec_1), color = 'red') +
#   geom_line(aes(y = cos_sim_rw_embedding_1), color = 'black') +
#   ggthemes::theme_clean() + labs(title = facility)
# 
table(nrc_df$facility)


#########################################################
#########################################################
########## Attempt ESW analyses
#########################################################
#########################################################
library(EWSmethods)

ews_df <- v |>
  # filter(facility == 'susquehanna') |>
  drop_na() |>
  # relocate(event_date2, .after = last_col()) |>
  select(facility,event_date2,cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1) |>
  group_by(facility) |>
  arrange(event_date2, .by_group = TRUE) |>
  mutate(t = row_number()) |> 
  relocate(t) |> 
  group_map(~EWSmethods::multiEWS(data = .x |> select(-event_date2),
                                     metrics = c("meanAR","maxAR","meanSD","maxSD","eigenMAF","mafAR","mafSD","pcaAR","pcaSD","eigenCOV","maxCOV","mutINFO"),
                                     method = "expanding",
                                     burn_in = 50,
                                     threshold = 2))

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

plot(ews_df[which(ews_df2$facility == 'beaver valley')]['ews_results'],  y_lab = "Density")

length(ews_df)

for (i in seq(from = 1, to = length(ews_df),by=1)) {
  p<-plot(ews_df[[i]],  y_lab = "Density")
  ggsave(here::here('plots',glue::glue("EWS_{i}.png")),p)
}

view(ews_df[[1]]$EWS$raw)
table(ews_df[[1]]$EWS$raw$threshold.crossed)

plot(ews_df[[1]],  y_lab = "Density")
m.ews <- EWSmethods::multiEWS(data = ews_df,
           metrics = c("meanAR","maxAR","meanSD","maxSD","eigenMAF","mafAR","mafSD","pcaAR","pcaSD","eigenCOV","maxCOV","mutINFO"),
           method = "expanding",
           burn_in = 50,
           threshold = 2)
plot(m.ews,  y_lab = "Density")
    



#########################################################
#########################################################
########## Attempt CCM analyses
#########################################################
#########################################################
library(multispatialCCM)
# r <- v |> 
#   filter(facility == 'columbia generating station') |>
#   select(event_date2,cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1)# |>
#   # drop_na() |>
#   # complete(event_date2 = seq.Date(min(event_date2), max(event_date2), by = "month")) #|>
#   # fill_na(cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1)

r <- v |>
  select(facility,event_date2, cos_sim_rw_composite_climate_vec_1,cos_sim_rw_embedding_1) |>
  drop_na() |>
  group_by(facility) |>
  arrange(event_date2, .by_group = TRUE) |>
  group_modify(~add_row(.x,event_date2 = NA, cos_sim_rw_composite_climate_vec_1 = NA,cos_sim_rw_embedding_1 = NA)) |> ungroup()
v_emb <- unname(unlist(r$cos_sim_rw_embedding_1))
v_cli <- unname(unlist(r$cos_sim_rw_composite_climate_vec_1))

#Calculate optimal E
maxE<-10 #Maximum E to test
#Matrix for storing output
Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
#Loop over potential E values and calculate predictive ability
#of each process for its own dynamics
for(E in 2:maxE) {
  #Uses defaults of looking forward one prediction step (predstep)
  #And using time lag intervals of one time step (tau)
  Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_emb, E=E, predstep=1, tau=1)$rho
  Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_cli, E=E, predstep=1, tau=1)$rho
}

# ccm_data_out<-multispatialCCM::make_ccm_data()

#Look at plots to find E for each process at which
#predictive ability rho is maximized
matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,
        xlab="E", ylab="rho", lwd=2)
legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")

E_A <- 5
E_B <- 5

#Check data for nonlinear signal that is not dominated by noise
#Checks whether predictive ability of processes declines with
#increasing time distance
#See manuscript and R code for details
signal_A_out<-multispatialCCM::SSR_check_signal(A=v_emb, E=E_A, tau=1,
                               predsteplist=1:10)
signal_B_out<-multispatialCCM::SSR_check_signal(A=v_cli, E=E_B, tau=1,
                               predsteplist=1:10)

#Run the CCM test
#E_A and E_B are the embedding dimensions for A and B.
#tau is the length of time steps used (default is 1)

#iterations is the number of bootsrap iterations (default 100)
# Does A "cause" B?
#Note - increase iterations to 100 for consistant results
CCM_boot_A<-multispatialCCM::CCM_boot(v_emb, v_cli, E_A, tau=1, iterations=10)
# Does B "cause" A?
CCM_boot_B<-multispatialCCM::CCM_boot(v_cli, v_emb, E_B, tau=1, iterations=10)

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
