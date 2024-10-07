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
org_unit <- 'reporting_railroad_code'
e_date <- 'event_date'
sys_source <- 'rail'
winSize <- 5
minFacilityReport <- 100

# Get similarity metrics
tictoc::tic()
t <- get_clean_vecs(
  vCol = 'composite_climate_vec', 
  minFacilityReport = minFacilityReport, 
  winSize = winSize, 
  sys_source = sys_source,
  org_unit = org_unit, 
  e_date = e_date,
  con = con) |> select(eid,!!sym(glue::glue('cos_sim_rw_composite_climate_vec_{winSize}')))
tictoc::toc()
tictoc::tic()
u <- get_clean_vecs(
  vCol = 'embedding', 
  minFacilityReport = minFacilityReport, 
  winSize = winSize, 
  sys_source = sys_source,
  org_unit = org_unit, 
  e_date = e_date,
  con = con)
tictoc::toc()
v <- full_join(t,u, by = 'eid')

saveRDS(
  v |> select(-starts_with('rw_'),-embedding),
  glue::glue('v_{sys_source}_{winSize}.rds'))
rr_codes <- as.data.frame(table(v$reporting_railroad_code))

#########################################################
#########################################################
########## ESW analyses
#########################################################
#########################################################
library(EWSmethods)
library(future)
library(furrr)

key <- v |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  mutate(t = row_number()) |> ungroup()

tictoc::tic()
future::plan(#multicore,
             multisession, 
             workers = future::availableCores())
ews_df2 <- v |>
  drop_na() |>
  select(!!sym(org_unit),!!sym(e_date),
         !!sym(glue::glue('cos_sim_rw_composite_climate_vec_{winSize}')),
         !!sym(glue::glue('cos_sim_rw_embedding_{winSize}'))) |>
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
future::plan(sequential)
saveRDS(ews_df2, glue::glue("{sys_source}_{winSize}_ews_df2.rds"))
# ews_df2 <- readRDS(glue::glue("{sys_source}_{winSize}_ews_df2.rds"))
beepr::beep()

ews_df3 <- purrr::pmap_dfr(ews_df2, ~ data.frame(..3$EWS$raw) |>
                       mutate(!!sym(org_unit) := ..1) |>
                       group_by(!!sym(org_unit),time) |>
                       summarise(threshold.crossed.count = sum(threshold.crossed))) |>
  left_join(key,by = c(org_unit,'time' = 't'))

plot(ews_df2$ews_results[[40]],  y_lab = "Density")

for (i in seq(from = 1, to = nrow(ews_df2),by=1)) {
  p<-plot(ews_df2$ews_results[[i]],  y_lab = "Density")
  ggsave(here::here('plots',glue::glue("EWS_{sys_source}_{winSize}_{i}.png")),p)
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
########## CCM analyses
#########################################################
#########################################################
library(multispatialCCM)

######### Try with outcomes 

t <- tbl(con,'rail_raw')
rail_raw <- t  |> collect() |>
  select(eid,Total.Persons.Killed,Total.Persons.Injured,Total.Damage.Cost) |>
  janitor::clean_names()
skimr::skim(rail_raw)

ews_df4 <- ews_df3 |> left_join(rail_raw, by = 'eid')
min(ews_df4$total_damage_cost, na.rm = TRUE)

r <- ews_df4 |>
  select(!!sym(org_unit),!!sym(e_date), 
         total_persons_killed,total_persons_injured,total_damage_cost, threshold.crossed.count,
         !!sym(glue::glue('cos_sim_rw_composite_climate_vec_{winSize}')),
         !!sym(glue::glue('cos_sim_rw_embedding_{winSize}'))) |>
  drop_na() |>
  group_by(!!sym(org_unit)) |>
  arrange(!!sym(e_date), .by_group = TRUE) |>
  group_modify(~add_row(.x,!!sym(e_date) := NA, total_persons_killed = NA, total_persons_injured = NA,
                        total_damage_cost = NA,threshold.crossed.count = NA,
                        !!sym(glue::glue('cos_sim_rw_composite_climate_vec_{winSize}')) := NA,
                        !!sym(glue::glue('cos_sim_rw_embedding_{winSize}')) := NA
                        )) |> ungroup()
v_killed <- unname(unlist(r$total_persons_killed))
v_injured <- unname(unlist(r$total_persons_injured))
v_cost <- unname(unlist(r$total_damage_cost))
v_thresh <- unname(unlist(r$threshold.crossed.count))
v_rawEmbed <- unname(unlist(r |> select(!!sym(glue::glue('cos_sim_rw_embedding_{winSize}')))))
v_rawClim <- unname(unlist(r |> select(!!sym(glue::glue('cos_sim_rw_composite_climate_vec_{winSize}')))))

#Calculate optimal E
maxE<-10 #Maximum E to test
#Matrix for storing output
Emat<-matrix(nrow=maxE-1, ncol=2); colnames(Emat)<-c("A", "B")
#Loop over potential E values and calculate predictive ability
#of each process for its own dynamics
for(E in 2:maxE) {
  #Uses defaults of looking forward one prediction step (predstep)
  #And using time lag intervals of one time step (tau)
  # Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_cost, E=E, predstep=1, tau=1)$rho
  # Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_rawEmbed, E=E, predstep=1, tau=1)$rho
  Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_injured, E=E, predstep=1, tau=1)$rho
  Emat[E-1,"A"]<-multispatialCCM::SSR_pred_boot(A=v_killed, E=E, predstep=1, tau=1)$rho
  # Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_thresh, E=E, predstep=1, tau=1)$rho
  # Emat[E-1,"B"]<-multispatialCCM::SSR_pred_boot(A=v_rawClim, E=E, predstep=1, tau=1)$rho
}
beepr::beep()
# ccm_data_out<-multispatialCCM::make_ccm_data()

#Look at plots to find E for each process at which
#predictive ability rho is maximized
matplot(2:maxE, Emat, type="l", col=1:2, lty=1:2,
xlab="E", ylab="rho", lwd=2)
legend("bottomleft", c("A", "B"), lty=1:2, col=1:2, lwd=2, bty="n")

E_A <- 10 # cost
E_A <- 10 # killed
E_A <- 3 # injured
E_B <- 10 # thresh
E_B <- 10 # rawEmbed
E_B <- 10 # rawClim

#Check data for nonlinear signal that is not dominated by noise
#Checks whether predictive ability of processes declines with
#increasing time distance
#See manuscript and R code for details
signal_A_out<-multispatialCCM::SSR_check_signal(A=v_killed, E=E_A, tau=1,
                               predsteplist=1:10)
signal_B_out<-multispatialCCM::SSR_check_signal(A=v_rawEmbed, E=E_B, tau=1,
                               predsteplist=1:10)
beepr::beep()
#Run the CCM test
#E_A and E_B are the embedding dimensions for A and B.
#tau is the length of time steps used (default is 1)

#iterations is the number of bootsrap iterations (default 100)
# Does A "cause" B?
#Note - increase iterations to 100 for consistant results
tictoc::tic()
CCM_boot_A<-multispatialCCM::CCM_boot(A = v_cost, B = v_rawClim, E = E_A, tau=1, iterations=10)
tictoc::toc()
beepr::beep()
# Does B "cause" A?
tictoc::tic()
CCM_boot_B<-multispatialCCM::CCM_boot(A = v_rawClim, B = v_cost, E = E_B, tau=1, iterations=10)
tictoc::toc()

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
