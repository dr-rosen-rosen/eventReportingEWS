#####
# Build DBs

#
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = 'event_reporting',
                      host     = '192.168.7.212',
                      port     = 5424,
                      user     = 'postgres',
                      password = 'LetMeIn21')

# # create link file
# create_link_table(con)
# 
# # nrc
# nrc_df <- nrc_df |> select(-X)
# nrc_df <- updateLinkTable(
#   con = con,
#   df = nrc_df,
#   system_source = 'nrc',
#   return_eid = TRUE
# )
# create_raw_table(
#   con = con, 
#   df = nrc_df[,1:23], 
#   table_name = "nrc_raw",
#   update_values = TRUE)

# asrs

# asrs_df <- asrs_df |> select(-X)
# asrs_df <- updateLinkTable(
#   con = con,
#   df = asrs_df,
#   sys_source = 'asrs',
#   return_eid = TRUE
# )
# 
# create_raw_table(
#   con = con,
#   df = asrs_df[,1:123], # update end point
#   table_name = "asrs_raw",
#   update_values = TRUE)

# rail
# rail_df <- rail_df |> select(-X)
# rail_df <- updateLinkTable(
#   con = con,
#   df = rail_df,
#   sys_source = 'rail',
#   return_eid = TRUE
# )
# create_raw_table(
#   con = con,
#   df = rail_df[,1:162], # update end point
#   table_name = "rail_raw",
#   update_values = TRUE)

# phmsa

# phmsa_df <- phmsa_df |> select(-X,-X.1)
# phmsa_df <- updateLinkTable(
#   con = con,
#   df = phmsa_df,
#   sys_source = 'phmsa',
#   return_eid = TRUE
# )
# create_raw_table(
#   con = con, 
#   df = phmsa_df[,1:13], # update end point
#   table_name = "phmsa_raw",
#   update_values = TRUE)

# psn

psn_df <- ...
psn_df <- updateLinkTable(
    con = con,
    df = psn_df,
    sys_source = 'psn',
    return_eid = TRUE
  )

create_raw_table(
  con = con,
  df = psn_df, # update end point
  table_name = "psn_raw",
  update_values = TRUE)