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

q <- DBI::sqlInterpolate(
  conn = con,
  glue::glue_sql(
    "SELECT et.eid, et.embedding, rt.facility, rt.event_date, rt.event_date2 FROM embeddings as et LEFT JOIN link_table as lt USING (eid) LEFT JOIN ?raw_table as rt USING (eid) WHERE lt.system_source = ?sys;"),
  raw_table = DBI::dbQuoteIdentifier(con, 'nrc_raw'),
  sys = 'nrc'
)
nrc_embeds <- DBI::dbGetQuery(conn = con, q) 
# saveRDS(nrc_embeds,'nrc_embeds.rds')

# Create
nrc_df <- nrc_embeds |>
  group_by(facility) |>
  filter(n() >= 100) |> # drop facilities with low n
  ungroup() |>
  mutate(event_date2 = lubridate::ymd(event_date2)) |>
  filter(between(lubridate::year(event_date2),1999,2023)) |> 
  group_by(facility) |>
  arrange(event_date2, .by_group = TRUE) |>
  group_modify(
    ~ get_rw_cs(
      df = .x,
      winSize = 1,
      vCol = 'embedding'
    )
  ) |> ungroup()

