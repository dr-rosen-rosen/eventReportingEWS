#####
# Build DBS

# phmsa built on scrapping (and migrated to calculon)

# nrc
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = 'nrc_events',
                      host     = '192.168.7.212',
                      port     = 5424,
                      user     = 'postgres',
                      password = 'LetMeIn21')
# DBI::dbExecute(conn = con, "CREATE EXTENSION vector;")
nrc_events <- read.csv(here::here(config$nrc_data_path,config$nrc_events_file)) |> select(-X)
DBI::dbWriteTable(conn = con, 'events_raw', nrc_events, overwrite = TRUE, row.names = FALSE)
nrc_liwc <- read.csv(here::here(config$nrc_data_path,config$nrc_liwc_file))
nrc_liwc <- nrc_liwc[,c('event_num',names(nrc_liwc)[23:139])]
DBI::dbWriteTable(conn = con, 'liwc', nrc_liwc, overwrite = TRUE, row.names = FALSE)
nrc_emo_voc <- read.csv(here::here(config$nrc_data_path, config$nrc_emo_voc_file)) |>
  select(-TextID,-Segment,-SegmentID)
nrc_emo_voc$event_num <- nrc_liwc$event_num
DBI::dbWriteTable(conn = con, 'emo_voc', nrc_emo_voc, overwrite = TRUE, row.names = FALSE)
nrc_butter <- read.csv(here::here(config$nrc_data_path,config$nrc_butter_file)) |>
  select(-TextID,-Segment,-SegmentID)
nrc_butter$event_num <- nrc_liwc$event_num
DBI::dbWriteTable(conn = con, 'butter', nrc_butter, overwrite = TRUE, row.names = FALSE)

# rail
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = 'rail_events',
                      host     = '192.168.7.212',
                      port     = 5424,
                      user     = 'postgres',
                      password = 'LetMeIn21')
# DBI::dbExecute(conn = con, "CREATE EXTENSION vector;")
rail_events <- read.csv(here::here(config$rail_data_path,config$rail_events_file))
DBI::dbWriteTable(conn = con, 'events_raw', rail_events, overwrite = TRUE, row.names = FALSE)

rail_liwc <- read.csv(here::here(config$rail_data_path,config$rail_liwc_file))
rail_liwc <- rail_liwc[,c('Accident.Number',names(rail_liwc)[163:279])]
DBI::dbWriteTable(conn = con, 'liwc', rail_liwc, overwrite = TRUE, row.names = FALSE)
rail_emo_voc <- read.csv(here::here(config$rail_data_path, config$rail_emo_voc_file)) |>
  select(-TextID,-Segment,-SegmentID)
rail_emo_voc$Accident.Number <- rail_liwc$Accident.Number
DBI::dbWriteTable(conn = con, 'emo_voc', rail_emo_voc, overwrite = TRUE, row.names = FALSE)

rail_butter <- read.csv(here::here(config$rail_data_path,config$rail_butter_file)) |>
  select(-TextID,-Segment,-SegmentID)
rail_butter$Accident.Number <- nrc_liwc$Accdient.Number
DBI::dbWriteTable(conn = con, 'butter', rail_butter, overwrite = TRUE, row.names = FALSE)
# asrs
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = 'asrs_events',
                      host     = '192.168.7.212',
                      port     = 5424,
                      user     = 'postgres',
                      password = 'LetMeIn21')
DBI::dbExecute(conn = con, "CREATE EXTENSION vector;")
rail_events <- readxl::read_excel(here::here(config$asrs_data_path,config$asrs_events_file))
DBI::dbWriteTable(conn = con, 'asrs_events_raw', rail_events, overwrite = TRUE, row.names = FALSE)

DBI::dbDisconnect(conn = con)
