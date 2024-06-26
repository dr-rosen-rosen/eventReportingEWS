###################################
###### Read in and merge all of the
###### event reports files by year; 
###### and separate out links and 
###### report IDs
###################################
library(tidyverse)
# config <- config::get()
# files <- list.files(
#   path = here::here('/Volumes/calculon/event_reporting/phmsa/link_pages'),
#   pattern = "*.csv",
#   full.names = TRUE,
#   include.dirs = TRUE,
#   recursive = TRUE
#   )
# # phsma_event_df <- lapply(files, read.csv) |>
#   bind_rows() |>
#   filter(str_detect(Incident.Number, '<A HREF')) |>
#   mutate(report_no = str_extract(Incident.Number, "(?<=target=\"_blank\">).*(?=</A>)"),
#          doc_link = str_extract(Incident.Number, "(?<=HREF = ).*(?= target)"))
# write.csv(phsma_event_df,'phmsa_links.csv')


# files_test_full3 <- list.files(
#   path = here::here('/Volumes/calculon/event_reporting/phmsa/reports3'),
#   pattern = '*.pdf',
#   full.names = TRUE
# )

### Set up DB for pdf scraping
con <- DBI::dbConnect(RPostgres::Postgres(),
                   dbname   = config$db_name,
                   host     = 'localhost',
                   port     = config$port,
                   user     = config$db_user,
                   password = config$db_pw)
# phsma_event_df <- phsma_event_df |> janitor::clean_names()
# DBI::dbWriteTable(con,"event_data",phsma_event_df, overwrite = TRUE)

# get reports with non-null narrative fields
# t <- dplyr::tbl(con, 'event_data')
# done_reports <- t |> filter(!is.null(narrative1)) |> dplyr::collect()
# done_report_nos <- unique(done_reports$report_no)

# file_links_df <- data.frame(pdf_link = files_test_full3, report_no = stringr::str_remove(files_test3, '.pdf')) |>
#   filter(!(report_no %in% done_report_nos))
# write.csv(file_links_df, 'file_list_3.csv')
# 
# length(files2)
# files_cmb <- c(files1,files2,files3)
# files_cmb <- stringr::str_remove(files_cmb, '.pdf')
# phsma_event_df2 <- phsma_event_df |> filter(!(report_no %in% files_cmb))
# write.csv(phsma_event_df2,'phmsa_links_03-07-2024.csv')

# pull report data from db
# t <- dplyr::tbl(con,'event_data')
# phmsa_df <- t |> dplyr::collect()
# phmsa_df <- phmsa_df |> 
#   mutate(
#     narrative1 = str_remove(narrative1,'NO COMMENTS PROVIDED.'),
#     narrative2 = str_remove(narrative2,'NO COMMENTS PROVIDED.'),
#     narrative1 = str_remove(narrative1,'NO COMMENTS PROVIDED'),
#     narrative2 = str_remove(narrative2,'NO COMMENTS PROVIDED'),
#     narrative1 = str_remove(narrative1,'n/a'),
#     narrative2 = str_remove(narrative2,'n/a'),
#     ) |>
#   filter(
#     !(is.na(narrative1) & is.na(narrative2)),
#     !str_detect(narrative1,'PART VIII – CONTACT INFORMATION')
#   ) |>
#   unite('cmbd_narrative',narrative1:narrative2, sep = ' ', na.rm = TRUE) |>
#   filter(cmbd_narrative != '',cmbd_narrative != ' ')
# 
# write.csv(phmsa_df,'phmsa_reports_combined_03-11-2024.csv')



######

