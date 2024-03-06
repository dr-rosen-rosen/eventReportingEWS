###################################
###### Read in and merge all of the
###### event reports files by year; 
###### and separate out links and 
###### report IDs
###################################

files <- list.files(
  path = here::here('/Volumes/calculon/event_reporting/phmsa/link_pages'),
  pattern = "*.csv",
  full.names = TRUE,
  include.dirs = TRUE,
  recursive = TRUE
  )
test <- lapply(files, read.csv) |>
  bind_rows() |>
  filter(str_detect(Incident.Number, '<A HREF')) |>
  mutate(report_no = str_extract(Incident.Number, "(?<=target=\"_blank\">).*(?=</A>)"),
         doc_link = str_extract(Incident.Number, "(?<=HREF = ).*(?= target)"))
write.csv(test,'phmsa_links.csv')

files <- list.files(
  path = here::here('/Volumes/calculon/event_reporting/phmsa/reports'),
  pattern = "*.pdf",
  full.names = FALSE#,
  # include.dirs = TRUE,
  # recursive = TRUE
)
files <- stringr::str_remove(files, '.pdf')
length(files)

test2 <- test |> filter(report_no %in% files)
