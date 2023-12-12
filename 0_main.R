################################################################
################################################################
###################
################### Scripts for applying EWS metrics to event 
################### reporting data.
###################
################################################################
################################################################

library(tidyverse)
library(quanteda)
library(ggplot2)
library(EWSmethods)

get_and_clean_ASRS <- function(asrs.files) {
  dfs <- NULL
  for (f_path in asrs.files) {
    l1 <- readxl::read_xlsx(asrs.files[1],
                            n_max = 0,
                            .name_repair = 'minimal') |> names()
    l2 <- readxl::read_xlsx(asrs.files[1],
                            skip = 1,
                            n_max = 0,
                            .name_repair = 'minimal') |> names()
    if (length(l1) == length(l2)) {
      header <- mapply(FUN = function(x,y) {
        paste0(x,y,sep = '.')}, x = l1,y = l2
      )
    }
    df <- readxl::read_xlsx(f_path,
                            skip = 3,
                            col_names = header,
                            range = readxl::cell_cols("A:DU")) |>
      janitor::clean_names()
    if (is.null(dfs)) {
      dfs <- df
    } else {dfs <- bind_rows(dfs,df)}
  }
  dfs <- dfs |>
    filter(!is.na(acn), acn != 'ACN') |>
    mutate(
      date = as.Date(paste0(substr(time_date,start = 1, stop = 4),'-',substr(time_date,start = 5, stop = 6),'-01'),format = "%Y-%m-%d")
    ) |>
    unite('cmbd_narrative',starts_with('report_'),sep = " ", na.rm = TRUE,remove = FALSE) |>
    mutate(tot_wc = stringr::str_count(cmbd_narrative, '\\w+'))
  return(dfs)
}

df <- get_and_clean_ASRS(
  asrs.files = list.files(path = '/Volumes/LaCie/ASRS_data',
                          pattern = "*.xlsx$",
                          full.names = TRUE,
                          recursive = FALSE)
) 
df |> write.csv('/Volumes/LaCie/event_ews/cmb_asrs.csv')
hist(df$tot_wc)
df2<- df |>
  filter(aircraft_1aircraft_operator == 'Air Carrier', person_1reporter_organization == 'Air Carrier') |>
  filter(aircraft_1mission == 'Passenger') |>
  group_by(date) |>
  summarize(
    n_reports = n(),
    #cmbd_mnthly_narrative = paste0(cmbd_narrative, collapse = " ")
    cmbd_mnthly_narrative = paste0(report_1narrative, collapse = " ")
  )

# df |> writexl::write_xlsx(path = 'cmbd_ASRS.xlsx')


asrs_corpus <- quanteda::corpus(df2$cmbd_mnthly_narrative)
docvars(asrs_corpus,'date') <- df2$date 

token_info <- summary(asrs_corpus, n = nrow(df2))
ggplot(token_info, aes(x = date, y = Tokens)) + geom_line()

asrs_dfm <- dfm(asrs_corpus, 
                remove = stopwords("english"), stem = TRUE, remove_punct = TRUE)
asrs_sim <- quanteda.textstats::textstat_simil(asrs_dfm,method = 'cosine',margin = 'documents')
m <- as.matrix(asrs_sim)

test <- lapply(seq(2,nrow(m),1), function(x) {
  m[[x,x-1]]
})
x <- data.frame(
  date = df2[2:nrow(df2),'date'],
  t = seq(1,nrow(df2)-1,1),
  similarity = unlist(test))
y <- x |> filter(date < lubridate::ymd('2023-01-01'))
#y <- x
ggplot(y, aes(x = date, y = similarity)) + geom_line()

z <- EWSmethods::uniEWS(data = y[,2:3],
       metrics = c("ar1","SD","skew"),
       method = "expanding",
       burn_in = 50,
       threshold = 2)
plot(z,  y_lab = "Density")

EWSmethods::ewsnet_init(envname = "EWSNET_env", pip_ignore_installed = FALSE, auto = FALSE)
