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


# nrc_df <- dplyr::tbl(con,'link_table') |>
#   # dplyr::filter(system_source == 'nrc') |>
#   dplyr::collect()


# y <- sQuote(x)
# quoted <- lapply(x, function(elt) {dbQuoteString(con, elt)})
# generated = paste0(do.call(paste, c(quoted, sep = ',')))
# 
q <- DBI::sqlInterpolate(
  conn = con,
  glue::glue("SELECT * FROM embeddings as et JOIN link_table as lt ON et.eid = lt.eid WHERE lt.system_source = ?sys;"),
  # glue::glue("SELECT * FROM ?embeddings WHERE eid in (SELECT eid FROM ?link_table WHERE system_source = ?sys);"),
  # glue::glue_sql("SELECT * FROM ?embeddings WHERE eid::text IN ({nrc_df$eid*});", .con = con),
  # embeddings = DBI::dbQuoteIdentifier(con, 'embeddings')#,
  # eid_list = quoted
  # link_table = DBI::dbQuoteIdentifier(con, 'link_table'),
  sys = 'nrc'
)
nrc_embeds <- DBI::dbGetQuery(conn = con, q)
# DBI::dbExecute(conn = con, q)

lsa::cosine(
  unlist(pgvector.unserialize(nrc_embeds$embedding[[1]])),
  unlist(pgvector.unserialize(nrc_embeds$embedding[[2]]))
)

# embeddings

# # this sucks because it pulls the entire embedding and link tables, but... DBI kinda sucks?
# nrc_embeds <- dplyr::tbl(con,'embeddings') |>
#   dplyr::collect()  |>
#   full_join(
#     dplyr::collect(dplyr::tbl(con,'link_table')),
#     by = 'eid') |>
#   filter(system_source == 'nrc') |>
#   select(-event_num,-event_date) |>
#   left_join(
#     dplyr::collect(dplyr::tbl(con,'nrc_raw')), by = 'eid'
#   )

# simple one case try
fermi <- nrc_embeds |>
  filter(facility == 'fermi') |>
  select(eid,embedding,event_date) |>
  mutate(
    event_date = lubridate::mdy(event_date),
    embedding = pgvector.unserialize(embedding)
  ) |>
  rowwise() |>
  mutate(embedding2 = unlist(embedding, use.names = F)) |> ungroup() |>
  arrange(event_date)
# fermi$embedding2 <- lapply(fermi$embedding, function(x) unlist(x,use.names = F))

get_cos_sim <- function(x,y) {
  print(x)
  print(y)
  return(lsa::cosine(unlist(x),unlist(y)))
}

fermi <- fermi |>
  # rowwise() |>
  mutate(
    embedding2 = list(lag(embedding))
    # cos_sim = get_cos_sim(embedding,lag(embedding))
  ) #|> ungroup()



get_cos_sim(fermi[200,'embedding'],fermi[2,'embedding'])

lsa::cosine(unlist(fermi[200,'embedding']),unlist(fermi[12,'embedding']))
lsa::cosine(fermi[1,'embedding'],fermi[2,'embedding'])
# Create



