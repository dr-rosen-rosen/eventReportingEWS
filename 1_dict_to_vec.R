#####
# Store bigger dictionaries as vectors

con <- DBI::dbConnect(RPostgres::Postgres(),
                      user = config$dbUser,
                      password = config$dbPW,
                      host = config$db_host,
                      dbname = config$db_name,
                      port = config$dbPort
)

# create column if not exists
insert_new_column(
  con = con, 
  table = 'embeddings', 
  column_name = 'gi_vec', 
  column_type = 'vector', 
  vec_len = length(rail_df |> select(starts_with("gi_")))
)

# serialize for storage
nrc_df$gi_vec <- dict_to_vec(
  nrc_df[,grepl("^gi_",names(nrc_df))],
  serialize = TRUE)

# save as vector in db


by(nrc_df[1:2,], seq_len(nrow(nrc_df[1:2,])), insert_vec, con = con, sys_source = 'nrc', col_name = 'gi_vec')

insert_vec(con = con,
           vec = nrc_df[1,'gi_vec'],
           )

nrc_embeds <- DBI::dbExecute(conn = con, s)
