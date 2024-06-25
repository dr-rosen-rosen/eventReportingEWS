#####
# Store bigger dictionaries as vectors

con <- DBI::dbConnect(RPostgres::Postgres(),
                      user = config$dbUser,
                      password = config$dbPW,
                      host = config$db_host,
                      dbname = config$db_name,
                      port = config$dbPort
)

##### General Inquirer
### NOTE: probably need a better system for rolling this across system and dictionarie

# create column if not exists
insert_new_column(
  con = con, 
  table = 'embeddings', 
  column_name = 'gi_vec', #'liwc_vec'
  column_type = 'vector', 
  vec_len = length(rail_df |> select(starts_with("gi_")))
)

############## GI
# serialize for storage
# nrc_df$gi_vec <- dict_to_vec(
#   nrc_df[,grepl("^gi_",names(nrc_df))],
#   serialize = TRUE)
# # save as vector in db

# which(nrc_df == '55550', arr.ind = TRUE)
# nrc_df2 <- nrc_df |> slice(10644:n())
# by(nrc_df2, seq_len(nrow(nrc_df2)), insert_vec, con = con, sys_source = 'nrc', col_name = 'gi_vec')
# rm(nrc_df2)

# serialize for storage
asrs_df <- as.data.frame(asrs_df)
asrs_df$gi_vec <- dict_to_vec(
  asrs_df[,grepl("^gi_",names(asrs_df))],
  serialize = TRUE)
# save as vector in db
by(asrs_df, seq_len(nrow(asrs_df)), insert_vec, con = con, sys_source = 'asrs', col_name = 'gi_vec')

# serialize for storage
# rail_df <- as.data.frame(rail_df)
# rail_df$gi_vec <- dict_to_vec(
#   rail_df[,grepl("^gi_",names(rail_df))],
#   serialize = TRUE)
# # save as vector in db
# by(rail_df, seq_len(nrow(rail_df)), insert_vec, con = con, sys_source = 'rail', col_name = 'gi_vec')

# serialize for storage
phmsa_df <- as.data.frame(phmsa_df)
phmsa_df$gi_vec <- dict_to_vec(
  phmsa_df[,grepl("^gi_",names(phmsa_df))],
  serialize = TRUE)
# save as vector in db
by(phmsa_df, seq_len(nrow(phmsa_df)), insert_vec, con = con, sys_source = 'phmsa', col_name = 'gi_vec')


############## LIWC
# create column if not exists
insert_new_column(
  con = con, 
  table = 'embeddings', 
  column_name = 'liwc_vec',
  column_type = 'vector', 
  vec_len = length(rail_df |> select(starts_with("gi_")))
)

# serialize for storage
# nrc_df$gi_vec <- dict_to_vec(
#   nrc_df[,grepl("^gi_",names(nrc_df))],
#   serialize = TRUE)
# # save as vector in db

# which(nrc_df == '55550', arr.ind = TRUE)
# nrc_df2 <- nrc_df |> slice(10644:n())
# by(nrc_df2, seq_len(nrow(nrc_df2)), insert_vec, con = con, sys_source = 'nrc', col_name = 'gi_vec')
# rm(nrc_df2)

# serialize for storage
asrs_df <- as.data.frame(asrs_df)
asrs_df$liwc_vec <- dict_to_vec(
  asrs_df[,grepl("^liwc_",names(asrs_df))],
  serialize = TRUE)
# save as vector in db
by(asrs_df, seq_len(nrow(asrs_df)), insert_vec, con = con, sys_source = 'asrs', col_name = 'liwc_vec')

# serialize for storage
rail_df <- as.data.frame(rail_df)
rail_df$liwc_vec <- dict_to_vec(
  rail_df[,grepl("^liwc_",names(rail_df))],
  serialize = TRUE)
# save as vector in db
by(rail_df, seq_len(nrow(rail_df)), insert_vec, con = con, sys_source = 'rail', col_name = 'liwc_vec')

# serialize for storage
phmsa_df <- as.data.frame(phmsa_df)
phmsa_df$liwc_vec <- dict_to_vec(
  phmsa_df[,grepl("^liwc_",names(phmsa_df))],
  serialize = TRUE)
# save as vector in db
by(phmsa_df, seq_len(nrow(phmsa_df)), insert_vec, con = con, sys_source = 'phmsa', col_name = 'liwc_vec')