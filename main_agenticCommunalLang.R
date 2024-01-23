library(quanteda)
library(spacyr)

agen_com_dic <- quanteda::dictionary(
  file = '/Volumes/LaCie/event_ews/agency_communion/a_AgencyCommunion.dic',
  format = 'LIWC'
)

rawTwoQs <- readxl::read_excel('/Volumes/LaCie/event_ews/old_culture_data/2013_JHMI_Pascal_all_QualComms.xlsx') #|>
  # filter(!is.na(comments))

TwoQs_corpus <- quanteda::corpus(
  rawTwoQs$comments
)

TwoQs_agenCom <- quanteda::tokens(TwoQs_corpus) |>
  tokens_lookup(dictionary = agen_com_dic) |>
  quanteda::dfm() 

TwoQs_agenCom_by_workSet <- rawTwoQs |>
  bind_cols(
    convert(TwoQs_agenCom,to = 'data.frame')
  ) |>
  filter(hospital == 'JHH') |>
  select(work_setting, agency,communion) |>
  group_by(work_setting) |>
  summarise(across(everything(), .f = list(mean = mean, std = sd), na.rm = TRUE))



d <- spacyr::spacy_parse(rawTwoQs$comments, dependency = T)
aux_pass <- d |>
  mutate(doc_id = as.numeric(str_remove_all(doc_id, "text"))) |>
  group_by(doc_id) |>
  summarise(aux_pass = sum(dep_rel=="auxpass"))
AuxPass_by_workSet <- rawTwoQs |>
  bind_cols(
    aux_pass
  ) |>
  filter(hospital == 'JHH') |>
  select(work_setting,aux_pass) |>
  group_by(work_setting) |>
  summarize(across(everything(), .f = list(mean = mean, std = sd), na.rm = TRUE))




getAgenCom <- function(df, text_col) {
  agen_com_dic <- quanteda::dictionary(
    file = '/Volumes/LaCie/event_ews/agency_communion/a_AgencyCommunion.dic',
    format = 'LIWC')
  print(df[text_col])
  corp <- quanteda::corpus(df[text_col])
  agCo_df <- quanteda::tokens(corp) |>
    tokens_lookup(dictionary = agen_com_dic) |>
    quanteda::dfm() 
  df <- df |>
    bind_cols(
      quanteda::convert(agCo_df,to = 'data.frame')
    )
  return(df)
}

getPassiveVoice <- function(df,text_col,ratio) {
  # largely taken from here: https://osf.io/nwsx3/
  d <- spacyr::spacy_parse(df[,text_col], dependency = T)
  aux_pass <- d |>
    mutate(doc_id = as.numeric(str_remove_all(doc_id, "text"))) |>
    group_by(doc_id) |>
    summarise(aux_pass = sum(dep_rel=="auxpass"))
  if (ratio){
    total_words <- df |> 
      select(!!sym(text_col)) |>
      as_tibble() |>
      rowwise() |>
      mutate(wc = ngram::wordcount(value))

    return(aux_pass$aux_pass/total_words$wc)}
  else{
    return(aux_pass$aux_pass)}
}

test <- getAgenCom(
  df = as.data.frame(rawTwoQs),
  text_col = 'comments'
)

rawTwoQs$passive_count <- getPassiveVoice(
  df = rawTwoQs,
  text_col = 'comments',
  ratio = FALSE
)


