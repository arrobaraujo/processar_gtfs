pacman::p_load(googlesheets4)

# Force the email to use the existing token (or prompt if needed)
gs4_auth("erick.araujo@prefeitura.rio")

desvios_tabela_id <- "1mIyuCK8EhQWTrFqzBJJ-_K2o9lzmTD5Nekmc6bX__ko"
pasta_saida <- "C:/R_SMTR/dados/insumos/desvios"

if (!dir.exists(pasta_saida)) {
  dir.create(pasta_saida, recursive = TRUE)
}

cat("Baixando 'linhas_desvios'...\n")
tabela_desvios <- read_sheet(desvios_tabela_id, sheet = "linhas_desvios")
write.csv(tabela_desvios, file.path(pasta_saida, "linhas_desvios.csv"), row.names = FALSE)

cat("Baixando 'descricao_desvios'...\n")
descricao_desvios <- read_sheet(desvios_tabela_id, sheet = "descricao_desvios")
write.csv(descricao_desvios, file.path(pasta_saida, "descricao_desvios.csv"), row.names = FALSE)

cat("Planilhas baixadas com sucesso nos arquivos CSV em", pasta_saida, "!\n")
