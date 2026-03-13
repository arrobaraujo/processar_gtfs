# Script solicitado para converter o arquivo .parquet final de volta para .rds

if (!requireNamespace("arrow", quietly = TRUE)) install.packages("arrow")

library(arrow)

# Caminhos
caminho_parquet <- "../../resultados/partidas/partidas.parquet"
caminho_rds <- "../../resultados/partidas/partidas.rds"

if (file.exists(caminho_parquet)) {
  cat("Lendo arquivo parquet...\n")
  df <- read_parquet(caminho_parquet)
  
  cat("Salvando arquivo rds...\n")
  saveRDS(df, caminho_rds)
  
  cat("✓ Conversão concluída com sucesso! Arquivo salvo em:", caminho_rds, "\n")
} else {
  stop("Erro: Arquivo parquet não encontrado: ", caminho_parquet)
}
