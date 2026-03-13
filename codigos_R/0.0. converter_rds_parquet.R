pacman::p_load(dplyr, purrr, readr, arrow)

ano_velocidade <- "2025"
mes_velocidade <- "10"
gtfs_processar <- "sppo"

path_nm <- paste0(
  "../../dados/viagens/", gtfs_processar, "/",
  ano_velocidade, "/", mes_velocidade, "/"
)

# Lista todos os RDS
nm <- list.files(
  path = path_nm, full.names = TRUE,
  pattern = "*.rds", recursive = TRUE
)

cat(sprintf("Encontrados %d arquivos RDS para conversão.\n", length(nm)))

converter_para_parquet <- function(caminho_rds) {
  # Lê o rds
  df <- read_rds(caminho_rds)

  # Define o novo caminho mudando a extensão
  caminho_parquet <- sub("\\.rds$", ".parquet", caminho_rds)

  # Salva como parquet
  write_parquet(df, caminho_parquet)

  cat(sprintf("Convertido: %s -> %s\n", basename(caminho_rds), basename(caminho_parquet)))
}

# Executa para todos
purrr::walk(nm, purrr::possibly(converter_para_parquet, otherwise = NULL))

cat("Conversão para parquet concluída!\n")
