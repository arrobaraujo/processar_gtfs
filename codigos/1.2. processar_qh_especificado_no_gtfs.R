# Este codigo pega quadros horarios ja armazenados no GTFS e retorna o quadro
# de partidas, em formato pronto para importacao, apenas para as linhas e
# servicos definidas neste codigo.
# Util para reimportar frequencias quando outro calendario for aplicado.

pacman::p_load(gtfstools, dplyr, data.table, googlesheets4)

# Define GTFS que será usado

ano_gtfs <- "2024"
mes_gtfs <- "10"
quinzena_gtfs <- "01"

end_gtfs <- paste0(
  "../../dados/gtfs/", ano_gtfs, "/gtfs_combi_", ano_gtfs, "-",
  mes_gtfs, "-", quinzena_gtfs, "Q.zip"
)

gtfs <- read_gtfs(end_gtfs)

linhas_rodar <- c("913")


frequencias_desvios <- gtfs$frequencies %>%
  left_join(select(gtfs$trips, trip_id, trip_short_name, trip_headsign, service_id)) %>%
  filter(trip_short_name %in% linhas_rodar) %>%
  filter(service_id %in% c("S_REG"))  ##, "S_REG", "D_REG"

linhas_processar <- frequencias_desvios %>%
  select(trip_short_name, trip_headsign, service_id) %>%
  distinct() %>%
  arrange(trip_short_name, desc(service_id))

servicos <- linhas_processar$trip_short_name
vistas <- linhas_processar$trip_headsign
calendarios <- linhas_processar$service_id

pasta_qh <- paste0("../../resultados/quadro_horario_extraido/", ano_gtfs, "/", mes_gtfs, "/qh_por_linha/",quinzena_gtfs,"Q/")
purrr::possibly(dir.create)(pasta_qh, recursive = TRUE)

separarQuadros <- function(servicos, vistas, calendarios) {
  quadro_linha <- frequencias_desvios %>%
    filter(trip_short_name == !!servicos) %>%
    filter(trip_headsign == !!vistas) %>%
    filter(service_id == !!calendarios) %>%
    arrange(start_time) %>%
    mutate(trip_id = "") %>%
    select(trip_id, trip_headsign, trip_short_name, start_time, end_time, headway_secs)

  nome_arq <- paste0("qh_", servicos, "-", vistas, "_", calendarios, ".csv")

  espaco <- data.frame(trip_id = "")

  readr::write_csv(quadro_linha, file.path(pasta_qh, nome_arq))
}

purrr::pmap(list(servicos, vistas, calendarios), separarQuadros)