pacman::p_load(gtfstools, dplyr, data.table, sf, lubridate, gt, stringr, webshot2)

ano_gtfs <- "2026"
mes_gtfs <- "03"
quinzena_gtfs <- "04"

endereco_gtfs <- file.path(
  "../../dados/gtfs", ano_gtfs,
  paste0("sppo_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q_PROC.zip")
)

gtfs <- read_gtfs(endereco_gtfs)

# Filtrar apenas trips que contenham [ no trip_headsign
trips_desvio <- gtfs$trips %>%
  filter(grepl("\\[", trip_headsign)) %>%
  select(trip_id, trip_short_name, trip_headsign, shape_id, route_id, direction_id)

# Extrair o texto do desvio (entre [ e ])
trips_desvio <- trips_desvio %>%
  mutate(desvio = str_extract(trip_headsign, "\\[.*?\\]"))

trips_desvio_ids <- trips_desvio$trip_id

gtfs_desvio <- gtfs %>%
  filter_by_trip_id(trips_desvio_ids)

# Remover frescões (se aplicável)
frescoes <- gtfs$routes %>% 
  filter(route_type == '200') %>% 
  pull(route_id)

gtfs_desvio <- filter_by_route_id(gtfs_desvio, frescoes, keep = F)

# Calcular extensões dos shapes
gtfs_desvio$shapes <- as.data.table(gtfs_desvio$shapes) %>% 
  group_by(shape_id) %>% 
  arrange(shape_id, shape_pt_sequence)

shapes_sf <- convert_shapes_to_sf(gtfs_desvio) %>% 
  st_transform(31983) %>% 
  mutate(extensao = as.numeric(st_length(.)) / 1000) %>% 
  st_drop_geometry()

# Juntar informações para criar o relatório final
relatorio_desvios <- trips_desvio %>%
  left_join(select(gtfs_desvio$routes, route_id, route_short_name, route_long_name, agency_id), 
            by = "route_id") %>%
  left_join(select(gtfs_desvio$agency, agency_id, agency_name), 
            by = "agency_id") %>%
  left_join(shapes_sf, by = "shape_id") %>%
  mutate(
    Sentido = case_when(
      as.character(direction_id) == "0" ~ "Ida",
      as.character(direction_id) == "1" ~ "Volta",
      TRUE ~ "Circular"
    )
  ) %>%
  # Uma linha por serviço + evento + sentido
  group_by(trip_short_name, route_long_name, agency_name, Sentido, desvio) %>%
  summarise(
    Extensão = sprintf("%.3f", round(sum(extensao, na.rm = TRUE), 3)),
    .groups = "drop"
  ) %>%
  select(
    Serviço = trip_short_name,
    Vista = route_long_name,
    Consórcio = agency_name,
    Sentido,
    Extensão,
    Evento = desvio
  ) %>%
  # Ordenar por serviço, evento e sentido
  arrange(Serviço, Evento, Sentido)

# Salvar o arquivo
fwrite(relatorio_desvios, 
       paste0("C:/R_SMTR/dados/os/os_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "_excep.csv"),
       sep = ',', 
       dec = '.')