pacman::p_load(gtfstools, dplyr, data.table, sf, lubridate, gt, stringr, webshot2)

ano_gtfs <- "2026"
mes_gtfs <- "03"
quinzena_gtfs <- "02"

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
  mutate(extensao = as.integer(st_length(.))) %>% 
  st_drop_geometry()

# Juntar informações para criar o relatório final
relatorio_desvios <- trips_desvio %>%
  left_join(select(gtfs_desvio$routes, route_id, route_short_name, route_long_name, agency_id), 
            by = "route_id") %>%
  left_join(select(gtfs_desvio$agency, agency_id, agency_name), 
            by = "agency_id") %>%
  left_join(shapes_sf, by = "shape_id") %>%
  # Criar uma linha por combinação de serviço + desvio + sentido
  # Como cada desvio tem apenas 1 viagem, pegamos a extensão diretamente
  mutate(
    extensao_ida = ifelse(direction_id == '0', extensao, NA),
    extensao_volta = ifelse(direction_id == '1', extensao, NA)
  ) %>%
  # Agrupar por serviço e desvio para consolidar ida e volta
  group_by(trip_short_name, desvio, route_long_name, agency_name) %>%
  summarise(
    extensao_ida = sum(extensao_ida, na.rm = TRUE),
    extensao_volta = sum(extensao_volta, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  # Selecionar e renomear colunas finais
  select(
    Serviço = trip_short_name,
    Vista = route_long_name,
    Consórcio = agency_name,
    `Extensão de Ida` = extensao_ida,
    `Extensão de Volta` = extensao_volta,
    Evento = desvio
  ) %>%
  # Ordenar por serviço e depois por desvio
  arrange(Serviço, Evento)

# Salvar o arquivo
fwrite(relatorio_desvios, 
       paste0("C:/R_SMTR/dados/os/os_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "_excep.csv"),
       sep = ';', 
       dec = ',')