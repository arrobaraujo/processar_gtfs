pacman::p_load(gtfstools, dplyr, sf, data.table, googlesheets4)

ano_gtfs <- '2026'
mes_gtfs <- '03'
quinzena_gtfs <- '01'

endereco_gtfs_combi <- paste0("../../dados/gtfs/", ano_gtfs, "/gtfs_rio-de-janeiro_pub.zip")

gs4_auth("erick.araujo@prefeitura.rio")

gtfs <- read_gtfs(endereco_gtfs_combi)

# ==============================================================================
# ORDENAÇÃO (apenas ordena, não remove nada)
# ==============================================================================
setDT(gtfs$shapes)
gtfs$shapes[, shape_pt_sequence := as.integer(shape_pt_sequence)]
setorder(gtfs$shapes, shape_id, shape_pt_sequence)

# ==============================================================================
# IDENTIFICAR shapes com menos de 2 pontos (apenas alerta, sem remoção)
# ==============================================================================
shapes_count <- gtfs$shapes[, .N, by = shape_id]
shapes_invalidos <- shapes_count[N < 2, shape_id]

if (length(shapes_invalidos) > 0) {
  cat("\n⚠️  ATENÇÃO: Foram encontrados shapes com menos de 2 pontos!\n")
  cat("  Esses shapes NÃO foram removidos, mas precisam ser corrigidos na fonte.\n")
  cat("  Lista de shape_id inválidos:\n")
  print(shapes_invalidos)
  
  # Buscar informações das trips que usam esses shapes
  trips_invalidas <- gtfs$trips %>%
    filter(shape_id %in% shapes_invalidos) %>%
    select(shape_id, route_id, trip_short_name, direction_id, service_id) %>%
    distinct()
  
  if (nrow(trips_invalidas) > 0) {
    cat("\n  Trips associadas a esses shapes:\n")
    print(trips_invalidas)
    
    # Salvar relatório em CSV
    fwrite(trips_invalidas, "shapes_invalidos_com_trips.csv")
    cat("  Relatório salvo em 'shapes_invalidos_com_trips.csv'\n")
  }
  
  cat("\n  ⚠️  A execução continuará, mas recomenda-se fortemente corrigir os shapes na origem.\n\n")
}

# ==============================================================================
# 1. Preparar tabela de shapes com informações das trips
# ==============================================================================

# (restante do código inalterado, apenas substituindo %like% por grepl)
trips_base_u_reg <- gtfs$trips %>%
  filter(service_id == 'U_REG') %>%
  select(trip_id, shape_id, trip_short_name, trip_headsign, direction_id, service_id) %>%
  distinct(shape_id, .keep_all = TRUE) %>%
  select(-trip_id)

trips_base_reg <- gtfs$trips %>%
  filter(!(shape_id %in% trips_base_u_reg$shape_id)) %>%
  filter(grepl("REG", service_id)) %>%
  select(trip_id, shape_id, trip_short_name, trip_headsign, direction_id, service_id) %>%
  distinct(shape_id, .keep_all = TRUE) %>%
  select(-trip_id)

trip_especial_u <- gtfs$trips %>%
  filter(!(shape_id %in% trips_base_u_reg$shape_id)) %>%
  filter(!(shape_id %in% trips_base_reg$shape_id)) %>%
  filter(grepl("U", service_id)) %>%
  select(trip_id, shape_id, trip_short_name, trip_headsign, direction_id, service_id) %>%
  distinct(shape_id, .keep_all = TRUE) %>%
  select(-trip_id)

trip_especial <- gtfs$trips %>%
  filter(!(shape_id %in% trips_base_u_reg$shape_id)) %>%
  filter(!(shape_id %in% trips_base_reg$shape_id)) %>%
  filter(!(shape_id %in% trip_especial_u$shape_id)) %>%
  select(trip_id, shape_id, trip_short_name, trip_headsign, direction_id, service_id) %>%
  distinct(shape_id, .keep_all = TRUE) %>%
  select(-trip_id)

trips_base <- rbindlist(list(trips_base_u_reg,
                             trips_base_reg,
                             trip_especial_u,
                             trip_especial))

# Converter shapes para sf (agora com shapes ordenados, mesmo os inválidos)
shapes <- convert_shapes_to_sf(gtfs) %>%
  left_join(trips_base, by = "shape_id") %>%
  mutate(
    service_id_original = service_id,
    service_id = if_else(grepl("_DESAT_", service_id), substr(service_id, 1, 1), service_id),
    service_id = if_else(grepl("REG", service_id), substr(service_id, 1, 1), service_id)
  )

# ==============================================================================
# 2. Estatísticas por shape (opcional)
# ==============================================================================
shapes_por_linha <- as.data.frame(table(shapes$trip_short_name))

# ==============================================================================
# 3. Carregar descrição dos desvios e preparar dados para exportação
# ==============================================================================
descricao_desvios <- read_sheet("1QYSf_E7HrDcSDVVaF_KrolS-LRL3kTF5WnhQGh3RMy0", 'descricao_desvios') %>%
  select(cod_desvio, descricao_desvio, data_inicio, data_fim)

trips_join <- gtfs$trips %>%
  select(shape_id, route_id) %>%
  distinct(shape_id, .keep_all = TRUE)

shapes_ext <- shapes %>%
  st_transform(31983) %>%
  mutate(extensao = as.integer(st_length(.))) %>%
  filter(service_id != 'AN31') %>%
  select(trip_short_name, trip_headsign, direction_id, service_id, service_id_original,
         extensao, shape_id) %>%
  st_transform(4326) %>%
  rename(servico      = trip_short_name,
         destino      = trip_headsign,
         direcao      = direction_id,
         tipo_dia     = service_id) %>%
  mutate(
    cod_desvio = substr(service_id_original, 3, nchar(service_id_original))
  ) %>%
  left_join(descricao_desvios, by = "cod_desvio") %>%
  select(-cod_desvio) %>%
  left_join(trips_join, by = "shape_id") %>%
  left_join(select(gtfs$routes, route_id, agency_id, route_type), by = "route_id") %>%
  mutate(
    route_type = as.character(route_type),
    tipo_rota = case_when(
      route_type == '200' ~ 'frescao',
      route_type == '702' ~ 'brt',
      route_type == '700' ~ 'regular',
      TRUE ~ NA_character_
    )
  ) %>%
  left_join(select(gtfs$agency, agency_name, agency_id), by = "agency_id") %>%
  select(-route_id, -agency_id, -route_type, -service_id_original) %>%
  rename(consorcio = agency_name)

# ==============================================================================
# 4. Exportar shapes (trajetos)
# ==============================================================================
pasta_shape_sppo <- paste0("../../dados/shapes/", ano_gtfs)

if (!dir.exists(pasta_shape_sppo)) {
  dir.create(pasta_shape_sppo, recursive = TRUE)
}

shapes_ext_shp <- shapes_ext %>%
  rename(desvio = descricao_desvio) %>%
  select(servico, destino, direcao, tipo_dia, extensao, desvio, consorcio, tipo_rota)

endereco_shape_sppo_trajetos_shp <- paste0(pasta_shape_sppo, "/shapes_trajetos_",
                                           ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q.shp")
endereco_shape_sppo_trajetos_gpkg <- paste0(pasta_shape_sppo, "/shapes_trajetos_",
                                            ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q.gpkg")

st_write(shapes_ext_shp, endereco_shape_sppo_trajetos_shp, append = FALSE)
st_write(shapes_ext, endereco_shape_sppo_trajetos_gpkg, append = FALSE)

# ==============================================================================
# 5. Exportar pontos de parada
# ==============================================================================
pontos_usados <- unique(gtfs$stop_times$stop_id)
gtfs_filtrado <- filter_by_stop_id(gtfs, pontos_usados)
pontos <- convert_stops_to_sf(gtfs_filtrado)

endereco_shape_sppo_pontos_shp <- paste0(pasta_shape_sppo, "/shapes_pontos_",
                                         ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q.shp")
endereco_shape_sppo_pontos_gpkg <- paste0(pasta_shape_sppo, "/shapes_pontos_",
                                          ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q.gpkg")

st_write(pontos, endereco_shape_sppo_pontos_shp, append = FALSE)
st_write(pontos, endereco_shape_sppo_pontos_gpkg, append = FALSE)