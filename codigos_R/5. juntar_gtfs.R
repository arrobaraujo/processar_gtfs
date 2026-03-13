pacman::p_load(gtfstools, dplyr, data.table, stringr, zip)

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================

ano_gtfs      <- "2026"
mes_gtfs      <- "03"
quinzena_gtfs <- "04"

sufixo <- paste0(ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q")

endereco_sppo       <- file.path("../../dados/gtfs", ano_gtfs, paste0("sppo_", sufixo, "_PROC.zip"))
endereco_brt        <- file.path("../../dados/gtfs", ano_gtfs, paste0("brt_",  sufixo, "_PROC.zip"))
endereco_gtfs_combi <- file.path("../../dados/gtfs", ano_gtfs, paste0("gtfs_combi_", sufixo, ".zip"))

# Caminhos absolutos para as pastas com os arquivos a serem substituídos
pasta_substituicao_combi <- "../../dados/insumos/gtfs_combi"
pasta_substituicao_pub   <- "../../dados/insumos/gtfs_pub"

# ==============================================================================
# FUNÇÕES AUXILIARES
# ==============================================================================

log_msg <- function(msg) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%H:%M:%S"), msg))
}

# Adiciona sufixo _REG nos service_ids simples (U, S, D)
ajustar_service_id <- function(dt, coluna = "service_id") {
  dt[[coluna]] <- fifelse(
    dt[[coluna]] %in% c("U", "S", "D"),
    paste0(dt[[coluna]], "_REG"),
    dt[[coluna]]
  )
  return(dt)
}

# Remove colunas de um data.table caso existam (por referência)
remover_colunas <- function(dt, colunas) {
  cols_existentes <- intersect(colunas, names(dt))
  if (length(cols_existentes) > 0) {
    dt[, (cols_existentes) := NULL]
  }
  invisible(dt)
}

atualizar_cores_gtfs <- function(gtfs, caminho_cores) {
  
  # Lê o arquivo de cores (colunas já no padrão GTFS)
  if (!file.exists(caminho_cores)) {
    stop("Arquivo de cores não encontrado: ", caminho_cores)
  }
  cores <- fread(caminho_cores, colClasses = "character")
  
  # Verifica se as colunas necessárias existem
  cols_necessarias <- c("route_short_name", "route_color", "route_text_color")
  if (!all(cols_necessarias %in% names(cores))) {
    stop("O arquivo de cores deve conter as colunas: ",
         paste(cols_necessarias, collapse = ", "))
  }
  
  # Remove duplicatas no CSV (prioriza a primeira ocorrência)
  cores <- unique(cores, by = "route_short_name")
  
  # Garante que gtfs$routes seja data.table
  setDT(gtfs$routes)
  
  # Cria cópia das cores originais para rotas 702 (serão preservadas)
  cores_702 <- gtfs$routes[route_type == 702, 
                           .(route_id, route_short_name, route_color, route_text_color)]
  
  # Aplica as cores do CSV apenas para rotas que NÃO são 702
  # Primeiro, remove as colunas de cor dessas rotas (para evitar conflito no merge)
  gtfs$routes[route_type != 702, c("route_color", "route_text_color") := NA_character_]
  
  # Faz o merge apenas nas rotas que não são 702 (usando route_short_name)
  # Nota: se houver mais de uma rota com mesmo short_name, o merge repetirá linhas – evitamos com unique no CSV e assumimos que é 1:1
  gtfs$routes <- merge(
    gtfs$routes,
    cores,
    by = "route_short_name",
    all.x = TRUE,
    suffixes = c("", "_csv")
  )
  
  # Para as rotas que receberam cores do CSV, copia para as colunas padrão
  gtfs$routes[!is.na(route_color_csv), 
              `:=`(route_color = route_color_csv,
                   route_text_color = route_text_color_csv)]
  gtfs$routes[, c("route_color_csv", "route_text_color_csv") := NULL]
  
  # Restaura as cores originais das rotas 702 (garantindo que não foram alteradas)
  # Aqui usamos route_id como chave para precisão
  gtfs$routes[cores_702, on = "route_id", 
              `:=`(route_color = i.route_color,
                   route_text_color = i.route_text_color)]
  
  # Define cores fixas para rotas do tipo 200 (frescão/executivo)
  gtfs$routes[route_type == 200, 
              `:=`(route_color      = "030478",
                   route_text_color = "FFFFFF")]
  
  # Avisa sobre rotas que ficaram sem cor (excluindo 200 e 702)
  sem_cor <- gtfs$routes[is.na(route_color) & !route_type %in% c(200, 702), 
                         .(route_id, route_short_name, route_type)]
  if (nrow(sem_cor) > 0) {
    message("⚠️  Linhas sem correspondência no CSV de cores (", nrow(sem_cor), "):")
    print(sem_cor)
    # Opcional: salvar lista para análise
    fwrite(sem_cor, "rotas_sem_cor.csv")
  }
  
  return(gtfs)
}

# ==============================================================================
# FUNÇÃO PARA SUBSTITUIR ARQUIVOS DENTRO DE UM GTFS ZIP
# ==============================================================================

substituir_arquivos_gtfs <- function(caminho_zip, pasta_origem, arquivos = c("calendar_dates.txt", 
                                                                             "fare_attributes.txt", 
                                                                             "fare_rules.txt", 
                                                                             "feed_info.txt")) {
  if (!file.exists(caminho_zip)) {
    stop("Arquivo ZIP não encontrado: ", caminho_zip)
  }
  if (!dir.exists(pasta_origem)) {
    stop("Pasta de origem não encontrada: ", pasta_origem)
  }
  
  log_msg(sprintf("Substituindo arquivos em %s usando %s", basename(caminho_zip), pasta_origem))
  
  # Criar diretório temporário
  temp_dir <- tempfile("gtfs_replace_")
  dir.create(temp_dir)
  on.exit(unlink(temp_dir, recursive = TRUE), add = TRUE)
  
  # Descompactar o zip
  unzip(caminho_zip, exdir = temp_dir)
  
  # Copiar os arquivos da origem (sobrescrevendo)
  for (arq in arquivos) {
    origem_arq <- file.path(pasta_origem, arq)
    if (file.exists(origem_arq)) {
      file.copy(origem_arq, file.path(temp_dir, arq), overwrite = TRUE)
      log_msg(sprintf("  ✔ Substituído: %s", arq))
    } else {
      log_msg(sprintf("  ⚠ Arquivo %s não encontrado na origem, mantido o original", arq))
    }
  }
  
  # Recriar o zip (substituindo o original)
  # Primeiro, apagar o arquivo zip original para evitar conflitos
  file.remove(caminho_zip)
  # Usar zip::zipr para criar o novo zip com os arquivos do diretório temporário
  zip::zipr(caminho_zip, files = list.files(temp_dir, full.names = TRUE), recurse = FALSE)
  
  log_msg(sprintf("  ✓ Arquivo %s recriado com sucesso", basename(caminho_zip)))
}

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================

cat("\n")
cat("╔════════════════════════════════════════════════════════════════════════════╗\n")
cat("║                    COMBINAÇÃO DE GTFS - SPPO + BRT                        ║\n")
cat("╚════════════════════════════════════════════════════════════════════════════╝\n")
cat("\n")

tempo_inicio <- Sys.time()

# ==============================================================================
# 1. CARREGAR E PROCESSAR GTFS SPPO
# ==============================================================================

log_msg("Carregando GTFS SPPO...")
if (!file.exists(endereco_sppo)) stop("Arquivo SPPO não encontrado: ", endereco_sppo)

gtfs_sppo <- read_gtfs(endereco_sppo)

log_msg("Processando SPPO...")

setDT(gtfs_sppo$routes)
gtfs_sppo$routes[, numero := as.integer(str_extract(route_short_name, "[0-9]+"))]
gtfs_sppo$routes[, route_type := fcase(
  numero > 1000,                                    "200",
  route_short_name %in% c("LECD124", "LECD125"),   "200",
  default =                                          "700"
)]
gtfs_sppo$routes[, numero := NULL]

setDT(gtfs_sppo$trips)
setDT(gtfs_sppo$calendar)
setDT(gtfs_sppo$stop_times)
gtfs_sppo$trips    <- ajustar_service_id(gtfs_sppo$trips)
gtfs_sppo$calendar <- ajustar_service_id(gtfs_sppo$calendar)

# Validar horários SPPO antes de continuar
stops_vazios_sppo <- gtfs_sppo$stop_times[
  is.na(arrival_time) | arrival_time == "" |
    is.na(departure_time) | departure_time == ""
]

if (nrow(stops_vazios_sppo) > 0) {
  log_msg(sprintf("⚠️  SPPO tem %d stops com horários vazios!", nrow(stops_vazios_sppo)))
  trips_problema <- unique(stops_vazios_sppo$trip_id)
  log_msg(sprintf("   Trips afetadas: %d", length(trips_problema)))
  stop("⛔ SPPO processado está com horários vazios. Execute o processamento_velocidades.R corrigido primeiro.")
}

trips_com_st   <- unique(gtfs_sppo$stop_times$trip_id)
trips_excep    <- gtfs_sppo$trips[service_id == "EXCEP", trip_id]
trips_manter   <- unique(c(trips_com_st, trips_excep))

trips_fantasma <- unlist(fread("../../dados/insumos/trip_id_fantasma.txt"))

gtfs_filt_sppo <- filter_by_trip_id(gtfs_sppo, trips_manter) |>
  filter_by_trip_id(trips_fantasma, keep = FALSE)

rm(gtfs_sppo, trips_com_st, trips_excep, trips_manter, trips_fantasma)

log_msg(sprintf("✓ SPPO processado — %d rotas, %d trips",
                nrow(gtfs_filt_sppo$routes), nrow(gtfs_filt_sppo$trips)))

# ==============================================================================
# 2. CARREGAR E PROCESSAR GTFS BRT
# ==============================================================================

log_msg("Carregando GTFS BRT...")
if (!file.exists(endereco_brt)) stop("Arquivo BRT não encontrado: ", endereco_brt)

gtfs_brt <- read_gtfs(endereco_brt)

log_msg("Processando BRT...")

setDT(gtfs_brt$routes)
gtfs_brt$routes[, route_type := fcase(
  grepl("EXEC", route_id), "200",
  default =                 "702"
)]

routes_usar_brt <- gtfs_brt$routes[
  route_short_name %in% unique(gtfs_brt$trips$trip_short_name),
  route_id
]

gtfs_filt_brt           <- filter_by_route_id(gtfs_brt, routes_usar_brt)
rm(gtfs_brt, routes_usar_brt)

setDT(gtfs_filt_brt$trips)
setDT(gtfs_filt_brt$calendar)
setDT(gtfs_filt_brt$stop_times)
gtfs_filt_brt$trips     <- ajustar_service_id(gtfs_filt_brt$trips)
gtfs_filt_brt$calendar  <- ajustar_service_id(gtfs_filt_brt$calendar)
gtfs_filt_brt$feed_info <- data.table()

# Validar horários BRT antes de continuar
stops_vazios_brt <- gtfs_filt_brt$stop_times[
  is.na(arrival_time) | arrival_time == "" |
    is.na(departure_time) | departure_time == ""
]

if (nrow(stops_vazios_brt) > 0) {
  log_msg(sprintf("⚠️  BRT tem %d stops com horários vazios!", nrow(stops_vazios_brt)))
  trips_problema <- unique(stops_vazios_brt$trip_id)
  log_msg(sprintf("   Trips afetadas: %d", length(trips_problema)))
  stop("⛔ BRT processado está com horários vazios. Execute o processamento_velocidades.R corrigido primeiro.")
}

log_msg(sprintf("✓ BRT processado — %d rotas, %d trips",
                nrow(gtfs_filt_brt$routes), nrow(gtfs_filt_brt$trips)))

log_msg("Salvando BRT processado...")
write_gtfs(gtfs_filt_brt, endereco_brt)

# ==============================================================================
# 3. COMBINAR GTFS
# ==============================================================================

log_msg("Combinando GTFS SPPO + BRT...")

routes_sppo_pre  <- copy(gtfs_filt_sppo$routes)
routes_brt_pre   <- copy(gtfs_filt_brt$routes)
todas_routes_pre <- rbindlist(list(routes_sppo_pre, routes_brt_pre), fill = TRUE)
rm(routes_sppo_pre, routes_brt_pre)

gtfs_combi <- merge_gtfs(gtfs_filt_sppo, gtfs_filt_brt)
rm(gtfs_filt_sppo, gtfs_filt_brt)

routes_perdidas <- todas_routes_pre[!(route_id %in% gtfs_combi$routes$route_id)]

if (nrow(routes_perdidas) > 0) {
  log_msg(sprintf("  ↳ Reintegrando %d routes sem trips perdidas no merge",
                  nrow(routes_perdidas)))
  gtfs_combi$routes <- rbindlist(list(gtfs_combi$routes, routes_perdidas), fill = TRUE)
}

rm(todas_routes_pre, routes_perdidas)
log_msg("✓ GTFS combinados")

# ==============================================================================
# 4. LIMPEZA E AJUSTES DO GTFS COMBINADO
# ==============================================================================

log_msg("Aplicando limpezas e ajustes...")

lapply(names(gtfs_combi), function(nm) {
  if (is.data.frame(gtfs_combi[[nm]])) setDT(gtfs_combi[[nm]])
})

pontos_apagar <- gtfs_combi$stops[stop_name == "APAGAR", stop_id]
gtfs_combi$stops <- gtfs_combi$stops[
  !duplicated(stop_id) & !(stop_id %in% pontos_apagar)
]

remover_colunas(gtfs_combi$agency,
                c("agency_phone", "agency_fare_url", "agency_email", "agency_branding_url"))

if (nrow(gtfs_combi$feed_info) > 0) {
  remover_colunas(gtfs_combi$feed_info, c("default_lang", "feed_contact_url", "feed_id"))
  gtfs_combi$feed_info <- gtfs_combi$feed_info[1L]
}

remover_colunas(gtfs_combi$routes,
                c("route_sort_order", "continuous_pickup",
                  "route_branding_url", "continuous_drop_off", "route_url"))

remover_colunas(gtfs_combi$trips,
                c("block_id", "wheelchair_accessible", "bikes_allowed"))
gtfs_combi$trips[trip_headsign == "", trip_headsign := "Circular"]

gtfs_combi$calendar       <- unique(gtfs_combi$calendar)
gtfs_combi$calendar_dates <- unique(gtfs_combi$calendar_dates)

remover_colunas(gtfs_combi$stop_times,
                c("pickup_type", "drop_off_type", "continuous_pickup", "continuous_drop_off"))
gtfs_combi$stop_times[, timepoint            := 0L]
gtfs_combi$stop_times[, shape_dist_traveled  := round(shape_dist_traveled, 2)]
gtfs_combi$stop_times <- gtfs_combi$stop_times[!(stop_id %in% pontos_apagar)]

gtfs_combi$fare_attributes[, currency_type := "BRL"]

log_msg("✓ Limpezas aplicadas")

# ==============================================================================
# CORREÇÃO: Ordenar shapes e verificar consistência
# ==============================================================================

log_msg("Verificando e ordenando shapes...")

if (!is.null(gtfs_combi$shapes) && nrow(gtfs_combi$shapes) > 0) {
  setDT(gtfs_combi$shapes)
  
  # Garantir que shape_pt_sequence seja numérico e ordenar
  gtfs_combi$shapes[, shape_pt_sequence := as.integer(shape_pt_sequence)]
  setorder(gtfs_combi$shapes, shape_id, shape_pt_sequence)
  
  # Identificar shapes com menos de 2 pontos (não formam linha)
  shapes_count <- gtfs_combi$shapes[, .N, by = shape_id]
  shapes_invalidos <- shapes_count[N < 2, shape_id]
  
  if (length(shapes_invalidos) > 0) {
    log_msg(sprintf("⚠️  Encontrados %d shapes com menos de 2 pontos.", length(shapes_invalidos)))
    
    # Buscar informações das trips que usam esses shapes
    trips_invalidas <- gtfs_combi$trips %>%
      filter(shape_id %in% shapes_invalidos) %>%
      select(shape_id, route_id, trip_short_name, direction_id, service_id) %>%
      distinct()
    
    if (nrow(trips_invalidas) > 0) {
      log_msg("  Trips associadas a shapes inválidos:")
      print(trips_invalidas)
      fwrite(trips_invalidas, "shapes_invalidos_com_trips.csv")
      log_msg("  Lista salva em 'shapes_invalidos_com_trips.csv'")
    }
    
    # Não removemos os shapes, apenas alertamos – a decisão de corrigir ou não fica com o usuário.
    # O GTFS pode até funcionar sem esses shapes, mas algumas ferramentas podem ignorar a rota.
    log_msg("  Os shapes foram mantidos, mas recomenda-se verificar a origem dos dados.")
  } else {
    log_msg("✓ Todos os shapes possuem pelo menos 2 pontos e estão ordenados.")
  }
} else {
  log_msg("✓ Nenhum shape para processar.")
}

# ==============================================================================
# 4.5 VALIDAÇÃO FINAL DE HORÁRIOS
# ==============================================================================

log_msg("Validando horários no GTFS combinado...")

stops_vazios_final <- gtfs_combi$stop_times[
  is.na(arrival_time) | arrival_time == "" |
    is.na(departure_time) | departure_time == ""
]

if (nrow(stops_vazios_final) > 0) {
  log_msg(sprintf("❌ ERRO: GTFS combinado tem %d stops com horários vazios!", 
                  nrow(stops_vazios_final)))
  trips_problema <- unique(stops_vazios_final$trip_id)
  log_msg(sprintf("   Trips afetadas: %d", length(trips_problema)))
  log_msg("   Primeiros 5 exemplos:")
  print(head(trips_problema, 5))
  stop("⛔ GTFS combinado contém horários vazios. Verifique os arquivos PROC de entrada.")
}

log_msg(sprintf("✓ Todos os %d stops têm horários válidos", nrow(gtfs_combi$stop_times)))

# ==============================================================================
# 5. VALIDAÇÃO DE ESTRUTURA
# ==============================================================================

log_msg("Validando estrutura do GTFS...")

stops_sem_location_type <- gtfs_combi$stops[is.na(location_type)]

if (nrow(stops_sem_location_type) > 0) {
  log_msg(sprintf("⚠ Atenção: %d stops sem location_type", nrow(stops_sem_location_type)))
} else {
  log_msg("✓ Todos os stops possuem location_type")
}

log_msg("═══════════════════════════════════════════════════════════════")
log_msg("ESTATÍSTICAS DO GTFS COMBINADO:")
log_msg(sprintf("  ├─ Agências:        %d",                   nrow(gtfs_combi$agency)))
log_msg(sprintf("  ├─ Rotas:           %d",                   nrow(gtfs_combi$routes)))
log_msg(sprintf("  ├─ Trips:           %d",                   nrow(gtfs_combi$trips)))
log_msg(sprintf("  ├─ Stops:           %d",                   nrow(gtfs_combi$stops)))
log_msg(sprintf("  ├─ Stop times:      %d",                   nrow(gtfs_combi$stop_times)))
log_msg(sprintf("  ├─ Shapes únicos:   %d",                   uniqueN(gtfs_combi$shapes$shape_id)))
log_msg(sprintf("  └─ Calendars:       %d",                   nrow(gtfs_combi$calendar)))
log_msg("═══════════════════════════════════════════════════════════════")

# ==============================================================================
# 6. SALVAMENTO
# ==============================================================================

log_msg(sprintf("Salvando GTFS combinado em: %s", basename(endereco_gtfs_combi)))
write_gtfs(gtfs_combi, endereco_gtfs_combi)

log_msg("✓ Arquivos GTFS salvos com sucesso")

# ==============================================================================
# SUBSTITUIÇÃO DE ARQUIVOS NO GTFS COMBINADO
# ==============================================================================

substituir_arquivos_gtfs(endereco_gtfs_combi, pasta_substituicao_combi)

gtfs_pub <- gtfs_combi

# ==============================================================================
# FILTRAGEM FINAL E APLICAÇÃO DE CORES
# ==============================================================================

log_msg("Removendo viagens com service_id EXCEP...")
gtfs_pub1 <- gtfs_pub %>%
  filter_by_service_id(service_id = "EXCEP", keep = FALSE)

log_msg("Aplicando cores personalizadas às rotas...")
gtfs_pub1 <- atualizar_cores_gtfs(
  gtfs          = gtfs_pub1,
  caminho_cores = "../../dados/insumos/gtfs_cores.csv"
)

log_msg("Salvando GTFS público final...")
caminho_gtfs_pub <- paste0("../../dados/gtfs/", ano_gtfs, "/gtfs_rio-de-janeiro_pub.zip")
write_gtfs(gtfs_pub1, caminho_gtfs_pub)

# ==============================================================================
# SUBSTITUIÇÃO DE ARQUIVOS NO GTFS PÚBLICO FINAL
# ==============================================================================

substituir_arquivos_gtfs(caminho_gtfs_pub, pasta_substituicao_pub)

# ==============================================================================
# FINALIZAÇÃO
# ==============================================================================

tempo_total <- as.numeric(difftime(Sys.time(), tempo_inicio, units = "secs"))

cat("\n")
cat("╔════════════════════════════════════════════════════════════════════════════╗\n")
cat("║                       PROCESSAMENTO FINALIZADO!                           ║\n")
cat(sprintf("║                    Tempo total: %.1f segundos%s║\n",
            tempo_total, strrep(" ", max(0L, 27L - nchar(sprintf("%.1f", tempo_total))))))
cat("╚════════════════════════════════════════════════════════════════════════════╝\n")
cat("\n")

if (nrow(stops_sem_location_type) > 0) {
  cat("⚠ ATENÇÃO: Alguns stops estão sem location_type\n")
  cat("Execute: View(stops_sem_location_type) para visualizar\n\n")
}