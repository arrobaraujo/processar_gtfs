pacman::p_load(gtfstools, dplyr, data.table, sf, lubridate, stringr, purrr)

# ============================================================================
# CONFIGURAÇÕES INICIAIS
# ============================================================================

endereco_gtfs <- paste0("../../dados/gtfs/2026/gtfs_rio-de-janeiro_pub.zip")
tipo_dia <- c('du', 'dom', 'sab')
`%nin%` <- function(x, table) !(x %in% table)

# FILTRO DE LINHAS A EXCLUIR (adicione os trip_short_name que não quer)
linhas_excluir <- c("SE867")

# Cria diretório de saída se não existir
if (!dir.exists("../../resultados/partidas")) {
  dir.create("../../resultados/partidas", recursive = TRUE)
}

# ============================================================================
# SUBSTITUIÇÃO DA FUNÇÃO frequencies_to_stop_times (versão corrigida)
# ============================================================================

frequencies_to_stop_times <- function(gtfs, trip_id = NULL, force = FALSE) {
  gtfs <- gtfstools:::assert_and_assign_gtfs_object(gtfs)
  checkmate::assert_character(trip_id, null.ok = TRUE, any.missing = FALSE)
  checkmate::assert_logical(force, len = 1, any.missing = FALSE)
  gtfsio::assert_field_class(gtfs, "frequencies", c("trip_id", 
                                                    "start_time", "end_time", "headway_secs"), c("character", 
                                                                                                 "character", "character", "integer"))
  gtfsio::assert_field_class(gtfs, "stop_times", c("trip_id", 
                                                   "arrival_time", "departure_time"), c("character", "character", 
                                                                                        "character"))
  if (!is.null(trip_id)) {
    relevant_trips <- trip_id
  } else {
    relevant_trips <- unique(gtfs$frequencies$trip_id)
  }
  if (!is.null(trip_id)) {
    invalid_trip_id <- trip_id[!trip_id %chin% unique(gtfs$frequencies$trip_id)]
    if (!identical(invalid_trip_id, character(0))) {
      warning("'frequencies' doesn't contain the following trip_id(s): ", 
              paste0("'", invalid_trip_id, "'", collapse = ", "), 
              call. = FALSE)
      relevant_trips <- setdiff(relevant_trips, invalid_trip_id)
    }
  }
  stop_times_trips <- unique(gtfs$stop_times$trip_id)
  missing_from_stop_times <- setdiff(relevant_trips, stop_times_trips)
  if (!force) 
    relevant_trips <- setdiff(relevant_trips, missing_from_stop_times)
  if (!identical(missing_from_stop_times, character(0))) {
    warning("The following trip_id(s) are listed in 'frequencies' ", 
            "but not in 'stop_times': ", paste0("'", missing_from_stop_times, 
                                                "'", collapse = ", "), call. = FALSE)
  }
  if (!gtfsio::check_field_exists(gtfs, "frequencies", "start_time_secs")) {
    gtfs$frequencies[trip_id %chin% relevant_trips, `:=`(start_time_secs, 
                                                         gtfstools:::string_to_seconds(start_time))]
    created_start_secs <- TRUE
  }
  if (!gtfsio::check_field_exists(gtfs, "frequencies", "end_time_secs")) {
    gtfs$frequencies[trip_id %chin% relevant_trips, `:=`(end_time_secs, 
                                                         gtfstools:::string_to_seconds(end_time))]
    created_end_secs <- TRUE
  }
  if (!gtfsio::check_field_exists(gtfs, "stop_times", "departure_time_secs")) {
    gtfs$stop_times[trip_id %chin% relevant_trips, `:=`(departure_time_secs, 
                                                        gtfstools:::string_to_seconds(departure_time))]
    created_departure_secs <- TRUE
  }
  if (!gtfsio::check_field_exists(gtfs, "stop_times", "arrival_time_secs")) {
    gtfs$stop_times[trip_id %chin% relevant_trips, `:=`(arrival_time_secs, 
                                                        gtfstools:::string_to_seconds(arrival_time))]
    created_arrival_secs <- TRUE
  }
  departure_times <- lapply(relevant_trips, function(trip) {
    trip_frequencies <- gtfs$frequencies[trip_id == trip]
    trip_frequencies[, `:=`(departure_times, mapply(function(from, to, by) as.integer(head(seq(from, to, by), -1)), 
                                                    from = start_time_secs, to = end_time_secs, by = headway_secs, 
                                                    SIMPLIFY = FALSE))]
    departure_secs <- unlist(trip_frequencies$departure_times)
    departure_secs <- unique(departure_secs)
    new_trips_names <- sprintf("%s_%d", trip, seq_along(departure_secs))
    departure_secs <- structure(departure_secs, names = new_trips_names)
  })
  templates <- lapply(relevant_trips, function(trip) {
    template <- gtfs$stop_times[trip_id == trip]
    if (identical(template$departure_time_secs, integer(0))) {
      first_departure <- integer(0)
    } else {
      first_departure <- min(template$departure_time_secs, 
                             na.rm = TRUE)
    }
    template[, `:=`(departure_time_secs = departure_time_secs - 
                      first_departure, arrival_time_secs = arrival_time_secs - 
                      first_departure)][]
  })
  stop_times_to_add <- mapply(departure_times, templates, 
                              SIMPLIFY = FALSE, FUN = function(departures, template) {
                                n_stops <- nrow(template)
                                n_departures <- length(departures)
                                seconds_to_add <- as.integer(rep(departures, each = n_stops))
                                template_dep <- as.integer(rep(template$departure_time_secs, times = n_departures))
                                template_arr <- as.integer(rep(template$arrival_time_secs, times = n_departures))
                                new_departures <- as.integer(template_dep + seconds_to_add)
                                new_arrivals <- as.integer(template_arr + seconds_to_add)
                                adjusted_times <- data.table::data.table(trip_id = rep(names(departures), 
                                                                                       each = n_stops), departure_time_secs = new_departures, 
                                                                         arrival_time_secs = new_arrivals)
                                adjusted_cols <- c("trip_id", "departure_time", 
                                                   "departure_time_secs", "arrival_time", "arrival_time_secs")
                                other_cols <- setdiff(names(template), adjusted_cols)
                                template_excess <- template[, ..other_cols]
                                adjusted_times <- cbind(adjusted_times, template_excess[rep(seq_len(n_stops), 
                                                                                            times = n_departures)])
                              })
  stop_times_to_add <- data.table::rbindlist(stop_times_to_add)
  if (ncol(stop_times_to_add) > 0) {
    stop_times_to_add[, `:=`(departure_time = gtfstools:::seconds_to_string(departure_time_secs), 
                             arrival_time = gtfstools:::seconds_to_string(arrival_time_secs))]
  }
  if (exists("created_departure_secs")) {
    gtfs$stop_times[, `:=`(departure_time_secs, NULL)]
    if (ncol(stop_times_to_add) > 0) {
      stop_times_to_add[, `:=`(departure_time_secs, NULL)]
    }
  }
  if (exists("created_arrival_secs")) {
    gtfs$stop_times[, `:=`(arrival_time_secs, NULL)]
    if (ncol(stop_times_to_add) > 0) {
      stop_times_to_add[, `:=`(arrival_time_secs, NULL)]
    }
  }
  filtered_stop_times <- gtfs$stop_times[!trip_id %chin% relevant_trips]
  gtfs$stop_times <- rbind(filtered_stop_times, stop_times_to_add)
  trips_to_add <- mapply(relevant_trips, departure_times, 
                         SIMPLIFY = FALSE, FUN = function(trip, departures) {
                           n_departures <- length(departures)
                           new_trips <- gtfs$trips[trip_id == trip]
                           new_trips <- new_trips[rep(1, n_departures)]
                           new_trips[, `:=`(trip_id, names(departures))][]
                         })
  trips_to_add <- data.table::rbindlist(trips_to_add)
  filtered_trips <- gtfs$trips[!trip_id %chin% relevant_trips]
  gtfs$trips <- rbind(filtered_trips, trips_to_add)
  if (exists("created_start_secs")) {
    gtfs$frequencies[, `:=`(start_time_secs, NULL)]
  }
  if (exists("created_end_secs")) {
    gtfs$frequencies[, `:=`(end_time_secs, NULL)]
  }
  filtered_frequencies <- gtfs$frequencies[!trip_id %chin% 
                                             relevant_trips]
  if (nrow(filtered_frequencies) > 0) {
    gtfs$frequencies <- filtered_frequencies
  } else {
    gtfs$frequencies <- NULL
  }
  return(gtfs)
}

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

horario_para_segundos <- function(horario) {
  resultado <- rep(NA_real_, length(horario))
  validos <- !is.na(horario) & horario != ""
  if (any(validos)) {
    horarios_validos <- horario[validos]
    partes_list <- strsplit(horarios_validos, ":", fixed = TRUE)
    partes_matrix <- do.call(rbind, lapply(partes_list, as.numeric))
    resultado[validos] <- partes_matrix[,1] * 3600 + partes_matrix[,2] * 60 + partes_matrix[,3]
  }
  resultado
}

segundos_para_horario <- function(segundos) {
  resultado <- rep(NA_character_, length(segundos))
  validos <- !is.na(segundos)
  if (any(validos)) {
    seg_validos <- round(segundos[validos])
    horas <- floor(seg_validos / 3600)
    minutos <- floor((seg_validos %% 3600) / 60)
    segs <- seg_validos %% 60
    resultado[validos] <- sprintf("%02d:%02d:%02d", horas, minutos, segs)
  }
  resultado
}

get_pattern <- function(tipo_dia) {
  case_when(
    tipo_dia == "du"  ~ "U",
    tipo_dia == "sab" ~ "S",
    tipo_dia == "dom" ~ "D"
  )
}

filter_gtfs_base <- function(gtfs_path) {
  gtfs <- read_gtfs(gtfs_path)
  
  # Filtrar frescões
  frescoes <- gtfs$routes %>% 
    filter(route_type == '200') %>% 
    pull(route_id)
  
  gtfs <- filter_by_route_id(gtfs, frescoes, keep = FALSE)
  
  return(gtfs)
}

filter_trips_by_day <- function(gtfs, pattern) {
  trips_service_id <- gtfs$trips %>%
    filter(grepl(pattern, service_id)) %>%
    pull(trip_id)
  
  trips_fantasma <- fread("../../dados/insumos/trip_id_fantasma.txt", header = FALSE)[[1]]
  
  trips_desat <- gtfs$trips %>% 
    filter(service_id %like% 'DESAT') %>% 
    pull(trip_id)
  
  gtfs <- gtfs %>% 
    filter_by_trip_id(trips_service_id, keep = TRUE) %>% 
    filter_by_trip_id(trips_desat, keep = FALSE) %>% 
    filter_by_trip_id(trips_fantasma, keep = FALSE)
  
  return(gtfs)
}

process_shapes <- function(gtfs) {
  gtfs$shapes <- as.data.table(gtfs$shapes) %>% 
    group_by(shape_id) %>% 
    arrange(shape_id, shape_pt_sequence)
  
  shapes_sf <- convert_shapes_to_sf(gtfs) %>% 
    st_transform(31983) %>% 
    mutate(extensao = as.integer(st_length(.))) %>% 
    st_drop_geometry()
  
  return(list(gtfs = gtfs, shapes_sf = shapes_sf))
}

# Calcular extensões por linha e direção
calculate_extensions <- function(gtfs, shapes_sf, viagens_freq) {
  trips_manter <- gtfs$trips %>%
    mutate(
      letras = stringr::str_extract(trip_short_name, "[A-Z]+"),
      numero = stringr::str_extract(trip_short_name, "[0-9]+")
    ) %>%
    tidyr::unite(., trip_short_name, letras, numero, na.rm = TRUE, sep = "") %>%
    left_join(select(viagens_freq, trip_id, partidas)) %>%
    mutate(partidas = if_else(is.na(partidas), 1, partidas)) %>%
    group_by(shape_id) %>%
    mutate(ocorrencias = sum(partidas)) %>%
    ungroup() %>%
    group_by(route_id, direction_id) %>%
    slice_max(ocorrencias, n = 1) %>%
    ungroup() %>%
    distinct(shape_id, trip_short_name, .keep_all = TRUE) %>%
    select(trip_id, trip_short_name, shape_id, direction_id, route_id) %>% 
    left_join(select(shapes_sf, shape_id, extensao)) %>% 
    group_by(trip_short_name, direction_id, route_id) %>% 
    slice_min(extensao, n = 1) %>%
    ungroup() %>%
    select(trip_short_name, direction_id, route_id, extensao)
  
  return(trips_manter)
}

process_frequency_trips <- function(gtfs, current_tipo_dia) {
  viagens_freq <- gtfs$frequencies %>%
    mutate(
      start_time = as.character(start_time),
      end_time = as.character(end_time)
    ) %>%
    filter(!is.na(start_time), !is.na(end_time),
           start_time != "", end_time != "",
           start_time != "NA", end_time != "NA") %>%
    mutate(
      start_time_char = start_time,
      end_time_char = end_time
    ) %>%
    mutate(
      hora_inicio = as.numeric(substr(start_time_char, 1, 2)),
      hora_fim = as.numeric(substr(end_time_char, 1, 2))
    ) %>%
    mutate(
      start_time_adj = if_else(
        hora_inicio >= 24,
        paste0(sprintf("%02d", hora_inicio - 24), substr(start_time_char, 3, 8)),
        start_time_char
      ),
      end_time_adj = if_else(
        hora_fim >= 24,
        paste0(sprintf("%02d", hora_fim - 24), substr(end_time_char, 3, 8)),
        end_time_char
      )
    ) %>%
    mutate(
      start_time_posix = as.POSIXct(paste(Sys.Date(), start_time_adj), format = "%Y-%m-%d %H:%M:%S"),
      end_time_posix = as.POSIXct(paste(Sys.Date(), end_time_adj), format = "%Y-%m-%d %H:%M:%S")
    ) %>%
    mutate(
      start_time_final = if_else(hora_inicio >= 24, 
                                 start_time_posix + 86400,
                                 start_time_posix),
      end_time_final = if_else(hora_fim >= 24, 
                               end_time_posix + 86400,
                               end_time_posix)
    ) %>%
    select(-c(start_time, end_time, start_time_char, end_time_char, start_time_adj, end_time_adj, start_time_posix, end_time_posix)) %>%
    rename(
      start_time = start_time_final,
      end_time = end_time_final
    ) %>%
    mutate(
      duracao = as.numeric(difftime(end_time, start_time, units = "secs")),
      partidas = as.numeric(duracao / headway_secs)
    ) %>% 
    left_join(select(gtfs$trips, trip_id, trip_short_name, trip_headsign, direction_id, service_id, route_id)) %>%
    filter(!(service_id %like% 'DESAT')) %>% 
    mutate(circular = if_else(nchar(trip_headsign) == 0, TRUE, FALSE)) %>% 
    mutate(tipo_dia = substr(service_id, 1, 1))
  
  return(viagens_freq)
}

expand_frequency_trips <- function(viagens_freq) {
  viagens_freq_a <- viagens_freq %>%
    mutate(seq_start = mapply(seq, from = start_time,
                              to = end_time,
                              by = headway_secs))
  
  viagens_freq_exp <- viagens_freq_a %>%
    slice(rep(row_number(), lengths(seq_start))) %>%
    arrange(trip_id, start_time) %>%
    group_by(trip_id) %>%
    mutate(
      start_time = start_time + (row_number() * headway_secs) - headway_secs,
      end_time = start_time + headway_secs
    ) %>%
    slice(-n()) %>%
    ungroup()
  
  viagens_freq_exp <- viagens_freq_exp %>%
    dplyr::select(trip_id, trip_short_name, trip_headsign, 
                  start_time, direction_id, route_id)
  
  return(viagens_freq_exp)
}

process_regular_trips <- function(gtfs_proc, linhas_freq) {
  viagens_qh_regular <- gtfs_proc$stop_times %>%
    filter(stop_sequence == '0') %>%
    select(trip_id, departure_time) %>%
    left_join(select(gtfs_proc$trips, trip_id, trip_short_name, trip_headsign, direction_id, service_id, route_id)) %>%
    filter(trip_short_name %nin% linhas_freq) %>%
    arrange(direction_id, departure_time) %>%
    mutate(
      departure_time_char = as.character(departure_time),
      hora_partida = as.numeric(substr(departure_time_char, 1, 2)),
      departure_time_adj = if_else(
        hora_partida >= 24,
        paste0(sprintf("%02d", hora_partida - 24), substr(departure_time_char, 3, 8)),
        departure_time_char
      ),
      start_time = as.POSIXct(paste(Sys.Date(), departure_time_adj), format = "%Y-%m-%d %H:%M:%S"),
      start_time = if_else(hora_partida >= 24, 
                           start_time + 86400,
                           start_time)
    ) %>%
    select(trip_id, trip_short_name, trip_headsign, 
           direction_id, start_time, route_id)
  
  return(viagens_qh_regular)
}

consolidate_trips <- function(viagens_freq_exp, viagens_qh_regular, current_tipo_dia, gtfs, extensoes) {
  viagens_completo <- viagens_freq_exp %>%
    rbind(viagens_qh_regular) %>%
    filter(trip_short_name %nin% linhas_excluir) %>%
    select(-c(trip_id)) %>%
    mutate(departure_time = paste(sprintf("%02d", if_else(lubridate::day(start_time) != lubridate::day(Sys.Date()),
                                                          as.integer(lubridate::hour(start_time)) + 24,
                                                          lubridate::hour(start_time))), 
                                  sprintf("%02d", lubridate::minute(start_time)),
                                  sprintf("%02d", lubridate::second(start_time)), sep = ':')) %>%
    arrange(trip_short_name, direction_id, start_time) %>%
    mutate(tipo_dia = current_tipo_dia) %>%
    left_join(select(gtfs$routes, route_id, route_long_name, route_type, agency_id), by = "route_id") %>%
    left_join(select(gtfs$agency, agency_id, agency_name), by = "agency_id") %>%
    left_join(extensoes, by = c("trip_short_name", "direction_id", "route_id")) %>%
    mutate(
      hora = lubridate::hour(start_time),
      faixa = case_when(
        hora < 1 ~ "00:00-01:00",
        hora < 2 ~ "01:00-02:00",
        hora < 3 ~ "02:00-03:00",
        hora < 4 ~ "03:00-04:00",
        hora < 5 ~ "04:00-05:00",
        hora < 6 ~ "05:00-06:00",
        hora < 9 ~ "06:00-09:00",
        hora < 12 ~ "09:00-12:00",
        hora < 15 ~ "12:00-15:00",
        hora < 18 ~ "15:00-18:00",
        hora < 21 ~ "18:00-21:00",
        hora < 22 ~ "21:00-22:00",
        hora < 23 ~ "22:00-23:00",
        hora < 24 ~ "23:00-24:00",
        TRUE ~ "24:00+"
      )
    ) %>%
    select(trip_short_name, route_long_name, trip_headsign, direction_id, departure_time, faixa, 
           agency_name, extensao, route_type, tipo_dia)
  
  return(viagens_completo)
}

# ============================================================================
# FUNÇÃO PARA CALCULAR INTERVALOS ENTRE PARTIDAS (formato HH:MM:SS)
# ============================================================================
calcular_intervalos <- function(dt) {
  # Converte departure_time para segundos
  dt[, departure_seg := horario_para_segundos(departure_time)]
  # Ordena por linha, direção e horário
  setorder(dt, trip_short_name, direction_id, departure_seg)
  # Calcula diferença em segundos para a partida anterior dentro do grupo
  dt[, intervalo_seg := departure_seg - shift(departure_seg, type = "lag"), 
     by = .(trip_short_name, direction_id)]
  # Converte para formato HH:MM:SS
  dt[, intervalo := segundos_para_horario(intervalo_seg)]
  # Substitui NA (primeira de cada grupo) por "--"
  dt[is.na(intervalo), intervalo := "--"]
  # Remove colunas auxiliares
  dt[, `:=`(departure_seg = NULL, intervalo_seg = NULL)]
  # Move a coluna para o final
  setcolorder(dt, c(setdiff(names(dt), "intervalo"), "intervalo"))
  return(dt)
}

calculate_operation_hours <- function(viagens_completo) {
  horario_operacao <- viagens_completo %>%
    arrange(trip_short_name, departure_time) %>%
    group_by(trip_short_name) %>%
    reframe(
      hora_inicio = first(departure_time),
      hora_ultima_partida = last(departure_time)
    )
  
  return(horario_operacao)
}

calculate_planned_departures <- function(viagens_completo) {
  planejado_final <- viagens_completo %>%
    group_by(trip_short_name, direction_id) %>%
    reframe(partidas_planejadas = n())
  
  return(planejado_final)
}

# ============================================================================
# EXECUÇÃO PRINCIPAL - PREPARAÇÃO
# ============================================================================

cat("\n==========================================================\n")
cat("INICIANDO PROCESSAMENTO\n")
cat("==========================================================\n\n")

# Inicializar resultado final (usaremos lista para eficiência)
x_list <- list()

# Processamento base do GTFS (uma única vez)
cat("ETAPA 1: Lendo GTFS base...\n")
gtfs_base <- filter_gtfs_base(endereco_gtfs)
cat("  ✓ GTFS base carregado\n")
cat("  - Número de rotas:", nrow(gtfs_base$routes), "\n")
cat("  - Número de trips:", nrow(gtfs_base$trips), "\n\n")

# ============================================================================
# CORREÇÃO: Converter headway_secs para integer (com verificação de existência)
# ============================================================================

if(!is.null(gtfs_base$frequencies) && nrow(gtfs_base$frequencies) > 0) {
  cat("CORREÇÃO: Convertendo headway_secs para integer...\n")
  gtfs_base$frequencies$headway_secs <- as.integer(gtfs_base$frequencies$headway_secs)
  cat("  ✓ Conversão concluída\n\n")
}

# ============================================================================
# LOOP PRINCIPAL - PROCESSAR CADA TIPO DE DIA
# ============================================================================

for(current_tipo_dia in tipo_dia) {
  
  cat("\n==========================================================\n")
  cat("PROCESSANDO TIPO DE DIA:", toupper(current_tipo_dia), "\n")
  cat("==========================================================\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 1: Obter padrão do tipo de dia
  # ----------------------------------------------------------------------
  cat("PASSO 1: Obtendo padrão do tipo de dia...\n")
  pattern <- get_pattern(current_tipo_dia)
  cat("  ✓ Padrão definido:", pattern, "\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 2: Filtrar trips por tipo de dia
  # ----------------------------------------------------------------------
  cat("PASSO 2: Filtrando trips por tipo de dia...\n")
  gtfs_filtered <- filter_trips_by_day(gtfs_base, pattern)
  cat("  ✓ Trips filtradas:", nrow(gtfs_filtered$trips), "\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 3: Processar shapes
  # ----------------------------------------------------------------------
  cat("PASSO 3: Processando shapes...\n")
  shapes_result <- process_shapes(gtfs_filtered)
  gtfs_processed <- shapes_result$gtfs
  shapes_sf <- shapes_result$shapes_sf
  cat("  ✓ Shapes processados\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 4: Diagnóstico antes da conversão
  # ----------------------------------------------------------------------
  cat("PASSO 4: DIAGNÓSTICO PRÉ-CONVERSÃO\n")
  cat("  - Registros em stop_times:", nrow(gtfs_processed$stop_times), "\n")
  cat("  - Registros em frequencies:", nrow(gtfs_processed$frequencies), "\n")
  
  if(nrow(gtfs_processed$stop_times) > 0) {
    cat("  - NAs em departure_time:", sum(is.na(gtfs_processed$stop_times$departure_time)), "\n")
    cat("  - Classe de departure_time:", class(gtfs_processed$stop_times$departure_time), "\n")
    cat("  - Primeiros valores de departure_time:\n")
    print(head(gtfs_processed$stop_times$departure_time, 5))
  }
  cat("\n")
  
  # ----------------------------------------------------------------------
  # PASSO 4.5: Corrigindo tipos de dados (com verificação de nrow)
  # ----------------------------------------------------------------------
  cat("PASSO 4.5: Corrigindo tipos de dados...\n")
  
  if(!is.null(gtfs_processed$frequencies) && nrow(gtfs_processed$frequencies) > 0) {
    gtfs_processed$frequencies$headway_secs <- as.integer(gtfs_processed$frequencies$headway_secs)
    cat("  ✓ frequencies$headway_secs corrigido\n")
  }
  
  if(!is.null(gtfs_processed$stop_times) && nrow(gtfs_processed$stop_times) > 0) {
    if("departure_time_secs" %in% names(gtfs_processed$stop_times)) {
      gtfs_processed$stop_times$departure_time_secs <- as.integer(gtfs_processed$stop_times$departure_time_secs)
    }
    if("arrival_time_secs" %in% names(gtfs_processed$stop_times)) {
      gtfs_processed$stop_times$arrival_time_secs <- as.integer(gtfs_processed$stop_times$arrival_time_secs)
    }
    if("shape_dist_traveled" %in% names(gtfs_processed$stop_times)) {
      gtfs_processed$stop_times$shape_dist_traveled <- as.integer(gtfs_processed$stop_times$shape_dist_traveled)
      cat("  ✓ stop_times$shape_dist_traveled corrigido\n")
    }
    cat("  ✓ stop_times corrigido\n")
  }
  
  cat("\n")
  
  # ----------------------------------------------------------------------
  # PASSO 4.9: Diagnóstico expandido de frequencies (se existir)
  # ----------------------------------------------------------------------
  cat("PASSO 4.9: DIAGNÓSTICO EXPANDIDO\n")
  
  if(!is.null(gtfs_processed$frequencies) && nrow(gtfs_processed$frequencies) > 0) {
    cat("  Estrutura de frequencies:\n")
    print(str(gtfs_processed$frequencies))
    
    cat("\n  Primeiras linhas de frequencies:\n")
    print(head(gtfs_processed$frequencies, 3))
    
    cat("\n  Classes de cada coluna:\n")
    for(col in names(gtfs_processed$frequencies)) {
      cat("  -", col, ":", class(gtfs_processed$frequencies[[col]]), "\n")
    }
  } else {
    cat("  - frequencies não presente ou vazio\n")
  }
  cat("\n")
  
  # ----------------------------------------------------------------------
  # PASSO 5: Converter frequencies para stop_times (usa a função corrigida)
  # ----------------------------------------------------------------------
  cat("PASSO 5: Convertendo frequencies para stop_times...\n")
  
  tryCatch({
    gtfs_proc <- frequencies_to_stop_times(gtfs_processed)
    cat("  ✓ Conversão bem-sucedida!\n\n")
  }, error = function(e) {
    cat("\n  ✗✗✗ ERRO DETECTADO ✗✗✗\n")
    cat("  Mensagem:", e$message, "\n")
    stop(paste("Execução interrompida no tipo de dia:", current_tipo_dia))
  })
  
  # ----------------------------------------------------------------------
  # PASSO 6: Processar viagens com frequência
  # ----------------------------------------------------------------------
  cat("PASSO 6: Processando viagens com frequência...\n")
  viagens_freq <- process_frequency_trips(gtfs_processed, current_tipo_dia)
  cat("  ✓ Viagens processadas:", nrow(viagens_freq), "\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 6.5: Calcular extensões
  # ----------------------------------------------------------------------
  cat("PASSO 6.5: Calculando extensões por linha e direção...\n")
  extensoes <- calculate_extensions(gtfs_proc, shapes_sf, viagens_freq)
  cat("  ✓ Extensões calculadas para", nrow(extensoes), "combinações linha-direção\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 7: Expandir viagens com frequência
  # ----------------------------------------------------------------------
  cat("PASSO 7: Expandindo viagens...\n")
  viagens_freq_exp <- expand_frequency_trips(viagens_freq)
  cat("  ✓ Viagens expandidas:", nrow(viagens_freq_exp), "\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 8: Obter linhas com frequência
  # ----------------------------------------------------------------------
  cat("PASSO 8: Identificando linhas com frequência...\n")
  linhas_freq <- unique(viagens_freq_exp$trip_short_name)
  cat("  ✓ Linhas identificadas:", length(linhas_freq), "\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 9: Processar viagens regulares
  # ----------------------------------------------------------------------
  cat("PASSO 9: Processando viagens regulares...\n")
  viagens_qh_regular <- process_regular_trips(gtfs_proc, linhas_freq)
  cat("  ✓ Viagens regulares processadas:", nrow(viagens_qh_regular), "\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 10: Consolidar viagens
  # ----------------------------------------------------------------------
  cat("PASSO 10: Consolidando viagens...\n")
  viagens_completo <- consolidate_trips(viagens_freq_exp, viagens_qh_regular, current_tipo_dia, gtfs_processed, extensoes)
  cat("  ✓ Total de viagens consolidadas:", nrow(viagens_completo), "\n\n")
  
  # ==========================================================================
  # MODIFICAÇÃO: Criar versões com e sem route_long_name e com intervalos
  # ==========================================================================
  viagens_completo <- as.data.table(viagens_completo)
  
  # Versão para o consolidado (mantém route_long_name)
  viagens_consolidado <- copy(viagens_completo)
  
  # Versão para salvar individualmente (remove route_long_name)
  viagens_individual <- copy(viagens_completo)
  viagens_individual[, route_long_name := NULL]
  
  # Calcular intervalos na versão individual
  viagens_individual <- calcular_intervalos(viagens_individual)
  
  # ----------------------------------------------------------------------
  # PASSO 11: Calcular horário de operação (opcional)
  # ----------------------------------------------------------------------
  cat("PASSO 11: Calculando horário de operação...\n")
  horario_operacao <- calculate_operation_hours(viagens_individual)
  cat("  ✓ Horários calculados\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 12: Calcular partidas planejadas (opcional)
  # ----------------------------------------------------------------------
  cat("PASSO 12: Calculando partidas planejadas...\n")
  planejado_final <- calculate_planned_departures(viagens_individual)
  cat("  ✓ Partidas calculadas\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 13: Salvar arquivo do tipo de dia (APENAS CSV, com intervalos e sem route_long_name)
  # ----------------------------------------------------------------------
  cat("PASSO 13: Salvando arquivo...\n")
  fwrite(viagens_individual, paste0("../../resultados/partidas/partidas_", current_tipo_dia, ".csv"))
  cat("  ✓ Arquivo salvo: partidas_", current_tipo_dia, ".csv\n\n")
  
  # ----------------------------------------------------------------------
  # PASSO 14: Adicionar ao consolidado (com route_long_name)
  # ----------------------------------------------------------------------
  cat("PASSO 14: Adicionando ao resultado consolidado...\n")
  x_list[[current_tipo_dia]] <- viagens_consolidado
  cat("  ✓ Adicionado\n")
  
  cat("\n✓✓✓ TIPO DE DIA", toupper(current_tipo_dia), "CONCLUÍDO COM SUCESSO ✓✓✓\n")
}

# ============================================================================
# FINALIZAÇÃO
# ============================================================================

cat("\n==========================================================\n")
cat("SALVANDO RESULTADO FINAL\n")
cat("==========================================================\n\n")

x <- rbindlist(x_list)
fwrite(x, "../../resultados/partidas/partidas.csv")
saveRDS(x, "../../resultados/partidas/partidas.rds")
cat("✓ Arquivos finais salvos: partidas.csv e partidas.rds\n")
cat("✓ Total de linhas:", nrow(x), "\n\n")

cat("==========================================================\n")
cat("PROCESSAMENTO COMPLETO FINALIZADO!\n")
cat("==========================================================\n")