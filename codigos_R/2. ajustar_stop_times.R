pacman::p_load(dplyr, gtfstools, lubridate, bizdays, data.table, tidyverse,
               tibble, tidyr, hms, progress)

# ==============================================================================
# CONFIGURAÇÕES
# ==============================================================================

ano_velocidade <- '2025'
mes_velocidade <- '10'

ano_gtfs <- '2026'
mes_gtfs <- '03'
quinzena_gtfs <- '04'

gtfs_processar <- 'sppo'  # "brt" ou "sppo"

endereco_gtfs <- paste0("../../dados/gtfs/", ano_gtfs, "/", gtfs_processar,
                        "_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q.zip")

velocidade_padrao_kmh <- 15

# ==============================================================================
# FUNÇÕES AUXILIARES OTIMIZADAS
# ==============================================================================

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

# Corrige horários faltantes usando shape_dist_traveled (modifica stop_times_dt in-place)
corrigir_por_distancia_inplace <- function(stop_times_dt, trip_id_atual,
                                           velocidade_padrao_kmh, horario_inicial) {
  horario_inicio_seg <- horario_para_segundos(horario_inicial)[1]
  idx <- stop_times_dt$trip_id == trip_id_atual
  dist_inicial <- stop_times_dt[idx][order(stop_sequence)][1, shape_dist_traveled_num]
  distancias <- stop_times_dt[idx, shape_dist_traveled_num]
  tempo_desde_inicio <- ((distancias - dist_inicial) / 1000) / velocidade_padrao_kmh * 3600
  horarios_seg <- horario_inicio_seg + tempo_desde_inicio
  novos_horarios <- segundos_para_horario(horarios_seg)
  
  stop_times_dt[idx & (is.na(arrival_time) | arrival_time == ""),
                arrival_time := novos_horarios[is.na(stop_times_dt[idx, arrival_time]) |
                                                 stop_times_dt[idx, arrival_time] == ""]]
  stop_times_dt[idx & (is.na(departure_time) | departure_time == ""),
                departure_time := novos_horarios[is.na(stop_times_dt[idx, departure_time]) |
                                                   stop_times_dt[idx, departure_time] == ""]]
  invisible(NULL)
}

# Ajusta shape_dist_traveled para que a primeira parada tenha valor zero
ajustar_shape_dist_traveled <- function(stop_times_dt) {
  cat("\n==============================================================================\n")
  cat("AJUSTANDO SHAPE_DIST_TRAVELED\n")
  cat("==============================================================================\n\n")
  
  stop_times_dt[, shape_dist_num := as.numeric(shape_dist_traveled)]
  
  trips_para_ajustar <- stop_times_dt[, .(
    primeira_dist = first(shape_dist_num[order(stop_sequence)]),
    min_sequence = min(stop_sequence, na.rm = TRUE)
  ), by = trip_id][!is.na(primeira_dist) & primeira_dist != 0]
  
  cat("DIAGNÓSTICO:\n")
  cat("├─ Total de trips no GTFS:", uniqueN(stop_times_dt$trip_id), "\n")
  cat("├─ Trips que não começam em 0:", nrow(trips_para_ajustar), "\n")
  cat("└─ Trips que já estão corretas:",
      uniqueN(stop_times_dt$trip_id) - nrow(trips_para_ajustar), "\n\n")
  
  if (nrow(trips_para_ajustar) == 0) {
    cat("✓ Todas as trips já começam com shape_dist_traveled = 0!\n\n")
    stop_times_dt[, shape_dist_num := NULL]
    return(invisible(NULL))
  }
  
  cat("PROCESSANDO AJUSTES...\n\n")
  pb <- progress_bar$new(
    format = "  [:bar] :percent | :current/:total | ETA: :eta",
    total = nrow(trips_para_ajustar), clear = FALSE, width = 80
  )
  
  for (i in 1:nrow(trips_para_ajustar)) {
    trip_id_atual <- trips_para_ajustar$trip_id[i]
    valor_inicial <- trips_para_ajustar$primeira_dist[i]
    stop_times_dt[trip_id == trip_id_atual,
                  shape_dist_num := shape_dist_num - valor_inicial]
    pb$tick()
  }
  
  stop_times_dt[!is.na(shape_dist_num),
                shape_dist_traveled := sprintf("%.2f", shape_dist_num)]
  stop_times_dt[, shape_dist_num := NULL]
  
  cat("\n")
  cat("==============================================================================\n")
  cat("RELATÓRIO DE AJUSTE\n")
  cat("==============================================================================\n")
  cat(sprintf("✓ Trips ajustadas: %d\n", nrow(trips_para_ajustar)))
  cat("✓ Todas as trips agora começam com shape_dist_traveled = 0.00\n")
  cat("==============================================================================\n\n")
  invisible(NULL)
}

# Corrige horários faltantes ANTES do processamento GPS
corrigir_horarios_faltantes <- function(stop_times_dt, frequencies_dt = NULL,
                                        velocidade_padrao_kmh = 15) {
  cat("\n==============================================================================\n")
  cat("CORRIGINDO HORÁRIOS FALTANTES (PRÉ-PROCESSAMENTO GPS)\n")
  cat("==============================================================================\n\n")
  
  stop_times_dt[, stop_sequence := as.numeric(stop_sequence)]
  
  stop_times_problematicos <- stop_times_dt[, .(
    total_paradas = .N,
    sem_arrival = sum(is.na(arrival_time) | arrival_time == ""),
    sem_departure = sum(is.na(departure_time) | departure_time == "")
  ), by = trip_id][sem_arrival > 0 | sem_departure > 0]
  
  cat("DIAGNÓSTICO:\n")
  cat("├─ Total de trips no GTFS:", uniqueN(stop_times_dt$trip_id), "\n")
  cat("├─ Trips com horários faltantes:", nrow(stop_times_problematicos), "\n")
  cat("└─ Total de paradas afetadas:", sum(stop_times_problematicos$sem_arrival), "\n\n")
  
  if (nrow(stop_times_problematicos) == 0) {
    cat("✓ Nenhum horário faltante! Pulando correção.\n\n")
    return(invisible(NULL))
  }
  
  freq_lookup <- NULL
  if (!is.null(frequencies_dt)) {
    freq_lookup <- frequencies_dt[, .(horario_inicial = first(start_time)), by = trip_id]
    setkey(freq_lookup, trip_id)
    cat("✓ Usando frequencies.txt para horários iniciais\n\n")
  }
  
  stop_times_dt[, shape_dist_traveled_num := as.numeric(shape_dist_traveled)]
  
  trips_problematicas <- stop_times_problematicos$trip_id
  
  cat("PROCESSANDO CORREÇÕES...\n\n")
  pb <- progress_bar$new(
    format = "  [:bar] :percent | :current/:total | ETA: :eta",
    total = length(trips_problematicas), clear = FALSE, width = 80
  )
  
  trips_corrigidas <- 0
  trips_impossivel <- 0
  
  for (i in seq_along(trips_problematicas)) {
    trip_id_atual <- trips_problematicas[i]
    pb$tick()
    
    paradas_trip <- stop_times_dt[trip_id == trip_id_atual][order(stop_sequence)]
    tem_distancia <- !all(is.na(paradas_trip$shape_dist_traveled_num))
    
    if (!tem_distancia) {
      # Sem shape_dist, não é possível corrigir – apenas registra
      trips_impossivel <- trips_impossivel + 1
      next
    }
    
    horario_inicial <- "00:00:00"
    if (!is.null(freq_lookup) && trip_id_atual %in% freq_lookup$trip_id) {
      horario_inicial <- freq_lookup[trip_id_atual, horario_inicial]
    }
    
    corrigir_por_distancia_inplace(stop_times_dt, trip_id_atual,
                                   velocidade_padrao_kmh, horario_inicial)
    trips_corrigidas <- trips_corrigidas + 1
  }
  
  stop_times_dt[, shape_dist_traveled_num := NULL]
  
  cat("\n\n")
  cat("RESULTADO DA CORREÇÃO:\n")
  cat(sprintf("├─ Trips corrigidas: %d\n", trips_corrigidas))
  cat(sprintf("└─ Trips impossíveis de corrigir (sem shape_dist): %d\n\n", trips_impossivel))
  invisible(NULL)
}

# Função para criar sumário de trips
criar_sumario_trips <- function(trips, days, service_id) {
  cat(sprintf("Processando sumário para service_id: %s (dias: %s)\n",
              service_id, paste(days, collapse = ",")))
  
  trips_filtered <- trips %>%
    filter(lubridate::wday(data) %in% days) %>%
    filter(ifelse(lubridate::wday(data) %in% c(2,3,4,5,6),
                  is.bizday(data, "Rio_Janeiro"),
                  TRUE)) %>%
    select(-c(data)) %>%
    mutate(datetime_chegada = as.POSIXct(datetime_chegada, format = "%Y-%m-%dT%H:%M:%SZ"),
           datetime_partida = as.POSIXct(datetime_partida, format = "%Y-%m-%dT%H:%M:%SZ")) %>%
    mutate(
      tempo_viagem_ajustado = case_when(
        servico == '851' & direction_id == 0 ~
          as.integer(difftime(datetime_chegada, datetime_partida, units = 'secs')) + 1800,
        servico == 'SP485' & direction_id == 0 ~
          as.integer(difftime(datetime_chegada, datetime_partida, units = 'secs')) + 1200,
        TRUE ~
          as.integer(difftime(datetime_chegada, datetime_partida, units = 'secs')) + 360),
      velocidade_media = round((distancia_planejada * 1000) / (tempo_viagem_ajustado) * 3.6, 2))
  
  trips_filtered_boxplot <- trips_filtered %>%
    group_by(servico, direction_id) %>%
    filter(!tempo_viagem_ajustado %in% boxplot.stats(tempo_viagem_ajustado)$out) %>%
    ungroup()
  
  cat(sprintf("  ├─ Viagens originais: %d\n", nrow(trips_filtered)))
  cat(sprintf("  ├─ Outliers removidos: %d\n",
              nrow(trips_filtered) - nrow(trips_filtered_boxplot)))
  cat(sprintf("  └─ Viagens finais: %d\n\n", nrow(trips_filtered_boxplot)))
  
  trips_summary <- trips_filtered_boxplot %>%
    group_by(servico, direction_id, hora = lubridate::hour(datetime_partida)) %>%
    summarise(velocidade = mean(velocidade_media), .groups = "drop") %>%
    rename(trip_short_name = servico) %>%
    mutate(service_id = service_id)
  
  vel_media <- trips_filtered_boxplot %>%
    group_by(hora = lubridate::hour(datetime_partida)) %>%
    summarise(velocidade_geral = mean(velocidade_media), .groups = "drop") %>%
    mutate(service_id = service_id)
  
  list(trips_summary, vel_media)
}

# ==============================================================================
# PROCESSAMENTO PRINCIPAL
# ==============================================================================

cat("\n")
cat("╔════════════════════════════════════════════════════════════════════════════╗\n")
cat("║              PROCESSAMENTO DE GTFS COM VELOCIDADES REAIS                   ║\n")
cat("║         VERSÃO CORRIGIDA - COM PRÉ-CORREÇÃO DE HORÁRIOS FALTANTES         ║\n")
cat("╚════════════════════════════════════════════════════════════════════════════╝\n")
cat("\n")

tempo_inicio_total <- Sys.time()

# ==============================================================================
# 1. CARREGAR DADOS DE VIAGENS
# ==============================================================================

cat("==============================================================================\n")
cat("ETAPA 1: CARREGANDO DADOS DE VIAGENS\n")
cat("==============================================================================\n\n")

if (gtfs_processar == "brt") {
  path_nm <- paste0("../../dados/viagens/", gtfs_processar, "/",
                    ano_velocidade, "/", mes_velocidade, "/validas")
  nm <- list.files(path = path_nm, full.names = TRUE,
                   pattern = "*.csv", recursive = TRUE)
  cat(sprintf("Carregando %d arquivos CSV de BRT...\n", length(nm)))
  trips <- map_df(nm, fread, colClasses = 'character') %>%
    select(servico, direction_id, datetime_partida, datetime_chegada,
           distancia_planejada, data) %>%
    mutate(distancia_planejada = as.numeric(distancia_planejada),
           distancia_planejada = distancia_planejada / 1000)
} else {
  path_nm <- paste0("../../dados/viagens/", gtfs_processar, "/",
                    ano_velocidade, "/", mes_velocidade, "/")
  nm <- list.files(path = path_nm, full.names = TRUE,
                   pattern = "*.rds", recursive = TRUE)
  cat(sprintf("Carregando %d arquivos RDS de SPPO...\n", length(nm)))
  trips <- map_df(nm, read_rds) %>%
    select(servico_informado, sentido, datetime_partida, datetime_chegada,
           distancia_planejada, data) %>%
    mutate(servico = servico_informado,
           direction_id = case_when(sentido == "I" ~ 0,
                                    sentido == "V" ~ 1,
                                    sentido == "C" ~ 0)) %>%
    select(servico, direction_id, datetime_partida, datetime_chegada,
           distancia_planejada, data)
  
  path_frescao <- paste0("../../dados/viagens/frescao/", ano_velocidade,
                         "/", mes_velocidade, "/validas/")
  if (dir.exists(path_frescao)) {
    nm_frescao <- list.files(path = path_frescao, full.names = TRUE,
                             pattern = "*.csv", recursive = TRUE)
    if (length(nm_frescao) > 0) {
      cat(sprintf("Carregando %d arquivos CSV de Frescão...\n", length(nm_frescao)))
      fread_character <- function(file_path) {
        fread(file_path, colClasses = c(servico = "character"))
      }
      trips_frescao <- map_df(nm_frescao, fread_character) %>%
        select(servico, direction_id, datetime_partida, datetime_chegada,
               distancia_planejada, data) %>%
        mutate(data = as.Date(data),
               distancia_planejada = distancia_planejada / 1000)
      trips <- rbindlist(list(trips, trips_frescao))
      rm(trips_frescao)
    }
  }
}

cat(sprintf("\n✓ Total de viagens carregadas: %d\n\n", nrow(trips)))

# ==============================================================================
# 2. CARREGAR CALENDÁRIO E PROCESSAR SUMÁRIOS
# ==============================================================================

cat("==============================================================================\n")
cat("ETAPA 2: PROCESSANDO SUMÁRIOS DE VELOCIDADE\n")
cat("==============================================================================\n\n")

load_calendar("../../dados/calendario.json")

trips_summary_list <- criar_sumario_trips(trips, c(2, 3, 4, 5, 6), "U")
sumario_du <- trips_summary_list[[1]]
vel_media_du <- trips_summary_list[[2]]

trips_summary_list <- criar_sumario_trips(trips, 7, "S")
sumario_sab <- trips_summary_list[[1]]
vel_media_sab <- trips_summary_list[[2]]

trips_summary_list <- criar_sumario_trips(trips, 1, "D")
sumario_dom <- trips_summary_list[[1]]
vel_media_dom <- trips_summary_list[[2]]

rm(trips, trips_summary_list)

sumario_combinado <- rbindlist(list(sumario_du, sumario_sab, sumario_dom)) %>%
  distinct(trip_short_name, direction_id, service_id, hora, .keep_all = TRUE) %>%
  rename(service_id_join = service_id) %>%
  mutate(trip_short_name = as.character(trip_short_name),
         direction_id = as.character(direction_id))

velocidade_combinado <- rbindlist(list(vel_media_du, vel_media_sab, vel_media_dom)) %>%
  distinct(service_id, hora, .keep_all = TRUE) %>%
  rename(service_id_join = service_id)

cat("✓ Sumários de velocidade processados com sucesso!\n\n")

# ==============================================================================
# 3. PROCESSAR GTFS E CORRIGIR HORÁRIOS FALTANTES
# ==============================================================================

cat("==============================================================================\n")
cat("ETAPA 3: PROCESSANDO GTFS\n")
cat("==============================================================================\n\n")

cat(sprintf("Lendo GTFS: %s\n\n", endereco_gtfs))
gtfs <- read_gtfs(endereco_gtfs)

# Garantir que colunas de tempo sejam character
setDT(gtfs$stop_times)
gtfs$stop_times[, `:=`(
  arrival_time = as.character(arrival_time),
  departure_time = as.character(departure_time)
)]

if (!is.null(gtfs$frequencies)) {
  setDT(gtfs$frequencies)
  gtfs$frequencies[, `:=`(
    start_time = as.character(start_time),
    end_time = as.character(end_time)
  )]
}

# Remover rotas sem trips
routes_com_trips <- unique(gtfs$trips$route_id)
routes_antes <- nrow(gtfs$routes)
gtfs$routes <- gtfs$routes[gtfs$routes$route_id %in% routes_com_trips, ]
cat(sprintf("✓ Rotas removidas (sem trips): %d | Rotas mantidas: %d\n\n",
            routes_antes - nrow(gtfs$routes), nrow(gtfs$routes)))

setDT(gtfs$stop_times)
gtfs$stop_times[, stop_sequence := as.numeric(stop_sequence)]

# PRIMEIRO: Ajustar shape_dist_traveled
ajustar_shape_dist_traveled(gtfs$stop_times)

# SEGUNDO: Corrigir horários faltantes (pré-processamento)
frequencies_dt <- NULL
if (!is.null(gtfs$frequencies)) {
  setDT(gtfs$frequencies)
  frequencies_dt <- gtfs$frequencies
}

corrigir_horarios_faltantes(gtfs$stop_times, frequencies_dt, velocidade_padrao_kmh)

cat("==============================================================================\n")
cat("PREPARANDO DADOS PARA CÁLCULO DE HORÁRIOS COM GPS\n")
cat("==============================================================================\n\n")

colunas_originais <- colnames(gtfs$stop_times)

setDT(gtfs$trips)

# Merge stop_times com trips
cat("Fazendo join com trips...\n")
stp_tms <- merge(gtfs$stop_times, gtfs$trips,
                 by = "trip_id",
                 all.x = TRUE,
                 sort = FALSE)

# Extrair start_time do frequencies (apenas o primeiro, sem duplicar linhas)
if (!is.null(gtfs$frequencies)) {
  cat("Extraindo start_time das frequencies...\n")
  setDT(gtfs$frequencies)
  freq_start <- gtfs$frequencies[, .(start_time_freq = first(start_time)), by = trip_id]
  # Faz o merge; a nova coluna será "start_time_freq" (sem conflito, pois não foi criada antes)
  stp_tms <- merge(stp_tms, freq_start, by = "trip_id", all.x = TRUE)
} else {
  # Se não há frequencies, cria coluna vazia para manter a consistência nas etapas seguintes
  stp_tms[, start_time_freq := NA_character_]
}

setorder(stp_tms, trip_id, stop_sequence)

# Define o horário de início da viagem: prioridade para frequencies, depois arrival_time da primeira parada
stp_tms[, start_time_calc := fifelse(
  !is.na(start_time_freq),
  as.character(start_time_freq),
  first(arrival_time[!is.na(arrival_time) & arrival_time != ""])
), by = trip_id]

# Normalizar horários >= 24h para formato HH:MM:SS com horas reduzidas (para conversão em ITime)
stp_tms[, start_time_normalized := {
  time_str <- first(start_time_calc)
  if (is.na(time_str) || time_str == "") {
    "00:00:00"
  } else {
    parts <- as.numeric(strsplit(time_str, ":")[[1]])
    hours <- parts[1] %% 24
    sprintf("%02d:%02d:%02d", hours, parts[2], parts[3])
  }
}, by = trip_id]

stp_tms[, horario_inicio := as.ITime(start_time_normalized)]
stp_tms[, direction_id := as.character(direction_id)]

cat("✓ Dados básicos preparados\n\n")

# ==============================================================================
# 4. CALCULAR NOVOS HORÁRIOS COM VELOCIDADES REAIS
# ==============================================================================

cat("==============================================================================\n")
cat("ETAPA 4: CALCULANDO NOVOS HORÁRIOS COM VELOCIDADES GPS\n")
cat("==============================================================================\n\n")

cat("Extraindo hora de início das trips...\n")
stp_tms[, hora := hour(horario_inicio)]

stp_tms[, service_id_join := substr(service_id, 1, 1)]
stp_tms[service_id == 'AN31', service_id_join := 'D']

cat("✓ Hora extraída\n\n")

cat("Realizando joins com velocidades GPS...\n")

setDT(sumario_combinado)
setDT(velocidade_combinado)

setkey(stp_tms, trip_short_name, direction_id, service_id_join, hora)
setkey(sumario_combinado, trip_short_name, direction_id, service_id_join, hora)

stp_tms <- merge(stp_tms, sumario_combinado,
                 by = c("trip_short_name", "direction_id", "service_id_join", "hora"),
                 all.x = TRUE, sort = FALSE)

setkey(stp_tms, service_id_join, hora)
setkey(velocidade_combinado, service_id_join, hora)

stp_tms <- merge(stp_tms, velocidade_combinado,
                 by = c("service_id_join", "hora"),
                 all.x = TRUE, sort = FALSE)

cat("✓ Joins completados\n\n")

# Identificar trips que têm dados de GPS E possuem shape_dist_traveled
stp_tms[, shape_dist_num := as.numeric(shape_dist_traveled)]
trips_com_shape <- stp_tms[, .(has_shape = !all(is.na(shape_dist_num))), by = trip_id][has_shape == TRUE, trip_id]
trips_com_gps <- stp_tms[!is.na(velocidade) | !is.na(velocidade_geral), unique(trip_id)]
trips_com_gps <- intersect(trips_com_gps, trips_com_shape)

cat(sprintf("Trips com dados GPS e shape: %d\n", length(trips_com_gps)))
cat(sprintf("Trips sem dados GPS (mantêm horários corrigidos): %d\n\n",
            uniqueN(stp_tms$trip_id) - length(trips_com_gps)))

# Após ajustar shape_dist_traveled, verificar negativos
negativos <- stp_tms[shape_dist_num < 0, .(trip_id, stop_sequence, stop_id, shape_dist_num)]
if (nrow(negativos) > 0) {
  cat("⚠  ATENÇÃO: shape_dist_traveled negativo após ajuste.\n")
  cat("Gerando relatório detalhado...\n")
  
  # Juntar com informações das trips para mais contexto
  negativos_detalhado <- merge(negativos, 
                               unique(stp_tms[, .(trip_id, route_id, trip_short_name, direction_id, service_id)]),
                               by = "trip_id", all.x = TRUE)
  
  # Salvar CSV
  fwrite(negativos_detalhado, "shape_dist_negativos.csv")
  
  # Mostrar resumo
  cat(sprintf("Total de registros com shape negativo: %d\n", nrow(negativos)))
  cat("Arquivo 'shape_dist_negativos.csv' gerado com detalhes.\n")
  cat("Verifique e corrija a fonte dos dados.\n")
}

# Marcar trips para recalcular
stp_tms[, recalcular_gps := trip_id %in% trips_com_gps]

# Calcular velocidade para trips que serão recalculadas
stp_tms[recalcular_gps == TRUE, velocidade_seg := fcase(
  !is.na(velocidade),       velocidade / 3.6,
  !is.na(velocidade_geral), velocidade_geral / 3.6,
  default = 15.0 / 3.6
)]

cat("Calculando novos horários com velocidades GPS (usando segundos absolutos)...\n")

# Calcular tempo de viagem acumulado em segundos
stp_tms[recalcular_gps == TRUE, chegada_seg := shape_dist_num / velocidade_seg]

# Converter horário de início para segundos (absolutos)
stp_tms[, inicio_seg := as.numeric(horario_inicio)]

# Calcular arrival/departure em segundos absolutos e converter para string HH:MM:SS (suporta >24h)
stp_tms[recalcular_gps == TRUE,
        `:=`(
          arrival_time = segundos_para_horario(inicio_seg + chegada_seg),
          departure_time = segundos_para_horario(inicio_seg + chegada_seg)
        )]

# Remover colunas temporárias
stp_tms[, `:=`(chegada_seg = NULL, inicio_seg = NULL, velocidade_seg = NULL)]

cat("✓ Horários GPS calculados com sucesso!\n\n")

# Garantir monotonicidade temporal por trip
cat("Garantindo monotonicidade dos horários...\n")
setorder(stp_tms, trip_id, stop_sequence)

stp_tms[, arr_seg := horario_para_segundos(arrival_time)]
stp_tms[, dep_seg := horario_para_segundos(departure_time)]

# departure >= arrival em cada parada
stp_tms[, dep_seg := pmax(dep_seg, arr_seg, na.rm = TRUE)]

# cummax para não retroceder
stp_tms[, dep_seg := {
  d <- dep_seg
  d[!is.na(d)] <- cummax(d[!is.na(d)])
  d
}, by = trip_id]

# arrival >= departure anterior
stp_tms[, arr_seg := {
  prev <- shift(dep_seg, fill = NA_real_)
  fifelse(!is.na(prev) & !is.na(arr_seg) & arr_seg < prev, prev, arr_seg)
}, by = trip_id]

# departure final >= arrival corrigida
stp_tms[, dep_seg := pmax(dep_seg, arr_seg, na.rm = TRUE)]

stp_tms[!is.na(arr_seg), arrival_time   := segundos_para_horario(arr_seg)]
stp_tms[!is.na(dep_seg), departure_time := segundos_para_horario(dep_seg)]
stp_tms[, `:=`(arr_seg = NULL, dep_seg = NULL)]

cat("✓ Monotonicidade garantida\n\n")

# ==============================================================================
# 5. VERIFICAÇÃO FINAL DE INTEGRIDADE (SEM PREENCHIMENTO AUTOMÁTICO)
# ==============================================================================

cat("==============================================================================\n")
cat("ETAPA 5: VERIFICANDO INTEGRIDADE DOS HORÁRIOS\n")
cat("==============================================================================\n\n")

# 5.1. Horários ausentes ou inválidos (interrompem o processo)
inconsistentes <- stp_tms[
  is.na(arrival_time) | arrival_time == "" |
    is.na(departure_time) | departure_time == ""
  , .(trip_id, stop_sequence, stop_id, arrival_time, departure_time)]

if (nrow(inconsistentes) > 0) {
  cat("⚠  FORAM ENCONTRADOS HORÁRIOS AUSENTES OU INVÁLIDOS!\n")
  cat(sprintf("Total de registros inconsistentes: %d\n", nrow(inconsistentes)))
  cat("Listando as primeiras ocorrências:\n")
  print(head(inconsistentes, 20))
  
  relatorio_path <- paste0("relatorio_inconsistentes_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv")
  fwrite(inconsistentes, relatorio_path)
  cat(sprintf("\nRelatório completo salvo em: %s\n", relatorio_path))
  
  stop("\n❌ Processamento interrompido devido a inconsistências nos horários. Corrija os dados de origem e execute novamente.")
} else {
  cat("✓ Nenhum horário ausente ou inválido encontrado.\n\n")
}

# 5.2. Trips com todos os horários idênticos (apenas alerta, não interrompe)
cat("Verificando trips com todos os horários iguais...\n")
trips_identicos <- stp_tms[, .(
  n_stops = .N,
  unique_arr = uniqueN(arrival_time[!is.na(arrival_time) & arrival_time != ""]),
  unique_dep = uniqueN(departure_time[!is.na(departure_time) & departure_time != ""])
), by = trip_id][n_stops > 1 & unique_arr == 1 & unique_dep == 1, trip_id]

if (length(trips_identicos) > 0) {
  cat("⚠  ATENÇÃO: As seguintes trips possuem TODOS os horários iguais (ex: 00:00:00):\n")
  print(trips_identicos)
  
  # Opcional: salvar lista em CSV
  fwrite(data.table(trip_id = trips_identicos), "trips_horarios_identicos.csv")
  cat("Lista salva em 'trips_horarios_identicos.csv'\n\n")
} else {
  cat("✓ Nenhuma trip com horários todos idênticos encontrada.\n\n")
}

# ==============================================================================
# 6. ATUALIZAR E SALVAR GTFS
# ==============================================================================

cat("==============================================================================\n")
cat("ETAPA 6: SALVANDO GTFS PROCESSADO\n")
cat("==============================================================================\n\n")

# Limpar colunas temporárias restantes
stp_tms[, `:=`(shape_dist_num = NULL, recalcular_gps = NULL,
               start_time_normalized = NULL, start_time_calc = NULL,
               velocidade = NULL, velocidade_geral = NULL,
               service_id_join = NULL, hora = NULL, horario_inicio = NULL,
               start_time_freq = NULL)]

# Manter apenas as colunas originais
stp_tms <- stp_tms[, ..colunas_originais]

gtfs$stop_times <- as.data.frame(stp_tms)

cat("✓ stop_times atualizado\n")
cat("✓ Todas as trips preservadas\n")
cat("✓ Todas as routes preservadas\n\n")

cat(sprintf("Estatísticas finais:\n"))
cat(sprintf("├─ Stops: %d\n", nrow(gtfs$stop_times)))
cat(sprintf("├─ Trips: %d\n", nrow(gtfs$trips)))
cat(sprintf("└─ Routes: %d\n\n", nrow(gtfs$routes)))

endereco_gtfs_proc <- paste0("../../dados/gtfs/", ano_gtfs, "/", gtfs_processar,
                             "_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "Q_PROC.zip")

cat(sprintf("Salvando em: %s\n\n", endereco_gtfs_proc))
write_gtfs(gtfs, endereco_gtfs_proc)

cat("✓ GTFS processado salvo com sucesso!\n\n")

# ==============================================================================
# FINALIZAÇÃO
# ==============================================================================

tempo_total <- difftime(Sys.time(), tempo_inicio_total, units = "secs")

cat("\n")
cat("╔════════════════════════════════════════════════════════════════════════════╗\n")
cat("║                       PROCESSAMENTO FINALIZADO!                            ║\n")
cat(sprintf("║                    Tempo total: %.1f segundos                           ║\n",
            as.numeric(tempo_total)))
cat("╚════════════════════════════════════════════════════════════════════════════╝\n")
cat("\n")