pacman::p_load(gtfstools,dplyr,data.table,sf,lubridate,gt,stringr,webshot2)

endereco_gtfs <- paste0("../../dados/gtfs/2025/gtfs_rio-de-janeiro_pub22.zip")

tipo_dia <- c('du', 'dom', 'sab')

`%nin%` <- function(x, table) !(x %in% table)

x <- data.frame()

for(tipo_dia in tipo_dia) {

pattern <- case_when(
  tipo_dia == "du"  ~ "U",
  tipo_dia == "sab" ~ "S",
  tipo_dia == "dom" ~ "D"
)

gtfs <- read_gtfs(endereco_gtfs)

frescoes <- gtfs$routes %>% 
  filter(route_type == '200') %>% 
  pull(route_id)

gtfs <- filter_by_route_id(gtfs,frescoes,keep = F)

trips_service_id <- gtfs$trips %>%
  filter(grepl(pattern, service_id)) %>%
  pull(trip_id)


trips_fantasma <- fread("../../dados/insumos/trip_id_fantasma.txt") %>%
  unlist()

trips_desat <- gtfs$trips %>% 
  filter(service_id %like% 'DESAT') %>% 
  pull(trip_id)

gtfs <- gtfs %>% 
  filter_by_trip_id(trips_service_id, keep = T) %>% 
  filter_by_trip_id(trips_desat, keep = F) %>% 
  filter_by_trip_id(trips_fantasma, keep = F)

gtfs$shapes <- as.data.table(gtfs$shapes) %>% 
  group_by(shape_id) %>% 
  arrange(shape_id,shape_pt_sequence)

shapes_sf <- convert_shapes_to_sf(gtfs) %>% 
  st_transform(31983) %>% 
  mutate(extensao = as.integer(st_length(.))) %>% 
  st_drop_geometry()

gtfs_proc <- gtfstools::frequencies_to_stop_times(gtfs)

viagens_freq <- gtfs$frequencies %>%
  mutate(hora_inicio = lubridate::hour(hms(start_time)),
         hora_fim = lubridate::hour(hms(end_time))) %>%
           
  mutate(start_time = if_else(lubridate::hour(hms(start_time)) >= 24,
                              hms(start_time) - lubridate::hours(24),
                              hms(start_time)
  )) %>%
  mutate(end_time = if_else(lubridate::hour(hms(end_time)) >= 24,
                            hms(end_time) - lubridate::hours(24),
                            hms(end_time)
  )) %>%
  mutate(
    start_time = paste(sprintf("%02d", lubridate::hour(start_time)),
                       sprintf("%02d", lubridate::minute(start_time)),
                       sprintf("%02d", lubridate::second(start_time)),
                       sep = ":"
    ),
    end_time = paste(sprintf("%02d", lubridate::hour(end_time)),
                     sprintf("%02d", lubridate::minute(end_time)),
                     sprintf("%02d", lubridate::second(end_time)),
                     sep = ":"
    )
  ) %>%
  mutate(
    start_time = as.POSIXct(start_time,
                            format = "%H:%M:%S", origin =
                              "1970-01-01"
    ),
    end_time = as.POSIXct(end_time,
                          format = "%H:%M:%S", origin =
                            "1970-01-01"
    )
  ) %>%
 # mutate(start_time = if_else(as.ITime(start_time) < as.ITime("02:00:00"),
 #                            start_time + 86400,
 #                            start_time
#  )) %>%
#  mutate(end_time = if_else(end_time < start_time,
#                            end_time + 86400,
#                            end_time
#  )) %>%
  mutate(start_time = ifelse(hora_inicio >= 24, 
                             start_time + 86400,
                             start_time)) %>%
  mutate(end_time = ifelse(hora_fim >= 24, 
                             end_time + 86400,
                             end_time)) %>%
  mutate(start_time = as.POSIXct(start_time, origin = "1970-01-01"),
         end_time = as.POSIXct(end_time, origin = "1970-01-01")) %>%
  select(-c(hora_inicio, hora_fim)) %>%
  mutate(
    duracao = as.numeric(difftime(end_time, start_time, units = "secs")),
    partidas = as.numeric(duracao / headway_secs)
  ) %>% 
  left_join(select(gtfs$trips,trip_id,trip_short_name,trip_headsign,direction_id,service_id)) %>% 
  filter(!(service_id %like% 'DESAT')) %>% 
  mutate(circular = if_else(nchar(trip_headsign)==0,T,F)) %>% 
  mutate(tipo_dia = substr(service_id,1,1))


viagens_freq_a <- viagens_freq %>%
    mutate(seq_start = mapply(seq, from = start_time,
                              to = end_time,
                              by = headway_secs))
  
  viagens_freq_exp <- viagens_freq_a %>%
    slice(rep(row_number(), lengths(seq_start))) %>%
    arrange(trip_id, start_time) %>%
    group_by(trip_id) %>%
    mutate(
      start_time = start_time + (row_number()*headway_secs) - headway_secs,
      end_time = start_time + headway_secs
    ) %>%
    slice(-n()) %>%
    ungroup()
  
#class(destrinchar_viagens(viagens_freq))

viagens_freq_exp <- viagens_freq_exp %>%
  dplyr::select(trip_id, trip_short_name, trip_headsign, 
         start_time, direction_id)


  linhas_freq <- unique(viagens_freq_exp$trip_short_name)

#Linhas QH regular 


viagens_qh_regular <- gtfs_proc$stop_times %>%
  filter(stop_sequence == '0') %>%
  select(trip_id, departure_time) %>%
  left_join(select(gtfs_proc$trips, trip_id, trip_short_name, trip_headsign, direction_id, service_id)) %>%
  filter(trip_short_name %nin% linhas_freq) %>%
  arrange(direction_id, departure_time) %>%
  mutate(
    datetime = paste0(Sys.Date(), " ", departure_time),
    start_time = as.POSIXct(datetime)
  ) %>%
  select(trip_id, trip_short_name, trip_headsign, 
         direction_id, start_time)

viagens_completo <- viagens_freq_exp %>%
  rbind(viagens_qh_regular) %>%
  select(-c(trip_id)) %>%
  mutate(departure_time = paste(sprintf("%02d", if_else(lubridate::day(start_time) != lubridate::day(Sys.Date()),
                                                as.integer(lubridate::hour(start_time))+24,
                                                lubridate::hour(start_time))), 
                        sprintf("%02d", lubridate::minute(start_time)),
                        sprintf("%02d", lubridate::second(start_time)),sep=':')) %>%
  arrange(trip_short_name, direction_id, start_time) %>%
  mutate(tipo_dia = tipo_dia)

horario_operacao <- viagens_completo %>%
  arrange(trip_short_name, departure_time) %>%
  group_by(trip_short_name) %>%
  reframe(
    hora_inicio = first(departure_time),
    hora_ultima_partida = last(departure_time)
  )

planejado_final <-   viagens_completo %>%
  group_by(trip_short_name, direction_id) %>%
  reframe(partidas_planejadas = n())
fwrite(viagens_completo, paste0("../../resultados/partidas/partidas_", tipo_dia, ".csv"))

x <- x %>%
  rbind(viagens_completo)
}


fwrite(x, "../../resultados/partidas/partidas.csv")