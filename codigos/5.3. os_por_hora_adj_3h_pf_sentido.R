library(Hmisc)
library(gtfstools)
library(dplyr)
library(tidyr)
library(data.table)
library(sf)

ano_gtfs <- "2025"
mes_gtfs <- "01"
quinzena_gtfs <- "02"

gtfs <- read_gtfs (paste0("../../dados/gtfs/", ano_gtfs, "/gtfs_rio-de-janeiro_pub.zip"))

brt <- gtfs$routes %>% 
  filter(agency_id == '20001') %>% 
  pull(route_id)

gtfs <- filter_by_route_id(gtfs,brt,keep = F)

frescao <- gtfs$routes %>% 
  filter(route_type == '200') %>% 
  pull(route_id)

gtfs <- filter_by_route_id(gtfs,frescao,keep = F)

#lecd101 <- gtfs$routes %>% 
#  filter(route_short_name == 'LECD101') %>% 
#  pull(route_id)

#gtfs <- filter_by_route_id(gtfs,lecd101,keep = T)

#filtro_linhas <- gtfs$routes %>% 
#  filter(route_short_name %in% c('LECD100','753','756','757')) %>% 
#  pull(route_id)

#gtfs <- filter_by_route_id(gtfs,filtro_linhas,keep = T)

viagens_stop_times_original <- gtfs$stop_times %>% 
  select(trip_id) %>% 
  distinct()

viagens_frequencies_original <- gtfs$frequencies %>% 
  select(trip_id) %>% 
  distinct()

viagens_stop_times_original <- viagens_stop_times_original %>% 
  filter(trip_id %nin% viagens_frequencies_original$trip_id)

trace(frequencies_to_stop_times, edit = TRUE)
### Na linha 58, substituir a palavra seq pelo valor abaixo.
### function(from, to, by) head(seq(from, to, by), -1)

gtfs_proc <- frequencies_to_stop_times(gtfs)

gtfs_proc$shapes <- as.data.table(gtfs_proc$shapes) %>% 
  group_by(shape_id) %>% 
  arrange(shape_id,shape_pt_sequence)

shapes_sf <- convert_shapes_to_sf(gtfs_proc) %>% 
  st_transform(31983) %>% 
  mutate(extensao = as.integer(st_length(.))) %>% 
  st_drop_geometry()

os_stop_times <- gtfs_proc$stop_times %>% 
  left_join(select(gtfs_proc$trips,trip_id,trip_short_name,trip_headsign,direction_id,service_id,shape_id)) %>% 
  left_join(shapes_sf) %>% 
  filter(!(service_id %like% 'DESAT')) %>% 
  mutate(circular = if_else(nchar(trip_headsign)==0,T,F)) %>% 
  mutate(tipo_dia = substr(service_id,1,1)) %>% 
  filter(stop_sequence == '0')

os_cod_novo_du <- os_stop_times %>% 
  filter(tipo_dia == 'U') %>% 
  group_by(trip_short_name,direction_id) %>% 
  distinct(arrival_time, .keep_all = T) %>% 
  ungroup() %>% 
  mutate(fx_hora = floor(as.integer(substr(arrival_time,1,2))/3)*3,
         fx_hora = if_else(fx_hora >=27,fx_hora-24,fx_hora),
         #fx_hora = if_else(fx_hora ==0,24,fx_hora)
         ) %>% 
  group_by(trip_short_name, direction_id) %>%
  ungroup()

partidas_du <- os_cod_novo_du %>% 
  group_by(trip_short_name, fx_hora, direction_id) %>% 
  summarise(partidas = n(),
            extensao = sum(extensao)) %>% 
  # mutate(fx_hora_agp = case_when(fx_hora %in% c('3','6','9') ~ 3,
  #                                fx_hora %in% c('12','15','18') ~ 12,
  #                                fx_hora %in% c('21') ~ 21,
  #                                fx_hora %in% c('0') ~ 0,
  #                                fx_hora %in% c('24') ~ 24,
  #                                TRUE ~ NA)) %>% 
  mutate(tipo_dia = 'U')

os_cod_novo_sab <- os_stop_times %>% 
  filter(tipo_dia == 'S') %>% 
  group_by(trip_short_name,direction_id) %>% 
  distinct(arrival_time, .keep_all = T) %>% 
  ungroup() %>% 
  mutate(fx_hora = floor(as.integer(substr(arrival_time,1,2))/3)*3,
         fx_hora = if_else(fx_hora >=27,fx_hora-24,fx_hora),
         #fx_hora = if_else(fx_hora ==0,24,fx_hora)
         ) %>% 
  group_by(trip_short_name, direction_id) %>%
  ungroup()

partidas_sab <- os_cod_novo_sab %>% 
  group_by(trip_short_name, fx_hora, direction_id) %>% 
  summarise(partidas = n(),
            extensao = sum(extensao)) %>% 
  # mutate(fx_hora_agp = case_when(fx_hora %in% c('3','6','9') ~ 3,
  #                                fx_hora %in% c('12','15','18') ~ 12,
  #                                fx_hora %in% c('21') ~ 21,
  #                                fx_hora %in% c('0') ~ 0,
  #                                fx_hora %in% c('24') ~ 24,
  #                                TRUE ~ NA)) %>% 
  mutate(tipo_dia = 'S')

os_cod_novo_dom <- os_stop_times %>% 
  filter(tipo_dia == 'D') %>% 
  group_by(trip_short_name,direction_id) %>% 
  distinct(arrival_time, .keep_all = T) %>% 
  ungroup() %>% 
  mutate(fx_hora = floor(as.integer(substr(arrival_time,1,2))/3)*3,
         fx_hora = if_else(fx_hora >=27,fx_hora-24,fx_hora),
         #fx_hora = if_else(fx_hora ==0,24,fx_hora)
         ) %>% 
  group_by(trip_short_name, direction_id) %>%
  ungroup()

partidas_dom <- os_cod_novo_dom %>% 
  group_by(trip_short_name, fx_hora, direction_id) %>% 
  summarise(partidas = n(),
            extensao = sum(extensao)) %>% 
  # mutate(fx_hora_agp = case_when(fx_hora %in% c('3','6','9') ~ 3,
  #                                fx_hora %in% c('12','15','18') ~ 12,
  #                                fx_hora %in% c('21') ~ 21,
  #                                fx_hora %in% c('0') ~ 0,
  #                                fx_hora %in% c('24') ~ 24,
  #                                TRUE ~ NA)) %>% 
  mutate(tipo_dia = 'D')


partidas_pf_sentido <- partidas_du %>%
  group_by(trip_short_name, direction_id, fx_hora, .drop = TRUE) %>%
  summarise(total_partidas = sum(partidas),
            total_extensao = round(sum(extensao),2)) %>%
  mutate(extensao_sentido = round(total_extensao/total_partidas, 2)) %>%
  mutate(total_partidas_PF = ceiling(total_partidas*0.625),
         total_extensao_PF = round(total_partidas_PF*extensao_sentido,2)) %>%
  mutate(tipo_dia = "PF",
         partidas = total_partidas_PF,
         extensao = total_extensao_PF) %>%
  select(trip_short_name, direction_id, partidas, extensao, fx_hora, tipo_dia)

partidas_pf <- partidas_pf_sentido %>%
  group_by(trip_short_name, fx_hora, .drop = TRUE) %>%
  summarise(total_partidas = sum(partidas),
            total_extensao = round(sum(extensao)/1000,2)) %>%
  mutate(tipo_dia = "PF") %>%
  mutate(fx_hora_agp = paste(fx_hora,tipo_dia,sep='_')) %>%
  select(-c(fx_hora, tipo_dia))

partidas <- partidas_du %>% 
  bind_rows(partidas_sab,partidas_dom, partidas_pf_sentido) %>% 
  mutate(fx_hora_agp = paste(fx_hora,tipo_dia,sep='_')) 

partidas_adj <- partidas %>%
  group_by(trip_short_name, direction_id, fx_hora_agp) %>%
  summarise(total_partidas = sum(partidas),
            total_extensao = round(sum(extensao)/1000,2)) %>%
  arrange(trip_short_name, fx_hora_agp) %>%
  pivot_wider(names_from = c(direction_id, fx_hora_agp), names_sep = "__", values_from = c(total_partidas, total_extensao), values_fill = list(total_partidas = 0,total_extensao=0)) %>% 
  rename(Serviço = trip_short_name) %>%
  rename_with(~ gsub("total_partidas", "Partidas ", .x)) %>%
  rename_with(~ gsub("total_extensao", "Quilometragem ", .x)) %>%
  rename_with(~ gsub("_U", "_du", .x)) %>%
  rename_with(~ gsub("_S", "_sab", .x)) %>%
  rename_with(~ gsub("_D", "_dom", .x)) %>%
  rename_with(~ gsub("_PF", "_pf", .x)) %>%
  rename_with(~ gsub("__0__", "Ida entre", .x))  %>%
  rename_with(~ gsub("__1__", "Volta entre", .x)) %>%
  select(Serviço, contains("0_du"), contains("3_du"), contains("6_du"), contains("9_du"),contains("12_du"), contains("15_du"),contains("18_du"),contains("21_du"), contains("24_du"),
         contains("0_sab"), contains("3_sab"), contains("6_sab"), contains("9_sab"), contains("12_sab"), contains("15_sab"), contains("18_sab"), contains("21_sab"), contains("24_sab"),
         contains("0_dom"), contains("3_dom"), contains("6_dom"), contains("9_dom"), contains("12_dom"), contains("15_dom"), contains("18_dom"), contains("21_dom"), contains("24_dom"),
         contains("0_pf"), contains("3_pf"), contains("6_pf"), contains("9_pf"), contains("12_pf"), contains("15_pf"), contains("18_pf"), contains("21_pf"), contains("24_pf")) %>% 
#rename_with(~ gsub("partidas", "Partidas entre ", .x)) %>% 
  rename_with(~ gsub("0_", " 00h e 03h ", .x)) %>%
  rename_with(~ gsub("3_", " 03h e 06h ", .x)) %>% 
  rename_with(~ gsub("6_", " 06h e 09h ", .x)) %>% 
  rename_with(~ gsub("9_", " 09h e 12h ", .x)) %>% 
  rename_with(~ gsub("12_", " 12h e 15h ", .x)) %>% 
  rename_with(~ gsub("15_", " 15h e 18h ", .x)) %>% 
  rename_with(~ gsub("18_", " 18h e 21h ", .x)) %>% 
  rename_with(~ gsub("21_", " 21h e 24h ", .x)) %>%  
  rename_with(~ gsub("24_", " 24h e 03h (dia seguinte) ", .x)) %>% 
  rename_with(~ gsub(" du", " — Dias Úteis", .x)) %>%
  rename_with(~ gsub(" sab", " — Sábado", .x)) %>% 
  rename_with(~ gsub(" dom", " — Domingo", .x))  %>%
  rename_with(~ gsub(" pf", " — Ponto Facultativo", .x))
  

fwrite(partidas_adj, paste0("C:/R_SMTR/dados/os/os_porhora_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, ".csv"),sep=';',dec=',')

library(openxlsx)
# Assumindo que partidas_adj é seu dataframe
wb <- createWorkbook()
addWorksheet(wb, "Sheet1")

# Escrever dados
writeData(wb, "Sheet1", partidas_adj)

# Congelar primeira linha e coluna
freezePane(wb, "Sheet1", firstRow = TRUE, firstCol = TRUE)

# Estilo para a primeira linha
headerStyle <- createStyle(fgFill = "#4F81BD", textDecoration = "Bold", fontColour = "white")
addStyle(wb, "Sheet1", headerStyle, rows = 1, cols = 1:ncol(partidas_adj))

# Cores alternadas para as demais linhas
evenRowStyle <- createStyle(fgFill = "#DCE6F1")
oddRowStyle <- createStyle(fgFill = "#FFFFFF")
for (i in 2:nrow(partidas_adj)) {
  addStyle(wb, "Sheet1", 
           if (i %% 2 == 0) evenRowStyle else oddRowStyle, 
           rows = i, cols = 1:ncol(partidas_adj))
}

# Salvar o arquivo
saveWorkbook(wb, paste0("../../resultados/", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, ".xlsx"), overwrite = TRUE)
fwrite(partidas_pf_sentido, paste0("C:/R_SMTR/dados/os/partidas_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "_pf.csv"),sep=';',dec=',')