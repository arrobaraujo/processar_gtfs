library("openxlsx")
library("dplyr")
library("data.table")
library("stringr")
library("gtfstools")

# Parâmetros para preencher autimaticamente o nome dos arquivos
ano_gtfs <- "2025"
mes_gtfs <- "07"
quinzena_gtfs <- "02"

# Puxar trips para ler as circulares
gtfs <- read_gtfs(paste0("../../dados/gtfs/",ano_gtfs,"/gtfs_rio-de-janeiro_pub.zip"))

brt <- gtfs$routes %>% 
  filter(agency_id == '20001') %>% 
  pull(route_id)

gtfs <- filter_by_route_id(gtfs,brt,keep = F)

frescoes <- gtfs$routes %>% 
  filter(route_type == '200') %>% 
  pull(route_id)

gtfs <- filter_by_route_id(gtfs,frescoes,keep = F)

gtfs_trip <- gtfs$trips %>% 
  rename("Serviço" = "trip_short_name") %>% 
  mutate("AUX" = ifelse(trip_headsign == "Circular", 1, 2)) %>% 
  select ("Serviço","AUX") %>% 
  distinct(Serviço, .keep_all = TRUE)

# Usar output do 4.fazer_os.R
os_parc <- read.csv(paste0("C:/R_SMTR/dados/os/os_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, ".csv"),sep=';',dec=',',check.names = FALSE)
os_parc_tratada <- os_parc %>%
  select(-"Intervalo de Pico Dia Útil", 
         -"Quilometragem Dia Útil",
         -"Intervalo de Pico Sábado", 
         -"Quilometragem Sábado",
         -"Intervalo de Pico Domingo", 
         -"Quilometragem Domingo",
         -"Quilometragem Ponto Facultativo") %>% 
  rename("Partidas Ida - Dia Útil" = "Partidas Ida Dia Útil",
         "Partidas Volta - Dia Útil" = "Partidas Volta Dia Útil",
         "Viagens - Dia Útil" = "Viagens Dia Útil",
         "Partidas Ida - Sábado" = "Partidas Ida Sábado",
         "Partidas Volta - Sábado" = "Partidas Volta Sábado",
         "Viagens - Sábado" = "Viagens Sábado",
         "Partidas Ida - Domingo" = "Partidas Ida Domingo",
         "Partidas Volta - Domingo" = "Partidas Volta Domingo",
         "Viagens - Domingo" = "Viagens Domingo",
         "Partidas Ida - Ponto Facultativo" = "Partidas Ida Ponto Facultativo",
         "Partidas Volta - Ponto Facultativo" = "Partidas Volta Ponto Facultativo",
         "Viagens - Ponto Facultativo" = "Viagens Ponto Facultativo",
         "Horário Inicial - Dia Útil" = "Horário Inicial Dia Útil",
         "Horário Inicial - Sábado" = "Horário Inicial Sábado",
         "Horário Inicial - Domingo" = "Horário Inicial Domingo",
         "Horário Fim - Dia Útil" = "Horário Fim Dia Útil",
         "Horário Fim - Sábado" = "Horário Fim Sábado",
         "Horário Fim - Domingo" = "Horário Fim Domingo"
         )

# Junção e tratamento dos csv
os_parc_tratada <- os_parc_tratada %>%
  left_join(gtfs_trip, by = "Serviço")

os_parc_tratada <- os_parc_tratada %>% 
  distinct(Serviço, .keep_all = TRUE)

# Usar output do 5.3 os_por_hora_adj_3h_pf_sentido.R
partida_hora <- read.csv(paste0("C:/R_SMTR/dados/os/os_porhora_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "_sent.csv"),sep=';',dec=',',check.names = FALSE)

partida_hora <- partida_hora  %>% 
  rename(
  "Partidas Ida entre 00h e 03h — Dia Útil" = "Partidas Ida entre 00h e 03h — Dias Úteis",
  "Partidas Ida entre 03h e 06h — Dia Útil" = "Partidas Ida entre 03h e 06h — Dias Úteis",
  "Partidas Ida entre 06h e 09h — Dia Útil" = "Partidas Ida entre 06h e 09h — Dias Úteis",
  "Partidas Ida entre 09h e 12h — Dia Útil" = "Partidas Ida entre 09h e 12h — Dias Úteis",
  "Partidas Ida entre 12h e 15h — Dia Útil" = "Partidas Ida entre 12h e 15h — Dias Úteis",
  "Partidas Ida entre 15h e 18h — Dia Útil" = "Partidas Ida entre 15h e 18h — Dias Úteis",
  "Partidas Ida entre 18h e 21h — Dia Útil" = "Partidas Ida entre 18h e 21h — Dias Úteis",
  "Partidas Ida entre 21h e 24h — Dia Útil" = "Partidas Ida entre 21h e 24h — Dias Úteis",
  "Partidas Ida entre 00h e 03h — Dia Útil" = "Partidas Ida entre 00h e 03h — Dias Úteis",
  "Partidas Ida entre 24h e 03h (dia seguinte) — Dia Útil" = "Partidas Ida entre 24h e 03h (dia seguinte) — Dias Úteis",
  "Partidas Volta entre 03h e 06h — Dia Útil" = "Partidas Volta entre 03h e 06h — Dias Úteis",
  "Partidas Volta entre 06h e 09h — Dia Útil" = "Partidas Volta entre 06h e 09h — Dias Úteis", 
  "Partidas Volta entre 09h e 12h — Dia Útil" = "Partidas Volta entre 09h e 12h — Dias Úteis", 
  "Partidas Volta entre 12h e 15h — Dia Útil" = "Partidas Volta entre 12h e 15h — Dias Úteis", 
  "Partidas Volta entre 15h e 18h — Dia Útil" = "Partidas Volta entre 15h e 18h — Dias Úteis", 
  "Partidas Volta entre 18h e 21h — Dia Útil" = "Partidas Volta entre 18h e 21h — Dias Úteis", 
  "Partidas Volta entre 21h e 24h — Dia Útil" = "Partidas Volta entre 21h e 24h — Dias Úteis", 
  "Partidas Volta entre 00h e 03h — Dia Útil" = "Partidas Volta entre 00h e 03h — Dias Úteis",
  "Partidas Volta entre 24h e 03h (dia seguinte) — Dia Útil" = "Partidas Volta entre 24h e 03h (dia seguinte) — Dias Úteis"
)
  
# Junção e tratamento dos csv
os_fim <- os_parc_tratada %>%
  left_join(partida_hora, by = "Serviço")

os_fim <- os_fim %>% mutate(across(starts_with("Partidas"),~ as.numeric(gsub("[^0-9.-]", "", .))))
os_fim <- os_fim %>% mutate(across(starts_with("Extensão"),~ as.numeric(gsub("[^0-9.-]", "", .))))
os_fim <- os_fim %>%   mutate(across(starts_with("Viagens"), ~ as.numeric(gsub(",", ".", gsub("[^0-9,-]", "", .)))))

#Igualar partidas dos SN no Ponto Facultativo ao Dia Útil
os_fim <- os_fim %>%   
  mutate(
    `Partidas Ida entre 00h e 03h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 00h e 03h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 00h e 03h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 00h e 03h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 00h e 03h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 00h e 03h — Ponto Facultativo`),
    
    `Partidas Ida entre 03h e 06h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 03h e 06h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 03h e 06h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 03h e 06h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 03h e 06h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 03h e 06h — Ponto Facultativo`),
    
    `Partidas Ida entre 06h e 09h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 06h e 09h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 06h e 09h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 06h e 09h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 06h e 09h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 06h e 09h — Ponto Facultativo`),
    
    `Partidas Ida entre 09h e 12h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 09h e 12h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 09h e 12h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 09h e 12h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 09h e 12h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 09h e 12h — Ponto Facultativo`),
    
    `Partidas Ida entre 12h e 15h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 12h e 15h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 12h e 15h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 12h e 15h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 12h e 15h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 12h e 15h — Ponto Facultativo`),
    
    `Partidas Ida entre 15h e 18h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 15h e 18h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 15h e 18h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 15h e 18h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 15h e 18h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 15h e 18h — Ponto Facultativo`),
    
    `Partidas Ida entre 18h e 21h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 18h e 21h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 18h e 21h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 18h e 21h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 18h e 21h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 18h e 21h — Ponto Facultativo`),
    
    `Partidas Ida entre 21h e 24h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 21h e 24h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 21h e 24h — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 21h e 24h — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 21h e 24h — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 21h e 24h — Ponto Facultativo`),
    
    `Partidas Ida entre 24h e 03h (dia seguinte) — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Ida entre 24h e 03h (dia seguinte) — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Ida entre 24h e 03h (dia seguinte) — Ponto Facultativo`), # Mantém o valor original nas outras linhas
    
    `Partidas Volta entre 24h e 03h (dia seguinte) — Ponto Facultativo` = case_when(
      str_detect(Serviço, "^SN") ~ `Partidas Volta entre 24h e 03h (dia seguinte) — Dia Útil`,  # Substitui apenas quando "servico" começa com "SN"
      TRUE ~ `Partidas Volta entre 24h e 03h (dia seguinte) — Ponto Facultativo`)# Mantém o valor original nas outras linhas
    )

#Usando a extensão regular em todas as viagens
os_fim <- os_fim %>%
  mutate(`Quilometragem entre 00h e 03h — Dia Útil` = round(`Partidas Ida entre 00h e 03h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 00h e 03h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 03h e 06h — Dia Útil` = round(`Partidas Ida entre 03h e 06h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 03h e 06h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 06h e 09h — Dia Útil` = round(`Partidas Ida entre 06h e 09h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 06h e 09h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 09h e 12h — Dia Útil` = round(`Partidas Ida entre 09h e 12h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 09h e 12h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 12h e 15h — Dia Útil` = round(`Partidas Ida entre 12h e 15h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 12h e 15h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 15h e 18h — Dia Útil` = round(`Partidas Ida entre 15h e 18h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 15h e 18h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 18h e 21h — Dia Útil` = round(`Partidas Ida entre 18h e 21h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 18h e 21h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 21h e 24h — Dia Útil` = round(`Partidas Ida entre 21h e 24h — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 21h e 24h — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 24h e 03h (dia seguinte) — Dia Útil` = round(`Partidas Ida entre 24h e 03h (dia seguinte) — Dia Útil` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 24h e 03h (dia seguinte) — Dia Útil` * `Extensão de Volta`/ 1000, 3),
         
         `Quilometragem entre 00h e 03h — Sábado` = round(`Partidas Ida entre 00h e 03h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 00h e 03h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 03h e 06h — Sábado` = round(`Partidas Ida entre 03h e 06h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 03h e 06h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 06h e 09h — Sábado` = round(`Partidas Ida entre 06h e 09h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 06h e 09h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 09h e 12h — Sábado` = round(`Partidas Ida entre 09h e 12h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 09h e 12h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 12h e 15h — Sábado` = round(`Partidas Ida entre 12h e 15h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 12h e 15h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 15h e 18h — Sábado` = round(`Partidas Ida entre 15h e 18h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 15h e 18h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 18h e 21h — Sábado` = round(`Partidas Ida entre 18h e 21h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 18h e 21h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 21h e 24h — Sábado` = round(`Partidas Ida entre 21h e 24h — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 21h e 24h — Sábado` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 24h e 03h (dia seguinte) — Sábado` = round(`Partidas Ida entre 24h e 03h (dia seguinte) — Sábado` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 24h e 03h (dia seguinte) — Sábado` * `Extensão de Volta`/ 1000, 3),
         
         `Quilometragem entre 00h e 03h — Domingo` = round(`Partidas Ida entre 00h e 03h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 00h e 03h — Domingo` * `Extensão de Volta`/ 1000, 3), # Domingo
         `Quilometragem entre 03h e 06h — Domingo` = round(`Partidas Ida entre 03h e 06h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 03h e 06h — Domingo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 06h e 09h — Domingo` = round(`Partidas Ida entre 06h e 09h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 06h e 09h — Domingo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 09h e 12h — Domingo` = round(`Partidas Ida entre 09h e 12h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 09h e 12h — Domingo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 12h e 15h — Domingo` = round(`Partidas Ida entre 12h e 15h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 12h e 15h — Domingo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 15h e 18h — Domingo` = round(`Partidas Ida entre 15h e 18h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 15h e 18h — Domingo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 18h e 21h — Domingo` = round(`Partidas Ida entre 18h e 21h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 18h e 21h — Domingo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 21h e 24h — Domingo` = round(`Partidas Ida entre 21h e 24h — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 21h e 24h — Domingo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 24h e 03h (dia seguinte) — Domingo` = round(`Partidas Ida entre 24h e 03h (dia seguinte) — Domingo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 24h e 03h (dia seguinte) — Domingo` * `Extensão de Volta`/ 1000, 3),
         
         `Quilometragem entre 00h e 03h — Ponto Facultativo` = round(`Partidas Ida entre 00h e 03h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 00h e 03h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 03h e 06h — Ponto Facultativo` = round(`Partidas Ida entre 03h e 06h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 03h e 06h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 06h e 09h — Ponto Facultativo` = round(`Partidas Ida entre 06h e 09h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 06h e 09h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 09h e 12h — Ponto Facultativo` = round(`Partidas Ida entre 09h e 12h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 09h e 12h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 12h e 15h — Ponto Facultativo` = round(`Partidas Ida entre 12h e 15h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 12h e 15h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 15h e 18h — Ponto Facultativo` = round(`Partidas Ida entre 15h e 18h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 15h e 18h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 18h e 21h — Ponto Facultativo` = round(`Partidas Ida entre 18h e 21h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 18h e 21h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 21h e 24h — Ponto Facultativo` = round(`Partidas Ida entre 21h e 24h — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 21h e 24h — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         `Quilometragem entre 24h e 03h (dia seguinte) — Ponto Facultativo` = round(`Partidas Ida entre 24h e 03h (dia seguinte) — Ponto Facultativo` * `Extensão de Ida`/ 1000, 3) + round(`Partidas Volta entre 24h e 03h (dia seguinte) — Ponto Facultativo` * `Extensão de Volta`/ 1000, 3),
         
         `Partidas Ida - Ponto Facultativo` = `Partidas Ida entre 00h e 03h — Ponto Facultativo`+`Partidas Ida entre 03h e 06h — Ponto Facultativo`+  `Partidas Ida entre 06h e 09h — Ponto Facultativo`+
          `Partidas Ida entre 09h e 12h — Ponto Facultativo` + `Partidas Ida entre 12h e 15h — Ponto Facultativo` + `Partidas Ida entre 15h e 18h — Ponto Facultativo` + `Partidas Ida entre 18h e 21h — Ponto Facultativo`+
           `Partidas Ida entre 21h e 24h — Ponto Facultativo`+ `Partidas Ida entre 24h e 03h (dia seguinte) — Ponto Facultativo`,
        
          `Partidas Volta - Ponto Facultativo` = `Partidas Volta entre 00h e 03h — Ponto Facultativo`+`Partidas Volta entre 03h e 06h — Ponto Facultativo`+  `Partidas Volta entre 06h e 09h — Ponto Facultativo`+
          `Partidas Volta entre 09h e 12h — Ponto Facultativo` + `Partidas Volta entre 12h e 15h — Ponto Facultativo` + `Partidas Volta entre 15h e 18h — Ponto Facultativo` + `Partidas Volta entre 18h e 21h — Ponto Facultativo`+
           `Partidas Volta entre 21h e 24h — Ponto Facultativo`+ `Partidas Volta entre 24h e 03h (dia seguinte) — Ponto Facultativo`,
         
         `Viagens - Dia Útil` = (`Partidas Ida - Dia Útil` + `Partidas Volta - Dia Útil`) / `AUX`,
         `Viagens - Sábado` = (`Partidas Ida - Sábado` + `Partidas Volta - Sábado`) / `AUX`,
         `Viagens - Domingo` = (`Partidas Ida - Domingo` + `Partidas Volta - Domingo`) / `AUX`,
         `Viagens - Ponto Facultativo` = (`Partidas Ida - Ponto Facultativo` + `Partidas Volta - Ponto Facultativo`) / `AUX`
         )

# Substituir valores NA por 0, caso todas as colunas sejam NA
os_fim$`Partidas Ida - Ponto Facultativo`[is.na(os_fim$`Partidas Ida - Ponto Facultativo`)] <- 0
os_fim$`Partidas Volta - Ponto Facultativo`[is.na(os_fim$`Partidas Volta - Ponto Facultativo`)] <- 0
os_fim$`Viagens - Sábado`[is.na(os_fim$`Viagens - Sábado`)] <- 0
os_fim$`Viagens - Dia Útil`[is.na(os_fim$`Viagens - Dia Útil`)] <- 0
os_fim$`Viagens - Domingo`[is.na(os_fim$`Viagens - Domingo`)] <- 0
os_fim$`Viagens - Ponto Facultativo`[is.na(os_fim$`Viagens - Ponto Facultativo`)] <- 0

# Igualando os horários de início e fim do Ponto Facultativo aos horários do Dia Útil
os_fim$`Horário Inicial - Ponto Facultativo` <- os_fim$`Horário Inicial - Dia Útil`
os_fim$`Horário Fim - Ponto Facultativo` <- os_fim$`Horário Fim - Dia Útil`

# Colocando na ordem das colunas
os_fim <- os_fim %>%
  select('Serviço','Vista','Consórcio','Extensão de Ida','Extensão de Volta',
         'Horário Inicial - Dia Útil','Horário Fim - Dia Útil', # Dia útil
         'Partidas Ida - Dia Útil','Partidas Volta - Dia Útil','Viagens - Dia Útil',
         'Partidas Ida entre 00h e 03h — Dia Útil', 'Partidas Volta entre 00h e 03h — Dia Útil',
         'Quilometragem entre 00h e 03h — Dia Útil',
         'Partidas Ida entre 03h e 06h — Dia Útil', 'Partidas Volta entre 03h e 06h — Dia Útil',
         'Quilometragem entre 03h e 06h — Dia Útil',
         'Partidas Ida entre 06h e 09h — Dia Útil', 'Partidas Volta entre 06h e 09h — Dia Útil',
         'Quilometragem entre 06h e 09h — Dia Útil',
         'Partidas Ida entre 09h e 12h — Dia Útil', 'Partidas Volta entre 09h e 12h — Dia Útil',
         'Quilometragem entre 09h e 12h — Dia Útil',
         'Partidas Ida entre 12h e 15h — Dia Útil', 'Partidas Volta entre 12h e 15h — Dia Útil',
         'Quilometragem entre 12h e 15h — Dia Útil',
         'Partidas Ida entre 15h e 18h — Dia Útil', 'Partidas Volta entre 15h e 18h — Dia Útil',
         'Quilometragem entre 15h e 18h — Dia Útil',
         'Partidas Ida entre 18h e 21h — Dia Útil', 'Partidas Volta entre 18h e 21h — Dia Útil',
         'Quilometragem entre 18h e 21h — Dia Útil',
         'Partidas Ida entre 21h e 24h — Dia Útil', 'Partidas Volta entre 21h e 24h — Dia Útil',
         'Quilometragem entre 21h e 24h — Dia Útil',
         'Partidas Ida entre 24h e 03h (dia seguinte) — Dia Útil', 'Partidas Volta entre 24h e 03h (dia seguinte) — Dia Útil',
         'Quilometragem entre 24h e 03h (dia seguinte) — Dia Útil',
         'Horário Inicial - Sábado','Horário Fim - Sábado',  # Sábado
         'Partidas Ida - Sábado','Partidas Volta - Sábado','Viagens - Sábado',
         'Partidas Ida entre 00h e 03h — Sábado', 'Partidas Volta entre 00h e 03h — Sábado',
         'Quilometragem entre 00h e 03h — Sábado',
         'Partidas Ida entre 03h e 06h — Sábado', 'Partidas Volta entre 03h e 06h — Sábado',
         'Quilometragem entre 03h e 06h — Sábado',
         'Partidas Ida entre 06h e 09h — Sábado', 'Partidas Volta entre 06h e 09h — Sábado',
         'Quilometragem entre 06h e 09h — Sábado',
         'Partidas Ida entre 09h e 12h — Sábado', 'Partidas Volta entre 09h e 12h — Sábado',
         'Quilometragem entre 09h e 12h — Sábado',
         'Partidas Ida entre 12h e 15h — Sábado', 'Partidas Volta entre 12h e 15h — Sábado',
         'Quilometragem entre 12h e 15h — Sábado',
         'Partidas Ida entre 15h e 18h — Sábado', 'Partidas Volta entre 15h e 18h — Sábado',
         'Quilometragem entre 15h e 18h — Sábado',
         'Partidas Ida entre 18h e 21h — Sábado', 'Partidas Volta entre 18h e 21h — Sábado',
         'Quilometragem entre 18h e 21h — Sábado',
         'Partidas Ida entre 21h e 24h — Sábado', 'Partidas Volta entre 21h e 24h — Sábado',
         'Quilometragem entre 21h e 24h — Sábado',
         'Partidas Ida entre 24h e 03h (dia seguinte) — Sábado', 'Partidas Volta entre 24h e 03h (dia seguinte) — Sábado',
         'Quilometragem entre 24h e 03h (dia seguinte) — Sábado',
         'Horário Inicial - Domingo','Horário Fim - Domingo',  # Domingo
         'Partidas Ida - Domingo','Partidas Volta - Domingo','Viagens - Domingo',
         'Partidas Ida entre 00h e 03h — Domingo', 'Partidas Volta entre 00h e 03h — Domingo',
         'Quilometragem entre 00h e 03h — Domingo',
         'Partidas Ida entre 03h e 06h — Domingo', 'Partidas Volta entre 03h e 06h — Domingo',
         'Quilometragem entre 03h e 06h — Domingo', 
         'Partidas Ida entre 06h e 09h — Domingo', 'Partidas Volta entre 06h e 09h — Domingo',
         'Quilometragem entre 06h e 09h — Domingo',
         'Partidas Ida entre 09h e 12h — Domingo', 'Partidas Volta entre 09h e 12h — Domingo',
         'Quilometragem entre 09h e 12h — Domingo',
         'Partidas Ida entre 12h e 15h — Domingo', 'Partidas Volta entre 12h e 15h — Domingo',
         'Quilometragem entre 12h e 15h — Domingo',
         'Partidas Ida entre 15h e 18h — Domingo', 'Partidas Volta entre 15h e 18h — Domingo',
         'Quilometragem entre 15h e 18h — Domingo',
         'Partidas Ida entre 18h e 21h — Domingo', 'Partidas Volta entre 18h e 21h — Domingo',
         'Quilometragem entre 18h e 21h — Domingo',
         'Partidas Ida entre 21h e 24h — Domingo', 'Partidas Volta entre 21h e 24h — Domingo',
         'Quilometragem entre 21h e 24h — Domingo',
         'Partidas Ida entre 24h e 03h (dia seguinte) — Domingo', 'Partidas Volta entre 24h e 03h (dia seguinte) — Domingo',
         'Quilometragem entre 24h e 03h (dia seguinte) — Domingo',
         'Horário Inicial - Ponto Facultativo','Horário Fim - Ponto Facultativo',  # Ponto Facultativo
         'Partidas Ida - Ponto Facultativo','Partidas Volta - Ponto Facultativo','Viagens - Ponto Facultativo',
         'Partidas Ida entre 00h e 03h — Ponto Facultativo', 'Partidas Volta entre 00h e 03h — Ponto Facultativo',
         'Quilometragem entre 00h e 03h — Ponto Facultativo',
         'Partidas Ida entre 03h e 06h — Ponto Facultativo', 'Partidas Volta entre 03h e 06h — Ponto Facultativo',
         'Quilometragem entre 03h e 06h — Ponto Facultativo',
         'Partidas Ida entre 06h e 09h — Ponto Facultativo', 'Partidas Volta entre 06h e 09h — Ponto Facultativo',
         'Quilometragem entre 06h e 09h — Ponto Facultativo',
         'Partidas Ida entre 09h e 12h — Ponto Facultativo', 'Partidas Volta entre 09h e 12h — Ponto Facultativo',
         'Quilometragem entre 09h e 12h — Ponto Facultativo',
         'Partidas Ida entre 12h e 15h — Ponto Facultativo', 'Partidas Volta entre 12h e 15h — Ponto Facultativo',
         'Quilometragem entre 12h e 15h — Ponto Facultativo',
         'Partidas Ida entre 15h e 18h — Ponto Facultativo', 'Partidas Volta entre 15h e 18h — Ponto Facultativo',
         'Quilometragem entre 15h e 18h — Ponto Facultativo',
         'Partidas Ida entre 18h e 21h — Ponto Facultativo', 'Partidas Volta entre 18h e 21h — Ponto Facultativo',
         'Quilometragem entre 18h e 21h — Ponto Facultativo',
         'Partidas Ida entre 21h e 24h — Ponto Facultativo', 'Partidas Volta entre 21h e 24h — Ponto Facultativo',
         'Quilometragem entre 21h e 24h — Ponto Facultativo',
         'Partidas Ida entre 24h e 03h (dia seguinte) — Ponto Facultativo', 'Partidas Volta entre 24h e 03h (dia seguinte) — Ponto Facultativo',
         'Quilometragem entre 24h e 03h (dia seguinte) — Ponto Facultativo'
         )

# Gerando o arquivo csv
fwrite(os_fim, paste0("C:/R_SMTR/dados/os/os_porhora_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "_final.csv"),sep=';',dec=',')

# Gerando o arquivo excel
write.xlsx(os_fim,  file = paste0("C:/R_SMTR/dados/os/os_porhora_", ano_gtfs, "-", mes_gtfs, "-", quinzena_gtfs, "_final.xlsx"), overwrite = TRUE)