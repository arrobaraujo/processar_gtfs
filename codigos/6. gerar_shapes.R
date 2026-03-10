pacman::p_load(gtfstools, dplyr, Hmisc, sf, data.table, googlesheets4)

ano_gtfs <- '2025'
mes_gtfs <- '03'
quinzena_gtfs <- '02'

endereco_gtfs_combi <- paste0("../../dados/gtfs/", ano_gtfs, "/gtfs_rio-de-janeiro_pub.zip")

gs4_auth("erickaraujo.smtr@gmail.com")

gtfs <- read_gtfs(endereco_gtfs_combi)

#trips_desat <- gtfs$trips %>% 
#  filter(service_id %like% 'EXCEP') %>% 
#  pull(trip_id)

#gtfs <- gtfs %>% 
#  filter_by_trip_id(trips_desat, keep = F)

trips_base_u_reg <- gtfs$trips %>% 
  filter(service_id == 'U_REG') %>% 
  select(trip_id,shape_id,trip_short_name,trip_headsign,direction_id,service_id) %>% 
  distinct(shape_id,.keep_all = T) %>% 
  select(-c(trip_id))

trips_base_reg <- gtfs$trips %>% 
  filter(!(shape_id %in% trips_base_u_reg$shape_id)) %>% 
  filter(service_id %like% 'REG') %>% 
  select(trip_id,shape_id,trip_short_name,trip_headsign,direction_id,service_id) %>% 
  distinct(shape_id,.keep_all = T) %>% 
  select(-c(trip_id))

trip_especial_u <- gtfs$trips %>% 
  filter(!(shape_id %in% trips_base_u_reg$shape_id)) %>% 
  filter(!(shape_id %in% trips_base_reg$shape_id)) %>% 
  filter(service_id %like% 'U') %>% 
  select(trip_id,shape_id,trip_short_name,trip_headsign,direction_id,service_id) %>% 
  distinct(shape_id,.keep_all = T) %>% 
  select(-c(trip_id))

trip_especial <- gtfs$trips %>% 
  filter(!(shape_id %in% trips_base_u_reg$shape_id)) %>% 
  filter(!(shape_id %in% trips_base_reg$shape_id)) %>% 
  filter(!(shape_id %in% trip_especial_u$shape_id)) %>% 
  select(trip_id,shape_id,trip_short_name,trip_headsign,direction_id,service_id) %>% 
  distinct(shape_id,.keep_all = T) %>% 
  select(-c(trip_id))

trips_base <- rbindlist(list(trips_base_u_reg,
                             trips_base_reg,
                             trip_especial_u,
                             trip_especial))

shapes <- convert_shapes_to_sf(gtfs) %>% 
  left_join(trips_base) %>% 
  mutate(service_id = if_else(service_id %like% '_DESAT_',substr(service_id,1,1),service_id)) %>% 
  mutate(service_id = if_else(service_id %like% 'REG',substr(service_id,1,1),service_id))

shapes_por_linha <- as.data.frame(table(shapes$trip_short_name))

descricao_desvios <- read_sheet("1QYSf_E7HrDcSDVVaF_KrolS-LRL3kTF5WnhQGh3RMy0",'descricao_desvios') %>% 
  select(cod_desvio,descricao_desvio,data_inicio,data_fim)

trips_join <- gtfs$trips %>% 
  select(shape_id,route_id) %>% 
  distinct(shape_id,.keep_all = T)

shapes_ext <- shapes %>% 
  st_transform(31983) %>% 
  mutate(extensao = as.integer(st_length(.))) %>% 
  filter(service_id != 'AN31') %>% 
  select(trip_short_name,trip_headsign,direction_id,service_id,extensao,shape_id) %>% 
  st_transform(4326) %>% 
  rename(servico = trip_short_name,
         destino = trip_headsign,
         direcao = direction_id,
         tipo_rota = service_id) %>% 
  mutate(cod_desvio = substr(tipo_rota,3,length(tipo_rota))) %>% 
  left_join(descricao_desvios) %>% 
  select(-c(cod_desvio)) %>% 
  left_join(select(trips_join,shape_id,route_id)) %>% 
  left_join(select(gtfs$routes,route_id,agency_id,route_type)) %>% 
  mutate(tipo_rota = case_when(route_type == '200' ~ 'frescao',
                               route_type == '702' ~ 'brt',
                               route_type == '700' ~ 'regular')) %>% 
  left_join(select(gtfs$agency,agency_name,agency_id)) %>% 
  select(-c(route_id,agency_id,route_type)) %>% 
  rename(consorcio = agency_name)

pasta_shape_sppo <- paste0("../../dados/shapes/",ano_gtfs)

ifelse(!dir.exists(file.path(getwd(), pasta_shape_sppo)),
       dir.create(file.path(getwd(), pasta_shape_sppo),recursive = T), FALSE)

endereco_shape_sppo_trajetos_shp <- paste0(pasta_shape_sppo,"/shapes_trajetos",
                                           "_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q.shp")

#endereco_shape_sppo_trajetos_shp_datario <- paste0(pasta_shape_sppo,
#                                                   "/trajetos_onibus_rio.shp")

endereco_shape_sppo_trajetos_gpkg <- paste0(pasta_shape_sppo,"/shapes_trajetos",
                                            "_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q.gpkg")

#st_write(shapes_ext,endereco_shape_sppo_trajetos_shp_datario,append = F)

st_write(shapes_ext,endereco_shape_sppo_trajetos_shp,append = F)

st_write(shapes_ext,endereco_shape_sppo_trajetos_gpkg,append = F)

### Shapes para pontos de parada
pontos_usados <- unique(gtfs$stop_times$stop_id)

gtfs_filtrado <- filter_by_stop_id(gtfs,pontos_usados)

pontos <- convert_stops_to_sf(gtfs_filtrado)

endereco_shape_sppo_pontos_shp <- paste0(pasta_shape_sppo,"/shapes_pontos",
                                         "_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q.shp")

endereco_shape_sppo_pontos_gpkg <- paste0(pasta_shape_sppo,"/shapes_pontos",
                                          "_",ano_gtfs,"-",mes_gtfs,"-",quinzena_gtfs,"Q.gpkg")

st_write(pontos,endereco_shape_sppo_pontos_shp,append = F)

st_write(pontos,endereco_shape_sppo_pontos_gpkg,append = F)