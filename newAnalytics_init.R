# --- RECONSTRUCCIÓN HISTÓRICA V16 MONITOR ---
# Carga las funciones y rutas del script principal
source("/home/rserranoga/v16monitor/analytics.R")
#source("/Users/Ramiro/Documents/Rproj/v16/newAnalytics.R")

message("Iniciando carga masiva de datos históricos...")
df_all <- load_clean_data() # Carga todos los archivos en /data

if(!is.null(df_all) && nrow(df_all) > 0) {
  
  # 1. Generar estadísticas horarias HISTÓRICAS (Nacional y Provincial)
  message("Generando stats_hourly_summary...")
  stats_hist <- bind_rows(
    df_all %>% group_by(Timestamp = Hour_Bucket) %>%
      summarise(Scope = "National", Count = n(), Avg_Mins_Prov = mean(mins, na.rm=T), Med_Mins_Prov = median(mins, na.rm=T), .groups = "drop"),
    df_all %>% group_by(Timestamp = Hour_Bucket, Scope) %>%
      summarise(Count = n(), Avg_Mins_Prov = mean(mins, na.rm=T), Med_Mins_Prov = median(mins, na.rm=T), .groups = "drop")
  ) %>%
    select(Scope, everything()) # Scope en columna 1
  
  write_csv(stats_hist, output_hourly)
  
  # 2. Generar el historial de Carreteras (3a: PK Bracket)
  message("Generando stats_hourly_roads_pk_bracket...")
  roads_pk_hist <- df_all %>%
    group_by(Scope, Timestamp = Hour_Bucket, Road) %>%
    reframe(Count = n(), Min_PK = min(PK, na.rm = TRUE), Max_PK = max(PK, na.rm = TRUE)) %>%
  select(Scope, everything()) # Scope en columna 1

    write_csv(roads_pk_hist, output_roads_pk)

  # 3. Generar el historial GPS Granular (3b: Lat/Lon)
  message("Generando stats_hourly_roads_gps...")
  roads_gps_hist <- df_all %>%
    select(Scope, Timestamp = Hour_Bucket, Road, lat, lon) %>%
    distinct() %>%   # Asegura una fila por incidente/ubicación
    select(Scope, everything()) # Scope en columna 1
  
  write_csv(roads_gps_hist, output_roads_gps)
  
  # 4. Generar estadísticas DETALLADAS por carretera (Performance)
  message("Generando stats_hourly_roads_summary...")
  detailed_roads_hist <- df_all %>% 
    group_by(Timestamp = Hour_Bucket, Scope, Road) %>%
    reframe(
      Scope_Road = paste0(Scope, "-", Road), # Usamos un nombre interno temporal para evitar conflictos
      Count = n(),
      Avg_Mins_Prov_Road = mean(mins, na.rm=T),
      Med_Mins_Prov_Road = median(mins, na.rm=T)
    ) %>%
    rename(Scope_Detailed = Scope_Road) %>%
    select(Scope, everything()) # Scope en columna 1
  
  write_csv(detailed_roads_hist, output_hourly_roads)
  
  # 5. Ejecutar la función diaria para consolidar basándose en los nuevos archivos
  message("Ejecutando consolidación diaria...")
  run_daily()
  
  message("¡Reconstrucción completa con éxito! Todos los archivos han sido actualizados al nuevo formato.")
} else {
  message("Error: No se encontraron datos para procesar.")
}
