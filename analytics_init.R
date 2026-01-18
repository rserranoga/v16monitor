# Cargar todos los datos sin límite de tiempo
df_all <- load_clean_data(hours_back = NULL)

if(!is.null(df_all)) {
  # 1. Generar estadísticas horarias para TODO el historial
  stats_hist <- df_all %>%
    group_by(Level = "Nacional", Scope = "Total", Timestamp = Hour_Bucket) %>%
    summarise(Count = n(), Avg_Mins = mean(mins, na.rm=T), Med_Mins = median(mins, na.rm=T), .groups = "drop")
  
  stats_prov_hist <- df_all %>%
    group_by(Level = "Provincial", Scope = Prov, Timestamp = Hour_Bucket) %>%
    summarise(Count = n(), Avg_Mins = mean(mins, na.rm=T), Med_Mins = median(mins, na.rm=T), .groups = "drop")
  
  write_csv(bind_rows(stats_hist, stats_prov_hist), output_hourly)
  
  # 2. Generar el top de carreteras histórico
  roads_hist <- df_all %>%
    group_by(Prov, Road, Timestamp = Hour_Bucket) %>%
    summarise(Count = n(), Min_PK = min(PK, na.rm=T), Max_PK = max(PK, na.rm=T), Bracket = first(Bracket), .groups = "drop") %>%
    group_by(Prov, Timestamp) %>%
    slice_max(Count, n = 1, with_ties = FALSE)
  
  write_csv(roads_hist, output_roads_top)
  
  # 3. Ejecutar la función diaria para consolidar
  run_daily()
  
  message("¡Reconstrucción completa! Revisa la carpeta 'analysis'.")
}

