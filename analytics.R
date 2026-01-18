library(dplyr)
library(lubridate)
library(readr)
library(tidyr)
library(purrr)
library(data.table)

# --- 1. CONFIGURACIÓN ---
# Usamos una ruta absoluta para evitar problemas de carpetas
# base_path <- "/home/rserranoga/v16monitor"
# setwd(base_path)

data_dir     <- "data"
analysis_dir <- "analysis"
if(!dir.exists(analysis_dir)) dir.create(analysis_dir)

output_hourly      <- file.path(analysis_dir, "stats_hourly_summary.csv")
output_daily       <- file.path(analysis_dir, "stats_daily_summary.csv")
output_roads_top   <- file.path(analysis_dir, "stats_roads_top.csv")
output_roads_daily <- file.path(analysis_dir, "stats_roads_daily.csv")

Sys.setlocale("LC_TIME", "es_ES.UTF-8")

# --- 2. MOTOR DE DEDUPLICACIÓN ---
load_clean_data <- function(hours_back = NULL) {
  files <- list.files(data_dir, pattern = "_dgt_balizas\\.csv$", full.names = TRUE)
  if(length(files) == 0) return(NULL)
  
  if(!is.null(hours_back)) {
    file_info <- file.info(files)
    # Filtramos por tiempo de modificación
    files <- files[difftime(Sys.time(), file_info$mtime, units = "hours") < hours_back]
  }
  
  if(length(files) == 0) return(NULL)
  
  raw_df <- map_dfr(files, ~{
    dt <- data.table::fread(.x, colClasses = "character", showProgress = FALSE)
    if(!"Case_id" %in% names(dt)) return(NULL)
    return(as_tibble(dt))
  })
  
  clean_df <- raw_df %>%
    mutate(
      mins = as.numeric(mins),
      Start_Time = as_datetime(Start_Time),
      PK_num = as.numeric(gsub("[^0-9.]", "", as.character(PK)))
    ) %>%
    group_by(Case_id) %>%
    summarise(
      Start_Time = min(Start_Time, na.rm = TRUE),
      mins = max(mins, na.rm = TRUE),
      Prov = first(Prov), Road = first(Road), PK = first(PK_num),
      .groups = "drop"
    ) %>%
    mutate(
      Hour_Bucket = floor_date(Start_Time, "hour"),
      Bracket = case_when(
        hour(Hour_Bucket) < 6  ~ "00:00-06:00",
        hour(Hour_Bucket) < 12 ~ "06:00-12:00",
        hour(Hour_Bucket) < 18 ~ "12:00-18:00",
        TRUE                   ~ "18:00-00:00"
      )
    )
  
  return(clean_df)
}

# --- 3. LÓGICA HORARIA ---
run_hourly <- function() {
  message(paste("Iniciando análisis horario:", Sys.time()))
  df <- load_clean_data(hours_back = 6) # Miramos 6h atrás para asegurar cobertura
  # df <- load_clean_data(hours_back = NULL) # To populate the analysis for the first time
  
  if(is.null(df) || nrow(df) == 0) {
    return(message("No se encontraron datos en las últimas 6 horas."))
  }
  
  target_hour <- floor_date(Sys.time() - hours(1), "hour")
  
  # Evitar duplicados en el CSV
  if(file.exists(output_hourly)) {
    existing <- read_csv(output_hourly, show_col_types = FALSE)
    if(target_hour %in% as_datetime(existing$Timestamp)) {
      return(message(paste("La hora", target_hour, "ya está procesada. Saltando...")))
    }
  }
  
  hourly_data <- df %>% filter(Hour_Bucket == target_hour)
  if(nrow(hourly_data) == 0) return(message("No hay incidentes iniciados en la hora objetivo."))
  
  stats <- bind_rows(
    hourly_data %>% reframe(
      Level = "Nacional", Scope = "Total", Timestamp = target_hour,
      Count = n(), Avg_Mins = mean(mins, na.rm=T), Med_Mins = median(mins, na.rm=T)
    ),
    hourly_data %>% group_by(Prov) %>% reframe(
      Level = "Provincial", Scope = Prov, Timestamp = target_hour,
      Count = n(), Avg_Mins = mean(mins, na.rm=T), Med_Mins = median(mins, na.rm=T)
    )
  )
  
  roads <- hourly_data %>%
    group_by(Prov, Road) %>%
    reframe(Count = n(), Min_PK = min(PK, na.rm=T), Max_PK = max(PK, na.rm=T), Bracket = first(Bracket)) %>%
    group_by(Prov) %>%
    slice_max(Count, n = 1, with_ties = FALSE) %>%
    mutate(Timestamp = target_hour)
  
  write_csv(stats, output_hourly, append = file.exists(output_hourly))
  write_csv(roads, output_roads_top, append = file.exists(output_roads_top))
  message("Actualización horaria completada.")
}

# --- 4. LÓGICA DIARIA ---
run_daily <- function() {
  message("Iniciando análisis diario acumulado...")
  if(!file.exists(output_hourly)) return(message("Error: No existe el resumen horario."))
  
  full_h <- read_csv(output_hourly, show_col_types = FALSE) %>%
    distinct(Level, Scope, Timestamp, .keep_all = TRUE)
  
  daily_summary <- full_h %>%
    mutate(Date = as.Date(Timestamp), Wday = lubridate::wday(Date, label = TRUE, abbr = FALSE)) %>%
    group_by(Level, Scope, Date, Wday) %>%
    summarise(
      Daily_Count = sum(Count),
      Daily_Avg_Mins = mean(Avg_Mins),
      Daily_Med_Mins = median(Med_Mins),
      .groups = "drop"
    )
  write_csv(daily_summary, output_daily)
  
  if(file.exists(output_roads_top)) {
    full_r <- read_csv(output_roads_top, show_col_types = FALSE) %>%
      mutate(Date = as.Date(Timestamp)) %>%
      group_by(Prov, Road, Date) %>%
      summarise(Total_Day_Count = sum(Count), .groups = "drop") %>%
      group_by(Prov, Date) %>%
      slice_max(Total_Day_Count, n = 1, with_ties = FALSE)
    
    write_csv(full_r, output_roads_daily)
  }
  message("Análisis diario completado.")
}

# --- 5. DISPATCHER SEGURO ---
# Detectamos si estamos en RStudio o en la Terminal
args <- base::commandArgs(trailingOnly = TRUE)

if (interactive()) {
  message("Modo interactivo (RStudio) detectado.")
  message("Puedes ejecutar manualmente: run_hourly() o run_daily()")
} else if (length(args) > 0) {
  if (args[1] == "hourly") run_hourly()
  else if (args[1] == "daily") run_daily()
} else {
  message("Uso: Rscript v16_analytics.R [hourly|daily]")
}


run_hourly()

run_daily()
