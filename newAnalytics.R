library(data.table)
library(dplyr)
library(lubridate)
library(readr)
library(tidyr)
library(purrr)

# --- 1. CONFIGURACIÓN ---
base_path <- "/home/rserranoga/v16monitor"
#base_path <- "/Users/Ramiro/Documents/Rproj/v16"
setwd(base_path)

data_dir     <- file.path(base_path, "data")
analysis_dir <- file.path(base_path, "analysis")

if(!dir.exists(analysis_dir)) dir.create(analysis_dir, recursive = TRUE)

# File Paths
output_hourly       <- file.path(analysis_dir, "stats_hourly_summary.csv")
output_daily        <- file.path(analysis_dir, "stats_daily_summary.csv")
output_roads_pk     <- file.path(analysis_dir, "stats_hourly_roads_pk_bracket.csv")      # 3a
output_roads_gps    <- file.path(analysis_dir, "stats_hourly_roads_gps.csv")             # 3b
output_roads_daily  <- file.path(analysis_dir, "stats_daily_roads.csv")
output_hourly_roads <- file.path(analysis_dir, "stats_hourly_roads_summary.csv")

Sys.setlocale("LC_TIME", "es_ES.UTF-8")

# --- 2. MOTOR DE DEDUPLICACIÓN ---
load_clean_data <- function() {
  files <- list.files(data_dir, pattern = "_dgt_balizas\\.csv$", full.names = TRUE)
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
      PK_num = as.numeric(gsub("[^0-9.]", "", as.character(PK))),
      lat = as.numeric(lat),
      lon = as.numeric(lon)
    ) %>%
    group_by(Case_id) %>%
    reframe(
      Start_Time = min(Start_Time, na.rm = TRUE),
      mins = max(mins, na.rm = TRUE),
      Scope = first(Prov), # We standardize Prov to Scope immediately
      Road = first(Road), 
      PK = first(PK_num),
      lat = first(lat),
      lon = first(lon)
    ) %>%
    mutate(
      Hour_Bucket = floor_date(Start_Time, "hour")
    )
  
  return(clean_df)
}

# --- 3. PROCESAMIENTO HORARIO ---
run_hourly <- function() {
  message(paste("Iniciando análisis horario:", Sys.time()))
  
  df <- load_clean_data()
  if(is.null(df) || nrow(df) == 0) return(message("No hay datos nuevos."))
  
  # --- SECCIÓN A: STATS GENERALES (National/Provincial) ---
  new_stats <- bind_rows(
    df %>% group_by(Timestamp = Hour_Bucket) %>%
      summarise(Scope = "National", Count = n(), Avg_Mins_Prov = mean(mins, na.rm=T), Med_Mins_Prov = median(mins, na.rm=T), .groups = "drop"),
    df %>% group_by(Timestamp = Hour_Bucket, Scope) %>%
      summarise(Count = n(), Avg_Mins_Prov = mean(mins, na.rm=T), Med_Mins_Prov = median(mins, na.rm=T), .groups = "drop")
  ) %>%
    select(Scope, everything()) # Mueve Scope a la primera columna
  
  # Update stats_hourly_summary.csv
  if(file.exists(output_hourly)) {
    existing <- read_csv(output_hourly, show_col_types = FALSE)
    final <- bind_rows(existing, new_stats) %>% distinct(Timestamp, Scope, .keep_all = TRUE) %>% arrange(desc(Timestamp))
    write_csv(final, output_hourly)
  } else { write_csv(new_stats, output_hourly) }
  
  # --- SECCIÓN 3a: ROADS PK BRACKET ---
  new_pk <- df %>%
    group_by(Scope, Timestamp = Hour_Bucket, Road) %>%
    reframe(Count = n(), Min_PK = min(PK, na.rm = TRUE), Max_PK = max(PK, na.rm = TRUE))
  
  if(file.exists(output_roads_pk)) {
    existing_pk <- read_csv(output_roads_pk, show_col_types = FALSE)
    final_pk <- bind_rows(existing_pk, new_pk) %>% distinct(Scope, Timestamp, Road, .keep_all = TRUE)
    write_csv(final_pk, output_roads_pk)
  } else { write_csv(new_pk, output_roads_pk) }

  # --- SECCIÓN 3b: ROADS GPS (Granular) ---
  new_gps <- df %>%
    select(Scope, Timestamp = Hour_Bucket, Road, lat, lon)
  
  if(file.exists(output_roads_gps)) {
    existing_gps <- read_csv(output_roads_gps, show_col_types = FALSE)
    final_gps <- bind_rows(existing_gps, new_gps) %>% distinct() # GPS uniqueness by lat/lon/time
    write_csv(final_gps, output_roads_gps)
  } else { write_csv(new_gps, output_roads_gps) }

  # --- SECCIÓN DETALLADA: ROADS SUMMARY ---
  new_detailed <- df %>% 
    group_by(Timestamp = Hour_Bucket, Scope, Road) %>%
    reframe(Count = n(), Avg_Mins_Prov_Road = mean(mins, na.rm=T), Med_Mins_Prov_Road = median(mins, na.rm=T))
  
  if(file.exists(output_hourly_roads)) {
    existing_detailed <- read_csv(output_hourly_roads, show_col_types = FALSE)
    final_detailed <- bind_rows(existing_detailed, new_detailed) %>% distinct(Timestamp, Scope, Road, .keep_all = TRUE)
    write_csv(final_detailed, output_hourly_roads)
  } else { write_csv(new_detailed, output_hourly_roads) }
}

# --- 4. PROCESAMIENTO DIARIO ---
run_daily <- function() {
  message(paste("Iniciando análisis diario:", Sys.time()))
  
  hourly_data <- read_csv(output_hourly, show_col_types = FALSE)
  
  new_daily <- hourly_data %>%
    mutate(Date = as.Date(Timestamp)) %>%
    group_by(Scope, Date) %>%
    reframe(
      Wday = format(first(Date), "%A"),
      Daily_Count = sum(Count, na.rm = TRUE),
      Daily_Avg_Mins = mean(Avg_Mins_Prov, na.rm = TRUE),
      Daily_Med_Mins = median(Med_Mins_Prov, na.rm = TRUE)
    )
  
  if(file.exists(output_daily)) {
    existing_daily <- read_csv(output_daily, show_col_types = FALSE)
    final_daily <- bind_rows(new_daily, existing_daily) %>% distinct(Scope, Date, .keep_all = TRUE)
    write_csv(final_daily, output_daily)
  } else { write_csv(new_daily, output_daily) }

  # --- SECCIÓN B: RESUMEN DE CARRETERAS DIARIAS ---
  roads_hourly <- read_csv(output_roads_pk, show_col_types = FALSE)
  new_daily_roads <- roads_hourly %>%
    mutate(Date = as.Date(Timestamp)) %>%
    group_by(Date, Scope, Road) %>%
    reframe(Hours_Active = n(), Total_Incidents = sum(Count, na.rm = TRUE)) %>%
    group_by(Date, Scope) %>%
    mutate(Total_Day_Count = sum(Total_Incidents, na.rm = TRUE)) %>%
    ungroup() %>%
    select(Scope, Date, Road, everything()) # Mueve Scope a la primera columna

  if(file.exists(output_roads_daily)) {
    existing_rd <- read_csv(output_roads_daily, show_col_types = FALSE)
    final_rd <- bind_rows(existing_rd, new_daily_roads) %>% distinct(Date, Scope, Road, .keep_all = TRUE)
    write_csv(final_rd, output_roads_daily)
  } else { write_csv(new_daily_roads, output_roads_daily) }
}

# --- 5. EXECUTION ---
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  if (args[1] == "hourly") run_hourly()
  if (args[1] == "daily") run_daily()
}
