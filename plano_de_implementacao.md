# Implementation Plan: Convert GTFS R Scripts to Python

## Goal Description
The objective is to convert a sequence of R scripts (`1. extrair...`, `2. ajustar...`, etc.) that process GTFS data into Python scripts. The R scripts use libraries like `gtfstools`, `dplyr`, `data.table`, and `googlesheets4`. We will migrate this data manipulation logic to Python.

## Proposed Architecture & Tech Stack
- **Language**: Python 3.x
- **Data Manipulation**: `pandas` (direct replacement for `dplyr` and `data.table`).
- **GTFS Operations**: We can use `pandas` coupled with `zipfile` to read and write GTFS (which are zip archives of CSVs), or use a specialized library if the user prefers (like `partridge` or `gtfs-kit`). Given the custom manipulations, `pandas` might be the most flexible and robust approach.
- **File Structure**: Create Python scripts correspondingly named (e.g., `1_extrair_qh_especificado_no_gtfs.py`) in the same or a dedicated directory.

## Proposed Changes

### Python Scripts Conversion
We will convert each R script to an equivalent Python script line-by-line while maintaining the sequence and logic.

#### [NEW] `1_extrair_qh_especificado_no_gtfs.py`
Will use `pandas` inner and left joins, filtering strings to accomplish what `left_join(select(...))` and `filter(...)` do in R. Will use standard `os` and `pathlib` for creating directories, and `pandas.DataFrame.to_csv` for writing output frequencies.

#### [NEW] `2_ajustar_stop_times.py`
Will implement the logic from `2. ajustar_stop_times.R`. This script does complex manipulations on GTFS `stop_times` utilizing average historical speeds derived from GPS traces.
- Reading `trips.txt`, `stop_times.txt`, `routes.txt`, `frequencies.txt`.
- Calculating `shape_dist_traveled` adjustments.
- Missing time extrapolations based on `velocidade_padrao_kmh` (15 km/h) and geometrical distances.
- Integration with historical trip data (`csv` and `rds` files via `pandas` or `pyarrow` for `.rds` if necessary, though we will need `pyarrow` or `rpy2` to read `.rds` files in Python natively. I will suggest using a pure CSV approach if the user can convert those RDS files, or we will use `pyreadr` / `rpy2` to read the RData/RDS files natively).
- Speed interpolations to rewrite `arrival_time` and `departure_time` into a new `GTFS_..._PROC.zip`.

#### [NEW] `3_desvios.py`
Will implement the logic from `3. desvios.R` which adjusts GTFS `calendar` and `calendar_dates` based on a Google Sheet tracking detours and events.
- **Google Sheets Integration**: Instead of `googlesheets4`, we will read the public Google Sheet using `pandas.read_csv()` directly from the Google Sheets export URL.
- **GTFS Modification**: Read the `GTFS_..._PROC.zip`.
- Modify `trips.txt` to suffix `service_id` with `_REG` or detour codes.
- Modify `calendar.txt` and calculate detour/exception dates using standard Python date ranges instead of `lubridate`.
- Add new rows to `calendar_dates.txt` (Exception Types 1 and 2).
- Filter `calendar_dates.txt` up to `feed_end_date` + 60 days.
- Overwrite existing GTFS ZIP with these modified files.

#### [NEW] `4_trajetos_alternativos.py`
Will implement the logic from `4. trajetos_alternativos.R` which generates a final report of detour segment lengths.
- **GTFS Reading & Filtering**: Read `trips` with `trip_headsign` containing brackets `[...]`.
- **Exclude Frescões**: Identify `route_type == '200'` routes and remove them from the detour processing.
- **Shape Length Calculation**: The R script uses the `sf` package to project geographies to EPSG:31983 and calculate distances using `st_length`. To avoid heavy GIS dependencies (`geopandas`/`shapely`/`pyproj`) which can be tricky to compile on Windows, we will use a **vectorized Haversine distance formula** natively in `pandas` using `shape_pt_lat` and `shape_pt_lon`, yielding lengths directly in kilometers securely and instantly.
- **Reporting**: Aggregate the total length by Service, Vista, Consórcio, Direction, and detour tag, writing to a new CSV file.

#### [NEW] `5_juntar_gtfs.py`
Will implement the logic from `5. juntar_gtfs.R` which combines the `sppo_..._PROC.zip` and `brt_..._PROC.zip` into a single GTFS feed.
- **Data Loading**: Read both GTFS ZIP files into dictionaries of pandas DataFrames.
- **BRT Route Adjustment**: Change `route_type` to `200` for EXEC BRT routes, else `702`. Filter to only include routes that have trips.
- **SPPO Route Adjustment**: Read `route_short_name` to determine `route_type` (`200` vs `700`).
- **File Merging**: Emulate `gtfstools::merge_gtfs()` by concatenating (`pd.concat`) corresponding DataFrames (trips, routes, stop_times, etc.) and dropping exact duplicates.
- **Cleaning**: Ensure no `stop_id == 'APAGAR'`, drop unused columns (e.g., `wheelchair_accessible`, `continuous_pickup`), validate shapes to have at least 2 points.
- **File Replacement**: Provide a function to overwrite specific `.txt` internal files (like `calendar_dates.txt`, `fare_attributes.txt`) using the raw text files provided in the user's `insumos/` directory matching the R `substituir_arquivos_gtfs` logic.
- **Public Feed Gen**: Apply custom route colors from `gtfs_cores.csv`, drop `EXCEP` services, and output `gtfs_rio-de-janeiro_pub.zip`.

#### [NEW] `6_gerar_shapes.py`
Will implement the logic from `6. gerar_shapes.R` which generates spatial vectors (Shapefiles & GeoPackages) from the combined GTFS data.
- **Dependency Loading**: We will need `geopandas` and `shapely`. (Note: Since `sf` was used in R for GIS exports, `geopandas` is the standard Python equivalent. We will install it via `pip install geopandas shapely`).
- **Data Filtering**: We will replicate the complex data intersections between `u_reg`, `reg`, `trip_especial_u`, and `trip_especial` identically using pandas logic.
- **Vector Creation**:
  - For **Trips/Shapes**: Use `shapely.geometry.LineString` looping grouped `shape_pt_lon` and `shape_pt_lat` to construct full trajectories. Set CRS to EPSG:4326.
  For **Stops**: Use `shapely.geometry.Point` from `stop_lon` and `stop_lat`.
- **Geographic Projection**: Transform the GeoDataFrames to EPSG:31983 to calculate the metric extension (length in meters, converted to integers) like `st_length()`.
- **Export**: Use `gdf.to_file(..., driver='GPKG')` and `gdf.to_file(..., driver='ESRI Shapefile')` to generate the `.shp` and `.gpkg` artifacts.

#### [NEW] `7_lista_partidas.py`
Will implement the logic from `7. lista_partidas.R` which generates the complete timetable (partidas) by expanding transit frequencies and combining them with regular scheduled trips.
- **Data Loading**: Read `gtfs_rio-de-janeiro_pub.zip` using `zipfile` and `pandas`.
- **Frequency Expansion (`frequencies_to_stop_times`)**: The R script uses a massive custom function to explode `frequencies.txt` into individual `stop_times`. In Python, we will use vectorized `pandas` operations and `pd.Timedelta` to loop over the `start_time`, `end_time`, and `headway_secs` to generate a list of exact departure times for each frequency-based trip.
- **Time Adjustments**: Correct trips that go past midnight (e.g., `24:30:00` or `25:00:00`) by wrapping them into standard datetime manipulations or seconds-since-midnight arithmetic.
- **Extensions Calculation**: Merge the generated geographic extensions (from Script 6 or recalculate straight from shapes via Haversine if they aren't loaded) to assign the length in meters to each trip.
- **Formatting and Output**: 
  - Group by direction and trip to calculate the `intervalo` (headway) formatting it as `HH:MM:SS`.
  - Export the daily operations to `resultados/partidas/partidas_du.csv`, `partidas_sab.csv`, and `partidas_dom.csv`.
  - Instead of saving an `.rds` file for the consolidated dataset, we will save it as a `.parquet` file (`partidas.parquet`), adhering to the user's earlier preference for Parquet over RDS.
  
## User Review Required
> [!IMPORTANT]
> - **Google Sheets Authentication**: Script `3. desvios.R` reads from a private Google Sheet. In R, `gs4_auth` was used. For Python, how would you like to handle this? Should I use a Service Account JSON file (e.g., `gspread`), prompt you for OAuth login, or will you make the sheet public/provide a local CSV?
> - **geopandas Installation**: Script 6 requires exporting `.shp` and `.gpkg` files. I will install `geopandas` automatically. If any C/C++ build errors happen on Windows for fiona/GDAL, we might have to use pre-built wheels, but usually `pip install geopandas` works flawlessly on recent Pythons.
> - Do you have a preference for any specific Python library for reading GTFS? (e.g., `pandas` directly handling CSVs in zip, or a specific package like `partridge`?)
> - Should the new Python scripts be placed in the same `codigos` folder alongside the R scripts, or a separate `python_scripts` directory?
> - Is there a specific Python environment/version I should target?

## Verification Plan
### Automated / Manual Tests
1. Run the new Python scripts.
2. Compare the output generated in `../../resultados/` by the Python script against known good outputs from the R scripts for the same `ano_gtfs`, `mes_gtfs`, etc.
