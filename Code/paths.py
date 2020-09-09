"""
paths.py

Specify paths to files required for workflow
"""

# Import builtins
import os
import re

# If running on a local PC, use files from an external drive
run_local = True
local_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
if run_local:
    root_dir = r"J:\opp-efed-data\scenarios"
    input_dir = r"J:\opp-efed-data\global"
else:
    root_dir = local_dir
    input_dir = os.path.join(root_dir, "Input")

# Root directories
intermediate_dir = os.path.join(root_dir, "Intermediate")
production_dir = os.path.join(root_dir, "Production")
staged_dir = os.path.join(root_dir, "Staged")
scratch_dir = os.path.join(root_dir, "Scratch")

# Raw input data
table_path = os.path.join(local_dir, "Tables")
nhd_path = os.path.join(input_dir, "NHDPlusV21", "NHDPlus{}", "NHDPlus{}")  # vpu, region
soil_path = os.path.join(input_dir, "SSURGO", "gSSURGO_{}.gdb")  # state
condensed_soil_path = os.path.join(input_dir, "CustomSSURGO", "{}", "{}.csv")  # state, table name
cdl_path = os.path.join(input_dir, "CDL", "cdl{}_{}.img")  # region, year

# Rasters
nhd_raster_path = os.path.join(nhd_path, "NHDPlusCatchment", "cat")
# soil_raster_path = os.path.join(soil_path, "MapunitRaster_10m")
soil_raster_path = os.path.join(input_dir, "SoilMap", "soil{}.tif")  # region

# Intermediate datasets
weather_path = os.path.join(input_dir, "WeatherFiles", "met{}")  # region
combo_path = os.path.join(intermediate_dir, "Combinations", "{}_{}.csv")  # region, state, year
met_grid_path = os.path.join(intermediate_dir, "Weather", "met_stations.csv")
processed_soil_path = os.path.join(intermediate_dir, "ProcessedSoils", "{}", "region_{}")  # mode, region
combined_raster_path = os.path.join(intermediate_dir, "CombinedRasters", "c{}_{}")

# Table paths
crop_params_path = os.path.join(table_path, "cdl_params.csv")
gen_params_path = os.path.join(table_path, "curve_numbers.csv")
crop_dates_path = os.path.join(table_path, "crop_dates.csv")
orchard_vine_dates_path = os.path.join(table_path, "orchard_vine_dates.csv")
crop_group_path = os.path.join(table_path, "crop_groups.csv")
met_attributes_path = os.path.join(table_path, "met_params.csv")
fields_and_qc_path = os.path.join(table_path, "fields_and_qc.csv")
irrigation_path = os.path.join(table_path, "irrigation.csv")


# Misc paths
shapefile_path = os.path.join(scratch_dir, "Shapefiles")
remote_shapefile_path = os.altsep.join(("National", "Shapefiles"))

# Production data
hydro_file_path = os.path.join(production_dir, "HydroFiles", "region_{}_{}.{}")  # region, type, ext
recipe_path = os.path.join(production_dir, "RecipeFiles", "r{}")  # region
sam_scenario_path = os.path.join(production_dir, "SamScenarios", "r{}_{}.csv")  # region, chunk
pwc_scenario_path = os.path.join(production_dir, "PwcScenarios", "{1}_{2}",
                                 "{0}_{1}_{2}.csv")  # region, crop num, crop name
pwc_outfile_path = os.path.join(production_dir, "PwcOutput", "{}_{}.txt")  # crop_num, koc
combined_scenario_path = os.path.join(production_dir, "PwcScenarios", "combined",
                                      "{}_{}_all.csv")  # crop num, crop name
pwc_metfile_path = os.path.join(production_dir, "PwcMetfiles", "s{}.csv")
pwc_selection_path = os.path.join(production_dir, "SelectedScenarios")
qc_path = os.path.join(production_dir, "QC_Files", "{].csv")  # Identifier

# PWC Selected scenario paths
summary_outfile = os.path.join(pwc_selection_path, "summary", "{}-koc{}-r{}-summary.csv")  # crop, koc, region
selected_outfile = "{}-koc{}-r{}-{}"  # crop, koc, region, test
combined_outfile = os.path.join(pwc_selection_path, "selected", "all_selected.csv")
combined_results = os.path.join(pwc_selection_path, "selected", "all_results.csv")
plot_outfile = os.path.join(pwc_selection_path, "plots", 'r{}_{}_koc{}_{}.png')  # region, crop, koc, label
results_dir = os.path.join(pwc_selection_path, "national_summary_files", '{}_{}_{}_{}')  # cdl_name, type, duration, koc



### AWS

# Remote input data
remote_nhd_path = os.altsep.join(("NHD", "NHDPlus{}", "NHDPlus{}"))  # vpu, region
remote_cdl_path = os.altsep.join(("CDL", "r{}_{}.zip"))  # region, year
remote_soil_path = os.altsep.join(("SSURGO", "gssurgo_g_{}.zip"))  # state
remote_weather_path = os.altsep.join(("Weather", "region{}.zip"))  # region
remote_table_path = os.altsep.join(("Parameters",))

# Remote production data
remote_metfile_path = os.altsep.join(("WeatherArray", "region{}"))
remote_hydrofile_path = os.altsep.join(("HydroFiles",))
remote_recipe_path = os.altsep.join(("Recipes", "region_{}_{}.npz"))
remote_scenario_path = os.altsep.join(("Scenarios", "region_{}.csv"))
