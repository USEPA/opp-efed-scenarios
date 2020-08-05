import os

data_dir = r"Tables"
production_dir = os.path.join("..", 'Tables')

# Met table (for state crosswalk)
met_id_field = 'stationID'

# Table paths
gdd_input_path = os.path.join(data_dir, "gdd_in.csv")
gdd_output_path = os.path.join(data_dir, "gdd_out.csv")
fixed_dates_path = os.path.join(data_dir, "fixed_dates.csv")
variable_dates_path = os.path.join(data_dir, "variable_dates.csv")
met_xwalk_path = os.path.join(data_dir, "met_crosswalk.csv")
ca_vegetable_path = os.path.join(data_dir, "california_veg.csv")

# Output paths
dates_output = os.path.join(production_dir, "crop_dates.csv")