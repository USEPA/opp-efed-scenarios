"""
parameters.py

Parameters and variables used by multiple scripts.
"""

# Import builtin and standard libraries
import numpy as np
import pandas as pd
from paths import fields_and_qc_path
from tools.efed_lib import FieldManager

date_fmt = "%d-%b"  # 06-Feb
fields = FieldManager(fields_and_qc_path)

# PWC scenario selection parameters
pwc_selection_field = 'pwc_class'
pwc_selection_pct = 35
pwc_min_selection = 1000

# Parameters for PWC scenario postprocessing
kocs = [10, 1000, 10000]
pwc_durations = ['acute', 'chronic', 'cancer']
selection_percentile = 90  # percentile for selection
selection_window = 0.5  # select scenarios within a range
area_weighting = True

# Chunk size for reading scenarios
chunk_size = 2000000

# Raster cell size
cell_size = 30

# Hydrologic soil groups
hydro_soil_group = pd.DataFrame(
    {'name': ['A', 'A/D', 'B', 'B/D', 'C', 'C/D', 'D'],
     'cultivated': ['A', 'A', 'B', 'B', 'C', 'C', 'D'],
     'non-cultivated': ['A', 'D', 'B', 'D', 'C', 'D', 'D']})

# USLEP (practices) values for aggregation based on Table 4 in SAM Scenario Input Parameter documentation.
# Original source: Table 5.6 in PRZM 3 Manual (Carousel et al, 2015).
# USLEP values for cultivated crops by slope bin (0-2, 2-5, 5-10, 10-15, 15-25, >25)
uslep_values = [0.6, 0.5, 0.5, 0.6, 0.8, 0.9]

# Soil depth bins;
depth_bins = np.array([5, 20, 50, 100])

# Aggegation bins for soil map units (see Addendum E of SAM Scenario Input Parameter Documentation)
aggregation_bins = {'slope': [0, 2, 5, 10, 15, 25, 200],
                    'orgC_5': [0, 0.5, 1, 1.5, 2, 3, 4, 5, 6, 12, 20, 100],
                    'sand_5': [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
                    'clay_5': [0, 5, 10, 15, 20, 25, 30, 40, 60, 80, 100]}

# Maximum thickness of 'O' horizons to be removed
o_horizon_max = 5.  # cm
slope_length_max = 300.
slope_min = 1

# Minimum soil evaporation depth
anetd = 5.  # cm

# m values for calculation of USLE LS
usle_m_bins = np.array([0, 1, 3, 3.5, 4.5, 5, 100])
usle_m_vals = np.array([0.2, 0.3, 0.35, 0.4, 0.45, 0.5])

erom_months = [str(x).zfill(2) for x in range(1, 13)] + ['ma']

# The number of soil horizons available in each soil table
# Set to the soil with the most horizons in 2016 SSURGO (in Kansas)
max_horizons = 8