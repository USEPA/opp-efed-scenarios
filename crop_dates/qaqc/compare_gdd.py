import io
import re
import urllib
import pandas as pd
import numpy as np
import read
import calculate_gdd
from gdd_paths import station_crosswalk, gdd_validation_dates, gdd_qaqc
from paths import gdd_output_path

overwrite = True







def main():
    gdd_table = read.gdd()
    base_temps = sorted(np.unique(gdd_table[['emergence_base_temp', 'maxcover_base_temp']].values))
    crosswalk = pd.read_csv(station_crosswalk)
    sample = random_sample(crosswalk, gdd_table)

    fetch_validation_dates(sample, base_temps, gdd_table)


main()
