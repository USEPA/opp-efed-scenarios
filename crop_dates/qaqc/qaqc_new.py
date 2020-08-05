import pandas as pd
from gdd_paths import station_file, station_info_file, station_crosswalk, gdd_validation_dates
from paths import gdd_output_path
import calculate_gdd
import read
import numpy as np

overwrite = False


def fetch_validation_dates(sample, base_temps, gdd_table):
    print(sample.shape)
    if overwrite:
        with open('lifeboat.csv', 'w') as f:
            all_dates = []
            for grid_cell, stations in sample.groupby("stationID"):
                for station_id in stations.station_id:
                    print(station_id)
                    try:
                        full_gdd = calculate_gdd.fetch_gdd(station_id, base_temps, names=None, return_temps=False)
                        for _, row in gdd_table.iterrows():
                            gdd = pd.DataFrame({'emergence_gdd': full_gdd[row.emergence_base_temp],
                                                'maxcover_gdd': full_gdd[row.maxcover_base_temp]})
                            dates = calculate_gdd.annual_dates(gdd, row.emergence_gdd, row.maxcover_gdd, row.maxcover_gdd_start)
                            new_row = [station_id, grid_cell] + calculate_gdd.index_to_date(dates)
                            f.write(",".join(map(str, new_row)) + "\n")
                        all_dates.append(new_row)
                    except Exception as e:
                        print(e)
        table = pd.concat(all_dates, axis=0)
        table.to_csv(gdd_validation_dates)
    else:
        table = pd.read_csv(gdd_validation_dates)
    return table


def random_sample(stations, field='stationID'):
    stations = stations[['station_id', field]]
    sample = stations.groupby(field).apply(pd.DataFrame.sample, n=1).reset_index(drop=True)
    return sample.values

def perform_qaqc():
    gdd_table = read.gdd()
    test_data = pd.read_csv(gdd_output_path)
    stations = pd.read_csv(station_crosswalk)
    base_temps = sorted(np.unique(gdd_table[['emergence_base_temp', 'maxcover_base_temp']].values))
    crosswalk = pd.read_csv(station_crosswalk)
    fetch_validation_dates(crosswalk[['station_id', 'stationID']].drop_duplicates(), base_temps, gdd_table)
    dates = pd.read_csv(gdd_validation_dates)
    dates = dates.merge(stations, how='left')
    dates = dates.merge(test_data, on=['ncep_index', 'stationID', 'crop'], how='left', suffixes=('_val', '_ncep'))

    dates.to_csv('tester68.csv')


perform_qaqc()
