import os
import xarray as xr
import pandas as pd
import scipy.interpolate
import numpy as np
import datetime as dt


def read_cdf(path):
    return xr.open_dataset(path).to_dataframe()


def map_stations(precip_path, bounds=None, sample_year=1990):
    """ Use a representative precip file to assess the number of precipitation stations """

    # Read file and adjust longitude
    precip_table = read_cdf(precip_path.format(sample_year)).reset_index()
    precip_table['lon'] -= 360

    # Filter out points by geography and completeness
    # This line probably not necessary once we're working with global, right?
    precip_table = precip_table.groupby(['lat', 'lon']).filter(lambda x: x['precip'].sum() > 0)
    if bounds is not None:
        precip_table = \
            precip_table[(precip_table.lat >= bounds[0]) & (precip_table.lat <= bounds[1]) &
                         (precip_table.lon >= bounds[2]) & (precip_table.lon <= bounds[3])]

    # Sort values and add an index
    precip_table = \
        precip_table[['lat', 'lon']].drop_duplicates().sort_values(['lat', 'lon'])

    return precip_table.reset_index(drop=True)


class WeatherCube(object):
    def __init__(self, scratch_path, years=None, ncep_vars=None, ncep_path=None, precip_path=None, bounds=None,
                 precip_points=None, overwrite=False):

        self.scratch_path = scratch_path
        self.storage_path = os.path.join(scratch_path, "weather_cube.dat")
        self.key_path = os.path.join(scratch_path, "weather_key.npz")
        self.output_header = ["precip", "pet", "temp", "wind"]
        self.overwrite = overwrite

        # If all the necessary input parameters are provided, generate the weather file
        if not any(map(lambda x: x is None, (years, ncep_vars, ncep_path, precip_path, bounds, precip_points))):
            self.years = years
            self.precip_points = pd.DataFrame(precip_points, columns=['lat', 'lon'])
            if self.overwrite or not (os.path.exists(self.storage_path) and os.path.exists(self.key_path)):
                self.populate(ncep_vars, ncep_path, precip_path, bounds)
                self.write_key()

        # If only a path is provided, initialize the cube from existing memory-mapped data
        elif os.path.exists(self.storage_path) and os.path.exists(self.key_path):
            self.years, self.precip_points = self.load_key()

        else:
            raise ValueError("No weather cube file found in the specified directory, can't build")

    def fetch(self, point_num):
        array = np.memmap(self.storage_path, mode='r', dtype=np.float32, shape=self.shape)
        out_array = array[:, point_num]
        del array
        dates = pd.date_range(self.start_date, self.end_date)
        return pd.DataFrame(data=out_array, columns=self.output_header, index=dates)

    def load_key(self):
        data = np.load(self.key_path)
        return data['years'], pd.DataFrame(data['points'], columns=['lat', 'lon'])

    @staticmethod
    def perform_interpolation(daily_precip, daily_ncep, date):
        daily_precip['date'] = date
        points = daily_ncep[['lat', 'lon']].as_matrix()
        new_points = daily_precip[['lat', 'lon']].as_matrix()
        for value_field in ('temp', 'pet', 'wind'):
            daily_precip[value_field] = \
                scipy.interpolate.griddata(points, daily_ncep[value_field].values, new_points)
        return daily_precip

    def populate(self, ncep_vars, ncep_path, precip_path, bounds):

        # Initialize the output in-memory array
        out_array = np.memmap(self.storage_path, mode='w+', dtype=np.float32, shape=self.shape)

        for year in self.years:
            print("Running year {}...\n\tLoading datasets...".format(year))

            # Read, combine, and adjust NCEP tables
            intermediate = "ncep{}.csv".format(year)
            if False and os.path.exists(intermediate):
                ncep_table = pd.read_csv(intermediate)
                ncep_table['time'] = pd.to_datetime(ncep_table['time'])
            else:
                ncep_table = self.read_ncep(year, ncep_path, ncep_vars, bounds)
                ncep_table.to_csv(intermediate)

            # Calculate PET and eliminate unneeded headings
            ncep_table = self.process_ncep(ncep_table)

            # Load precip table
            precip_table = self.read_precip(year, precip_path)

            # Determine the offset in days between the start of the year and the start of all years
            annual_offset = (dt.date(year, 1, 1) - self.start_date).days

            # Loop through each date and perform interpolation
            print("\tPreforming daily interpolation...")
            for i, (date, ncep_group) in enumerate(ncep_table.groupby('date')):
                daily_precip = precip_table[precip_table.time == date]

                # Interpolate NCEP data to resolution of precip data
                daily_table = self.perform_interpolation(daily_precip, ncep_group, date)

                # Write to memory map
                out_array[annual_offset + i] = daily_table[['precip', 'pet', 'temp', 'wind']]

    @staticmethod
    def process_ncep(table):

        def hargreaves_samani(t_min, t_max, solar_rad, temp):
            # ;Convert sradt from W/m2 to mm/d; using 1 MJ/m2-d = 0.408 mm/d per FAO
            # srt1 = (srt(time|:,lat|:,lon|:)/1e6) * 86400. * 0.408
            # ;Hargreaves-Samani Method - PET estimate (mm/day -> cm/day)
            # har = (0.0023*srt1*(tempC+17.8)*(rtemp^0.5))/10

            solar_rad = (solar_rad / 1e6) * 86400. * 0.408
            return ((0.0023 * solar_rad) * (temp + 17.8) * ((t_max - t_min) ** 0.5)) / 10  # (mm/d -> cm/d)

        # Adjust column names
        table.rename(columns={"air": "temp", "dswrf": "solar_rad"}, inplace=True)

        # Convert date-times to dates
        table['date'] = table['time'].dt.normalize()

        # Average out sub-daily data
        table = table.groupby(['lat', 'lon', 'date']).mean().reset_index()

        # Adjust units
        table['temp'] -= 273.15  # K -> deg C

        # Calculate potential evapotranspiration using Hargreaves-Samani method
        table['pet'] = \
            hargreaves_samani(table.pop('tmin'), table.pop('tmax'), table.pop('solar_rad'), table['temp'])

        # Compute vector wind speed from uwind and vwind in m/s to cm/s
        table['wind'] = np.sqrt((table.pop('uwnd') ** 2) + (table.pop('vwnd') ** 2)) * 100.

        return table

    def read_ncep(self, year, ncep_path, ncep_vars, bounds):
        y_min, y_max, x_min, x_max = bounds

        # Read and merge all NCEP data tables for the year
        table_paths = [ncep_path.format(var, year) for var in ncep_vars]
        full_table = None
        for table_path in table_paths:
            table = read_cdf(table_path).reset_index()
            table['lon'] -= 360
            table = table[(table.lat >= y_min) & (table.lat <= y_max) & (table.lon >= x_min) & (table.lon <= x_max)]
            if full_table is None:
                full_table = table
            else:
                full_table = full_table.merge(table, on=['lat', 'lon', 'time'])

        return full_table

    def read_precip(self, year, precip_path):
        precip_table = read_cdf(precip_path.format(year)).reset_index()
        precip_table['lon'] -= 360
        precip_table = self.precip_points.merge(precip_table, how='left', on=['lat', 'lon'])
        return precip_table

    def write_key(self):
        np.savez_compressed(self.key_path, points=self.precip_points, years=np.array(self.years))

    @property
    def end_date(self):
        return dt.date(self.years[-1], 12, 31)

    @property
    def shape(self):
        return (self.end_date - self.start_date).days + 1, self.precip_points.shape[0], len(self.output_header)

    @property
    def start_date(self):
        return dt.date(self.years[0], 1, 1)


def main():
    from paths import met_data_path, met_grid_path

    ncep_vars = ["tmin.2m", "tmax.2m", "air.2m", "dswrf.ntat", "uwnd.10m", "vwnd.10m"]
    ncep_path = os.path.join(met_data_path, "{}.gauss.{}.nc")  # var, year
    precip_path = os.path.join(met_data_path, "precip.V1.0.{}.nc")  # year

    # Specify run parameters
    years = range(1961, 1963)
    overwrite = True
    bounds = [20, 60, -130, -60]  # min lat, max lat, min long, max long

    # Get the coordinates for all precip stations being used and write to file
    precip_points = map_stations(precip_path, bounds)
    precip_points.to_csv(met_grid_path, index_label='weather_grid')
    exit()

    # Process all weather and store to memory
    WeatherCube(years, ncep_vars, ncep_path, precip_path, bounds, precip_points, overwrite)


if __name__ == '__main__':
    main()