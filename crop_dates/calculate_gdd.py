import os
import urllib
import re
import io
import pandas as pd
import numpy as np
import datetime as dt
from collections.abc import Iterable
from parameters import date_fmt
from crop_dates_paths import met_xwalk_path, gdd_output_path, gdd_input_path

sel_year = 2019
temp_max = 212
gdd_url = 'http://uspest.org/cgi-bin/' \
          'ddmodel2.pl?' \
          'wfl={0}{1}.txt' \
          '&uco=1&spp=aaa&cel=0' \
          '&tlow={2}' \
    f'&thi={temp_max}' \
          '&cal=A' \
          '&cel=1' \
          '&stm=1&std=1&styr={1}' \
          '&enm=12&end=31&spyr=0'
output_header = ["ncep_index", "gdd_crop", "spring_freeze", "fall_freeze", "emergence", "maxcover"]


def index_to_date(dates):
    output = []
    if not isinstance(dates, Iterable):
        dates = [dates]
    for date in dates:
        if not np.isnan(date):
            output.append((dt.date(2001, 1, 1) + dt.timedelta(days=int(date))).strftime(date_fmt))
        else:
            output.append("n/a")
    return output


def write_results(all_data):
    pd.DataFrame(all_data, columns=output_header).to_csv(gdd_output_path, index=None)


def annual_dates(gdd, emergence_gdd, maxcover_gdd, maxcover_event='new_year', return_dates=False):
    # get the emergence index (row in table where emergence occurs)
    try:
        emergence_idx = np.argwhere(gdd.emergence_gdd >= emergence_gdd).min()
    except ValueError:
        emergence_idx = np.nan

    # get the maxcover index
    try:
        if maxcover_event == 'new_year':
            maxcover_idx = np.argwhere(gdd.maxcover_gdd >= maxcover_gdd).min()
        elif maxcover_event == 'emergence':
            emergence_date_gdd = gdd.maxcover_gdd.values[emergence_idx]
            maxcover_idx = np.argwhere((gdd.maxcover_gdd - emergence_date_gdd) >= maxcover_gdd).min()
        else:
            raise KeyError(f"Invalid start type {maxcover_event}")
    except ValueError:
        maxcover_idx = np.nan
    if return_dates:
        return index_to_date([emergence_idx, maxcover_idx])
    else:
        return emergence_idx, maxcover_idx


def read_gdd():
    desc_fields = ['gdd_crop', 'gdd_crop_desc', 'emergence_event', 'maxcover_event', 'emergence_gdd_start',
                   'maxcover_gdd_start']
    table = pd.read_csv(gdd_input_path)
    for col in table.columns.values:
        if col.endswith("temp_f"):
            table[col.rstrip('_f')] = (table.pop(col) - 32.) * (5 / 9)

    table = table.groupby(desc_fields).mean().reset_index()
    return table


def compute_freeze_date(table, field='temp_min'):
    def freeze_date(sel, mode='first'):
        try:
            idx = {'first': 0, 'last': -1}[mode]
            date = sel.index[sel[field] <= 0][idx]
            return (date - sel.index.min()).days
        except IndexError:
            return np.nan

    # Designate spring and fall
    table['spring_year'] = table['fall_year'] = table.index.year
    table.loc[table.index.month >= 8, 'spring_year'] += 1
    table.loc[table.index.month < 8, 'fall_year'] -= 1

    # Calculate dates
    spring_dates = table.groupby('spring_year').apply(freeze_date, ('last'))
    fall_dates = table.groupby('fall_year').apply(freeze_date, ('first'))
    freezes = pd.concat((spring_dates, fall_dates), axis=1).iloc[1:-1].astype(np.float32)
    freezes.columns = ['spring', 'fall']
    freezes = (pd.to_datetime("2000-08-01") +
               pd.to_timedelta(freezes.median(), unit='D', errors='coerce')).dt.strftime("%d-%b")
    return freezes


def compute_gdd(temp, base_temp):
    return np.maximum(temp - base_temp, 0).cumsum()


def fetch_gdd(station, base_temps, names=None, return_temps=False):
    all_data = []
    for i, base_temp in enumerate(base_temps):
        url = gdd_url.format(station, str(sel_year)[2:], base_temp)
        response = urllib.request.urlopen(url)
        html = response.read()
        label = base_temp if names is None else names[i]
        results = parse_table(html.decode()).rename(columns={'CUMDD41': label})
        if not i:
            results = results[['mn', 'day', label]].rename(columns={'mn': 'month'})
            results['year'] = sel_year
            results['date'] = pd.to_datetime(results[['year', 'month', 'day']])
            all_data.append(results[['date']])
        if return_temps:
            all_data.append(results[[c for c in results.columns if c != 'date']])
        else:
            all_data.append(results[[label]])

    if results is None:
        print("Invalid")
    else:
        return pd.concat(all_data, axis=1).set_index('date')


def generalize(data):
    date_fields = ('emergence_date', 'maxcover_date', 'spring_freeze', 'fall_freeze')
    for date_field in date_fields:
        data.loc[data[date_field] == '29-Feb', date_field] = '28-Feb'
        data[date_field] = \
            (pd.to_datetime(data[date_field], format=date_fmt) - pd.to_datetime("1900-01-01")).dt.days
    g = data.groupby(['gdd_crop', 'gdd_crop_desc', 'ncep_index', 'state_met']).mean().round(0).reset_index()
    for date_field in date_fields:
        g[date_field] = (pd.to_datetime("1900-01-01") +
                         pd.to_timedelta(g[date_field], unit='D')).dt.strftime(date_fmt)
    return g


def gdd_average(time_series, emergence_base_temp, maxcover_base_temp, emergence_gdd, maxcover_gdd, maxcover_event,
                temp_field='temp_avg', return_all=False):
    results = []
    for year, annual_table in time_series.groupby(time_series.index.year):
        gdd = pd.DataFrame({'emergence_gdd': compute_gdd(annual_table[temp_field], emergence_base_temp),
                            'maxcover_gdd': compute_gdd(annual_table[temp_field], maxcover_base_temp)})
        emergence_date, maxcover_date = \
            annual_dates(gdd, emergence_gdd, maxcover_gdd, maxcover_event)
        results.append([emergence_date, maxcover_date])
    if return_all:
        results = np.array(results)
        return np.array(
            [[index_to_date(results[i, j]) for j in range(results.shape[1])] for i in range(results.shape[0])])
    results = np.array(results).mean(0)
    return index_to_date(results)


def iterate_stations(weather_stations, years):
    n_stations = len(weather_stations)
    for i, station in enumerate(weather_stations):
        if not i % 10:
            print(f"\t{i}/{n_stations}")
        if years != 'all':
            data = data[(data.index.year >= years[0]) & (data.index.year <= years[1])]
        yield station, data


def parse_table(in_string):
    table_fmt = re.compile("(^\s*1\s+1(.+)\s*12\s+31.+?$)", re.MULTILINE | re.DOTALL)
    match = re.search(table_fmt, in_string)
    if match:
        table = match.group(1)
        table = io.StringIO(table)
        table = np.genfromtxt(table, usecols=(0, 1, 2, 3, 4, 5, 6))
        table = pd.DataFrame(table, columns=['mn', 'day', 'max', 'min', 'precip', 'dd41', 'CUMDD41'])
        return table
    else:
        print("invalid")
        return None


def main():
    overwrite = True
    years = (1985, 2016)  # start, end
    cube = NcepArray()

    gdd_params = read_gdd()
    xwalk = pd.read_csv(met_xwalk_path)
    if overwrite or not os.path.exists(gdd_output_path):
        rows = []
        stations = sorted(xwalk.ncep_index.unique())
        for station, time_series in iterate_stations(stations, cube, years):
            spring_freeze, fall_freeze = compute_freeze_date(time_series)
            for _, row in gdd_params.iterrows():
                emergence_date, maxcover_date = \
                    gdd_average(time_series, row.emergence_base_temp, row.maxcover_base_temp,
                                row.emergence_gdd, row.maxcover_gdd, row.maxcover_gdd_start)
                rows.append([station, row.gdd_crop, spring_freeze, fall_freeze, emergence_date, maxcover_date])
        write_results(rows)


if __name__ == "__main__":
    main()
