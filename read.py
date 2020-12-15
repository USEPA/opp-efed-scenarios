"""
read.py

Read raw input files without modification of the underlying data, apart from type conversions
or formatting.
"""

# Import standard or builtin libraries
import os
import numpy as np
import pandas as pd
from parameters import kocs

# Import local modules and variables
from parameters import pwc_durations
from paths import condensed_soil_path, met_attributes_path, combo_path, crop_dates_path, \
    crop_params_path, gen_params_path, irrigation_path, \
    pwc_scenario_path, crop_group_path, pwc_outfile_path

from tools.efed_lib import report
from parameters import fields
from modify import date_to_num


def test_path(f):
    def wrapped(*args, **kwargs):
        try:
            r = f(*args, **kwargs)
            return r
        except Exception as e:
            raise e

    return wrapped


def combinations(region, years, nrows=None):
    """
    Read a table of soil/land cover/weather/watershed combinations for generating
    scenario files.
    :param region: NHD Plus Hydroregion (str)
    :param years: Years to fetch data for (iter, int)
    :param nrows: Number of rows to read (int, optional)
    :return: Combinations table (df)
    """
    all_combos = None
    for year in years:
        header = ['gridcode', 'cdl', 'weather_grid', 'mukey', 'area']
        combo_file = combo_path.format(region, year)
        combos = pd.read_csv(combo_file, dtype=np.uint32, nrows=nrows)[header]
        combos['year'] = np.int16(year)
        all_combos = combos if all_combos is None else pd.concat([all_combos, combos], axis=0)
    all_combos['region'] = str(region).zfill(2)
    return all_combos


def crop():
    """
    Read data from parameter tables linked to land use and combine into a single table
    for generating field scenarios.
    :param region: NHD+ Hydroregion (str)
    :return: Table of parameters linked to land use (df)
    """
    fields.refresh()

    # Read CDL/crop group index
    index_fields, dtypes = fields.fetch('crop_groups', dtypes=True)
    crop_index = pd.read_csv(crop_group_path, usecols=index_fields, dtype=dtypes)

    # Read parameters indexed to CDL
    param_fields, dtypes = fields.fetch('crop_params', dtypes=True, index_field='external_name')
    crop_params = pd.read_csv(crop_params_path, usecols=param_fields, dtype=dtypes)
    data = crop_index.merge(crop_params, on=['cdl', 'cdl_alias'], how='left')

    return data


def curve_numbers(region):
    group_fields, dtypes = fields.fetch('curve_numbers', dtypes=True)
    group_params = pd.read_csv(gen_params_path, usecols=group_fields, dtype=dtypes)
    return group_params[group_params.region == region]


def crop_dates(mode='pwc'):
    # Read crop dates
    dates = pd.read_csv(crop_dates_path)
    if mode == 'pwc':
        dates = dates[dates.sam_only != 1]

    # Convert dates to days since Jan 1
    dates = date_to_num(dates)
    # If date is earlier than preceeding event, move it forward a year
    # TODO - check this assumption. what if the dates are just off? should this be in modify.py?
    date_fields = fields.fetch("plant_stage")
    for i, stage_2 in enumerate(date_fields):
        if i > 0:
            stage_1 = date_fields[i - 1]
            dates.loc[(dates[stage_2] < dates[stage_1]), stage_2] += 365.

    return dates[fields.fetch('crop_dates')].rename(columns={'stationID': 'weather_grid'})


def irrigation():
    irrigation_fields, dtypes = fields.fetch('irrigation', dtypes=True)
    irrigation_data = pd.read_csv(irrigation_path, usecols=irrigation_fields, dtype=dtypes)
    return irrigation_data


def met():
    """
    Read data tables indexed to weather grid
    :return: Table of parameters indexed to weather grid
    """
    field_names, dtypes = fields.fetch("met_params", dtypes=True, index_field='external_name')
    met_data = pd.read_csv(met_attributes_path, usecols=field_names, dtype=dtypes)
    # met_data = met_data.rename(columns={"stationID": 'weather_grid'})  # these combos have old weather grids?
    return met_data.rename(columns=fields.convert)


def soil():
    """
    Read and aggregate all soils data for an NHD Hydroregion or state
    :return: Table of parameters indexed to soil map unit (df)
    """
    fields.refresh()
    table_fields, data_types = fields.fetch('ssurgo', True, index_field='external_name')
    table_map = [('muaggatt', 'mukey'), ('component', 'mukey'), ('chorizon', 'cokey'), ('Valu1', 'mukey')]
    full_table = None
    for table_name, key_field in table_map:
        table_path = condensed_soil_path.format(table_name)
        table = pd.read_csv(table_path, dtype=data_types, usecols=lambda f: f in table_fields)
        if full_table is None:
            full_table = table
        else:
            full_table = full_table.merge(table, on=key_field, how='outer')
    return full_table.rename(columns=fields.convert)


@test_path
def pwc_infile(class_num, class_name, path=None, fixed_base=None):
    """
    Read the tabular scenarios that were used to parameterize the PWC run for a given crop
    :param class_num: Numerical class ID (str, int)
    :param class_name: Descriptive class name (str)
    :param path: Override the default input path (optional, str)
    :param fixed_base: Override the default table name (optional, str)
    :return: Pandas dataframe of the PWC input scenarios
    """
    if path is None:
        path = pwc_outfile_path  # "{}_Corn_all_{}_koc{}"
    tables = []

    # Look for all chunked tables (so far the number of chunks has never exceeded 10)
    for i in range(10):
        try:
            # Read the input table from the koc10 folder. It should be identical in all folders
            p = path.format(class_num, class_name, i, 10)
            if fixed_base is None:
                base = os.path.basename(p).replace("_koc10", ".csv")
            else:
                base = fixed_base
            new_table = pd.read_csv(os.path.join(p, base), dtype={'area': np.int64})
            tables.append(new_table)
            report(f"Read file {p}")
        except FileNotFoundError as e:
            break
    if tables:
        # Join together all the chunks
        table = pd.concat(tables, axis=0)
        table['region'] = table.region.astype('str').str.zfill(2)
        return table
    else:
        print(f"No infiles found for {class_name}")
        return None


@test_path
def pwc_outfile(class_num=None, class_name=None, path=None):
    """
    Read a PWC output file (BatchOutputVVWM.txt) into a dataframe
    :param class_num: Numerical class ID (str, int)
    :param class_name: Descriptive class name (str)
    :param path: Override the default input path (optional, str)
    :return: Dataframe of PWC output (df)
    """
    pwc_header = ['line_num', 'run_id'] + pwc_durations
    tables = []
    if path is None:
        path = pwc_outfile_path
    for i in range(10):
        for koc in kocs:
            p = os.path.join(path.format(class_num, class_name, i, koc), 'BatchOutputVVWM.txt')
            try:
                new_table = pd.read_csv(p, names=pwc_header, delimiter=r'\s+')
                tables.append(new_table)
                report(f"Read file {p}")
            except FileNotFoundError:
                break
    table = pd.concat(tables, axis=0)

    # Adjust line number so that header is not included
    table['line_num'] = table.line_num.astype(np.int32) - 1

    # Split the Batch Run ID field into constituent parts
    data = table.pop('run_id').str.split('_', expand=True)
    data.columns = ['bunk', 'koc', 'scenario_id', 'rep']
    data['koc'] = data.koc.str.slice(3).astype(np.int32)
    table = pd.concat([data, table], axis=1)
    table = table.melt(id_vars=[f for f in table.columns if f not in pwc_durations], value_vars=pwc_durations,
                       var_name='duration', value_name='conc')

    return table
