"""
read.py

Read raw input files without modification of the underlying data, apart from type conversions
or formatting.
"""

# Import standard or builtin libraries
import numpy as np
import pandas as pd
from parameters import date_fmt

# Import local modules and variables
from parameters import states_nhd, pwc_durations
from paths import condensed_soil_path, met_attributes_path, combo_path, crop_dates_path, \
    crop_params_path, gen_params_path, irrigation_path, \
    pwc_scenario_path, crop_group_path, pwc_outfile_path

from efed_lib.efed_lib import report
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
    all_combos['region'] = region
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
    field_names, dtypes = fields.fetch("met_params", dtypes=True)
    met_data = pd.read_csv(met_attributes_path, usecols=field_names, dtype=dtypes)
    # met_data = met_data.rename(columns={"stationID": 'weather_grid'})  # these combos have old weather grids?
    return met_data


def soil(mode, region=None, state=None):
    """
    Read and aggregate all soils data for an NHD Hydroregion or state
    :param mode: 'sam' or 'pwc'
    :param region: NHD Hydroregion (str, optional)
    :param state: State abbreviation (str, optional)
    :return: Table of parameters indexed to soil map unit (df)
    """
    fields.refresh()

    if region is None and state is not None:
        region_states = [state]
    else:
        region_states = states_nhd[region]
    state_tables = []
    table_fields, data_types = fields.fetch('ssurgo', True, index_field='external_name')
    valu_table = pd.read_csv(condensed_soil_path.format("", "valu"),
                             dtype=data_types, usecols=lambda f: f in table_fields)

    # ssurgo
    """
    table_fields, data_types = fields.fetch(name, dtypes=True, index_field='external_name')
    table_path = condensed_soil_path.format(state, name)
    return pd.read_csv(table_path, dtype=data_types, usecols=table_fields)
    """
    for state in region_states:
        state_table = None
        for table_name, key_field in [('muaggatt', 'mukey'), ('component', 'mukey'), ('chorizon', 'cokey')]:
            table_path = condensed_soil_path.format(state, table_name)

            table = pd.read_csv(table_path, dtype=data_types, usecols=lambda f: f in table_fields)
            state_table = table if state_table is None else pd.merge(state_table, table, on=key_field, how='outer')
        state_table['state'] = state
        state_tables.append(state_table)
    soil_data = pd.concat(state_tables, axis=0)
    soil_data = soil_data.merge(valu_table, on='mukey')

    return soil_data.rename(columns=fields.convert)


@test_path
def pwc_infile(class_num=None, class_name=None, region=None):
    """
    Read a scenarios table as generated by scenarios_and_recipes.py. Add a line_num field that matches up
    with the PWC outfile
    :param region: NHD+ hydroregion (str)
    :param class_num: Land use identifier, usually CDL class (str)
    :param class_name: Name of CDL class (str)
    :return: Dataframe of scenarios (df)
    """
    path = pwc_scenario_path.format(region, class_num, class_name)
    table = pd.read_csv(path, dtype={'area': np.int64})
    table['region'] = region

    return table


@test_path
def pwc_outfile(class_num=None, koc=None):
    """
    Read a PWC output file
    :param in_file: Path to PWC output file (str)
    :return: Dataframe of PWC output (df)
    """

    pwc_header = ['line_num', 'run_id'] + pwc_durations
    table_path = pwc_outfile_path.format(class_num, koc)
    table = pd.read_csv(table_path, names=pwc_header, delimiter=r'\s+')

    # Adjust line number so that header is not included
    table['line_num'] = table.line_num.astype(np.int32) - 1

    # Split the Batch Run ID field into constituent parts
    data = table.pop('run_id').str.split('_', expand=True)
    data.columns = ['bunk', 'koc', 'scenario_id', 'rep']
    data['koc'] = koc
    table = pd.concat([data, table], axis=1)
    table = table.melt(id_vars=[f for f in table.columns if f not in pwc_durations], value_vars=pwc_durations,
                       var_name='duration', value_name='conc')

    return table
