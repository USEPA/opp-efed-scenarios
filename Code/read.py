"""
read.py

Read raw input files without modification of the underlying data, apart from type conversions
or formatting.
"""

# Import standard or builtin libraries
import os
import re
import numpy as np
import pandas as pd

# DBF module can be installed through Anaconda
from dbfread import DBF, FieldParser

# Import local modules and variables
import write
from parameters import states_nhd, vpus_nhd, erom_months, pwc_durations, scenario_id_field
from paths import condensed_soil_path, met_attributes_path, combo_path, crop_dates_path, \
    crop_params_path, gen_params_path, irrigation_path, nhd_paths, condensed_nhd_path, volume_path, \
    pwc_output_path, pwc_output_format, pwc_scenario_path, crop_group_path
from utilities import fields, report


def test_path(f):
    def wrapped(*args, **kwargs):
        try:
            r = f(*args, **kwargs)
            return r
        except Exception as e:
            report(e)

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
    return all_combos


def condense_nhd(region):
    """
    This function extracts data from the native dbf files that are packaged with NHD
    and writes the data to .csv files with a similar name for faster reading in future
    runs
    :param region: NHD Plus Hydroregion (str)
    """

    def append(master, new_table):
        return new_table if master is None else master.merge(new_table, on='comid', how='outer')

    fields.refresh()
    table_map = fields.table_map('NHD')
    master_table = None
    for table_name, new_fields, old_fields in table_map:
        if table_name == 'EROM':
            for month in erom_months:
                rename = dict(zip(old_fields, [f"{new}_{month}" for new in new_fields]))
                del rename['comid']
                table_path = nhd_paths[table_name].format(vpus_nhd[region], region, month)
                table = dbf(table_path)[old_fields]
                table = table.rename(columns=rename)
                table['table_name'] = table_name
                master_table = append(master_table, table)
        else:
            rename = dict(zip(old_fields, new_fields))
            table_path = nhd_paths[table_name].format(vpus_nhd[region], region)
            table = dbf(table_path)
            table = table[old_fields].rename(columns=rename)
            table['table_name'] = table_name
            if table_name == 'PlusFlow':
                table = table[table.comid > 0]
            master_table = append(master_table, table)
    write.condensed_nhd(region, master_table)


def crop(region):
    """
    Read data from parameter tables linked to land use and combine into a single table
    for generating field scenarios.
    :param region: NHD+ Hydroregion (str)
    :return: Table of parameters linked to land use (df)
    """
    fields.refresh()

    # Read CDL/crop group index
    index_fields, dtypes = fields.fetch('CropGroups', dtypes=True)
    crop_index = pd.read_csv(crop_group_path, usecols=index_fields, dtype=dtypes)

    # Read parameters indexed to CDL
    param_fields, dtypes = fields.fetch('CropParams', how='external', dtypes=True)
    crop_params = pd.read_csv(crop_params_path, usecols=param_fields, dtype=dtypes)

    # Read crop dates
    date_fields, dtypes = fields.fetch('CropDates', dtypes=True)
    crop_dates = pd.read_csv(crop_dates_path, usecols=date_fields, dtype=dtypes)
    # Convert dates to days since Jan 1
    for field in fields.fetch('date'):
        crop_dates[field] = (pd.to_datetime(crop_dates[field], format="%d-%b") - pd.to_datetime("1900-01-01")).dt.days
    # Where harvest is before plant, add 365 days (e.g. winter wheat)
    for stage in ['begin', 'end', 'begin_active', 'end_active']:
        crop_dates.loc[crop_dates[f'plant_{stage}'] > crop_dates[f'harvest_{stage}'], f'harvest_{stage}'] += 365

    # Read irrigation parameters
    irrigation_fields, dtypes = fields.fetch('Irrigation', dtypes=True)
    irrigation_data = pd.read_csv(irrigation_path, usecols=irrigation_fields, dtype=dtypes)

    # Read parameters indexed to crop groups
    group_fields, dtypes = fields.fetch('CurveNumbers', dtypes=True)
    group_params = pd.read_csv(gen_params_path, usecols=group_fields, dtype=dtypes)
    group_params = group_params[group_params.region == region]

    data = crop_index.merge(crop_params, on=['cdl', 'cdl_alias'], how='left') \
        .merge(crop_dates, on=['cdl', 'cdl_alias'], how='left', suffixes=('', '_burn')) \
        .merge(irrigation_data, on=['cdl_alias', 'state'], how='left') \
        .merge(group_params, on='pwc_class', how='left', suffixes=('_cdl', '_gen'))

    data[['evergreen', 'alt_date']] = data[['evergreen', 'alt_date']].fillna(0).astype(bool)

    return data


def dbf(dbf_file):
    """ Read the contents of a dbf file into a Pandas dataframe """

    class MyFieldParser(FieldParser):
        def parse(self, field, data):
            try:
                return FieldParser.parse(self, field, data)
            except ValueError as e:
                report(e)
                # raise e
                return None

    try:
        reader = DBF(dbf_file)
        table = pd.DataFrame(iter(reader))
    except ValueError:
        reader = DBF(dbf_file, parserclass=MyFieldParser)
        table = pd.DataFrame(iter(reader))

    table.rename(columns={column: column.lower() for column in table.columns}, inplace=True)

    return table


def gdb(gdb_file, select_table='all', input_fields=None):
    """ Reads the contents of a gdb table into a Pandas dataframe"""
    import ogr

    # Initialize file
    driver = ogr.GetDriverByName("OpenFileGDB")
    gdb_obj = driver.Open(gdb_file)

    # parsing layers by index
    tables = {gdb_obj.GetLayerByIndex(i).GetName(): i for i in range(gdb_obj.GetLayerCount())}
    table_names = sorted(tables.keys()) if select_table == 'all' else [select_table]
    for table_name in table_names:
        table = gdb_obj.GetLayer(tables[table_name])
        table_def = table.GetLayerDefn()
        table_fields = [table_def.GetFieldDefn(i).GetName() for i in range(table_def.GetFieldCount())]
        if input_fields is None:
            input_fields = table_fields
        else:
            missing_fields = set(input_fields) - set(table_fields)
            if any(missing_fields):
                report("Fields {} not found in table {}".format(", ".join(missing_fields), table_name))
                input_fields = [field for field in input_fields if field not in missing_fields]
        data = np.array([[row.GetField(f) for f in input_fields] for row in table])
        df = pd.DataFrame(data=data, columns=input_fields)
        if select_table != 'all':
            return df
        else:
            yield table_name, df


def lake_volumes(region):
    """ Read the waterbody volume dataset """
    return pd.read_csv(volume_path.format(region))


def met(mode):
    """
    Read data tables indexed to weather grid
    :param mode: 'sam' or 'pwc'
    :return: Table of parameters indexed to weather grid
    """
    field_names, dtypes = fields.fetch("MetParams", dtypes=True)
    met_data = pd.read_csv(met_attributes_path, usecols=field_names, dtype=dtypes)
    # met_data = met_data.rename(columns={"stationID": 'weather_grid'})  # these combos have old weather grids?
    return met_data


def nhd(region):
    """
    Loads data from the NHD Plus dataset and combines into a single table.
    :param region: NHD Hydroregion (str)
    :return:
    """

    fields.refresh()
    condensed_file = condensed_nhd_path.format(region)
    if not os.path.exists(condensed_file):
        condense_nhd(region, condensed_file)
    return pd.read_csv(condensed_file)


def soils(mode, region=None, state=None):
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
    valu_table = ssurgo(mode, "", "valu")
    for state in region_states:
        state_table = None
        for table_name, key_field in [('muaggatt', 'mukey'), ('component', 'mukey'), ('chorizon', 'cokey')]:
            table = ssurgo(mode, state, table_name)
            state_table = table if state_table is None else pd.merge(state_table, table, on=key_field, how='outer')
        state_table['state'] = state
        state_tables.append(state_table)
    soil_data = pd.concat(state_tables, axis=0)
    soil_data = soil_data.merge(valu_table, on='mukey')

    return soil_data.rename(columns=fields.convert)


def ssurgo(mode, state, name):
    """
    Read a condensed SSURGO soils data table
    :param mode: 'sam' or 'pwc'
    :param state: State (str)
    :param name: Table name (str)
    :return:
    """
    table_fields, data_types = fields.fetch(name, 'external', dtypes=True)
    table_path = condensed_soil_path.format(state, name)
    return pd.read_csv(table_path, dtype=data_types, usecols=table_fields)


@test_path
def pwc_infile(region, cdl, cdl_name=None, use_parent=False):
    """
    Read a scenarios table as generated by scenarios_and_recipes.py. Add a line_num field that matches up
    with the PWC outfile
    :param region: NHD+ hydroregion (str)
    :param cdl: Land use identifier, usually CDL class (str)
    :param cdl_name: Name of CDL class (str)
    :param use_parent: Workaround (bool)
    :return: Dataframe of scenarios (df)
    """
    if use_parent:
        path = pwc_scenario_path.format(region, 'parent')
    else:
        path = pwc_scenario_path.format(region, cdl, cdl_name)

    table = pd.read_csv(path, dtype={'area': np.int64})[fields.fetch('scenario_fields')]
    if use_parent:
        table = table[table.cdl == int(cdl)]
    return table


@test_path
def pwc_outfile(in_file):
    """
    Read a PWC output file
    :param in_file: Path to PWC output file (str)
    :return: Dataframe of PWC output (df)
    """
    pwc_header = fields.fetch('pwc_id_fields') + pwc_durations
    # Read the table, manually entering in the header (original header is tough to parse)
    table = pd.read_csv(in_file, names=pwc_header, delimiter=r'\s+')
    # TODO - what's up with this
    # print(os.path.basename(in_file), table.line_num.values[0])
    table['line_num'] = table.line_num.astype(np.int32)

    # Adjust line number so that header is not included
    table['line_num'] -= 1

    # Split the Batch Run ID field into constituent parts
    data = table.pop('run_id').str.split('_', expand=True)
    data.columns = fields.fetch('pwc_run_id')

    table = pd.concat([data, table], axis=1)
    table = table.melt(id_vars=[f for f in table.columns if not f in pwc_durations], value_vars=pwc_durations,
                       var_name='duration', value_name='conc')

    return table


def pwc_input_and_output(merge_field='line_num', use_parent=False):
    """
    Matches a PWC input file to a PWC output file and merges them together. The result is a PWC output file
    with scenario parameters attached. Uses RegEx patterns to identify filenames.
    :param merge_field: Field to merge the input and output tables
    :param use_parent: Optional workaround (bool)
    :return:
    """
    # TODO - delete 'index_fields' and 'file_fields' from fields_and_qc?
    tables = []
    for f in os.listdir(pwc_output_path):
        match = re.match(pwc_output_format, f)
        if match:
            region, cdl, cdl_name, koc = match.groups()
            pwc_outfile_path = os.path.join(pwc_output_path, f)
            pwc_input = pwc_infile(region, cdl, cdl_name, use_parent)
            if merge_field == 'line_num':
                pwc_input['line_num'] = pwc_input.index + 1
            pwc_output = pwc_outfile(pwc_outfile_path)
            if pwc_input is not None:
                report(f"Reading PWC input/output for Region {region}, class {cdl_name}...")
                combined_table = pwc_input.merge(pwc_output, on=merge_field, how='left')
                for field, val in (('region', region), ('cdl', cdl), ('cdl_name', cdl_name), ('koc', koc)):
                    combined_table[field] = val
                tables.append(combined_table)
    full_table = pd.concat(tables, axis=0).dropna()
    full_table[scenario_id_field] = full_table[scenario_id_field].astype(object)
    return full_table
