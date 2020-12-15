import pandas as pd
from crop_dates_paths import met_xwalk_path, gdd_output_path, fixed_dates_path, \
    dates_output, met_id_field, variable_dates_path, ca_vegetable_path
import numpy as np
from parameters import fields
from modify import date_to_num, num_to_date


def combine_dates(all_dates, xwalk):
    calculated_dates = pd.read_csv(gdd_output_path).fillna('n/a')

    # Append freeze dates
    freeze_dates = calculated_dates[['ncep_index', 'spring_freeze', 'fall_freeze']].drop_duplicates()
    all_dates = all_dates\
        .merge(xwalk[['weather_grid', 'ncep_index']], on='weather_grid', how='left') \
        .merge(freeze_dates, on='ncep_index', how='left')

    # Append GDD-derived dates by stage
    for stage in ['emergence', 'maxcover', 'harvest']:
        try:
            date_field = f'{stage}_date'
            gdd_dates = calculated_dates[['ncep_index', 'gdd_crop', stage]]
            all_dates = all_dates.merge(gdd_dates,
                                        left_on=['ncep_index', stage],
                                        right_on=['ncep_index', 'gdd_crop'],
                                        how='left', suffixes=("", "_date"))
            replace = pd.isnull(all_dates['gdd_crop'])
            all_dates.loc[replace, date_field] = all_dates.loc[replace, stage]
        except KeyError:
            all_dates[date_field] = all_dates[stage]
        frost_rows = (all_dates[stage] == 'fall_frost')
        all_dates.loc[frost_rows, date_field] = all_dates.loc[frost_rows, 'fall_freeze']

    out_fields = [f for f in fields.fetch('crop_dates') if f in all_dates.columns]
    return all_dates[out_fields]


def read_variable(met_xwalk):
    # There are two tables with variable dates - one indexed by CDL alone, and one by state and CDL
    # Read both and combine, with the output indexed by weather grid

    # Get station to state crosswalk
    station_state = met_xwalk[['weather_grid', 'state']].drop_duplicates()

    # Read dates indexed by state/cdl and expand to met grid
    dates = pd.read_csv(variable_dates_path)
    dates = dates.merge(station_state, on='state', how='left')

    # Initialize empty fields
    for field in 'plant_date', 'season':
        dates[field] = ''
    dates['cdl_alias'] = dates.cdl
    dates['cdl_alias_desc'] = dates.cdl_desc

    return dates


def select_fixed_dates(params):
    # Convert fields to boolean
    for field in ['sam_only', 'evergreen', 'alt_date']:
        try:
            params[field] = params[field].fillna(0).astype(bool)
        except KeyError:
            pass


def process_fixed_dates():
    most_fixed = pd.read_csv(fixed_dates_path)
    vegetables = pd.read_csv(ca_vegetable_path)
    vegetables['cdl_alias'] = vegetables['cdl']
    vegetables['cdl_alias_desc'] = vegetables['cdl_desc']
    dates = pd.concat([most_fixed, vegetables], axis=0)
    dates = date_to_num(dates)

    # Convert fields to boolean
    for field in ['sam_only', 'evergreen', 'alt_date']:
        dates[field] = dates[field].fillna(0).astype(bool)

    # Initialize fields
    for stage in ('plant', 'harvest', 'maxcover', 'emergence'):
        dates[f"{stage}_date"] = 0

    # Where harvest is before plant, add 365 days (e.g. winter wheat)
    for stage in ['begin', 'end', 'begin_active', 'end_active']:
        dates.loc[dates[f'plant_{stage}'] > dates[f'harvest_{stage}'], f'harvest_{stage}'] += 365

    # Use middle of active range for plant and harvest
    for stage in ('plant', 'harvest'):
        dates[f'{stage}_date'] = (dates[f'{stage}_begin'] + dates[f'{stage}_end']) / 2

    # Emergence is set to 7 days after plant
    dates['emergence_date'] = np.int32(dates.plant_date + 7)

    # Max cover is set to halfway between emergence and harvest
    dates['maxcover_date'] = np.int32((dates.emergence_date + dates.harvest_date) / 2)

    # If a value is provided in the '_desig' field, use it
    for stage in ('plant', 'emergence', 'maxcover', 'harvest'):
        sel = ~pd.isnull(dates[f'{stage}_desig'])
        dates.loc[sel, f'{stage}_date'] = dates.loc[sel, f'{stage}_desig']

    # For evergreen crops, canopy is always on the plant at maximum coverage
    dates.loc[dates.evergreen, ['plant_date', 'emergence_date', 'maxcover_date', 'harvest_date']] = \
        np.array([0, 0, 1, 364])

    # Convert from number (e.g., 1) back to date (e.g., 02-Jan)
    dates = num_to_date(dates)

    return dates[fields.fetch('crop_dates', field_filter=dates.columns)]


def main():
    # Read met crosswalk
    met_xwalk = pd.read_csv(met_xwalk_path).rename(columns={met_id_field: 'weather_grid'})

    # Read crops with variable dates indexed by CDL
    cdl_dates = read_variable(met_xwalk)

    # Join calculated dates
    variable_dates = combine_dates(cdl_dates, met_xwalk)

    # Read fixed dates
    fixed_dates = process_fixed_dates()

    # Write output
    all_dates = pd.concat([fixed_dates, variable_dates], axis=0) \
        .dropna(subset=['cdl']) \
        .sort_values(['cdl', 'state', 'weather_grid'])[fields.fetch('crop_dates')]
    all_dates.loc[pd.isnull(all_dates.season), 'season'] = 1
    all_dates.to_csv(dates_output, index=None)


main()
