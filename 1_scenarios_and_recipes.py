"""
scenarios_and_recipes.py

Create field scenarios and watershed recipes for the PWC and SAM aquatic pesticide
fate models. Field scenarios consist of parameters describing the soil, weather,
and land use properties of geographic areas. Recipe files describe the composition
of scenarios within watersheds.
"""

# Import builtin and standard packages
import os
import numpy as np
import pandas as pd

# Import local modules and variables
import modify
import read
import write

from paths import scratch_dir, condensed_nhd_path
from hydro import read_nhd
from tools.efed_lib import report
from parameters import nhd_regions, pwc_selection_field, pwc_min_selection, pwc_selection_pct


def create_recipes(combos, watershed_params):
    """
    Create a table with the scenario ids and areas of all scenarios in a watershed, along with a 'map' for indexing,
    and scenarios with watersheds removed.
    :param combos: Combinations table (df)
    :param watershed_params: Tabular data indexed to watershed (df)
    :return: Recipes, recipe map, scenarios (df, df, df)
    """

    # Create a unique integer scenario index for a more compact recipe file
    scenario_index = combos[['scenario_id']].drop_duplicates().reset_index().rename(columns={'index': 'scenario_index'})
    combos = combos.merge(scenario_index, on='scenario_id', how='left')

    # Join combinations table with watershed params and
    # convert watershed id field from 'gridcode' to 'comid'
    recipes = combos[['year', 'gridcode', 'scenario_index', 'area']] \
        .merge(watershed_params, on='gridcode') \
        .sort_values(['year', 'comid'])
    del recipes['gridcode']

    year_comid = recipes[['year', 'comid']]

    # Identify rows where 'comid' or 'year' change value
    # This will provide the row ranges for each unique comid-year pair
    diffs = year_comid.diff()
    diffs = diffs.sum(axis=1)
    changes = np.nonzero(diffs.values)
    changes = changes[0]
    starts = np.pad(changes, (1, 0), 'constant')
    ends = np.pad(changes, (0, 1), 'constant', constant_values=recipes.shape[0])
    recipe_map = year_comid.iloc[starts]
    recipe_map['start'], recipe_map['end'] = starts, ends

    # Once recipes are generated, watershed data is no longer needed.
    # Remove watershed parameters and aggregate common scenarios
    for field in 'gridcode', 'year':
        del combos[field]
    combos = combos.groupby([f for f in combos.columns if f != 'area']).sum().reset_index()

    return recipes[['scenario_index', 'area']], recipe_map, combos.reset_index()


def finalize_crop_dates(scenarios, crop_dates):
    # Merge crop dates
    dates_index = scenarios[['scenario_id', 'cdl', 'cdl_alias', 'weather_grid', 'state']]
    state_dates = crop_dates.loc[pd.isnull(crop_dates.weather_grid)].drop('weather_grid', axis=1).drop_duplicates()
    grid_dates = crop_dates.loc[~pd.isnull(crop_dates.weather_grid)].drop('state', axis=1).drop_duplicates()
    crop_dates = pd.concat([dates_index.merge(grid_dates, on=['cdl', 'cdl_alias', 'weather_grid']),
                            dates_index.merge(state_dates, on=['cdl', 'cdl_alias', 'state'])])
    for field in ['cdl', 'weather_grid', 'state']:
        del crop_dates[field]
    return crop_dates


def create_scenarios(combinations, soil_params, met_params, crop_params, crop_dates,
                     irrigation, curve_numbers):
    """
    Merge soil/weather/land use combinations with tabular parameter datasets.
    :param combinations: Combinations table (df)
    :param soil_data: Soils data table (df)
    :param crop_params: Cropping data table (df)
    :param met_data: Data indexed to weather grid (df)
    :return: Scenarios table (df)
    """

    # Merge all tables except crop dates
    scenarios = combinations.merge(met_params, how="left", on="weather_grid")
    scenarios = scenarios.merge(soil_params, how="left", on="soil_id", suffixes=("", "_soil"))
    scenarios = scenarios.merge(crop_params, how="left", on=['cdl', 'cdl_alias'])
    scenarios = scenarios.merge(irrigation, how="left", on=['cdl_alias', 'state'])
    scenarios = scenarios.merge(curve_numbers, how="left", on=['region', 'pwc_class'])

    # Crop dates are more complicated since there are variable
    crop_dates = finalize_crop_dates(scenarios, crop_dates)
    scenarios = scenarios.merge(crop_dates, how="left", on=['scenario_id', 'cdl_alias'])

    # 'season' occurs in both dates and cdl params. take the maximum
    scenarios['season'] = scenarios[['season_x', 'season_y']].max(axis=1)

    return scenarios


def select_pwc_scenarios(in_scenarios, crop_params):
    """
    Sort scenarios by crop group and perform random selection for creating PWC scenarios.
    :param in_scenarios: Table containing all possible scenarios (df)
    :param crop_params: Crop data table used for identifying crop groups
    :yield: Scenario selections
    """
    # Randomly sample from each crop group and save the sample
    meta_table = []  # table summarizing sample size for each crop
    crop_groups = crop_params[[pwc_selection_field, pwc_selection_field + '_desc']].drop_duplicates().values

    # First, write the entire scenario table to a 'parent' table
    yield 'all', 'parent', in_scenarios

    # Write a table for each crop or crop group
    for crop, crop_name in crop_groups:
        sample = in_scenarios.loc[in_scenarios[pwc_selection_field] == crop]
        n_scenarios = sample.shape[0]
        selection_size = max((pwc_min_selection, int(n_scenarios * (pwc_selection_pct / 100))))
        if n_scenarios > selection_size:
            sample = sample.sample(selection_size)
        if not sample.empty:
            meta_table.append([crop, crop_name, n_scenarios, min((n_scenarios, selection_size))])
            yield int(crop), crop_name, sample

    # Write a table describing how many scenarios were selected for each crop
    yield 'all', 'meta', pd.DataFrame(np.array(meta_table),
                                      columns=['crop', 'crop_name', 'n_scenarios', 'sample_size'])


def chunk_combinations(combos):
    """
    Break the master combinations table into smaller chunks to avoid memory overflow.
    :param combos: Master scenarios table (df)
    """
    from parameters import chunk_size
    n_combinations = combos.shape[0]
    n_chunks = int(n_combinations / chunk_size) + 1
    if n_combinations > chunk_size:
        report(f"Breaking {n_combinations} combinations into {n_chunks} chunks", 1)
        for i, start_row in enumerate(range(0, n_combinations, chunk_size)):
            end_row = min((start_row + chunk_size, n_combinations))
            report(f"Processing chunk {i + 1}...", 2)
            chunk = combos.iloc[start_row:end_row]
            yield i + 1, chunk
    else:
        report("Processing all combinations...", 2)
        yield 1, combos


def scenarios_and_recipes(regions, years, mode, class_filter=None):
    """
    Main program routine. Creates scenario and recipe (if applicable) files
    for specified NHD Plus Hydroregions and years. Years and regions provided
    must have corresponding input data. Specify paths to input data in paths.py
    Mode may be either 'sam' or 'pwc'. In 'sam' mode, recipes are created and
    aggregations are performed. In 'pwc' mode, different output files are created
    :param regions: NHD Plus Hydroregions to process (list of strings)
    :param years: Years to process (list of integers)
    :param mode: 'sam' or 'pwc'
    """
    report("Reading input files...")

    # Read and modify data indexed to weather grid
    report("Reading weather-indexed data...")
    met_params = read.met()
    met_params = modify.met(met_params)

    # Read crop related params. This has multiple functions since data are differently indexed
    report("Reading crop data...")
    crop_params = read.crop()
    crop_dates = read.crop_dates()
    irrigation = read.irrigation()

    # Read and modify data indexed to soil
    report("Reading soils data...")
    soil_params = read.soil()
    soil_params, aggregation_key = modify.soils(soil_params, mode)

    # Create a filter if only processing certain crops
    if class_filter is not None:
        class_filter = pd.DataFrame({pwc_selection_field: class_filter})

    # Soils, watersheds and combinations are broken up by NHD region
    for region in regions:
        report("Processing Region {}...".format(region))
        report("Reading regional input files...", 1)

        # Read curve numbers
        curve_numbers = read.curve_numbers(region)

        # Read and modify met/crop/land cover/soil/watershed combinations
        combinations = read.combinations(region, years)
        combinations = modify.combinations(combinations, crop_params, mode, aggregation_key)

        # Generate watershed 'recipes' for SAM and aggregate combinations after recipe fields removed
        if mode == 'sam':
            report(f"Creating watershed recipes and aggregating combinations...", 1)
            watershed_params = pd.read_csv(condensed_nhd_path.format(region))[['gridcode', 'comid']]
            recipes, recipe_map, combinations = create_recipes(combinations, watershed_params)
            write.recipes(region, recipes, recipe_map)

        # Create and modify scenarios, and write to file
        report(f"Creating scenarios...", 1)
        if mode == 'sam':
            # Because SAM datasets do not exclude any scenarios, break into pieces to avoid memory overload
            for chunk_num, chunk in chunk_combinations(combinations):
                scenarios = create_scenarios(chunk, soil_params, met_params, crop_params, crop_dates,
                                             irrigation, curve_numbers)

                # Filter out only the desired crop, if a filter is specified
                if class_filter is not None:
                    scenarios = scenarios.merge(class_filter, on=pwc_selection_field, how='inner')
                    if scenarios.empty:
                        continue

                scenarios = modify.scenarios(scenarios, mode, region, write_qc=False)
                report("Writing to file...", 2)
                write.scenarios(scenarios, mode, region, name=chunk_num)
        elif mode == 'pwc':
            scenarios = create_scenarios(combinations, soil_params, met_params, crop_params, crop_dates,
                                         irrigation, curve_numbers)

            scenarios = modify.scenarios(scenarios, mode, region)
            # For PWC, apply sampling and write crop-specific tables
            for crop_num, crop_name, crop_scenarios in select_pwc_scenarios(scenarios, crop_params):
                report("Writing table for Region {} {}...".format(region, crop_name), 2)
                write.scenarios(crop_scenarios, mode, region, name=crop_name, num=crop_num)

        #os.remove("combos.csv")

def main():
    """ Wraps scenarios_and_recipes.
    Specify mode, years, and regions for processing here """
    modes = ('sam',)  # pwc and/or sam
    years = range(2015, 2020)
    #regions = nhd_regions[1:]
    regions = ['07']
    class_filter = None
    for mode in modes:
        scenarios_and_recipes(regions, years, mode, class_filter)


if __name__ == "__main__":
    main()
