"""
select_pwc_scenarios.py

Once the field scenario tables have been run through PWC batch mode, this script processes
the output by ranking scenarios by concentration and selecting scenarios at a certain
threshold. The script also produces plots and tables to put the selections into context.
"""
# Import builtins and standard libraries
import os
import re
import pandas as pd
import numpy as np

# Import local modules and variables
import plot
import read
import write
from tools.efed_lib import report
from parameters import selection_percentile, area_weighting, pwc_durations, fields, max_horizons, nhd_regions, kocs
from paths import crop_group_path

# This silences some error messages being raised by Pandas
pd.options.mode.chained_assignment = None

def read_region_18(refresh=False):
    """
    This function is a temporary fix for a problem in November 2020 where Region 18 scenarios were not generated
    :param refresh:
    :return:
    """
    if refresh:
        region_18_path = r"E:\opp-efed-data\scenarios\Production\RerunProject\SecondBatch\all_region18_{}_{}_koc{}"
        region_18_input = read.pwc_infile('all', region_18_path, "all_region18.csv")
        region_18_output = read.pwc_outfile('all', region_18_path)
        r18 = region_18_output.merge(region_18_input, on='scenario_id', how='inner')
        r18.to_csv("r18_combined.csv")
    else:
        r18 = pd.read_csv("r18_combined.csv")
    return r18

def get_scenarios(region_filter=None, class_filter=None):
    """
    Iteratively get PWC input scenarios and corresponding PWC output data, match them up, and return
    for each crop and region
    :param region_filter: List of regions to confine to (list)
    :param class_filter: List of class numbers to confine to (list)
    :return:
    """
    from hydro.params_nhd import nhd_regions

    # Get a list of all the crop numbers and names used to identify PWC scnearios
    classes = pd.read_csv(crop_group_path)[['pwc_class', 'pwc_class_desc']] \
        .drop_duplicates().sort_values('pwc_class').values

    # Subset regions and classes based on filters
    regions = nhd_regions
    if region_filter is not None:
        regions = region_filter
    if class_filter is not None:
        classes = [row for row in classes if row[0] in class_filter]

    # TODO - DELETE THIS IN THE FUTURE
    #  This is to fix a problem that occurred in autumn of 2020 where Region 18 was left out
    r18_combined = read_region_18(False)

    count = 0
    # Iterate through each class and read tables, starting with the PWC infile
    for class_num, class_name in classes:
        print(f"Reading for {class_num} {class_name}")
        pwc_input = read.pwc_infile(class_num, class_name)
        if pwc_input is not None:
            pwc_output = read.pwc_outfile(class_num, class_name)  # all regions
            combined = pwc_output.merge(pwc_input, on='scenario_id', how='inner')
            for region in regions:
                # TODO - delete this in the future (see above TODO re: region 18)
                if region != '18':
                    regional_combined = combined[combined.region == region]
                else:
                    regional_combined = r18_combined
                if not regional_combined.empty:
                    yield count, region, class_num, class_name, regional_combined
                    count += 1
                else:
                    print(f"Nothing found for region {region} {class_name}")


def compute_percentiles(scenarios):
    """
    Rank all scenarios by EEC and assign a percentile value, weighted by the area of the scenario (if selected).
    :param scenarios: Table of scenarios and EECs (df)
    :return: Scenarios table with new percentiles field (df)
    """
    local_sets = []
    for koc in kocs:
        for duration in pwc_durations:
            local_set = scenarios[(scenarios.koc == koc) & (scenarios.duration == duration)].sort_values('conc')
            if area_weighting:
                local_set['%ile'] = ((np.cumsum(local_set.area) - 0.5 * local_set.area) / local_set.area.sum()) * 100
            else:
                local_set['%ile'] = ((local_set.index + 1) / local_set.shape[0]) * 100
            local_sets.append(local_set)
    scenarios = pd.concat(local_sets, axis=0).reset_index()

    return scenarios


def select_scenarios(scenarios):
    """
    Select scenarios nearest to the selection percentile (specified in parameters.py). Selects a set of scenarios
    for each selection threshold depending on the selection window (parameters.py).
    :param scenarios: Scenarios table (df)
    :return: Combined table of all selected scenarios (df)
    """

    # Select scenarios for each of the durations and combine
    all_selected = []
    for duration in pwc_durations:
        for koc in kocs:
            selection_set = scenarios[(scenarios.duration == duration) & (scenarios.koc == koc)]
            selection_set['dev'] = (selection_set['%ile'] - selection_percentile).abs()
            rank = selection_set.sort_values(['dev', 'area'], ascending=[True, False]).index
            selected_conc = selection_set.loc[rank].iloc[0].conc
            selection = selection_set[selection_set.conc == selected_conc] \
                .sort_values('area', ascending=False) \
                .iloc[0].to_frame().T
            all_selected.append(selection)
    all_selected = \
        pd.concat(all_selected, axis=0).sort_values(['koc', 'duration'], ascending=True).reset_index()

    # Partition selection into raw scenarios and a 'results' table containing the concentrations
    out_fields = list(fields.fetch('pwc_scenario')) + list(fields.fetch('selection'))
    selection_set = all_selected[out_fields]

    return selection_set


def report_region(scenarios, region, class_name, class_num):
    """
    Perform analysis (percentiles, plotting, etc.) on a single NHD Plus hydroregion and crop.
    :param region: NHD Plus hydroregion (str)
    :param class_num: Class number for the crop (str, int)
    :param class_name: Descriptive name of the class (str)
    :param scenarios: Scenarios table (df)
    :return: Path to all outfiles (iter, str)
    """

    # Calculate percentiles for test fields and append additional attributes. Write to file.
    scenarios = compute_percentiles(scenarios)
    write.scenario_summary_table(scenarios, region, class_name, class_num)

    # Select scenarios for each duration based on percentile, and write to file
    selection = select_scenarios(scenarios)

    # Plot the results
    try:
        plot.scenarios(scenarios, selection, region, class_name, class_num)
    except Exception as e:
        raise e

    return selection


def main():
    fields.expand('horizon', max_horizons)
    region_filter = None
    class_filter = None

    for i, region, class_num, class_name, scenarios in get_scenarios(region_filter, class_filter):
        report(f"Working on Region {region} {class_name}...")
        selection = report_region(scenarios, region, class_name, class_num)
        write.selected_scenarios(selection, i == 0)


if __name__ == "__main__":
    main()
