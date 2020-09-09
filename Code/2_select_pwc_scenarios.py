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
from efed_lib.efed_lib import report
from parameters import selection_percentile, area_weighting, pwc_durations, fields, max_horizons, nhd_regions, kocs
from paths import crop_group_path

# This silences some error messages being raised by Pandas
pd.options.mode.chained_assignment = None


def check_vals(combined_table, pwc_input, pwc_output):
    nulls = pd.isnull(combined_table.conc)
    if nulls.sum() > 0:
        report(f"Nulls: {nulls.sum()}/{combined_table.shape[0]}")
        input_scenarios = set(pwc_input.scenario_id.values)
        output_scenarios = set(pwc_output.scenario_id.values)
        in_missing = output_scenarios - input_scenarios
        out_missing = input_scenarios - output_scenarios
        report(f"missing from in/out: {len(in_missing)}, {len(out_missing)}")
        if in_missing:
            report(f"in missing: {list(in_missing)[:5]}")
        if out_missing:
            report(f"out missing: {list(out_missing)[:5]}")


def get_scenarios(region_filter=None, class_filter=None):
    from hydro.nhd import nhd_regions

    classes = pd.read_csv(crop_group_path)[['pwc_class', 'pwc_class_desc']] \
        .drop_duplicates().sort_values('pwc_class').values

    # Subset regions and classes based on filters
    regions = nhd_regions if region_filter is None else region_filter

    if class_filter is not None:
        classes = [row for row in classes if row[0] in class_filter]

    for class_num, class_name in classes:
        pwc_output = pd.concat([read.pwc_outfile(class_num, koc) for koc in kocs], axis=0)
        for region in regions:
            try:
                pwc_input = read.pwc_infile(class_num, class_name, region)
                combined = pwc_output.merge(pwc_input, on='scenario_id', how='inner')
                yield region, class_num, class_name, combined
            except FileNotFoundError:
                pass


def compute_percentiles(scenarios):
    """
    Rank all scenarios by EEC and assign a percentile value, weighted by the area of the scenario (if selected).
    :param scenarios: Table of scenarios and EECs (df)
    :param rank_fields: Fields on which to rank (iter, str)
    :param area_weight: Weight the percentiles based on the area of the scenario (bool)
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
    :param durations: Durations for which to make the selection (iter, str)
    :param method: 'nearest' or 'window' (str)
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
    out_fields = fields.fetch('pwc_scenario') + fields.fetch('selection')
    print(666, out_fields)
    exit()
    selection_set = all_selected[fields.fetch('pwc_scenario') + fields.fetch('selection')]

    return selection_set


def report_region(scenarios, region, class_name, class_num):
    """
    Perform analysis (percentiles, plotting, etc.) on a single NHD Plus hydroregion and crop.
    :param scenarios: Scenarios table (df)
    :param region: NHD Plus hydroregion (str)
    :param crop: Crop to select (str)
    :param koc: Selected koc value (int)
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
    class_filter = [70, 130, 140, 200]

    for run_num, (region, class_num, class_name, scenarios) in \
            enumerate(get_scenarios(region_filter=region_filter, class_filter=class_filter)):
        if scenarios is not None:
            report(f"Working on Region {region} {class_name}...")
            try:
                selection = report_region(scenarios, region, class_name, class_num)
                print(selection.columns.values)
                exit()
                write.selected_scenarios(selection, run_num == 0)
            except Exception as e:
                raise e


if __name__ == "__main__":
    main()
