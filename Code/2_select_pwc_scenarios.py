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
from paths import reconstituted_path, crop_group_path

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


def get_scenarios(preprocessed=False, use_parent=False, region_filter=None, class_filter=None):
    classes = pd.read_csv(crop_group_path)[['pwc_class', 'pwc_class_desc']] \
        .drop_duplicates().sort_values('pwc_class').values
    for class_num, class_name in classes:
        if class_filter is None or class_num in class_filter:
            try:
                pwc_input = read.pwc_infile(class_num, class_name, preprocessed=preprocessed, use_parent=use_parent)
            except FileNotFoundError:
                report(f"No file found for {class_name}", 1)
                continue
            for region in sorted(pwc_input.region.unique()):
                if region_filter is None or region in region_filter:
                    try:
                        all_kocs = []
                        for koc in kocs:
                            pwc_output = read.pwc_outfile(region, class_num, class_name, koc, preprocessed=preprocessed)
                            combined = pwc_input.merge(pwc_output, on='scenario_id', how='right')
                            for var in 'region', 'class_num', 'class_name', 'koc':
                                combined[var] = eval(var)
                            all_kocs.append(combined)
                        combined = pd.concat(all_kocs, axis=0)
                        nulls = pd.isnull(combined.conc).sum()
                        if nulls > 0:
                            report(f"Failure: {nulls}/{combined.shape[0]} null values")
                        yield region, class_num, class_name, combined
                    except Exception as e:
                        print(6789, e)
                        yield None, None, None, None


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
            print(selection_set.loc[rank][['%ile', 'conc', 'dev', 'area']])
            selected_conc = selection_set.loc[rank].iloc[0].conc
            print(selected_conc)
            conc_select = selection_set[selection_set.conc == selected_conc]
            print(conc_select[['conc', '%ile', 'dev']])
            exit()
            selection = selection_set[selection_set.conc == selected_conc] \
                .sort_values('area', ascending=False) \
                .iloc[0].to_frame().T
            print(selection)
            exit()
            all_selected.append(selection)
    all_selected = \
        pd.concat(all_selected, axis=0).sort_values(['koc', 'duration'], ascending=True).reset_index()

    # Partition selection into raw scenarios and a 'results' table containing the concentrations
    selection_fields = fields.fetch('pwc_scenario')
    results_fields = [f for f in fields.fetch('results') if f not in selection_fields]
    selection_set = all_selected[selection_fields + results_fields]

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
    region_filter = ['01']
    class_filter = [40]

    for run_num, (region, class_num, class_name, scenarios) in \
            enumerate(get_scenarios(preprocessed=True, region_filter=region_filter, class_filter=class_filter)):
        if scenarios is not None:
            report(f"Working on Region {region} {class_name}...")
            try:
                selection = report_region(scenarios, region, class_name, class_num)
                write.selected_scenarios(selection, run_num == 0)
            except Exception as e:
                print(e)


if __name__ == "__main__":
    main()
