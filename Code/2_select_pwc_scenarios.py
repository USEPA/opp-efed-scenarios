"""
select_pwc_scenarios.py

Once the field scenario tables have been run through PWC batch mode, this script processes
the output by ranking scenarios by concentration and selecting scenarios at a certain
threshold. The script also produces plots and tables to put the selections into context.
"""
# Import builtins and standard libraries
import os
import itertools
import pandas as pd
import numpy as np

# Import local modules and variables
import plot
import read
import write
from utilities import fields, report
from parameters import selection_window, selection_percentiles, area_weighting, kocs, \
    percentile_field, scenario_id_field, pwc_durations
from paths import pwc_selection_path

# This silences some error messages being raised by Pandas
pd.options.mode.chained_assignment = None


def initialize_output(region=None, crop=None, koc=None):
    national_run = None in [region, crop, koc]
    if not national_run:
        results_dir = os.path.join(pwc_selection_path, f"{region}_{crop}_summary_files")
        outfiles = [f'summary_koc{koc}.csv', f'selected_koc{koc}.csv', f'koc{koc}_{{}}.png']
        return [os.path.join(results_dir, f"{region}_{crop}_{t}") for t in outfiles]
    else:
        results_dir = os.path.join(pwc_selection_path, "national_summary_files")
        return os.path.join(results_dir, '{}_{}_{}_{}')  # cdl_name, type, duration, koc


def compute_percentiles(scenarios, rank_fields, area_weight=True):
    """
    Rank all scenarios by EEC and assign a percentile value, weighted by the area of the scenario (if selected).
    :param scenarios: Table of scenarios and EECs (df)
    :param rank_fields: Fields on which to rank (iter, str)
    :param area_weight: Weight the percentiles based on the area of the scenario (bool)
    :return: Scenarios table with new percentiles field (df)
    """
    for field in rank_fields:
        # Calculate percentiles
        try:
            scenarios = scenarios.sort_values(str(field))
        except KeyError:
            report(f"Unable to compute percentiles for {field}")
            continue
        if area_weight:
            percentiles = ((np.cumsum(scenarios.area) - 0.5 * scenarios.area) / scenarios.area.sum()) * 100
        else:
            percentiles = ((scenarios.index + 1) / scenarios.shape[0]) * 100
        scenarios[percentile_field.format(field)] = percentiles
    return scenarios


def pivot_scenarios(scenarios, mode):
    """
    Pivot the scenario table between Koc and Duration.
    :param scenarios: Scenarios table (df)
    :param mode: 'koc' or 'duration' (str)
    :return: Pivoted table (df)
    """
    field1, field2 = {'koc': ('koc', 'duration'), 'duration': ('duration', 'koc')}[mode]
    root_table = scenarios.drop(columns=[field1, 'conc']).drop_duplicates()
    local = scenarios[[scenario_id_field, field1, 'conc']]
    # TODO - there are scenarios with duplicate values... why??
    local = local.groupby([scenario_id_field, field1]).mean().reset_index()
    local = local.pivot(index=scenario_id_field, columns=field1, values='conc').reset_index()
    local = root_table.merge(local, on=scenario_id_field, how='left')
    return local.dropna()  # TODO - there are rows with nan values... why?


def report_national(cdl_name, table, field1, field2, normalize=False):
    """
    Perform analyses (percentile computation, plotting) on all scenarios for the entire country.
    :param cdl_name:
    :param table:
    :param field1:
    :param field2:
    :param normalize:
    """
    plot_outfile = initialize_output()
    # TODO - manage these fields with the field manager
    table = table[[scenario_id_field, 'duration', 'koc', 'conc', 'area']]
    f = sorted(table[field2].unique())
    for selection_val, subset in table.groupby(field1):
        subset = pivot_scenarios(subset, field2)
        subset = compute_percentiles(subset, f)
        if subset is not None:
            if normalize:
                plot_outfile = plot_outfile.format(cdl_name, 'national', selection_val, "by_" + field2 + "_n50")
            else:
                plot_outfile = plot_outfile.format(cdl_name, 'national', selection_val, "by_" + field2)
            plot.percentiles(subset, f, combined_label=field2.capitalize(), labels=f,
                             plot_outfile=plot_outfile, normalize_50=normalize)


def select_scenarios(scenarios, durations, method='nearest'):
    """
    Select scenarios nearest to the selection percentile (specified in parameters.py). Selects a set of scenarios
    for each selection threshold depending on the selection window (parameters.py).
    :param scenarios: Scenarios table (df)
    :param durations: Durations for which to make the selection (iter, str)
    :param method: 'nearest' or 'window' (str)
    :return: Combined table of all selected scenarios (df)
    """

    durations = list(map(str, durations))
    scenario_fields = fields.fetch('scenario_fields')
    # Designate the lower and upper bounds for the percentile selection

    # Select scenarios for each of the durations and combine
    all_selected = []
    for conc_field, selection_pct in itertools.product(durations, selection_percentiles):
        pct_field = percentile_field.format(conc_field)
        try:
            pct = scenarios[pct_field]
        except KeyError as e:
            raise e
            report(f"No percentiles found for {pct_field}")
            continue
        if method == 'window':
            # Selects all scenarios within the window, or outside but with equal value
            window = selection_window / 2  # half below, half above
            selected = scenarios[(pct >= (selection_pct - window)) & (pct <= (selection_pct + window))]
            min_val, max_val = selected[pct_field].min(), selected[pct_field].max()
            selection = scenarios[(pct >= min_val) & (pct <= max_val)]
        elif method == 'nearest':
            rank = (pct - selection_pct).abs().sort_values().index
            selection = scenarios.loc[rank].iloc[0].to_frame().T
        # Set new fields
        rename = {conc_field: 'concentration', pct_field: 'percentile'}
        # TODO - does 'scenario_fields' need a unique column and rows? should it use cdl_alias?
        selection = selection[scenario_fields + list(rename.keys())].rename(columns=rename)
        for col in ['concentration', 'percentile']:
            selection[col] = selection[col].astype(np.float32)
        selection['target'] = selection_pct
        selection['subject'] = conc_field
        all_selected.append(selection)

    all_selected = \
        pd.concat(all_selected, axis=0).sort_values(['subject', 'area'], ascending=[True, False]).reset_index()

    return all_selected


def report_region(scenarios, region, crop, koc):
    """
    Perform analysis (percentiles, plotting, etc.) on a single NHD Plus hydroregion and crop.
    :param scenarios: Scenarios table (df)
    :param region: NHD Plus hydroregion (str)
    :param crop: Crop to select (str)
    :param koc: Selected koc value (int)
    :return: Path to all outfiles (iter, str)
    """
    # TODO - more consistent with report_national
    scenarios = pivot_scenarios(scenarios, 'duration')
    summary_outfile, selected_scenario_outfile, plot_outfile = \
        initialize_output(region, crop, koc)

    # Calculate percentiles for test fields and append additional attributes
    scenarios = compute_percentiles(scenarios, pwc_durations, area_weighting)
    if scenarios is None:
        return []

    # Select scenarios for each duration based on percentile, and write to file
    selection = select_scenarios(scenarios, pwc_durations)

    # Write to files
    write.data_table(scenarios, summary_outfile)
    write.data_table(selection, selected_scenario_outfile)

    # Plot results and save tabular data to file
    outfiles = []
    for normalize in range(2):
        if normalize:
            plot_outfile = plot_outfile.rstrip(".png") + "_r50.png"

        outfiles += plot.percentiles(scenarios, pwc_durations, plot_outfile, selection, labels='Scenarios',
                                     selection_label='Percentile', combined=False, normalize_50=normalize)
        outfiles += plot.percentiles(scenarios, pwc_durations, plot_outfile, selection, labels=pwc_durations,
                                     normalize_50=normalize)

    return [summary_outfile, selected_scenario_outfile] + outfiles


def get_ratios(cdl_name, full_table, field1, field2):
    """
    Compute ratios between selection percentile (parameters.py) and median.
    :param cdl_name:
    :param full_table:
    :param field1:
    :param field2:
    """
    regions = sorted(full_table.region.unique())
    all_tables = []
    if field1 == 'koc':
        vars_1, vars_2 = kocs, pwc_durations
    else:
        vars_1, vars_2 = pwc_durations, kocs
    for region, var in itertools.product(regions, vars_1):
        # Limit table to Region, Koc/Duration

        regional_table = full_table[(full_table.region == region) & (full_table[field1] == str(var))]

        if not regional_table.empty:
            regional_table = pivot_scenarios(regional_table, field2)
            regional_table = compute_percentiles(regional_table, vars_2)
            if regional_table is None:
                report(f"Bailing on {region} {field1} == {var}")
                continue
            selection = select_scenarios(regional_table, vars_2)
            selection = selection.groupby(['subject', 'target']).mean().reset_index()
            selection = selection.pivot(index='subject', columns='target', values='concentration')
            for pct in map(int, selection_percentiles[1:]):
                try:
                    selection[f'r_{pct}/50'] = selection[pct] / selection[50]
                except KeyError:
                    report(f"No scenarios found for {region} {var}")
            selection['region'] = region
            selection[field1] = var
            all_tables.append(selection)
    ratio_table = pd.concat(all_tables, axis=0).reset_index()
    # TODO - clean this up (why do i need to do it? koc is str sometimes and int others)
    ratio_table[field1] = [str(v) for v in ratio_table[field1]]
    out_path = initialize_output()
    write.data_table(ratio_table, out_path.format(cdl_name, 'ratios', 'all', f'by_{field1}') + ".csv")
    plot.ratios(ratio_table, out_path, cdl_name, field1, field2, vars_1, vars_2)


def main():
    run_ratios = False
    run_national = False
    run_local = True
    region_filter = None  # None, ['r07']
    report("Finding files...")

    full_table = read.pwc_input_and_output('scenario_id', use_parent=True)

    for cdl_name, table in full_table.groupby('cdl_name'):
        # Get ratios
        if run_ratios:
            report(f"Calculating ratios for {cdl_name}...")
            get_ratios(cdl_name, table, 'koc', 'duration')
            get_ratios(cdl_name, table, 'duration', 'koc')

        # Plot national data by Koc and duration
        if run_national:
            report(f"Creating national plots for {cdl_name}...")
            for normalize_50 in range(2):
                report_national(cdl_name, table, 'koc', 'duration', normalize_50)
                report_national(cdl_name, table, 'duration', 'koc', normalize_50)

        # Break down data by region/crop/koc
        if run_local:
            report("Creating individual plots...")
            for (region, koc), scenarios in table.groupby(fields.fetch('slice')):
                if region_filter is None or region in region_filter:
                    report(f"Working on {region} {koc}...")
                    report_region(scenarios, region, cdl_name, koc)


if __name__ == "__main__":
    main()
