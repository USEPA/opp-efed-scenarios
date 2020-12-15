"""
write.py

Functions for writing output files.
"""

# Import builtin and standard libraries
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Import local variables
from parameters import fields, pwc_selection_field
from paths import sam_scenario_path, pwc_scenario_path, recipe_path, hydro_file_path, combo_path, \
    qc_path, summary_outfile, plot_outfile, combined_outfile, combined_results


# TODO - create a wrapper function to makedirs automatically

def combinations(region, year, table):
    """ Write a combinations table to a csv file """
    out_path = combo_path.format(region, year)
    # Create output directory
    create_dir(out_path)
    table.to_csv(out_path, index=None)


def create_dir(out_path):
    """ Create a directory for a file name if it doesn't exist """
    if not os.path.exists(os.path.dirname(out_path)):
        os.makedirs(os.path.dirname(out_path))


def scenario_summary_table(data, region, class_name, class_num):
    outfile = summary_outfile.format(class_name, 'all', region)
    """ Write a dataframe to a csv file """
    create_dir(outfile)
    data.to_csv(outfile, index=None)


def flow_file(table, region):
    """ Write a table with daily flow data to a .csv file """
    table.to_csv(hydro_file_path.format(region, 'flow', 'csv'))


def lake_file(table, region):
    """ Write a table with waterbody data to a .csv file """
    table[["outlet_comid", "wb_comid"]] = table[["outlet_comid", "wb_comid"]].astype(np.int32)
    table.to_csv(hydro_file_path.format(region, 'lake', 'csv'))


def navigator(region, nav):
    """ Write a Navigator object to a compressed numpy array file """
    outfile = hydro_file_path.format(region, 'nav', 'npz')
    np.savez_compressed(outfile, paths=nav.paths, time=nav.times, length=nav.length, path_map=nav.path_map,
                        alias_index=nav.conversion)


def qc_report(region, mode, qc, class_field=pwc_selection_field):
    # Summarize violations by class and field
    scenarios(qc, mode, region, 'qc')

    out_report = []
    for _class, class_table in qc.groupby(class_field):
        n_scenarios = class_table.shape[0]
        for field in qc.columns.values:
            if field not in ('scenario_id', class_field):
                n_violations = (class_table[field] > 1).sum()
                out_report.append([_class, field, n_scenarios, n_violations, (n_violations / n_scenarios) * 100.])
    out_report = pd.DataFrame(out_report,
                              columns=[pwc_selection_field, 'field', 'n_scenarios', 'n_violations', 'pct_excluded'])
    scenarios(out_report, mode, region, 'report')


def recipes(region, recipe_table, recipe_map, mode='mmap'):
    """ Write SAM watershed recipes table to a memory-mapped file,
     along with a .csv key file """
    path = recipe_path.format(region)
    if mode == 'csv':
        recipe_table.to_csv(f"{path}.csv", index=None)
    else:
        print(path)
        fp = np.memmap(path, np.int64, 'w+', shape=recipe_table.shape)
        fp[:] = recipe_table.values
        del fp
    recipe_map.to_csv(f"{path}_map.csv", index=None)
    with open(f"{path}_key.txt", 'w') as f:
        f.write(f"{recipe_table.shape}")


def scenarios(scenario_matrix, mode, region, name=None, num='all'):
    """ Write a scenarios table to file
    The 'name' parameter is used to specify crop group for PWC scenarios """
    if scenario_matrix is not None:
        if mode == 'sam':
            out_path = sam_scenario_path.format(region, name)
        elif mode == 'pwc':
            out_path = pwc_scenario_path.format(region, num, name)
        out_path = out_path.replace("/", "-")
        create_dir(out_path)
        scenario_matrix.to_csv(out_path, index=False)


def selected_scenarios(selection, first_run=False):
    create_dir(combined_outfile)

    # Add a filename field
    selection = selection.reset_index()
    selection['filename'] = selection.pwc_class.astype(np.int32).astype(str) + \
                            '_' + selection.koc.astype(str) + \
                            '_' + selection.region.astype(str) + \
                            '_' + selection.duration

    # Choose output fields
    scenario_fields = list(fields.fetch('pwc_scenario')) + ['filename']
    scenario_fields.remove("region")

    # Write to file
    flag = 'w' if first_run else 'a'
    selection[fields.fetch('selection')].to_csv(combined_results, mode=flag, header=(flag == 'w'), index=None)
    selection[scenario_fields].to_csv(combined_outfile, mode=flag, header=(flag == 'w'), index=None)


def plot(region, class_name, class_num, koc, label, legend=False, legend_title=None, clear=True, position='best'):
    outfile = plot_outfile.format(region, class_name, koc, label)
    create_dir(outfile)
    if legend:
        plt.legend(loc=position, title=legend_title)
    plt.savefig(outfile, dpi=600)
    if clear:
        plt.clf()


def qc_table(qc_table, write_id):
    outfile = qc_path.format(write_id)
    # Write QC file
    if outfile is not None:
        if not os.path.isdir(os.path.dirname(outfile)):
            os.makedirs(os.path.dirname(outfile))
        qc_table.to_csv(outfile)
