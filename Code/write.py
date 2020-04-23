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
from paths import sam_scenario_path, pwc_scenario_path, recipe_path, hydro_file_path, combo_path, condensed_nhd_path, \
    qc_path


# TODO - create a wrapper function to makedirs automatically

def combinations(region, year, table):
    """ Write a combinations table to a csv file """
    out_path = combo_path.format(region, year)
    # Create output directory
    if not os.path.exists(os.path.dirname(out_path)):
        os.makedirs(os.path.dirname(out_path))
    table.to_csv(out_path, index=None)


def condensed_nhd(region, table):
    """ Write a condensed NHD table to file """
    condensed_file = condensed_nhd_path.format(region)
    table.to_csv(condensed_file, index=None)


def create_dir(out_path):
    """ Create a directory for a file name if it doesn't exist """
    if not os.path.exists(os.path.dirname(out_path)):
        os.makedirs(os.path.dirname(out_path))


def data_table(data, outfile):
    """ Write a dataframe to a csv file """
    if not os.path.exists(os.path.dirname(outfile)):
        os.makedirs(os.path.dirname(outfile))
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


def qc_report(in_scenarios, qc_table, region, mode):
    """
    Write files describing the results of a QAQC. Writes a table of all missing
    or out-of-range data flags, along with a table of which fields were most frequently
    flagged.
    :param in_scenarios: Table containing data subjected to QAQC (df)
    :param qc_table: Table containing QAQC flags (df)
    :param region: NHD Plus Hydroregion (str)
    :param mode: 'sam' or 'pwc'
    """
    # Initialize a table with all violations for each scenario
    violation_table = in_scenarios[['scenario_id']]

    # Initialize a report with violations by field
    field_report = [['n scenarios', in_scenarios.shape[0]]]

    # Iterate through fields and test for violations
    for field in qc_table.columns:
        violation_table[field] = 0
        bogeys = np.where(qc_table[field] > 2)[0]
        if bogeys.sum() > 0:
            violation_table.loc[bogeys, field] = 1
            field_report.append([field, (bogeys > 0).sum()])

    # Report on the violations for each scenario
    violation_table['n_violations'] = violation_table[qc_table.columns].sum(axis=1)
    scenarios(violation_table, mode, region, '_qc')

    # Report on the number of violations for each field
    field_report = pd.DataFrame(field_report, columns=['field', 'n_violations'])
    scenarios(field_report, mode, region, '_report')


def recipes(region, recipe_table, recipe_map, mode='mmap'):
    """ Write SAM watershed recipes table to a memory-mapped file,
     along with a .csv key file """
    path = recipe_path.format(region)
    if mode == 'csv':
        recipe_table.to_csv(f"{path}.csv", index=None)
    else:
        print(recipe_table)
        fp = np.memmap(f"{path}", np.int64, 'w+', shape=recipe_table.shape)
        fp[:] = recipe_table.values
        del fp
    recipe_map.to_csv(f"{path}_map.csv", index=None)
    with open(f"{path}_key.txt", 'w') as f:
        f.write(f"{recipe_table.shape}")


def scenarios(scenario_matrix, mode, region, name=None):
    """ Write a scenarios table to file
    The 'name' parameter is used to specify crop group for PWC scenarios """
    if scenario_matrix is not None:
        if mode == 'sam':
            out_path = sam_scenario_path.format(region, name)
        elif mode == 'pwc':
            out_path = pwc_scenario_path.format(region, name)
        out_path = out_path.replace("/", "-")
        create_dir(out_path)
        scenario_matrix.to_csv(out_path, index=False)


def plot(outfile, legend=True, legend_title=None, clear=True, position='best'):
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
