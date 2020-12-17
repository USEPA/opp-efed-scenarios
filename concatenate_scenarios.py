"""
Concatenate all regions into a single table for each crop group to expedite PWC runs
"""
import pandas as pd
import os
from hydro.params_nhd import nhd_regions

from paths import pwc_scenario_path
from paths import crop_group_path

out_path = r"E:\opp-efed-data\scenarios\Production\Concatenated\{}_{}_all.csv"  # crop num, crop name
overwrite = True

crop_groups = pd.read_csv(crop_group_path)[['pwc_class', 'pwc_class_desc']].drop_duplicates()
for num, desc in crop_groups.values:
    all_tables = []
    out_file = out_path.format(num, desc)
    if overwrite or not os.path.exists(out_file):
        for region in nhd_regions:
            path = pwc_scenario_path.format(region, num, desc)
            if os.path.exists(path):
                table = pd.read_csv(path)
                field_order = table.columns.values
                all_tables.append(table)
                print(f"Appending table for region {region} {desc}")
            else:
                print(f"Table for region {region} {desc} not found")
        if len(all_tables) > 0:
            all_tables = pd.concat(all_tables, axis=0)[field_order]
            all_tables.to_csv(out_file, index=None)
