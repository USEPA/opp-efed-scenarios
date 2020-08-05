"""
Concatenate all regions into a single table for each crop group to expedite PWC runs
"""
import pandas as pd
import os
from hydro.nhd import nhd_regions

from paths import pwc_scenario_path
from paths import crop_group_path

out_path = r"J:\opp-efed-data\inputs\Production\Concatenated\{}_{}_all.csv"  # crop num, crop name
num_filter = [8]
crop_groups = pd.read_csv(crop_group_path)[['pwc_class', 'pwc_class_desc']].drop_duplicates()
for num, desc in crop_groups.values:
    if num in num_filter:
        print(num, desc)
        all_tables = []
        for region in nhd_regions:
            path = pwc_scenario_path.format(region, num, desc)
            if os.path.exists(path):
                table = pd.read_csv(path)
                field_order = table.columns.values
                print('region' in table.columns.values)
                all_tables.append(table)
        all_tables = pd.concat(all_tables, axis=0)[field_order]
        all_tables.to_csv(out_path.format(num, desc), index=None)
