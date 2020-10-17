import os
from hydro.nhd.params_nhd import nhd_regions
from tools.efed_lib import report
from paths import condensed_nhd_path, nhd_map_path

def condense_nhd():
    regions = nhd_regions
    regions = ['07']
    overwrite = True
    for region in regions:
        report(f"Condensing NHDPlus for Region {region}...")
        reach_table_path = condensed_nhd_path.format(region, 'reach')
        wb_table_path = condensed_nhd_path.format(region, 'waterbody')
        if overwrite or not all(map(os.path.exists, (reach_table_path, wb_table_path))):
            reach_table, lake_table = condense_nhd(region, sam_nhd_map, rename_field='internal_name')
            reach_table.to_csv(reach_table_path, index=None)
            lake_table.to_csv(wb_table_path, index=None)

