from paths import nhd_map_path, condensed_nhd_path
from hydro.nhd.process_nhd import condense_nhd
from hydro.nhd.params_nhd import nhd_regions

for region in nhd_regions:
    print(region)
    reach_file, _ = condense_nhd(region, nhd_map_path)
    reach_file.to_csv(condensed_nhd_path.format(region), index=None)
