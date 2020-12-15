import arcpy
import os
import numpy as np
import pandas as pd

from hydro.nhd.params_nhd import nhd_regions, vpus_nhd
from tools.efed_lib import report
from paths import nhd_raster_path, weather_raster_path, cdl_path, soil_raster_path, combined_raster_path, combo_path


def check_raster_RAT(raster):
    if not raster.hasRAT:
        report("Building RAT for {}".format(raster.catalogPath), 1)
        arcpy.BuildRasterAttributeTable_management(raster)


def overlay_rasters(outfile, *rasters):
    """
    Perform a GIS overlay of all input raster datasets
    :param outfile: File path for the resulting overlay raster
    :param rasters: Paths for each input raster datset
    """
    rasters += (weather_raster_path, soil_raster_path)

    # Build a raster attribute table (RAT) for layers that don't have one
    for raster in map(arcpy.Raster, rasters):
        check_raster_RAT(raster)

    # Use the ArcGIS Combine tools to create the overlay
    arcpy.gp.Combine_sa([arcpy.Raster(r) for r in rasters], outfile)


def generate_combos(combined_raster, year):
    """
    Read the attribute table from the combined raster and reformat into a csv table
    :param combined_raster: Combined raster (raster GIS file)
    :param combinations_table: Path to output table (string)
    """
    # Match column names in combo raster with new field names
    field_map = [('combo_id', 'VALUE'),
                 ('count', 'COUNT'),
                 ('mukey', 'MAPUNITRASTER_30'),
                 ('cdl', f'{year}_30M_CDLS'),
                 ('weather_grid', 'STATIONS'),
                 ('gridcode', 'CAT')]
    raw_fields = [f.name for f in arcpy.ListFields(combined_raster)]
    missing_fields = [k for k, v in field_map if v not in raw_fields]
    if missing_fields:
        raise KeyError(f"Fields {', '.join(missing_fields)} not found in RAT")
    # Pull data from raster attribute table and into a new csv table
    else:
        new_fields, old_fields = zip(*sorted(field_map))
        data = np.array([row for row in arcpy.da.SearchCursor(combined_raster, old_fields)])
        table = pd.DataFrame(data, columns=new_fields)
        # Calculate area by multiplying cell count by cell area in sq. meters
        table['area'] = table['count'] * 900
        return table


def main():
    years = range(2015, 2020)  # range(2010, 2016)
    overwrite_raster = False
    overwrite_combos = False
    regions = ['07'] + list(nhd_regions)
    for region in regions:
        nhd_raster = nhd_raster_path.format(vpus_nhd[region], region)
        arcpy.env.snapRaster = nhd_raster
        arcpy.env.mask = nhd_raster
        for year in years:
            print(region, year)
            cdl_raster = cdl_path.format(year)
            combined_raster = combined_raster_path.format(region, year)
            combinations_table = combo_path.format(region, year)
            if overwrite_raster or not os.path.exists(combined_raster):
                report("Performing raster overlay for Region {}, {}...".format(region, year))
                try:
                    overlay_rasters(combined_raster, cdl_raster, nhd_raster)
                    report(f"Combined raster saved to {combined_raster}")
                except Exception as e:
                    raise e
            if overwrite_combos or not os.path.exists(combinations_table):
                report("Building combinations table for Region {}, {}...".format(region, year))
                try:
                    combos = generate_combos(combined_raster, year)
                    combos.to_csv(combinations_table, index=None)
                except Exception as e:
                    raise e


if __name__ == "__main__":
    main()
