"""
spatial_overlay.py

Use Esri ArcGIS tools to create an overlay of soil, land use, watershed, and weather grid
rasters, and to create a table of unique combinations of each attribute with corresponding areas.
"""

# Import builtin and standard packages
import os
import numpy as np
import pandas as pd

# Import Esri ArcGIS functionality
import arcpy

# Import local modules and variables
import write
from utilities import report
from parameters import vpus_nhd, nhd_regions
from paths import nhd_raster_path, soil_raster_path, weather_path, cdl_path, combo_path, combined_raster_path

# Set ArcGIS parameters
arcpy.CheckOutExtension("Spatial")
arcpy.env.overwriteOutput = True


# TODO - add snap raster

def overlay_rasters(outfile, *rasters):
    """
    Perform a GIS overlay of all input raster datasets
    :param outfile: File path for the resulting overlay raster
    :param rasters: Paths for each input raster datset
    """
    # Build a raster attribute table (RAT) for layers that don't have one
    for raster in map(arcpy.Raster, rasters):
        if not raster.hasRAT:
            report("Building RAT for {}".format(raster.catalogPath), 1)
            arcpy.BuildRasterAttributeTable_management(raster)
    # Use the ArcGIS Combine tools to create the overlay
    arcpy.gp.Combine_sa([arcpy.Raster(r) for r in rasters], outfile)


def generate_combos(combined_raster):
    """
    Read the attribute table from the combined raster and reformat into a csv table
    :param combined_raster: Combined raster (raster GIS file)
    :param combinations_table: Path to output table (string)
    """
    # Match column names in combo raster with new field names
    field_map = [('combo_id', 'VALUE'),
                 ('count', 'COUNT'),
                 ('mukey', 'SOIL'),
                 ('cdl', 'CDL'),
                 ('weather_grid', 'MET'),
                 ('gridcode', 'CAT')]
    raw_fields = [f.name for f in arcpy.ListFields(combined_raster)]
    field_dict = {}
    for new_name, search in field_map:
        for old_name in raw_fields:
            if old_name.startswith(search):
                field_dict[old_name] = new_name
                break
        else:
            field_dict[old_name] = None

    # Pull data from raster attribute table and into a new csv table
    if all(field_dict.values()):
        old_fields, new_fields = zip(*sorted(field_dict.items()))
        data = np.array([row for row in arcpy.da.SearchCursor(combined_raster, old_fields)])
        table = pd.DataFrame(data, columns=new_fields)
        # Calculate area by multiplying cell count by cell area in sq. meters
        table['area'] = table['count'] * 900
        return table
    else:
        raise KeyError("Missing fields in attribute table")


def main():
    years = range(2018, 2019)  # range(2010, 2016)

    overwrite_rasters = False
    overwrite_tables = True

    # Iterate through year/region combinations
    regions = nhd_regions  # ['07']
    for region in regions:
        # Initialize raster file paths for the region
        nhd_raster = nhd_raster_path.format(vpus_nhd[region], region)
        soil_raster = soil_raster_path.format(region)
        weather_raster = weather_path.format(region) + ".tif"

        # Loop through annual land use rasters
        for year in years:

            # Initialize file paths for the region and year
            cdl_raster = cdl_path.format(region, year)
            combined_raster = combined_raster_path.format(region, year)
            combinations_table = combo_path.format(region, year)

            # Check if file exists or is to be overwritten
            if overwrite_rasters or not os.path.exists(combined_raster):
                try:
                    # Confirm that all required layers exist for the region and year
                    if all(map(os.path.exists, (nhd_raster, soil_raster, weather_raster, cdl_raster))):
                        report("Performing raster overlay for Region {}, {}...".format(region, year))
                        overlay_rasters(combined_raster, soil_raster, cdl_raster, weather_raster, nhd_raster)
                    else:
                        # If some layers are missing, identify which
                        paths = [('nhd', nhd_raster), ('soil', soil_raster),
                                 ('weather', weather_raster), ('cdl', cdl_raster)]
                        missing = ", ".join([name for name, path in paths if not os.path.exists(path)])
                        report("Missing {} layers for Region {}, {}".format(missing, region, year))
                except Exception as e:
                    print(e)
            if overwrite_tables or not os.path.exists(combinations_table):
                report("Generating combination table for Region {}, {}...".format(region, year))
                try:
                    combos = generate_combos(combined_raster)
                    write.combinations(region, year, combos)
                except Exception as e:
                    print(e)


main()
