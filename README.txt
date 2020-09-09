There are 3 scripts that must be run in sequence to generate scenarios. These scripts are identified and ordered by a leading number in the script name.

0_spatial_overlay.py performs a GIS overlay of input environmental datasets to form a spatial index from which scenarios are populated. This script requires an installation of Esri ArcGIS with ArcPy for Python 3.

1_scenarios_and_recipes.py joins the spatial index generated in the previous step and populates it with environmental data. The script generates scenarios for either SAM or PWC, and for SAM modeling, it also generates watershed recipes.

2_select_pwc_scenarios.py combines the PWC scenarios generated in the previous step with PWC output to select a subset of scenarios for implementation in OPP risk assessments. In order to run this script, it is first necessary to run the results of the previous step through the PWC in batch mode.
