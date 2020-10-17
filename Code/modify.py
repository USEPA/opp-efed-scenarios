"""
modify.py

This script contains functions that modify raw input data in the scenario and recipe generation process.
Examples of modifications include: renaming or removing fields, calculating additional parameters, or aggregating data
"""

# Import builtins and standard libraries
import numpy as np
import pandas as pd
import datetime as dt

# Import local modules and variables
import write
from tools.efed_lib import report
from parameters import fields, max_horizons, hydro_soil_group, uslep_values, aggregation_bins, depth_bins, usle_m_vals, \
    usle_m_bins, date_fmt

# This silences some error messages being raised by Pandas
pd.options.mode.chained_assignment = None


def date_to_num(params):
    # Convert dates to days since Jan 1
    for field in fields.fetch('date', field_filter=params.columns):
        params[field] = (pd.to_datetime(params[field], format=date_fmt) - pd.to_datetime("1900-01-01")).dt.days
    return params


def num_to_date(params):
    def n_to_d(date):
        try:
            return (dt.date(2001, 1, 1) + dt.timedelta(days=int(date))).strftime(date_fmt)
        except (ValueError, OverflowError):
            return 'n/a'

    for field in fields.fetch('date'):
        if field in params.columns:
            params[field] = params[field].apply(n_to_d)
    return params


def aggregate_soils(in_soils):
    """
    Reduce the number of unique soils by aggregating soils with similar properties, and generate a crosswalk
    (aggregation key) that links old soil ids to new aggregated ones. Aggregation is based on value in defined
    bins which are specified in parameters.py.

    This is only done in SAM mode.
    :param in_soils: Soil properties table (df)
    :return: Aggregated soil properties table (df), aggregation key (df)
    """
    from parameters import aggregation_bins

    # Sort data into bins
    out_data = [in_soils.hsg_letter]
    for field, field_bins in aggregation_bins.items():
        # Designate aggregated field labels (e.g., l1, l2 for slope) and apply with 'cut'
        labels = [field[2 if field == "slope" else 1] + str(i) for i in range(1, len(field_bins))]
        sliced = pd.cut(in_soils[field].fillna(0), field_bins, labels=labels, right=False, include_lowest=True)
        out_data.append(sliced.astype("str"))
    soil_agg = pd.concat(out_data, axis=1)

    # Create aggregation key in soil_id field
    invalid = pd.isnull(soil_agg[['hsg_letter', 'slope', 'orgC_5', 'sand_5', 'clay_5']]).any(axis=1)
    in_soils.loc[:, 'soil_id'] = 'invalid_soil_tp'
    in_soils.loc[~invalid, 'soil_id'] = \
        soil_agg['hsg_letter'] + \
        soil_agg['slope'] + \
        soil_agg['orgC_5'] + \
        soil_agg['sand_5'] + \
        soil_agg['clay_5']

    # Group by aggregation key and take the mean of all properties except HSG, which will use mode
    fields.refresh()
    fields.expand('depth_weight', depth_bins)
    averaged = in_soils.groupby('soil_id')[fields.fetch('agg_mean')].mean().reset_index()
    hydro_group = in_soils.groupby('soil_id')[['hydro_group']].max()
    aggregated = averaged.merge(hydro_group, on='soil_id')
    aggregation_key = in_soils[['mukey', 'soil_id']].drop_duplicates().sort_values(by=['mukey'])
    return aggregated, aggregation_key


def combinations(combos, crop_params, mode, agg_key):
    """
    Perform modifications to combinations table, including the addition of double-cropped classes,
    aggregation of PWC scenarios with different years, and creation of a unique scenario ID.
    :param combos: Combinations table (df)
    :param crop_params: Parameters linked to crop type (df)
    :param mode: 'sam' or 'pwc'
    :param agg_key: Table linking aggregated soil IDs to original soil ID (df)
    :return: Modified combinations table (df)
    """

    # Split double-cropped classes into individual scenarios
    double_crops = \
        crop_params[['cdl', 'cdl_alias']].drop_duplicates().sort_values('cdl').astype(np.uint16)
    combos = combos.merge(double_crops, on='cdl', how='left')

    # SAM - agg_key is ['mukey', 'state', 'soil_id']
    # PWC - agg_key is ['mukey', 'state']
    combos = combos.merge(agg_key, on='mukey', how='left')

    # Aggregate combinations by soil (SAM)
    # 'mukey' is replaced by aggregation id (soil_id) for sam, or renamed soil_id for pwc
    # from this point on soil_id is the index for soils
    # year field is not retained for pwc scenarios.
    if mode == 'sam':
        del combos['mukey']
    if mode == 'pwc':
        del combos['year']
        del combos['gridcode']
        combos = combos.rename(columns={'mukey': 'soil_id'})
    aggregate_fields = [c for c in combos.columns if c != "area"]
    combos = combos.groupby(aggregate_fields).sum().reset_index()  # big overhead jump

    # Create a unique identifier
    combos['scenario_id'] = '-' + combos.soil_id.astype("str") + \
                            '-' + combos.weather_grid.astype("str") + \
                            '-' + combos.cdl.astype("str")
    return combos


def depth_weight_soils(in_soils):
    """
    Creates standardized depth horizons for soils through averaging.
    Only used in SAM mode.
    :param in_soils: Soils data table (df)
    :return: Modified soils data table (df)
    """
    # Get the root name of depth weighted fields
    fields.refresh()
    depth_fields = fields.fetch('depth_weight')

    # Generate weighted columns for each bin
    depth_weighted = []
    for bin_top, bin_bottom in zip([0] + list(depth_bins[:-1]), list(depth_bins)):
        bin_table = np.zeros((in_soils.shape[0], len(depth_fields)))

        # Perform depth weighting on each horizon
        for i in range(max_horizons):
            # Adjust values by bin
            horizon_bottom = in_soils['horizon_bottom_{}'.format(i + 1)]
            horizon_top = in_soils['horizon_top_{}'.format(i + 1)]

            # Get the overlap between the SSURGO horizon and soil bin
            overlap = (horizon_bottom.clip(upper=bin_bottom) - horizon_top.clip(lower=bin_top)).clip(0)
            ratio = (overlap / (horizon_bottom - horizon_top)).fillna(0)

            # Add the values
            value_fields = ["{}_{}".format(f, i + 1) for f in depth_fields]
            bin_table += in_soils[value_fields].fillna(0).mul(ratio, axis=0).values

        # Add columns
        bin_table = \
            pd.DataFrame(bin_table, columns=["{}_{}".format(f, bin_bottom) for f in depth_fields])
        depth_weighted.append(bin_table)

    # Clear all fields corresponding to horizons, and add depth-binned data
    fields.expand('horizon', max_horizons)  # this will add all the _n fields
    for field in fields.fetch('horizon'):
        del in_soils[field]
    in_soils = pd.concat([in_soils.reset_index()] + depth_weighted, axis=1)

    return in_soils


def met(met_params):
    """
    Modify a table of parameters linked to weather grid.
    :param met_params: Table of parameters linked to weather grid (df)
    :return: Modified table of parameters linked to weather grid (df)
    """
    # 'stationID' is the id field corresponding to the original (2015) weather files
    # Eventually will likely move to a new scheme
    met_params['weather_grid'] = met_params.pop('stationID')
    met_params = met_params.astype({'weather_grid': np.int32})
    return met_params


def nhd(nhd_table):
    """
    Modify data imported from the NHD Plus dataset. These modifications are chiefly
    to facilitate watershed delination methods in generate_hydro_files.py.
    Remove rows in the condensed NHD table which signify a connection between a reach and a divergence.
    Retains only a single record for a given comid with the downstream divergence info for main divergence.
    :param nhd_table: Hydrographic data from NHD Plus (df)
    :return: Modified hydrographic data (df)
    """
    # Add the divergence and streamcalc of downstream reaches to each row
    downstream = nhd_table[['comid', 'divergence', 'streamcalc', 'fcode']]
    downstream.columns = ['tocomid'] + [f + "_ds" for f in downstream.columns.values[1:]]
    downstream = nhd_table[['comid', 'tocomid']].drop_duplicates().merge(
        downstream.drop_duplicates(), how='left', on='tocomid')

    # Where there is a divergence, select downstream reach with the highest streamcalc or lowest divergence
    downstream = downstream.sort_values('streamcalc_ds', ascending=False).sort_values('divergence_ds')
    downstream = downstream[~downstream.duplicated('comid')]

    nhd_table = nhd_table.merge(downstream, on=['comid', 'tocomid'], how='inner')

    # Calculate travel time, channel surface area, identify coastal reaches and
    # reaches draining outside a region as outlets and sever downstream connection
    # for outlet reaches

    nhd_table['tocomid'] = nhd_table.tocomid.fillna(-1)

    # Convert units
    nhd_table['length'] = nhd_table.pop('lengthkm') * 1000.  # km -> m
    for month in list(map(lambda x: str(x).zfill(2), range(1, 13))) + ['ma']:
        nhd_table["q_{}".format(month)] *= 2446.58  # cfs -> cmd
        nhd_table["v_{}".format(month)] *= 26334.7  # f/s -> md

    # Calculate travel time
    nhd_table["travel_time"] = nhd_table.length / nhd_table.v_ma

    # Calculate surface area
    stream_channel_a = 4.28
    stream_channel_b = 0.55
    cross_section = nhd_table.q_ma / nhd_table.v_ma
    nhd_table['surface_area'] = stream_channel_a * np.power(cross_section, stream_channel_b)

    # Indicate whether reaches are coastal
    nhd_table['coastal'] = np.int16(nhd_table.pop('fcode') == 56600)

    # Identify basin outlets
    nhd_table['outlet'] = 0

    # Identify all reaches that are a 'terminal path'. HydroSeq is used for Terminal Path ID in the NHD
    nhd_table.loc[nhd_table.hydroseq.isin(nhd_table.terminal_path), 'outlet'] = 1

    # Identify all reaches that empty into a reach outside the region
    nhd_table.loc[~nhd_table.tocomid.isin(nhd_table.comid) & (nhd_table.streamcalc > 0), 'outlet'] = 1

    # Designate coastal reaches as outlets. These don't need to be accumulated
    nhd_table.loc[nhd_table.coastal == 1, 'outlet'] = 1

    # Sever connection between outlet and downstream reaches
    nhd_table.loc[nhd_table.outlet == 1, 'tocomid'] = 0

    return nhd_table


def scenarios(in_scenarios, mode, region, write_qc=True):
    """
    Modify a table of field scenario parameters. This is primarly for computing parameters
    that are linked to multiple indices (e.g., land cover and soil). The major functions here include
    the assignment of runoff curve numbers, setting root and evaporation depth,
    and performing QAQC. QAQC parameters are specified in fields_and_qc.csv.
    :param in_scenarios: Input scenarios table (df)
    :param mode: 'sam' or 'pwc'
    :param region: NHD Plus region (str)
    :param write_qc: Write the results of the QAQC to file (bool)
    :return: Modified scenarios table (df)
    """
    from parameters import anetd

    # Assigns 'cover' and 'fallow' curve numbers for each scenario based on hydrologic soil group
    in_scenarios['cn_cov'] = in_scenarios['cn_fal'] = -1.

    # Do cultivated crops, then non-cultivated crops
    for cultivated, col in enumerate(('non-cultivated', 'cultivated')):
        # Convert from HSG number (hydro_group) to letter
        # For drained soils, fallow is set to D condition
        for hsg_num, hsg_letter in enumerate(hydro_soil_group[col]):
            sel = (in_scenarios.hydro_group == hsg_num + 1) & (in_scenarios.cultivated == cultivated)
            in_scenarios.loc[sel, 'cn_cov'] = in_scenarios.loc[sel, f'cn_cov_{hsg_letter}']
            in_scenarios.loc[sel, 'cn_fal'] = in_scenarios.loc[sel, f'cn_fal_{hsg_letter}']

    # Calculate max irrigation rate by the USDA curve number method
    in_scenarios['max_irrigation'] = ((2540. / in_scenarios.cn_cov) - 25.4)  # cm

    # Ensure that root and evaporation depths are 0.5 cm or more shallower than soil depth
    in_scenarios['root_depth'] = \
        np.minimum(in_scenarios.root_zone_max.values - 0.5, in_scenarios.max_root_depth)
    in_scenarios['evaporation_depth'] = \
        np.minimum(in_scenarios.root_zone_max.values - 0.5, anetd)

    # Choose output fields and perform data correction
    report("Performing data correction...", 3)
    fields.refresh()
    in_scenarios = in_scenarios.reset_index()

    if mode == 'pwc':
        test_fields = sorted({f for f in fields.fetch('pwc_scenario') if f not in fields.fetch('horizon')})
        qc_table = fields.perform_qc(in_scenarios[test_fields]).copy()
        in_scenarios = in_scenarios[qc_table.max(axis=1) < 2]
        fields.expand('horizon', max_horizons)
    else:
        fields.expand("depth_weight", depth_bins)
        in_scenarios = in_scenarios[fields.fetch('sam_scenario')]
        qc_table = fields.perform_qc(in_scenarios)
        in_scenarios = in_scenarios.mask(qc_table == 2, fields.fill(), axis=1)
    if write_qc:
        write.qc_report(in_scenarios, qc_table, region, mode)
    if mode == 'pwc':
        in_scenarios = in_scenarios[~in_scenarios.sam_only.fillna(0).astype(bool)]
    return in_scenarios[fields.fetch(mode + '_scenario')]


def soils(in_soils, mode):
    """
    Modify a table of parameters linked to soil. This is the most intensive modification
    in the scenarios workflow and includes selection of the main component for each
    soil map unit, combining mapunit and horizon data, assigning hydrologic soil group,
    and calculating USLE variables.
    :param in_soils: Table of parameters linked to soil (df)
    :param mode: 'sam' or 'pwc'
    :return: Modified table of parameters linked to soil
    """
    from parameters import o_horizon_max, slope_length_max, slope_min

    """  Identify component to be used for each map unit """
    fields.refresh()
    print(sorted(in_soils.columns.values))
    # Adjust soil data values
    in_soils.loc[:, 'orgC'] /= 1.724  # oc -> om
    in_soils.loc[:, ['water_max', 'water_min']] /= 100.  # pct -> decimal

    # Use defaults for slope and slope length where missing
    in_soils.loc[pd.isnull(in_soils.slope_length), 'slope_length'] = slope_length_max
    in_soils.loc[in_soils.slope < slope_min, 'slope'] = slope_min

    # Isolate unique map unit/component pairs and select major component with largest area (comppct)
    components = in_soils[['mukey', 'cokey', 'major_component', 'component_pct']].drop_duplicates(['mukey', 'cokey'])
    components = components[components.major_component == 'Yes']
    components = components.sort_values('component_pct', ascending=False)
    components = components[~components.mukey.duplicated()]
    in_soils = components[['mukey', 'cokey']].merge(in_soils, on=['mukey', 'cokey'], how='left')

    # Delete thin organic horizons
    in_soils = in_soils[~((in_soils.horizon_letter == 'O') & (in_soils.horizon_bottom <= o_horizon_max))]

    # Sort table by horizon depth and get horizon information
    in_soils = in_soils.sort_values(['cokey', 'horizon_top'])
    in_soils['thickness'] = in_soils['horizon_bottom'] - in_soils['horizon_top']
    in_soils['horizon_num'] = np.int16(in_soils.groupby('cokey').cumcount()) + 1
    in_soils = in_soils.sort_values('horizon_num', ascending=False)
    in_soils = in_soils[~(in_soils.horizon_num > max_horizons)]

    # Extend columns of data for multiple horizons
    horizon_data = in_soils.set_index(['cokey', 'horizon_num'])[fields.fetch('horizon')]
    horizon_data = horizon_data.unstack().sort_index(1, level=1)
    horizon_data.columns = ['_'.join(map(str, i)) for i in horizon_data.columns]

    # Initialize empty fields for fields linked to soil horizons
    for f in fields.fetch('horizon'):
        for i in range(in_soils.horizon_num.max(), max_horizons + 1):
            horizon_data["{}_{}".format(f, i)] = np.nan
        del in_soils[f]

    # Add horizontal data to table
    in_soils = in_soils.drop_duplicates(['mukey', 'cokey']).merge(horizon_data, left_on='cokey', right_index=True)
    in_soils = in_soils.rename(columns={'horizon_num': 'n_horizons'})

    # New HSG code - take 'max' of two versions of hsg
    hsg_to_num = {hsg: i + 1 for i, hsg in enumerate(hydro_soil_group.name)}
    num_to_hsg = {v: k.replace("/", "") for k, v in hsg_to_num.items()}
    in_soils['hydro_group'] = in_soils[['hydro_group', 'hydro_group_dominant']].applymap(
        lambda x: hsg_to_num.get(x)).max(axis=1).fillna(-1).astype(np.int32)
    in_soils['hsg_letter'] = in_soils['hydro_group'].map(num_to_hsg)

    # Calculate USLE variables
    # Take the value from the top horizon with valid kwfact values
    in_soils['usle_k'] = in_soils[["usle_k_horiz_{}".format(i + 1) for i in range(max_horizons)]].bfill(1).iloc[:, 0]
    m = usle_m_vals[np.int16(pd.cut(in_soils.slope.values, usle_m_bins, labels=False))]
    sine_theta = np.sin(np.arctan(in_soils.slope / 100))  # % -> sin(rad)
    in_soils['usle_ls'] = (in_soils.slope_length / 72.6) ** m * (65.41 * sine_theta ** 2. + 4.56 * sine_theta + 0.065)
    in_soils['usle_p'] = np.array(uslep_values)[
        np.int16(pd.cut(in_soils.slope, aggregation_bins['slope'], labels=False))]

    # Set n_horizons to the first invalid horizon
    horizon_fields = [f for f in fields.fetch('horizon') if f in fields.fetch('pwc_scenario')]
    in_soils = in_soils.reset_index()
    fields.expand('horizon', max_horizons)
    qc_table = fields.perform_qc(in_soils).copy()
    for field in horizon_fields:
        check_fields = ['{}_{}'.format(field, i + 1) for i in range(max_horizons)]
        if qc_table[check_fields].values.max() > 1:  # QC value of 2 indicates invalid data
            violations = (qc_table[check_fields] >= 2).values
            keep_horizons = np.where(violations.any(1), violations.argmax(1), max_horizons)
            in_soils['n_horizons'] = np.minimum(in_soils.n_horizons.values, keep_horizons)

    # Adjust cumulative thickness
    profile = in_soils[['thickness_{}'.format(i + 1) for i in range(max_horizons)]]

    a = in_soils.n_horizons.values
    b = np.arange(max_horizons)
    msk = np.greater.outer(a, b)
    profile_depth = profile.mask(~msk).sum(axis=1)
    in_soils['root_zone_max'] = np.minimum(in_soils.root_zone_max.values, profile_depth)
    if mode == 'pwc':
        # Set values for missing or zero slopes
        aggregation_key = in_soils[['mukey', 'state']]
        in_soils = in_soils.rename(columns={'mukey': 'soil_id'})
    else:
        in_soils = depth_weight_soils(in_soils)
        in_soils, aggregation_key = aggregate_soils(in_soils)
    in_soils = in_soils.astype(fields.data_type(cols=in_soils.columns))

    return in_soils, aggregation_key


if __name__ == "__main__":
    __import__('1_scenarios_and_recipes').main()
