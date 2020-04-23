"""
utilities.py

Classes and functions used by multiple scripts.
"""
# Import builtin and standard libraries
import os
import re
import numpy as np
import pandas as pd

# Import local variables
import write
from paths import fields_and_qc_path


class FieldManager(object):
    """
    The Field Manager loads the table fields_and_qc.csv
    and uses that table to manage fields. Field management functions include: field name
    conversions from raw input data sources to internal field names, extending fields
    that are expansible (such as those linked to soil horizon or month), and performing
    QAQC by comparing the values in a table with the specified QC ranges in fields_and_qc.py
    This class is inherited by other classes which wrap pandas DataFrames.
    """

    def __init__(self):
        """ Initialize a FieldManager object. """
        self.path = fields_and_qc_path
        self.extended = {'monthly': False, 'depth': False, 'horizon': False}
        self.refresh()
        self._convert = None

        self.qc_fields = ['range_min', 'range_max', 'range_flag',
                          'general_min', 'general_max', 'general_flag',
                          'blank_flag', 'fill_value']

    def data_type(self, fetch=None, how='internal', cols=None):
        """
        Give dtypes for fields in table
        :param fetch: Fetch a subset of keys, e.g. 'monthly' (str)
        :param how: 'internal' or 'external'
        :param cols: Only return certain columns (iter of str)
        :return: Dictionary with keys as field names and dtypes as values
        """
        if fetch:
            matrix = self.fetch(fetch, how, False)
        else:
            matrix = self.matrix
        data_types = matrix.set_index(how + "_name").data_type.to_dict()

        if cols is not None:
            data_types = {key: val for key, val in data_types.items() if key in cols}
        return {key: eval(val) for key, val in data_types.items()}

    def expand(self, mode='depth', n_horizons=None):
        """
        Certain fields are repeated during processing - for example, the streamflow (q) field becomes monthly
        flow (q_1, q_2...q_12), and soil parameters linked to soil horizon will have multiple values for a single
        scenario (e.g., sand_1, sand_2, sand_3). This function adds these extended fields to the FieldManager.
        :param mode: 'depth', 'horizon', or 'monthly'
        :param n_horizons: Optional parameter when specifying a number to expand to (int)
        """
        from parameters import depth_bins, erom_months, max_horizons
        if n_horizons is not None:
            max_horizons = n_horizons
        try:
            select_field, numbers = \
                {'depth': ('depth_weight', depth_bins),
                 'horizon': ('horizontal', range(1, max_horizons + 1)),
                 'monthly': ('monthly', erom_months)}[mode]

        except KeyError as e:
            message = "Invalid expansion mode '{}' specified: must be in ('depth', 'horizon', 'monthly')".format(
                mode)
            raise Exception(message) from e

        # Test to make sure it hasn't already been done
        # TODO - clean this up
        if not self.extended[mode]:
            condition = mode + 'extended'
            # Find each row that applies, duplicate, and append to the matrix
            self.matrix[condition] = 0
            burn = self.matrix[condition].copy()
            new_rows = []
            for idx, row in self.matrix[self.matrix[select_field] == 1].iterrows():
                burn.iloc[idx] = 1
                for i in numbers:
                    new_row = row.copy()
                    new_row['internal_name'] = row.internal_name + "_" + str(i)
                    new_row[condition] = 1
                    new_rows.append(new_row)
            new_rows = pd.concat(new_rows, axis=1).T

            # Filter out the old rows and add new ones
            self.matrix = pd.concat([self.matrix[~(burn == 1)], new_rows], axis=0)

            # Record that the duplication has occurred
            self.extended[mode] = True

    def fetch_field(self, col, how, names_only=True, dtypes=False):
        """
        Subset the FieldManager matrix (fields_and_qc.csv) based on the values in a given column, or a field group in
        the 'data_source' or 'source_table' columns. For example, fetch_field('sam_scenario', 'internal_name')
        would return all the fields with a value > 0 in the 'sam_scenario' column of fields_and_qc.csv, and
        fetch_field('SSURGO') would return all the fields with 'SSURGO' in the 'data_source' column'.
        If the numbers are ordered, the returned list of fields will be in the same order. The names_only parameter
        can be turned off to return all other fields (e.g., QAQC fields) from fields_and_qc.csv for the same subset.
        :param col: The column in fields_and_qc.csv used to make the selection (str)
        :param field: The field values to return, usually 'internal_name' or 'external_name'
        :param names_only: Return simply the selected field, or all fields (bool)
        :return: Subset of the field matrix (df)
        """

        def extract_num(field_name):
            match = re.search("(\d{1,2})$", field_name)
            if match:
                return float(match.group(1)) / 100.
            else:
                return 0.

        # Check to see if provided 'col' value is a group in the
        # 'data_source' or 'source_table' columns in fields_and_qc.csv
        out_fields = None
        for column in 'data_source', 'source_table':
            if col in self.matrix[column].values:
                out_fields = self.matrix[self.matrix[column] == col]
                break

        # If 'col' not found, check to see if provided 'col' value corresponds to a column in fields_and_qc.csv
        if out_fields is None:
            if col in self.matrix.columns:
                out_fields = self.matrix[self.matrix[col] > 0]
                if out_fields[col].max() > 1:  # field order is given
                    out_fields.loc[:, 'order'] = out_fields[col] + np.array(
                        [extract_num(f) for f in out_fields[f'{how}_name']])
                    out_fields = out_fields.sort_values('order')
        if out_fields is None:
            report("Unrecognized sub-table '{}'".format(col))
            return None
        if names_only:
            out_fields = out_fields[f'{how}_name'].tolist()
        if dtypes:
            return out_fields, self.data_type(cols=out_fields, how=how)
        else:
            return out_fields

    def fetch(self, item, how='internal', names_only=True, dtypes=False):
        """ Wraps fetch_field """
        # TODO - why is this necessary
        return self.fetch_field(item, how, names_only, dtypes)

    @property
    def convert(self):
        """ Dictionary that can be used to convert 'external' variable names to 'internal' names """
        if self._convert is None:
            self._convert = {row.external_name: row.internal_name for _, row in self.matrix.iterrows()}
        return self._convert

    @property
    def qc_table(self):
        """ Initializes an empty QAQC table with the QAQC fields from fields_and_qc_csv. """

        return self.matrix.set_index('internal_name')[self.qc_fields] \
            .apply(pd.to_numeric, downcast='integer') \
            .dropna(subset=self.qc_fields, how='all')

    def perform_qc(self, other, write_table=False, write_id=None):
        """
        Check the value of all parameters in table against the prescribed QAQC ranges in fields_and_qc.csv.
        There are 3 checks performed: (1) missing data, (2) out-of-range data, and (3) 'general' ranges.
        The result of the check is a copy of the data table with the data replaced with flags. The flag values are
        set in fields_and_qc.csv - generally, a 1 is a warning and a 2 is considered invalid. The outfile parameter
        gives the option of writing the resulting table to a csv file if a path is provided.
        :param other: The table upon which to perform the QAQC check (df)
        :param outfile: Path to output QAQC file (str)
        :return: QAQC table (df)
        """
        # Confine QC table to fields in other table
        active_fields = {field for field in self.qc_table.index.values if field in other.columns.tolist()}
        qc_table = self.qc_table.loc[active_fields]

        # Flag missing data
        # Note - if this fails, check for fields with no flag or fill attributes
        # This can also raise an error if there are duplicate field names in fields_and_qc with qc parametersz
        flags = pd.isnull(other).astype(np.int8)
        duplicates = qc_table.index[qc_table.index.duplicated()]
        if not duplicates.empty:
            raise ValueError(f"Multiple QC ranges specified for {', '.join(duplicates.values)} in fields_and_qc.csv")
        flags = flags.mask(flags > 0, qc_table.blank_flag, axis=1)

        # Flag out-of-range data
        for test in ('general', 'range'):
            ranges = qc_table[[test + "_min", test + "_max", test + "_flag"]].dropna()
            for param, (param_min, param_max, flag) in ranges.iterrows():
                if flag > 0:
                    out_of_range = ~other[param].between(param_min, param_max) * flag
                    flags[param] = np.maximum(flags[param], out_of_range).astype(np.int8)
        qc_table = pd.DataFrame(np.zeros(other.shape, dtype=np.int8), columns=other.columns)
        qc_table[flags.columns] = flags
        if write_table and write_id is not None:
            write.qc_table(qc_table, write_id)

        return qc_table

    @property
    def fill_value(self):
        """ Return the fill values for flagged data set in fields_and_qc.csv """
        return self.matrix.set_index('internal_name').fill_value.dropna()

    def refresh(self):
        """ Reload fields_and_qc.csv, undoes 'extend' and other modifications """
        # Read the fields/QC matrix
        if self.path is not None:
            self.matrix = pd.read_csv(self.path)
        for condition in self.extended.keys():
            self.extended[condition] = False

    def table_map(self, table):
        """ Returns table name and internal/external field variables for a data source (e.g. NHD)"""
        tables = self.matrix[self.matrix.data_source == table][
            ['data_source', 'source_table', 'internal_name', 'external_name']]
        out_map = []
        for table, others in tables.groupby('source_table'):
            out_map.append([str(table), others.internal_name.values, others.external_name.values])
        return out_map


def report(message, tabs=0):
    """ Display a message with a specified indentation """
    tabs = "\t" * tabs
    print(tabs + str(message))


# Initialize field matrix
fields = FieldManager()
postprocessor_fields = FieldManager()
