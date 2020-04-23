"""
generate_hydro_files.py

Generate Navigator files, flow files, and lake files, which are used as inputs for the SAM model. These files
are created from the National Hydrography Dataset Plus (NHD Plus).
"""
# Import builtin and standard libraries
import os
import numpy as np
import pandas as pd

# Import local modules and functions
import read
import write
import modify
from utilities import report, fields


def extract_lakes(nhd_table):
    """
    Create a separate table of static waterbodies from master NHD table
    :param nhd_table: Input NHD table (df)
    :return: Table of parameters indexed to waterbodies (df)
    """
    # Get a table of all lentic reaches, with the COMID of the reach and waterbody
    nhd_table = nhd_table[["comid", "wb_comid", "hydroseq", "q_ma"]].rename(columns={'q_ma': 'flow'})

    """ Identify the outlet reach corresponding to each reservoir """
    # Filter the reach table down to only outlet reaches by getting the minimum hydroseq for each wb_comid
    nhd_table = nhd_table.sort_values("hydroseq").groupby("wb_comid", as_index=False).first()
    nhd_table = nhd_table.rename(columns={'comid': 'outlet_comid'})
    del nhd_table['hydroseq']

    # Read and reformat volume table
    volume_table = read.lake_volumes()

    # Join reservoir table with volumes
    nhd_table = nhd_table.merge(volume_table, on="wb_comid")
    nhd_table['residence_time'] = nhd_table['volume'] / nhd_table.flow

    return nhd_table


def extract_flows(nhd_table):
    """
    Extract modeled flows from master NHD table
    :param nhd_table: Input NHD data table (df)
    :return: Table of modeled flows from NHD (df)
    """
    fields.refresh()
    fields.expand('monthly')
    return nhd_table[fields.fetch('flow_file')]


class NavigatorBuilder(object):
    def __init__(self, nhd_table):
        """
        Initializes the creation of a Navigator object, which is used for rapid
        delineation of watersheds using NHD Plus catchments.
        :param nhd_table: Table of stream reach parameters from NHD Plus (df)
        """
        report("Unpacking NHD...", 2)
        nodes, times, dists, outlets, self.conversion = self.unpack_nhd(nhd_table)

        report("Tracing upstream...", 2)
        # paths, times = self.upstream_trace(nodes, outlets, times)
        # TODO - add clean capability to cumulatively trace any attribute (e.g time, distance)
        paths, times, dists = self.rapid_trace(nodes, outlets, times, dists, self.conversion)

        report("Mapping paths...", 2)
        self.path_map = self.map_paths(paths)

        report("Collapsing array...", 2)
        self.paths, self.times, self.length, self.start_cols = \
            self.collapse_array(paths, times, dists)

    @staticmethod
    def unpack_nhd(nhd_table):
        """
        Extract nodes, times, distances, and outlets from NHD table
        :param nhd_table: Table of NHD Plus parameters (df)
        :return: Modified NHD table with selected fields
        """
        # Extract nodes and travel times
        nodes = nhd_table[['tocomid', 'comid']]
        times = nhd_table['travel_time'].values
        dists = nhd_table['length'].values

        convert = pd.Series(np.arange(nhd_table.comid.size), index=nhd_table.comid.values)
        nodes = nodes.apply(lambda row: row.map(convert)).fillna(-1).astype(np.int32)

        # Extract outlets from aliased nodes
        outlets = nodes.comid[nhd_table.outlet == 1].values

        # Create a lookup key to convert aliases back to comids
        conversion_array = convert.sort_values().index.values

        # Return nodes, travel times, outlets, and conversion
        return nodes.values, times, dists, outlets, conversion_array

    @staticmethod
    def map_paths(paths):
        """
        Get the starting row and column for each path in the path array
        :param paths: Path array (np.array)
        :return:
        """

        column_numbers = np.tile(np.arange(paths.shape[1]) + 1, (paths.shape[0], 1)) * (paths > 0)
        path_begins = np.argmax(column_numbers > 0, axis=1)
        max_reach = np.max(paths)
        path_map = np.zeros((max_reach + 1, 3))
        n_paths = paths.shape[0]
        for i, path in enumerate(paths):
            for j, val in enumerate(path):
                if val:
                    if i == n_paths:
                        end_row = 0
                    else:
                        next_row = (path_begins[i + 1:] <= j)
                        if next_row.any():
                            end_row = np.argmax(next_row)
                        else:
                            end_row = n_paths - i - 1
                    values = np.array([i, i + end_row + 1, j])
                    path_map[val] = values

        return path_map

    @staticmethod
    def collapse_array(paths, times, lengths):
        """
        Reduce the size of input arrays by truncating at the path length
        :param paths: Array with node IDs (np.array)
        :param times: Array with reach travel times (np.array)
        :param lengths: Array with reach lengths (np.array)
        :return:
        """
        out_paths = []
        out_times = []
        out_lengths = []
        path_starts = []
        for i, row in enumerate(paths):
            active_path = (row > 0)
            path_starts.append(np.argmax(active_path))
            out_paths.append(row[active_path])
            out_times.append(times[i][active_path])
            out_lengths.append(lengths[i][active_path])
        return map(np.array, (out_paths, out_times, out_lengths, path_starts))

    @staticmethod
    def rapid_trace(nodes, outlets, times, dists, conversion, max_length=3000, max_paths=500000):
        """
        Trace upstream through the NHD Plus hydrography network and record paths,
        times, and lengths of traversals.
        :param nodes: Array of to-from node pairs (np.array)
        :param outlets: Array of outlet nodes (np.array)
        :param times: Array of travel times corresponding to nodes (np.array)
        :param dists: Array of flow lengths corresponding to nodes (np.array)
        :param conversion: Array to interpret node aliases (np.array)
        :param max_length: Maximum length of flow path (int)
        :param max_paths: Maximum number of flow paths (int)
        :return:
        """
        # Output arrays
        all_paths = np.zeros((max_paths, max_length), dtype=np.int32)
        all_times = np.zeros((max_paths, max_length), dtype=np.float32)
        all_dists = np.zeros((max_paths, max_length), dtype=np.float32)

        # Bounds
        path_cursor = 0
        longest_path = 0

        progress = 0  # Master counter, counts how many reaches have been processed
        already = set()  # This is diagnostic - the traversal shouldn't hit the same reach more than once

        # Iterate through each outlet
        for i in np.arange(outlets.size):
            start_node = outlets[i]

            # Reset everything except the master. Trace is done separately for each outlet
            queue = np.zeros((nodes.shape[0], 2), dtype=np.int32)
            active_reach = np.zeros(max_length, dtype=np.int32)
            active_times = np.zeros(max_length, dtype=np.float32)
            active_dists = np.zeros(max_length, dtype=np.float32)

            # Cursors
            start_cursor = 0
            queue_cursor = 0
            active_reach_cursor = 0
            active_node = start_node

            # Traverse upstream from the outlet.
            while True:
                # Report progress
                progress += 1
                if not progress % 10000:
                    report(progress, 3)
                upstream = nodes[nodes[:, 0] == active_node]

                # Check to make sure active node hasn't already been passed
                l1 = len(already)
                already.add(conversion[active_node])
                if len(already) == l1:
                    report("Loop at reach {}".format(conversion[active_node]))
                    exit()

                # Add the active node and time to the active path arrays
                active_reach[active_reach_cursor] = active_node
                active_times[active_reach_cursor] = times[active_node]
                active_dists[active_reach_cursor] = dists[active_node]

                # Advance the cursor and determine if a longest path has been set
                active_reach_cursor += 1
                if active_reach_cursor > longest_path:
                    longest_path = active_reach_cursor

                # If there is another reach upstream, continue to advance upstream
                if upstream.size:
                    active_node = upstream[0][1]
                    for j in range(1, upstream.shape[0]):
                        queue[queue_cursor] = upstream[j]
                        queue_cursor += 1

                # If not, write the active path arrays into the output matrices
                else:
                    all_paths[path_cursor, start_cursor:] = active_reach[start_cursor:]
                    all_times[path_cursor] = np.cumsum(active_times) * (all_paths[path_cursor] > 0)
                    all_dists[path_cursor] = np.cumsum(active_dists) * (all_paths[path_cursor] > 0)
                    queue_cursor -= 1
                    path_cursor += 1
                    last_node, active_node = queue[queue_cursor]
                    if last_node == 0 and active_node == 0:
                        break
                    for j in range(active_reach.size):
                        if active_reach[j] == last_node:
                            active_reach_cursor = j + 1
                            break
                    start_cursor = active_reach_cursor
                    active_reach[active_reach_cursor:] = 0.
                    active_times[active_reach_cursor:] = 0.
                    active_dists[active_reach_cursor:] = 0.

        return all_paths[:path_cursor, :longest_path], \
               all_times[:path_cursor, :longest_path], \
               all_dists[:path_cursor, :longest_path]


def main():
    from parameters import nhd_regions

    for region in nhd_regions:
        report(f"Generating hydro files for Region {region}", 1)
        report("Reading NHD...", 2)

        # Read and modify NHD Plus tabular data
        nhd_table = read.nhd(region)
        nhd_table = modify.nhd(nhd_table)

        report("Building navigator...", 2)
        # Build Navigator object and write to file
        nav = NavigatorBuilder(nhd_table)
        write.navigator(region, nav)

        report("Building flow file...", 2)
        # Extract flow data from NHD and write to file
        flows = extract_flows(nhd_table)
        write.flow_file(flows, region)

        report("Building lake file...", 2)
        # Extract lake data from NHD and write to file
        lakes = extract_lakes(nhd_table)
        write.lake_file(lakes, region)


main()
