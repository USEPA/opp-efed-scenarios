import urllib.request
import re
import pandas as pd
import os
import numpy as np
import io
from gdd_paths import station_file, station_info_file

# Run variables
overwrite = False

year = 2019
temp_min = 32
temp_max = 212

# All state abbreviations and the region IDs used by the USPEST website
states = [["AL", "SE"],
          ["AK", ""],
          ["AZ", "SW"],
          ["AR", "SC"],
          ["CA", "SW"],
          ["CO", "SW"],
          ["CT", "NE"],
          ["DE", "NE"],
          ["DC", "NE"],
          ["FL", "SE"],
          ["GA", "SE"],
          ["HI", ""],
          ["ID", ""],
          ["IL", "NC"],
          ["IN", "GL"],
          ["IA", "NC"],
          ["KS", "NC"],
          ["KY", "SE"],
          ["ME", "NE"],
          ["MD", "NE"],
          ["MA", "NE"],
          ["MI", "GL"],
          ["MN", "NC"],
          ["MS", "SC"],
          ["MO", "NC"],
          ["MT", ""],
          ["NE", "NC"],
          ["NV", "SW"],
          ["NH", "NE"],
          ["NJ", "NE"],
          ["NM", "SW"],
          ["NY", "NE"],
          ["NC", "SE"],
          ["ND", "NC"],
          ["OH", "GL"],
          ["OK", "SC"],
          ["OR", ""],
          ["PA", "NE"],
          ["RI", "NE"],
          ["SC", "SE"],
          ["SD", "NC"],
          ["TN", "SE"],
          ["TX", "SC"],
          ["UT", "SW"],
          ["VT", "NE"],
          ["VA", "SE"],
          ["WA", ""],
          ["WV", "NE"],
          ["WI", "GL"],
          ["WY", ""]]

gdd_url = "http://uspest.org/cgi-bin/" \
          "ddmodel2.pl?" \
          "wfl={0}{1}.txt" \
          "&uco=1&spp=aaa&cel=0" \
    f"&tlow={temp_min}" \
    f"&thi={temp_max}" \
          "&cal=A" \
          "&stm=1&std=1&styr={1}" \
          "&enm=12&end=31&spyr=0"


def get_stations():
    """ Fetch a list of all the station IDs available on the USPEST website """
    stations = set()
    for state, region in states:
        path = f"http://uspest.org/{region}/{state}/"
        try:
            response = urllib.request.urlopen(path)
            html = response.read()
            results = re.findall('&station=(.+?)"', str(html), flags=0)
            stations |= set(results)
        except urllib.error.HTTPError:
            print(path)
    return pd.Series(sorted(stations), name='stations').to_frame()


def get_station_info(station_ids):
    station_fmt = re.compile(r"Weather station\:?(.+?)Lat\:?(.+?)Long\:?(.+?)Elev\:?(.+?)$", re.MULTILINE)
    n_stations = len(station_ids)
    with open(station_info_file, 'a') as g:
        # g.write("station_id,lat,lon,elev\n")
        for i, station_id in enumerate(station_ids):
            print(f"{i + 1}/{n_stations}")
            url = gdd_url.format(station_id, str(year)[:2])
            response = urllib.request.urlopen(url)
            html = response.read().decode()
            match = re.search(station_fmt, html)
            if match:
                station_info = [station_id] + [n.strip() for n in match.groups()]
                g.write(",".join(station_info) + "\n")
                g.flush()
            else:
                print("Station info not available")


def make_station_table():
    # Get all station IDs
    if overwrite or not os.path.exists(station_file):
        stations = get_stations()
        stations.to_csv(station_file, index=None)
    else:
        stations = pd.read_csv(station_file)

    # Get station parameters
    get_station_info(sorted(stations.stations.values))


make_station_table()
