import os
import pandas as pd
import numpy as np
from shutil import copyfile


# Set paths and create output directory
overwrite = True
scenario_dir = r"G:\Branch and IO Info\EISB\Scenarios\NewScenarioFiles"
in_metfile_dir = r"J:\opp-efed-data\global\NewWeatherFiles"
out_metfile_dir = r"G:\Branch and IO Info\EISB\Scenarios\NewScenarioFiles\metfiles"
metfile_map_path = os.path.join(out_metfile_dir, "metfile_map.csv")
if not os.path.exists(out_metfile_dir):
    os.makedirs(out_metfile_dir)

# Make a map of all the scenario files
if overwrite or not os.path.exists(metfile_map_path):
    scenario_map = [[f, os.path.join(a, f)] for a, _, c in os.walk(scenario_dir) for f in c]
    print(len(scenario_map))
    metfile_map = []
    for i, (f, p) in enumerate(scenario_map):
        print(f"{i + 1, len(scenario_map)}")
        with open(p) as g:
            for _ in range(2):
                metfile = next(g)
            metfile_map.append([f, p, metfile.strip()])
            print(metfile)
    metfile_map = pd.DataFrame(np.array(metfile_map), columns=['f', 'p', 'metfile'])

    # Save the metfile map to file
    metfile_map.to_csv(metfile_map_path, index=None)
else:
    metfile_map = pd.read_csv(metfile_map_path)

# Copy over all the affected metfiles
for metfile in metfile_map.metfile.unique():
    old_metfile = os.path.join(in_metfile_dir, metfile)
    new_metfile = os.path.join(out_metfile_dir, metfile)
    copyfile(old_metfile, new_metfile)