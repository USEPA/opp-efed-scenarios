import pandas as pd
import os

root = r"E:\opp-efed-data\scenarios\Production\RERUNS\region18"

tables = []
for f in os.listdir(root):
    print(f)
    table = pd.read_csv(os.path.join(root, f))
    tables.append(table)
out_table = pd.concat(tables, axis=1)
out_table.to_csv(r"E:\opp-efed-data\scenarios\Production\Concatenated111920\all_region18.csv", index=None)