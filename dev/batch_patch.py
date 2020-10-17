import os
import re
from shutil import copyfile

root_dir = r"G:\Branch and IO Info\EISB\Scenarios\NewScenarioFiles"

for a, _, c in os.walk(root_dir):
    if 'metfiles' not in a:
        for f in c:
            new_f = f + "2"
            old_p = os.path.join(a, f)
            new_p = os.path.join(a, new_f)
            print(old_p)
            print(new_p)
            os.rename(old_p, new_p)
