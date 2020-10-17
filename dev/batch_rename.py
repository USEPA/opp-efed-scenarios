import os
import re
from shutil import copyfile

"""
Naming:
I think Folders should be named with the relevant ranges to minimize thinking, The user can directly see what folder they should be using:
Folder Names =  “Koc<100”,   “Koc = 100 to 3000,”  “Koc >3000”

Scenario Names:
Template =  Crop_Region_KocIndicator

Use letter for koc indicator (important for tracking and other reasons as described below):

Example: Corn_2_A

Scenario selection can be done by sorting if you want crop specific scenarios . but sorting is not usually what I do.

I almost always use wildcard selection in Window File Browsers, so selections can be done like:

If you want all corn enter:  corn*.*

If you want all region = 2, enter *2*.*

If you want all corn in region 3, enter corn_3*.*

In this regard, its important to indicate the koc with a letter so as not to confuse the selection.
"""

root_dir = r"G:\Branch and IO Info\EISB\Scenarios\orchard pwc scenarios"
new_dir = r"G:\Branch and IO Info\EISB\Scenarios\NewScenarioFiles\{}\{}-r{}-{}.scn"  # koc_dir, crop, region, koc

# Corn-koc10-r01-acute.scn2
pattern = re.compile("([\.\d]{2,5})_(\d{2,5})_(.{2,3})_(.+?)\.scn2")

koc_rename = {'10': 'A', '1000': 'B', '10000': 'C'}

koc_dir_rename = {'10': 'Koc under 100', '1000': 'Koc 100 to 3000', '10000': 'Koc over 3000'}

crop_rename = \
    {"70": "Vegetables market",
     "130": "Sugarcane",
     "140": "Small fruit trellis",
     "200": "Orchard deciduous",
     "200.0": "Orchard deciduous"}

for a, _, c in os.walk(root_dir):
    for f in c:
        old_path = os.path.join(a, f)
        match = re.match(pattern, f)
        if match:
            crop, koc, region, duration = match.groups()
            if duration == 'cancer':
                try:
                    new_path = new_dir.format(koc_dir_rename[koc], crop_rename[crop], region, koc_rename[koc])
                except KeyError:
                    print(crop, koc)
                    continue
                if not os.path.exists(new_path):
                    nd = os.path.dirname(new_path)
                    if not os.path.exists(nd):
                        os.makedirs(nd)
                    print(new_path)
                    copyfile(old_path, new_path)
        else:
            print(f"fail {f}")
            continue
