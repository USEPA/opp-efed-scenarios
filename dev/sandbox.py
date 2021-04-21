import pandas as pd
from paths import gen_params_path


table = pd.read_csv(gen_params_path)
table['region'] = table.region.str.zfill(2)

table.to_csv(gen_params_path, index=None)