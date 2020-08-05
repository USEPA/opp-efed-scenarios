import pandas as pd
import numpy as np

a = pd.DataFrame({"a": [1, np.nan, 3], "b": [np.nan, 4, 5]})

a['c'] = a.max(axis=1)
print(a)