import pandas as pd

df = pd.DataFrame({'a': [1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6],
                   'b': [1, 1, 2, 3, 4, 4, 5, 6, 7, 7, 8, 8],
                   'c': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]})

a = df.sort_values(['a', 'b'])

a['dup'] = a.duplicated(['a', 'b'])

print(a)