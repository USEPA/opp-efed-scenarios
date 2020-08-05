import pandas as pd

from gdd_paths import freeze_dates_path, crosswalk_path, test_table


def read_freeze_dates():
    freeze_cols = ['coop_id', 'div', 'ifrzsn', 'iprob_5']
    freeze_dates = pd.read_csv(freeze_dates_path)
    freeze_dates = freeze_dates[freeze_dates.ifrzth == 32][freeze_cols] \
        .pivot(index='coop_id', columns='ifrzsn', values='iprob_5')
    freeze_dates.columns = ['spring', 'fall', 'season']
    for col in ('spring', 'fall'):
        freeze_dates[col] = \
            pd.to_datetime(freeze_dates[col].astype(str).str.zfill(4), format='%m%d', errors='coerce') \
                .dt.strftime("%d-%b")
    return freeze_dates


freeze_dates = read_freeze_dates()
crosswalk = pd.read_csv(crosswalk_path).merge(freeze_dates, on='coop_id', how='right')
test_table = pd.read_csv(test_table)
comparison = crosswalk.merge(test_table, on='stationID', how='left')
comparison.to_csv("freeze_dates_qaqc.csv")
