import os

import pandas as pd
from pandas.api.types import CategoricalDtype

from stations import StationLocator

DATA_DIR=r'data'
CRUISES=['at46', 'ar66b', 'en687', 'en695', 'hrs2303', 'en706', 'ar77']

dfs = []
for fn in os.listdir(os.path.join(DATA_DIR, 'input')):
    abspath = os.path.join(DATA_DIR, 'input', fn)
    if not abspath.endswith('.xlsx'):
        continue 
    df = pd.read_excel(abspath, dtype=object)
    dfs.append(df)

doc_table = pd.concat(dfs)
doc_table.columns = [c.lower() for c in doc_table.columns]

# replace "HRS2302" with "HRS2303" in cruise column
doc_table['cruise'] = doc_table['cruise'].str.replace('HRS2302', 'HRS2303')

dfs = []
for cruise in CRUISES:
    abspath = os.path.join(DATA_DIR, 'bottle_files', f'{cruise}_ctd_bottles.csv')
    df = pd.read_csv(abspath, dtype=object)
    # strip leading zeros from 'cast' column
    df['cast'] = df['cast'].str.replace(r'^0+','',regex=True)
    dfs.append(df)

bottle_table = pd.concat(dfs)
retain_columns = ['date', 'cruise','cast','niskin','latitude','longitude', 'depsm']
bottle_table = bottle_table[retain_columns]

doc_table = doc_table.astype(str)
bottle_table = bottle_table.astype(str)

doc = doc_table.merge(bottle_table, on=['cruise','cast','niskin'], how='left')

doc.rename(columns={'depsm': 'depth'}, inplace=True)

retain_columns = [
    'cruise', 'sample_type', 'cast', 'niskin', 'replicate', 'date run',
    'filename', 'npoc(um)', 'tn(um)', 'date', 'latitude', 'longitude', 'depth'
]

doc = doc[retain_columns]

doc = doc.rename(columns={
    'date run': 'date_analyzed',
    'npoc(um)': 'doc',
    'tn(um)': 'dtn',
})

doc.latitude = doc.latitude.astype(float)
doc.longitude = doc.longitude.astype(float)

station_locator = StationLocator()
nearest_station = station_locator.nearest_station(doc)

doc = doc.merge(nearest_station, left_index=True, right_index=True)

doc['distance_km'] = [f'{v:.3f}' for v in doc['distance_km']]
doc.rename(columns={'distance_km':'station_distance'}, inplace=True)

doc['doc'] = [f'{v:.1f}' for v in doc['doc'].astype(float)]
doc['dtn'] = [f'{v:.1f}' for v in doc['dtn'].astype('float')]

doc['depth'] = [f'{v:.3f}' for v in doc['depth'].astype(float)]

for col in ['latitude', 'longitude']:
    doc[col] = [f'{v:.4f}' for v in doc[col].astype(float)]

# doc['sample_type'] = doc['sample_type'].str.replace('C','cast')
doc['date'] = doc['date'].str.replace('+00:00','',regex=False)

# remove spurious single quotes from filenames

doc['filename'] = doc['filename'].str.replace(r'\'','',regex=False)

doc['quality_flag'] = 1

column_order = ['cruise', 'cast', 'niskin', 'date', 'latitude', 'longitude', 'depth',
               'sample_type', 'replicate', 'doc', 'dtn', 'quality_flag',
               'nearest_station', 'station_distance',  'date_analyzed', 'filename']

doc = doc[column_order]

cruise_dtype = CategoricalDtype(categories=[c.upper() for c in CRUISES], ordered=True)
doc['cruise'] = doc['cruise'].astype(cruise_dtype)

doc = doc.sort_values(['cruise','date'])

for col in ['date','nearest_station']:
    doc[col].fillna('nan', inplace=True)

doc.to_csv(os.path.join(DATA_DIR, 'output', 'nes-lter-doc-transect.csv'), index=None)