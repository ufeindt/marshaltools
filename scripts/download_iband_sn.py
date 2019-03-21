#!/usr/bin/env python
import marshaltools
from marshaltools import ProgramList
import numpy as np
import pandas as pd
import sys

pl = ProgramList('Cosmology')
t = pl.table.to_pandas().set_index('name')

sys.stdout.flush()
iband_data = []
for i, name in enumerate(t.reset_index()['name'][::-1]):
    lc = pl.get_lightcurve(name)
    if len(lc.table.to_pandas().query('filter =="i"')):
        iband_data.append(name)
    if i%10 == 0:
        print('done with {}'.format(i))

lcs = []
for name in iband_data:
    x = pl.get_lightcurve(name).table.to_pandas()
    if 'absmag' not in x.columns:
        x['absmag'] = None
    x['name'] = name
    lcs.append(x) 

lcsdf = pd.concat(lcs)
lcsdf['flux'] = 10.**(-0.4 * lcsdf.magpsf)
lcsdf['flux_err'] = 2.5/np.log(10) *lcsdf.flux * lcsdf.sigmamagpsf
lcsdf['SNR'] = lcsdf.flux / lcsdf.flux_err
lcsdf.set_index('name').to_csv('cosmology_lc_with_iband.csv', index=True)


mytable = []
for name in iband_data:
    data = pl.sources[name]
    mytable.append((name, data['classification'], data['ra'], 
                   data['dec'], data['redshift'], data['creationdate'],
                   data['lastmodified'],
                   data['iauname']))

summary = pd.DataFrame(mytable, columns=('name', 'classification', 'ra', 'dec',
                                         'redshift', 'creationdate',
                                         'lastmodified', 'iauname')).set_index('name')


summary.to_csv('cosmology_summary_with_iband.csv')
