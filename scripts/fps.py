#!/usr/bin/env python
import pandas as pd
import sys
from argparse import ArgumentParser
import datetime


username = "ztffps"
passwd = 'dontgocrazy!'
email = 'rahul.biswas@fysik.su.se' 

summary_fname = 'cosmology_summary_with_iband.csv'
lightcurve_fname = 'cosmology_lc_with_iband.csv'

summary = pd.read_csv(summary_fname).set_index('name')
lcs = pd.read_csv(lightcurve_fname)

parser = ArgumentParser(description='request fps for ztf for a transient')
parser.add_argument('--name', help='name of the transient', default='ZTF18aaermez')

args = parser.parse_args()

name = args.name
ra = summary.loc[name, 'ra']
dec = summary.loc[name, "dec"]
jdobs_max , mag_peak = lcs.query('name==@name').sort_values(by='magpsf').iloc[0][['jdobs', 'magpsf']]
jdstart = jdobs_max - 30.0
jdend = jdobs_max + 70.0

request = 'wget --http-user={0} --http-passwd={1} -O log_{2}.txt "https://ztfweb.ipac.caltech.edu/cgi-bin/requestForcedPhotometry.cgi?ra={3}&dec={4}&jdstart={5}&jdend={6}&email={7}"'.format(username, passwd, name, ra, dec, jdstart, jdend, email)
print(name, ra, dec, jdstart, jdend, mag_peak)
print('request is \n', request)



import seaborn as sns
import matplotlib.pyplot as plt
sns.set_style('whitegrid')
sns.set_context('talk')

grouped = lcs.query('name == @name and magpsf < 98').groupby('filter')
fig, axx = plt.subplots(1, 3, figsize=(18,6))
ax, ay, az = axx
for band in grouped.groups:
    x = grouped.get_group(band)
    ax.errorbar(x['jdobs'], x['magpsf'], yerr=x['sigmamagpsf'],
                label=band, fmt='.')
    ax.axvline(jdstart, ls='--', color='k')
    ax.axvline(jdend, ls='--', color='k')
    ax.legend(loc='best')
    ax.invert_yaxis()
    ay.errorbar(x['jdobs'], x['flux'], yerr=x['flux_err'],
                label=band, fmt='.')
    ay.axvline(jdstart, ls='--', color='k')
    ay.axvline(jdend, ls='--', color='k')
    ay.legend(loc='best')
    az.scatter(x['jdobs'], x['SNR'],
                label=band)
    az.axvline(jdstart, ls='--', color='k')
    az.axvline(jdend, ls='--', color='k')
    ax.set_xlabel('jdobs')
    ay.set_xlabel('jdobs')
    az.set_xlabel('jdobs')
    ax.set_ylabel('mag')
    ay.set_ylabel('flux')
    az.set_ylabel('snr')



plt.show()
datetime_str = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")
fig.savefig('lcs_{0}_{1}.png'.format(name, datetime_str))
