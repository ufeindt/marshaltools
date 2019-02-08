#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# class representing a lightcurve from a source in the marshall
#

import requests
import numpy as np
from astropy.table import Table
from astropy.io.ascii import InconsistentTableError

from marshaltools import BaseTable
from marshaltools.filters import _DEFAULT_FILTERS
from marshaltools.gci_utils import growthcgi

class MarshalLightcurve(BaseTable):
    """Class for the lightcurve of a single source in the Marshal
    Arguments:  
    name -- source name in the GROWTH marshal
    Options:
    ra          -- right ascension of source in deg
    dec         -- declination of source in deg
    sfd_dir     -- path to SFD dust maps if not set in $SFD_MAP
    user        -- Marshal username (overrides loading the name from file)
    passwd      -- Marshal password (overrides loading the name from file)
    filter_dict -- dictionary to assign the sncosmo bandpasses to combinations
                   of instrument and filter columns. This is only needed if there 
                   is non-P48 photometry. Keys are tuples of telescope+intrument 
                   and filter, values are the sncosmo bandpass names, 
                   see _DEFAULT_FILTERS for an example. 
    """
    def __init__(self, name, ra=None, dec=None, redshift=None, classification=None,
                 mwebv=0., **kwargs):
        """
        """
        kwargs = self._load_config_(**kwargs)

        self.name = name
        self.redshift = redshift
        self.classification = classification
        self.filter_dict = kwargs.pop('filter_dict', _DEFAULT_FILTERS)
        
        if ra is not None and dec is not None:
            self.ra = ra
            self.dec = dec

        self.mwebv = mwebv
        
        # get the light curve into a table
        r_text = growthcgi(
                            'print_lc.cgi',
                            to_json=False,
                            logger=None,
                            auth=(self.user, self.passwd),
                            data={'name': self.name}
                            )
        r = r_text.split('<table border=0 width=850>')[-1]
        r = r.replace(' ', '').replace('\n', '')
        r = '\n'.join(r.split('<br>'))

        # extra commas in text, and unescaped inverted commas from arc sec
        # Wrap in try/except and treat exception 
        try:
            self.table_orig = Table.read(r, format='ascii.csv')
        except InconsistentTableError:
            # do a line by line treatment
            x = r.split('\n')
            # Get number of columns from the header: `numcols`
            headers = x[0]
            numcols = len(headers.split(','))
            y = []
            for i, line in enumerate(x):
                # print('we have {0} columns and {1} rows'.format(numcols, len(x)))
                l = line.split(',')
                # Assume problems are due to more columns
                if len(l) > numcols:
                    # Assume that the problems are due to the
                    # commas in the [-2] position.
                    xx = '; '.join(l[numcols - 2: -1])
                    line = l[:numcols - 2]
                    line.append(xx)
                    line.append(l[-1])
                    l = line
                    line = ','.join(l)
                # y is a list of lines, each line is a string
                # collects lines which were good and (fixed) bad
                y.append(line)
            r = '\n'.join(y)
            # Makes r a text block; like r without the error
            self.table_orig = Table.read(r, format='ascii.csv')
            pass
        self._remove_duplicates_()

        
        
#        r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/print_lc.cgi',    #TODO: use gci_utils, add flag to not parse to json
#                          auth=(self.user, self.passwd),
#                          data={'name': self.name})
#        r = r.text.split('<table border=0 width=850>')[-1]
#        r = r.replace(' ', '').replace('\n', '')
#        r = '\n'.join(r.split('<br>'))

#        self.table_orig = Table.read(r, format='ascii.csv')
#        self._remove_duplicates_()

        # r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/view_source.cgi',
        #           auth=(self.user, self.passwd), 
        #           data={'name': self.name})
        # self.classification = (re.findall('150%\">.*<',
        #                                   r.text.replace('\n', ''))[0]
        #                        .split('<')[0].split('>')[-1].strip())
        # try:
        #     self.redshift = float(re.findall('[0-9]\.[0-9]*',
        #                                      re.findall('z = [0-9\.]*', r.text)[0])[0])
        # except IndexError:
        #     self.redshift = None

    @property
    def table_sncosmo(self):
        """Table of lightcurve data in the format sncosmo requires for fitting 
        """
        t = self.table

        zp = 25.0
        mag, magerr = t['magpsf'], t['sigmamagpsf']
        mjd   = t['jdobs'] - 2400000.5
        flux  = 10.**(-0.4*(mag-zp))
        eflux = flux * 0.4 * np.log(10.) * magerr
        zp = np.zeros(len(flux)) + zp

        mask = []
        zpsys = []
        band = []
        peakmag, peakmjd = 99,0.0
        for n,r in enumerate(t) :
            f = (r['instrument'], r['filter'])
            if r['magpsf'] > 90.: 
                flux[n] = 0.
                eflux[n] = 10**(-0.4*(r['limmag']-zp[n]))/5.

            if f in self.filter_dict.keys():
                band.append(self.filter_dict[f])
                zpsys.append('ab')
                mask.append(True)
            else:
                mask.append(False)

        mask = np.array(mask, dtype=bool)
        out = Table(data=[mjd[mask], band, flux[mask], eflux[mask], zp[mask], zpsys],
                    names=['mjd', 'band', 'flux', 'fluxerr', 'zp', 'zpsys'])
        out.meta['z'] = self.redshift
        if self.mwebv is not None:
            out.meta['mwebv'] = self.mwebv
        
        return out

    def _remove_duplicates_(self):
        """This function removes potential duplicates from the lightcurve,
        i.e. multiple entries for the same JD. If there are different values for the
        magnitude, the detections (mag < 99) are kept. Note that this may sometimes 
        leave two detections with different magnitudes at the same JD. 
        """
        t = self.table_orig
        mask = []
        t_obs = np.unique(t['jdobs'])
        for t_ in t_obs:
            if np.sum(t['jdobs'] == t_) == 1:
                mask.append(True)
            else:
                mags = t['magpsf'][t['jdobs'] == t_]
                if len(np.unique(mags)) == 1:
                    mask.append(True)
                    for k in range(len(mags) - 1):
                        mask.append(False)
                elif np.sum(np.unique(mags) < 90) == 1:
                    done = False
                    for m_ in mags:
                        if m_ < 90. and not done:
                            mask.append(True)
                            done = True
                        else:
                            mask.append(False)
                else:
                    mags_ = np.unique(mags)
                    mags_ = np.array(mags_[mags_ < 90])

                    done = [False for k in range(len(mags_))]
                    for m_ in mags:
                        if m_ < 90.:
                            k = np.where(mags_ == m_)[0][0]
                            if not done[k]:
                                mask.append(True)
                                done[k] = True
                        else:
                            mask.append(False)

        self.table = t[np.array(mask)]
