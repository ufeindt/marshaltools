# -*- coding: utf-8 -*-
"""
"""

import numpy as np
import requests
import json
import re
import os
import warnings

import base64
from Crypto.Cipher import DES

from astropy.table import Table

try:
    import sfdmap
    _HAS_SFDMAP = True
except ImportError:
    _HAS_SFDMAP = False

_DEFAULT_FILTERS = {
    ('P48+ZTF', 'g'): 'p48g',
    ('P48+ZTF', 'r'): 'p48r',
    ('P48+ZTF', 'i'): 'p48i',
}
    
###
### Config functions (modified from ztfquery)
###

_SOURCE = 'ZmV0Y2hsY3M='
_CONFIG_FILE = os.path.join(os.environ.get('HOME'), '.growthmarshal')

def pad(text):
    """ good length password """
    while len(text) % 8 != 0:
        text += ' '
    return text

def encrypt_config():
    """ """
    import getpass
    des = DES.new(base64.b64decode( _SOURCE ), DES.MODE_ECB)
    out = {}
    out['username'] = raw_input('Enter your GROWTH Marshal username: ')
    out['password'] = getpass.getpass()
    fileout = open(_CONFIG_FILE, "wb")
    fileout.write(des.encrypt(pad(json.dumps(out))))
    fileout.close()

def decrypt_config():
    """ """
    des = DES.new(  base64.b64decode( _SOURCE ), DES.MODE_ECB)
    out = json.loads(des.decrypt(open(_CONFIG_FILE, "rb").read()))
    return out['username'], out['password']

if not os.path.exists(_CONFIG_FILE):
    encrypt_config()

###
### Main functions
###
    
class BaseTable(object):
    """
    Virtual class that only contains the config loading method
    """
    def _load_config_(self, **kwargs):
        """
        """
        user = kwargs.pop('user', None)
        passwd = kwargs.pop('passwd', None)
        
        
        if user is None or passwd is None:
            if os.path.exists(_CONFIG_FILE):
                user, pw = decrypt_config()
                self.user = user
                self.passwd = pw
            else:
                raise ValueError('Please provide username and password' +
                                 ' as options "user" and "passwd".')
        else:
            self.user = user
            self.passwd = passwd

        return kwargs
    
class ProgramList(BaseTable):
    """Class to list all sources in one of your science programs in 
    the marshal.

    Arguments:
    program -- name of the science program you are looking for (case-sensitive)

    Options:
    user        -- Marshal username (overrides loading the name from file)
    passwd      -- Marshal password (overrides loading the name from file)
    filter_dict -- dictionary to assign the sncosmo bandpasses to combinations
                   of instrument and filter columns. This is only needed if there 
                   is non-P48 photometry. Keys are tuples of telescope+intrument 
                   and filter, values are the sncosmo bandpass names, 
                   see _DEFAULT_FILTERS for an example.
    """
    def __init__(self, program, sfd_dir=None, **kwargs):
        """
        """
        kwargs = self._load_config_(**kwargs)
        
        self.program = program
        self.sfd_dir = sfd_dir
        self.filter_dict = kwargs.pop('filter_dict', _DEFAULT_FILTERS)
        
        r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/list_programs.cgi', 
                          auth=(self.user, self.passwd))
        programs = json.loads(r.text)

        programidx = -1
        for index, program in enumerate(programs):
            if program['name'] == self.program:
                programidx = program['programidx']

        if programidx == -1:
            raise ValueError('Could not find program "%s". You are member of: %s'%(
                self.program, ', '.join([p['name'] for p in programs])
            ))
        
        r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/list_program_sources.cgi', 
                  auth=(self.user, self.passwd), 
                  data={'programidx': programidx})
        s_tmp = json.loads(r.text)
        self.sources = {}
        for s_ in s_tmp:
            self.sources[s_['name']] = s_
        
        # self.redshift = np.array([s['redshift'] for s in self.sources])
        # self.ra = np.array([s['ra'] for s in self.sources])
        # self.dec = np.array([s['dec'] for s in self.sources])

        self.lightcurves = None
        
    def fetch_all_lightcurves(self):
        """Download all lightcurves that have not been downloaded previously.
        """
        for name in self.sources.keys():
            self.get_lightcurve(name)

    def get_lightcurve(self, name):
        """Download the lightcurve for a source in the program. 
        Other sources will not be downloaded.

        Arguments:
        name -- source name in the GROWTH marshal
        """
        if name not in self.sources.keys():
            raise ValueError('Unknown transient name: %s'%name)

        if self.lightcurves is None:
            self.lightcurves = {}

        if name not in self.lightcurves.keys():
            lc = MarshalLightcurve(
                name, ra=self.sources[name]['ra'], dec=self.sources[name]['dec'],
                filter_dict = self.filter_dict, sfd_dir=self.sfd_dir
            )
            self.lightcurves[name] = lc
        else:
            lc = self.lightcurves[name]

        return lc

    def download_spec(self, name, filename):
        """Download all spectra for a source in the marshal as a tar.gz file
        
        Arguments:
        name     -- source name in the GROWTH marshal
        filename -- filename for saving the archive
        """
        if name not in self.sources.keys():
            raise ValueError('Unknown transient name: %s'%name)

        r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/batch_spec.cgi',
                          stream=True,
                          auth=(self.user, self.passwd), 
                          data={'name': name})
        r.raise_for_status()

        if r.text.startswith('No spectrum'):
            raise ValueError(r.text)
        else:
            with open(filename, 'wb') as handle:
                for block in r.iter_content(1024):
                    handle.write(block)
                    
    def download_all_specs(self, download_path=''):
        """Download all spectra for the science program. 
        (Will not create a file for sources without spectra)

        Options:
        download_path -- directory where to save the archives
        """
        if not os.path.exists(download_path):
            os.makedirs(download_path)
        
        for name in self.sources.keys():
            try:
                self.download_spec(name, os.path.join(download_path, name+'.tar.gz'))
            except ValueError:
                pass
                
    @property
    def table(self):
        """Table of source names, RA and Dec (redshift and classification will be added soon) """
        names = [s['name'] for s in self.sources.values()]
        ra = [s['ra'] for s in self.sources.values()]
        dec = [s['dec'] for s in self.sources.values()]

        return Table(data=[names, ra, dec], names=['name', 'ra', 'dec'])
        
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
    def __init__(self, name, ra=None, dec=None, sfd_dir=None, **kwargs):
        """
        """
        kwargs = self._load_config_(**kwargs)

        self.name = name
        self.sfd_dir = sfd_dir
        self.filter_dict = kwargs.pop('filter_dict', _DEFAULT_FILTERS)
        
        if ra is not None and dec is not None:
            self.ra = ra
            self.dec = dec
            if _HAS_SFDMAP:
                if self.sfd_dir is None:
                    self.dustmap = sfdmap.SFDMap()
                else:
                    self.dustmap = sfdmap.SFDMap(self.sfd_dir)
                self.mwebv = self.dustmap.ebv(ra, dec)
            
        r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/print_lc.cgi',
                          auth=(self.user, self.passwd),
                          data={'name': self.name})
        r = r.text.split('<table border=0 width=850>')[-1]
        r = r.replace(' ', '').replace('\n', '')
        r = '\n'.join(r.split('<br>'))

        self.table_orig = Table.read(r, format='ascii.csv')
        self._remove_duplicates_()

        r = requests.post('http://skipper.caltech.edu:8080/cgi-bin/growth/view_source.cgi',
                  auth=(self.user, self.passwd), 
                  data={'name': self.name})
        # self.classification = (re.findall('150%\">.*<',
        #                                   r.text.replace('\n', ''))[0]
        #                        .split('<')[0].split('>')[-1].strip())
        try:
            self.redshift = float(re.findall('[0-9]\.[0-9]*',
                                             re.findall('z = [0-9\.]*', r.text)[0])[0])
        except IndexError:
            self.redshift = None

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
                f = self.filter_dict[f]
                
                band.append(f)
                zpsys.append('ab')
                mask.append(True)
            else:
                mask.append(False)
            
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
