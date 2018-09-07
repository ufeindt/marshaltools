#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# class to store all sources belonging to a program in the Growth marhsall.
#


import requests, json, os
import numpy as np
from astropy.table import Table
import logging
logging.basicConfig(level = logging.DEBUG)

from marshaltools import BaseTable
from marshaltools import MarshalLightcurve
from marshaltools import SurveyFields, ZTFFields
from marshaltools.gci_utils import growthcgi


from marshaltools.filters import _DEFAULT_FILTERS


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


    def __init__(self, program, sfd_dir=None, logger=None, **kwargs):
        """
        """
        kwargs = self._load_config_(**kwargs)
        
        self.logger = logger if not logger is None else logging.getLogger(__name__)
        self.program = program
        self.sfd_dir = sfd_dir
        self.filter_dict = kwargs.pop('filter_dict', _DEFAULT_FILTERS)
        
        # look for the corresponding program id
        self.get_programidx()
        self.logger.info("Initialized ProgramList for program %s (ID %d)"%(self.program, self.programidx))
        
        # now load all the saved sources
        self.get_saved_sources()
        self.lightcurves = None


    def _list_programids(self):
        """
        get a list of all the programs the user is member of.
        """
        if not hasattr(self, 'program_list'):
            self.logger.debug("listing accessible programs")
            self.program_list = growthcgi('list_programs.cgi', logger=self.logger, auth=(self.user, self.passwd))


    def get_programidx(self):
        """
            assign the programID to this program
        """
        self.programidx = -1
        self._list_programids()
        for index, program in enumerate(self.program_list):
            if program['name'] == self.program:
                self.programidx = program['programidx']
        if self.programidx == -1:
            raise ValueError('Could not find program "%s". You are member of: %s'%(
                self.program, ', '.join([p['name'] for p in self.program_list])
            ))


    def get_saved_sources(self):
        """
            get all saved sources for this program
        """
        print (self.user, self.passwd)
        
        # execute request 
        s_tmp = growthcgi(
            'list_program_sources.cgi',
            logger=self.logger,
            auth=(self.user, self.passwd),
            data={
                'programidx': self.programidx,
                'getredshift': 1,
                'getclassification': 1,
                }
            )
        
        # now parse the json file into a dictionary of sources
        self.sources = {s_['name']: s_ for s_ in s_tmp}
        
        # assign field and ccd value depending on position
        sf = ZTFFields()
        ra_ = np.array([v_['ra'] for v_ in self.sources.values()])
        dec_ = np.array([v_['dec'] for v_ in self.sources.values()])
        fields_ = sf.coord2field(ra_, dec_)
        for name, f_, c_ in zip(self.sources.keys(), fields_['field'], fields_['ccd']):
            self.sources[name]['fields'] = f_
            self.sources[name]['ccds'] = c_
        self.logger.info("Loaded %d saved sources for program %s."%(len(self.sources), self.program))


    def get_scanning_sources(self):
        """
            download the list fo the sources belonging to this program
        """
        pass
        

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
                redshift=self.sources[name]['redshift'],
                classification=self.sources[name]['classification'],
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


    def check_spec(self, name):
        """Check if spectra for an object are available
        
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
            return 0
        else:
            return 1


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
