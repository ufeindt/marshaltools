#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# class to store all sources belonging to a program in the Growth marhsall.
#


import requests, json, os
import numpy as np
from astropy.table import Table
from astropy.time import Time
import astropy.units as u
import concurrent.futures

import logging
logging.basicConfig(level = logging.INFO)

from marshaltools import BaseTable
from marshaltools import MarshalLightcurve
from marshaltools import SurveyFields, ZTFFields
from marshaltools.gci_utils import growthcgi, query_scanning_page
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
        self.candidates = []            # candidates sources from the scanning page


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


    def query_candidate_page(self, start_date=None, end_date=None, showsaved="selected"):
        """
            query scanning page for sources ingested in a given time range.
        """
        
        if start_date is None:
            start_date = "2018-03-01 00:00:00"
        if end_date is None:
            end_date   = Time.now().datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        return query_scanning_page(
                                    start_date, 
                                    end_date,
                                    program_name=self.program,
                                    showsaved=showsaved,
                                    auth=(self.user, self.passwd),
                                    logger=self.logger)


    def get_candidates(self, trange=None, tstep=5*u.day, nworkers=12):
        """
            download the list fo the sources in the scanning page of this program.
            
            Parameters:
            -----------
                
                
                trange: `list` or `tuple` or None
                    time constraints for the query in the form of (start_date, end_date). The
                    two elements of tis list can be either strings or astropy.time.Time objects.
                    
                    if None, all the sources in the scanning page are retrieved slicing the 
                    query in smaller time steps. Since the marhsall returns at max 200 candidates
                    per query, if tis limit is reached, the time range of the query is 
                    subdivided iteratively.
                
                tstep: `astropy.quantity`
                    time step to use to splice the query.
                
                iteration: `bool`
                    distinguis subcalls from the parent 'get_all' one.
        """
        
        
        # parse time limts
        if not trange is None:
            start_date = trange[0]
            end_date   = trange[1]
        else:
            start_date = "2018-03-01 00:00:00"
            end_date   = Time.now().datetime.strftime("%Y-%m-%d %H:%M:%S")
        
        # subdivide the query in time steps
        start, end = Time(start_date), Time(end_date)
        times = np.arange(start, end, tstep).tolist()
        times.append(end)
        self.logger.info("Getting scanning page transient for program %s between %s and %s using dt: %.2f h"%
                (self.program, start_date, end_date, tstep.to('hour').value))
        
        # create list of time bounds
        tlims = [ [times[it-1], times[it]] for it in range(1, len(times))]
        
        # wrap function for multiprocessing
        def download_candidates(tlim):
            candids = self.query_candidate_page(tlim[0], tlim[1])
            return candids

        self.candidates = []
        failed_tlims = []
        with concurrent.futures.ThreadPoolExecutor(max_workers = nworkers) as executor:
            jobs = {
                executor.submit(download_candidates, tlim): tlim for tlim in tlims}
            for job in concurrent.futures.as_completed(jobs):
                tlim = jobs[job]
                try:
                    candids = job.result()
                    self.candidates+=candids
                    self.logger.debug("query from %s to %s returned %d candidates. Total: %d"%
                        (tlim[0].iso, tlim[1].iso, len(candids), len(self.candidates)))
                except Exception as e:
                    self.logger.error("query from %s to %s generated an exception"%
                        (tlim[0].iso, tlim[1].iso))
                    self.logger(e)
                    failed_tlims.append(tlim)
        self.logger.info("download of candidates complete.")
        if len(failed_tlims)>0:
            self.logger.error("query for the following time interavals failed:")
            for tl in failed_tlims: self.logger.errors("%s %s"%(tl[0].iso, tl[1].iso))
        return self.candidates


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
