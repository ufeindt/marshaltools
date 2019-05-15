#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# class to store all sources belonging to a program in the Growth marhsall.
#


import requests, json, os, time
import numpy as np
from astropy.table import Table
from astropy.time import Time
import astropy.units as u
import concurrent.futures

import logging
logging.basicConfig(level = logging.INFO)

from .BaseTable import BaseTable
from .MarshalLightcurve import MarshalLightcurve
from .surveyfields import SurveyFields, ZTFFields
from .gci_utils import (growthcgi, query_scanning_page, ingest_candidates,
                        SCIENCEPROGRAM_IDS, query_marshal_timeslice, get_saved_sources)
from .filters import _DEFAULT_FILTERS

try:
    import sfdmap
    _HAS_SFDMAP = True
except ImportError:
    _HAS_SFDMAP = False

def retrieve(in_dict, key, default=None):
    """
        modified dict.get method that allows to traverse
        nested dictionaries using a dotted notation and supports
        going though lists as well.
        
        Parameters:
        -----------
        
        in_dict: `dict`
            possibly complex dictionary
        
        key: `str`
            what to look for
        
        default:
            what to return if the key is not found

        Eg:

        dd = {
            'a': 1,
            'b': {
                'x': 10,
                'y': 11, 
                'z': {'l': 100, 'm': 101}
                },
            'z': [{'e': 200, 'f': 201}, {'e': 201, 'f': 202}],
            'c': 3
            }

        retrieve(dd, a) = 1
        retrieve(dd, 'b.y') = 11
        retrieve(dd, 'b.w', 'fuffa') = 'fuffa'
        retrieve(dd, 'b.z.m') = 101
        retrieve(dd, 'z.e') = [200, 201]
    """
    
    if not '.' in key:
        return in_dict.get(key, default)
    
    for k in key.split('.'):
        out = in_dict.get(k, default)
        klist = key.split('.')
        klist.remove(k)
        new_key = ".".join(klist)
        if type(out) == dict:
            return retrieve(out, new_key, default)
        elif type(out) in [tuple, list]:
            return [retrieve(x, new_key, default) for x in out]
        elif out == default:
            return default


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

    def __init__(self, program, load_sources=True, load_candidates=False, sfd_dir=None, logger=None, timeout=300, **kwargs):
        """
        """
        kwargs = self._load_config_(**kwargs)
        
        self.logger = logger if not logger is None else logging.getLogger(__name__)
        self.program = program
        self.sfd_dir = sfd_dir
        self.filter_dict = kwargs.pop('filter_dict', _DEFAULT_FILTERS)
        
        # set the timeout for the connection with the marshal
        self.timeout = timeout
        
        # look for the corresponding program id
        self.get_programidx()
        self.logger.info("Initialized ProgramList for program %s (ID %d)"%(self.program, self.programidx))
        self._dustmap = None
        
        # this is the general, 'program wide' ID used to query for candidates.
        # stars with a None, and it will be read from marshaltools.gci_utils.SCIENCEPROGRAM_IDS
        # use set_querycandidates_programid method to change its value
        self.science_program_id = None
        
        # now load all the saved sources
        if load_sources:
            self.get_saved_sources()
        if load_candidates:
            self.get_candidates()
        self.lightcurves = None


    def set_querycandidates_programid(self, science_program_id):
        """
            apparently the program ID you use to ingest/save candidates 
            is different from the one you use to query the candidate page.
            In particular, the first one seems to be 'user specific', while
            the second one is 'program specific'. This is, at least, what I 
            have understood for now.
            
            Some of these 'program specific' IDs are listed in marshaltools.gci_utils.SCIENCEPROGRAM_IDS.
            If your channel is not there, you can use this function to set 
            it on the fly.
            
            Parameters:
            -----------
            
                science_program_id: `int`
                    'program-specific' ID, used, e.g. when querying for candidates.
        """
        
        self.science_program_id = science_program_id
        self.logger.debug("setting SCIENCE program ID for program %s to %d"%
            (self.program, self.science_program_id))


    def ingest_avro(self, avro_id, be_anal=True, max_attempts=3):
        """
            ingest alert(s) into the marshall.
            
            Paramaters:
            -----------
                
                avro_id: `str` or `list`
                    ids of candidates to be ingested.
                
                be_anal: `bool`
                    if True after ingestion we'll look for recently ingested candidates
                    and verify which alert has failed and which has not.
                
                max_attempts: `int`
                    if be_anal is True, we'll try repeating ingestion max_attempts times
                    for the alerts that failed.
            
            Returns:
            --------
                
                list of avro_ids that failed to be ingested (None if you're not so anal about it)
        """
        return ingest_candidates(
            avro_ids = avro_id,
            program_name = self.program,
            program_id = self.programidx,
            be_anal = be_anal,
            query_program_id = self.science_program_id,
            max_attempts = max_attempts,
            auth=(self.user, self.passwd), 
            logger=self.logger,
            timeout=self.timeout
            )

    def save_sources(self, candidate, programidx=None, save_by='name', max_attempts=3, be_anal=True):
        """
            save given source(s) either specifying the name of the id.
            
            Parameters:
            -----------
                
                candidate: `str` or `list`
                    ID or name of candidate(s) to be saved, depending on the value
                    of save_from.
                
                save_by: `str`
                    either 'name' or 'id', specify if the candidate param
                    contains the id or the name of the candidate.
                
                programidx: `int`
                    ID of program to which the source(s) sould be saved. 
                
                be_anal: `bool`
                    if True after ingestion we'll look for recently ingested candidates
                    and verify which alert has failed and which has not.
                
                max_attempts: `int`
                    if be_anal is True, we'll try repeating ingestion max_attempts times
                    for the alerts that failed.
        """
        
        # if you don't pass the programID go read it from the static list of program-names & ids.
        if programidx is None:
            if self.science_program_id is None:
                programidx = SCIENCEPROGRAM_IDS.get(self.program, -666)
                if programidx == -666:
                    raise KeyError("program %s is not listed in `marshaltools.gci_utils.SCIENCEPROGRAM_IDS`. Go and add it yourself!")
                self.logger.debug("reading programid from `marshaltools.gci_utils.SCIENCEPROGRAM_IDS`")
            else:
                programidx = self.science_program_id
        self.logger.info("programid for saving candidates for program %s: %d"%(self.program, programidx))
        
        # parse save_by to cgi acceptable key
        if save_by == 'name':
            cgi_key = 'candname'
        elif save_by == 'id':
            cgi_key = 'candid'
        else:
            raise KeyError("save_by parameter should be either 'name' or 'id'. Got %s instead"%
                (save_by))
        
        # see if you want to ingest one or more candidates
        if type(candidate) in [str, int]:
            to_save = [candidate]
        else:
            to_save = [str(cc) for cc in candidate]
        self.logger.info("Saving %d candidate(s) into program %s"%(len(to_save), programidx))
        # save all the candidates, eventually veryfying and retrying
        n_attempts, failed = 0, []
        while len(to_save)>0 and n_attempts < max_attempts:
            
            n_attempts+=1
            self.logger.debug("attempt number %d of %d."%(n_attempts, max_attempts))
            
            # ingest them
            for cand in to_save:
                status = growthcgi(
                    'save_cand_growth.cgi',
                    logger=self.logger,
                    to_json=False,
                    auth=(self.user, self.passwd),
                    data={'program': programidx, cgi_key: cand},
                    timeout=self.timeout
                    )
                self.logger.debug("Ingesting candidate %s returned %s"%(cand, status))
            self.logger.info("Attempt %d: done ingesting candidates."%n_attempts)
            
            # if you take life easy then it's your problem. We'll exit the loop
            if not be_anal:
                return None
            
            # if you want to be anal about that look that all the candidates have been saved
            self.logger.info("verifying thet all candidates are saved")
            done, failed = [], []   # here overwite global one
            
            # refresh saved source and look for the ones you just saved
            self.get_saved_sources()
            saved_ids = self.sources.keys()
            if save_by == 'candid':
                saved_ids = [str(src['candid']) for _, src in self.sources.items()]
            
            # see what's there and what's missing
            for cand in to_save:
                if cand in saved_ids:
                    done.append(cand)
                else:
                    failed.append(cand)
            self.logger.info("attempt # %d. Of the desired candidates %d successfully saved, %d failed"%
                (n_attempts, len(done), len(failed)))
            
            # remember what is still to be done
            to_save = failed
        
        # return the list of ids that failed consistently after all the attempts
        return failed

    def save_source_name(self, candname, programidx=None):
        """
            save given source. DEPRECATED: use save_sources!!
        """
        if programidx is None:
            programidx = self.programidx
        self.logger.info("Saving source %s into program %s"%(candname, programidx))
        growthcgi(
            'save_cand_growth.cgi',
            logger=self.logger,
            to_json=False,
            auth=(self.user, self.passwd),
            data={'program': programidx, 'candname': candname},
            timeout=self.timeout
            )

    def _list_programids(self):
        """
        get a list of all the programs the user is member of.
        """
        if not hasattr(self, 'program_list'):
            self.logger.debug("listing accessible programs")
            self.program_list = growthcgi(
                'list_programs.cgi', logger=self.logger, auth=(self.user, self.passwd), timeout=self.timeout)

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
                self.program, ', '.join([p['name'] for p in self.program_list])))

    def get_saved_sources(self, trange=None, tstep=15*u.day, nworkers=12, max_attemps=2, raise_on_fail=False):
        """
            get all saved sources for this program. Optionally provide
            a (startdate, stopdate) list for trange to limt the query in
            time.
            
            see docs of ProgramList.get_candidates for explanation of the arguments.
        """
        
        # execute the request. For crowded programs this can time out
        # so we catch the exception and use time slices to get the 
        # sources.
        try:
            s_tmp = get_saved_sources(
                                        program_id=self.programidx, 
                                        trange=trange, 
                                        auth=(self.user, self.passwd), 
                                        logger=self.logger,
                                        timeout=self.timeout,
                                )
        except requests.exceptions.ReadTimeout as exc:
            self.logger.warning(
                "list_program_sources.cgi timed out. Exception: %s"%str(exc))
            self.logger.warning("Splitting query in time slices.")
            
            # create a query function
            def query_saved_sources(tstart, tstop):
                return get_saved_sources(
                                        program_id=self.programidx, 
                                        trange=(tstart, tstop), 
                                        auth=(self.user, self.passwd), 
                                        logger=self.logger,
                                        timeout=5*self.timeout,
                                )
            
            # run the query with parallel time slices
            s_tmp = query_marshal_timeslice(
                                    query_saved_sources, 
                                    trange=trange, 
                                    tstep=tstep, 
                                    nworkers=nworkers, 
                                    max_attemps=max_attemps, 
                                    raise_on_fail=raise_on_fail, 
                                    logger=None
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

    def get_source(self, name):
        """
            return desired source from the saved ones
        """
        
        if not hasattr(self, 'sources'):
            raise RuntimeError("no sources loaded for program %s."%self.program)
        src = self.sources.get(name)
        if src is None:
             self.logger.debug("can't find source %s in the saved sources of program %s"%
                (name, self.program))
        return src

    def find_source(self, name, include_candidates=True):
        """
            return the desired source from the sources belonging to this
            program. If not found in the saved sources, it will look among
            the candidates from the scanning page
        """
        src = self.sources.get(name)
        if src is None:
            self.logger.debug("can't find source named %s among saved sources of program %s"%
                (name, self.program))
            if include_candidates and hasattr(self, 'candidates'):
                src = self.candidates.get(name)
                if src is None:
                    self.logger.debug("can't find it among candidates either.")
                else:
                    self.logger.debug("found source %s among candidates of program %s"%
                        (name, self.program))
        else:
            self.logger.debug("found source %s among saved sources of program %s"%
                (name, self.program))
        return src

    def retrieve_from_src(self, name, keys, default=None, src_dict=None, append_summary=True, include_candidates=True):
        """
            read the desired key(s) for the requested source. Use retrieve to 
            support dotted notations to traverse nested dictionaries. 
            
            e.g. key=='annotations.username' returns a list of all the usernames
            found in the annotation list of dictionaries of the summary.
            
            See docstring of retrieve function at the top of this module.
            
            to be compatible with the candidates as well, one can use the src parameter
            to pass a 'source-like' dictionary to the function. This will overwrite the name.
        """
        
        # get the source (if no dictionary has been given, look for it)
        if src_dict is None:
            src = self.find_source(name, include_candidates)
        else:
            src = src_dict
        if src is None:
            return default
        
        # keys can be a list
        if type(keys) is str:
            keys = [keys]
        
        # loop trough the keys and get their values
        out = {}
        for k in keys:
            
            # first look into the source. Use silly default to distinguish from not found
            # if you don't find it, look in the summary (download if not there yet)
            val = retrieve(src, k, 666)
            if val == 666:
                summary = self.source_summary(name, append=append_summary)
                
                # check for no summary on the marhsall, unknown source, or missing 'id' key
                if summary == {} or summary is None:
                    val = default
                else:
                    val = retrieve(summary, k, 666)
                
            # output warning
            if val == 666:
                self.logger.warning(
                    "cannot find key %s in source dictionary or in it's summary. Available keys are %s"%
                    (k, repr(summary.keys())))
                val = default
            out[k] = val
        
        # if you requested just one key, just return the corresponding item
        if len(out) == 1:
            out = out.get(keys[0])
        return out

    def read_comments(self, name, comment_id=None, comment_type=None, 
        comment_text=None, comment_author='self', refresh=True):
        """
            read the comments for the given source, eventaully selecting them based on 
            type, author, content, or id.
            
            Parameters:
            -----------
            
                name: `str`
                    ZTF name of source.
                
                comment_type: `str`
                    type of comment. Allowed values are: ['info', 'phase', 'redshift', 'comment', 'classification']
                
                comment_id: `int`
                    ID of the comment. If None, don't filter on this field.
                
                comment_text: `str`
                    body of the comment message. If None, don't filter on this field.
                
                comment_auth: `str`
                    username of comment author, default 'self'==self.user. If None, don't filter
                    on this field.
                
                refresh: `bool`
                    if True, the source summary is refreshed posting to the marshall
                    prior to search in the comments
            
            Returns:
            --------
                list of comments (dicts)
        """
        
        self.logger.debug("reading comments for source: %s"%name)
        if refresh:
            self.source_summary(name, append=True, refresh=True)
        
        # replace author
        author = self.user if comment_author == 'self' else comment_author
        
        # read the comments
        comments = self.retrieve_from_src(name, keys=["annotations"])
        found_cmts = []
        if len(comments) > 0:
            self.logger.debug("found %d previous comments matching the criteria."%len(comments))
            
            # now filter them
            for cmt in comments:
                match_author = True if author is None else cmt.get('username') == author
                match_text   = True if comment_text is None else cmt.get("comment") == comment_text
                match_type   = True if comment_type is None else cmt.get("type") == comment_type
                match_id     = False if comment_id is None else cmt.get("id") == comment_id
                if match_id or (match_author and match_text and match_type):
                    found_cmts.append(cmt)
        else:
            self.logger.debug("no previous comments found for source %s"%name)
        return found_cmts

    def delete_comment(self, name, comment_id=None, comment_type=None, comment_text=None, 
        comment_author='self', refresh=True):
        """
            delete a comment with the given id for the chosen source. If comment_id
            is not given, delete the comment that matches the given text and type. 
            See docstring of read_comments for help.
        """
        
        # if ID it's not given, find out the comment ID from what it's in the summary
        if comment_id is None:
            prev_comments = self.read_comments(
                name, comment_id, comment_type, comment_text, comment_author, refresh=refresh)
            if prev_comments is None:
                raise ValueError(
                    "Cannot find previous comments for source. User refresh=True or provide valid comment ID")
            comment_id = [cmt['id'] for cmt in prev_comments]
        else:
            if type(comment_id) in (int, str):
                comment_id = [comment_id]
        
        # delete the comments
        for cmt_id in comment_id:
            self.logger.debug("deleting comment %s"%repr(cmt_id))
            req_data = {
                        "name"      : name,
                        "comment"   : comment_text,
                        "tablename" : "sources",
                        "type"      : comment_type,
                        "delete"    : "yes",
                        "id"       : cmt_id,
                     }
            ret = growthcgi(
                            'edit_comment.cgi', 
                            logger=self.logger,
                            auth=(self.user, self.passwd),
                            data=req_data,
                            to_json=False,
                            timeout=self.timeout)
        self.logger.debug("finished deleting comment(s).")

    def comment(self, name, text, comment_type='comment', duplicate_mode='no', comment_id=None):
        """
            add a comment ('annotation', as they are called by the marshal backend) 
            of the given type for a given source in the program.
            
            Parameters:
            -----------
                
                name: `str`
                    ZTF name of source.
                
                text: `str`
                    body of the comment message
                
                comment_type: `str`
                    type of comment. Default is 'comment'. Allowed values are: 
                    ['info', 'phase', 'redshift', 'comment', 'classification']
                
                duplicate_mode: `str`
                    either 'no' (default), 'edit', or 'add'. Specify what to do in
                    case a comment with the same text, type, and author (or ID) is
                    found for the source. In case of edit, the comment id has to be specified.
                
                comment_id: `int`
                    ID of the comment. If None, don't filter on this field.
        """
        
        # verify:
        allowed_types = ['info', 'phase', 'redshift', 'comment', 'classification']
        if comment_type not in allowed_types:
            raise ValueError("comment type '%s' not allowed. Possible values are: %s"%
                (comment_type, repr(allowed_types)))
        dup_opt = ['add', 'no', 'edit']
        if duplicate_mode not in dup_opt:
            raise ValueError("value '%s' not allowed for parameter 'dupliacte'. Possible values are: %s"%
                (duplicate, repr(dup_opt)))
        if duplicate_mode == 'edit' and comment_id is None:
            raise ValueError("you need to provide a comment_id for the comment you like to edit")
        
        # find the source among thesaved ones to access its id. You need it to comment.
        src = self.get_source(name)
        if src is None:
            raise ValueError("cannot comment a source that can't be found.")
        
        # check if the comment is new and if you want to duplicate it
        if duplicate_mode == 'no':
            prev_comments = self.read_comments(
                name, comment_type=comment_type, comment_text=text, comment_author='self', comment_id=comment_id)
            if len(prev_comments) != 0:
                self.logger.warning("Already existsing comment will not be duplicated.")
                return None
        
        # almost ready to post, here are the args
        req_data = {
                    "name"      : name,
                    "comment"   : text,
                    "tablename" : "sources",
                    "type"      : comment_type,
                    "commit"    : "yes",
                    "sourceid"  : src['id']
            }
        
        # if you want to edit an existing comment you have to have the ID
        if duplicate_mode == 'edit':
            req_data['id'] = comment_id
        
        # post the request
        self.logger.debug("posting comment '%s' (type: %s) to source: %s (id: %d)"%
            (text, comment_type, name, src['id']))
        ret = growthcgi(
                        'edit_comment.cgi', 
                        logger=self.logger,
                        auth=(self.user, self.passwd),
                        data=req_data,
                        to_json=False,
                        timeout=self.timeout)
        self.logger.debug("done commenting source.")

    def source_summary(self, name, append=False, refresh=False):
        """
            look for the source summary information for the given source, if not
            present, get it via the source_summary cgi script. 
            
            Parameters:
            -----------
            
                name: `str`
                    ZTF name of source.
                
                append: `bool`
                    if True and the source is present in this program's saved sources list, 
                    the information returned by this script is added to the source.
                
                refresh: `bool`
                    if True
            
            Returns:
            --------
                
                dict with source_summary.gci command output
        """
        
        # get the source, either from the saved or from the candidates
        src = self.find_source(name)
        if not src is None:
            
            # figure out which key contains the id
            if self.sources.get(name) is None:
                src_id = src.get('sourceId')
            else:
                src_id = src.get('id')
            if src_id is None:
                self.logger.warning(
                    "Unable to retrieve summary. '_id' or 'sourceId' not in src dictionary. Keys are: %s"%
                        (repr(src.keys())))
                return {}
            
            # see if it has a summary already
            summary = src.get('summary')
            
            # if not or if you want to update it, execute the script
            if summary is None or refresh:
                summary = growthcgi(
                        'source_summary.cgi',
                        logger=self.logger,
                        auth=(self.user, self.passwd),
                        data={'sourceid' : src_id},
                        timeout=self.timeout
                        )
                if summary == {}:
                    self.logger.warning("source %s has no summary"%(name))
                else:
                    self.logger.debug("got source summary for source %s."%name)
                # eventually append
                if append:
                    src['summary'] = summary
        else:
            summary = None
        return summary


    def get_summaries(self, refresh=False, nworkers=24):
        """
            get the summaries for all the saved sources and add them to the
            list of saved sources.
        """
        
        def dowload_summary(src_name):
            self.source_summary(src_name, append=True, refresh=refresh)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = nworkers) as executor:
                jobs = {
                    executor.submit(dowload_summary, src): src for src in self.sources}
                # inspect completed jobs
                for job in concurrent.futures.as_completed(jobs):
                    src_name = jobs[job]
                    # inspect job result
                    try:
                        # collect all the results
                        summ = job.result()
                        self.logger.debug("succesfully retrievd summary for source %s"%src_name)
                    except Exception as e:
                        self.logger.error("can't find summary for source %s"%src_name)
        self.logger.info("downloaded the summaries for all the saved sources.")


    def query_candidate_page(self, showsaved, start_date=None, end_date=None):
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
                                    program_id = self.programidx,
                                    logger=self.logger,
                                    timeout=self.timeout)


    def get_candidates(self, showsaved="selected", trange=None, tstep=5*u.day, nworkers=12, max_attemps=2, raise_on_fail=False):
        """
            download the list fo the sources in the scanning page of this program.
            
            Parameters:
            -----------
                
                showsaved: `str`
                    weather or not to include previously saved candidates. Possible options are:
                        * None ?
                        * 'selected'
                        * 'notSelected' ?
                        * 'onlySelected' ?
                        * 'onlyNotSelected' ?
                        * 'all' ?
                
                trange: `list` or `tuple` or None
                    time constraints for the query in the form of (start_date, end_date). The
                    two elements of tis list can be either strings or astropy.time.Time objects.
                    
                    if None, all the sources in the scanning page are retrieved slicing the 
                    query in smaller time steps. Since the marhsall returns at max 200 candidates
                    per query, if tis limit is reached, the time range of the query is 
                    subdivided iteratively.
                
                tstep: `astropy.quantity`
                    time step to use to splice the query.
                
                nworkers: `int`
                    number of threads in the pool that are used to download the stuff.
                
                max_attemps: `int`
                    this function will re-iterate the download on the jobs that fails until
                    complete success or until the maximum number of attemps is reaced.
                
                raise_on_fail: `bool`
                    if after the max_attemps is reached, there are still failed jobs, the
                    function will raise and exception if raise_on_fail is True, else it 
                    will simply throw a warning.
        """
        
        self.logger.info("Getting scanning page transient for program %s"%self.program)
        
        # create a query function
        def query_candidate_page(tstart, tstop):
            candids = self.query_candidate_page(showsaved, tstart, tstop)
            return candids
        
        # run the query with parallel time slices
        candidates = query_marshal_timeslice(
                                query_candidate_page, 
                                trange=trange, 
                                tstep=tstep, 
                                nworkers=nworkers, 
                                max_attemps=max_attemps, 
                                raise_on_fail=raise_on_fail, 
                                logger=self.logger
                            )
        
        # check for duplicates
        names = [s['name'] for s in candidates]
        if len(set(names)) != len(names):
            self.logger.warning("Duplicate candidates!")
        
        # turn the candidate list into a dictionary
        self.candidates = {s['name']:s for s in candidates}
        return self.candidates



#    def get_candidates_old(self, showsaved="selected", trange=None, tstep=5*u.day, nworkers=12, max_attemps=2, raise_on_fail=False):
#        """
#            download the list fo the sources in the scanning page of this program.
#            
#            Parameters:
#            -----------
#                
#                showsaved: `str`
#                    weather or not to include previously saved candidates. Possible options are:
#                        * None ?
#                        * 'selected'
#                        * 'notSelected' ?
#                        * 'onlySelected' ?
#                        * 'onlyNotSelected' ?
#                        * 'all' ?
#                
#                trange: `list` or `tuple` or None
#                    time constraints for the query in the form of (start_date, end_date). The
#                    two elements of tis list can be either strings or astropy.time.Time objects.
#                    
#                    if None, all the sources in the scanning page are retrieved slicing the 
#                    query in smaller time steps. Since the marhsall returns at max 200 candidates
#                    per query, if tis limit is reached, the time range of the query is 
#                    subdivided iteratively.
#                
#                tstep: `astropy.quantity`
#                    time step to use to splice the query.
#                
#                nworkers: `int`
#                    number of threads in the pool that are used to download the stuff.
#                
#                max_attemps: `int`
#                    this function will re-iterate the download on the jobs that fails until
#                    complete success or until the maximum number of attemps is reaced.
#                
#                raise_on_fail: `bool`
#                    if after the max_attemps is reached, there are still failed jobs, the
#                    function will raise and exception if raise_on_fail is True, else it 
#                    will simply throw a warning.
#        """
#        
#        # parse time limts
#        if not trange is None:
#            start_date = trange[0]
#            end_date   = trange[1]
#        else:
#            start_date = "2018-03-01 00:00:00"
#            end_date   = Time.now().datetime.strftime("%Y-%m-%d %H:%M:%S")
#        
#        # subdivide the query in time steps
#        start, end = Time(start_date), Time(end_date)
#        times = np.arange(start, end, tstep).tolist()
#        times.append(end)
#        self.logger.info("Getting scanning page transient for program %s between %s and %s using dt: %.2f h"%
#                (self.program, start_date, end_date, tstep.to('hour').value))
#        
#        # create list of time bounds
#        tlims = [ [times[it-1], times[it]] for it in range(1, len(times))]
#        
#        # utility functions for multiprocessing
#        def download_candidates(tlim):
#            candids = self.query_candidate_page(showsaved, tlim[0], tlim[1])
#            return candids
#        
#        def threaded_downloads(todo_tlims, candidates):
#            """
#                download the sources for specified tlims and keep track of what you've done
#            """
#            
#            n_total, n_failed = len(todo_tlims), 0
#            with concurrent.futures.ThreadPoolExecutor(max_workers = nworkers) as executor:
#                
#                jobs = {
#                    executor.submit(download_candidates, tlim): tlim for tlim in todo_tlims}
#                
#                # inspect completed jobs
#                for job in concurrent.futures.as_completed(jobs):
#                    tlim = jobs[job]
#                    
#                    # inspect job result
#                    try:
#                        # collect all the results
#                        candids = job.result()
#                        candidates += candids
#                        self.logger.debug("Query from %s to %s returned %d candidates. Total: %d"%
#                            (tlim[0].iso, tlim[1].iso, len(candids), len(candidates)))
#                        # if job is successful, remove the tlim from the todo list
#                        todo_tlims.remove(tlim)
#                        
#                    except Exception as e:
#                        self.logger.error("Query from %s to %s generated an exception %s"%
#                            (tlim[0].iso, tlim[1].iso, repr(e)))
#                        n_failed+=1
#            
#            # print some info
#            self.logger.info("jobs are done: total %d, failed: %d"%(n_total, n_failed))
#            
#            
#        # loop through the list of time limits and spread across multiple threads
#        start = time.time()
#        candidates = []                         # here you collect sources you've successfully downloaded
#        n_try, todo_tlims = 0, tlims            # here you keep track of what is done and what is still to be done
#        while len(todo_tlims)>0 and n_try<max_attemps:
#            self.logger.info("Downloading candidates. Iteration number %d: %d jobs to do"%
#                (n_try, len(todo_tlims)))
#            threaded_downloads(todo_tlims, candidates)
#            n_try+=1
#        end = time.time()
#        
#        # notify if it's still not enough
#        if len(todo_tlims)>0:
#            mssg = "Query for the following time interavals failed:\n"
#            for tl in todo_tlims: mssg += "%s %s\n"%(tl[0].iso, tl[1].iso)
#            if raise_on_fail:
#                raise RuntimeError(mssg)
#            else:
#                self.logger.error(mssg)
#        
#        # check for duplicates
#        names = [s['name'] for s in candidates]
#        if len(set(names)) != len(names):
#            self.logger.warning("Duplicate candidates!")
#        
#        # turn the candidate list into a dictionary
#        self.candidates = {s['name']:s for s in candidates}
#        self.logger.info("Fetched %d candidates in %.2e sec"%(len(self.candidates), (end-start)))
#        return self.candidates


    def fetch_all_lightcurves(self):
        """
            Download all lightcurves that have not been downloaded previously.
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
                filter_dict = self.filter_dict,
                mwebv=(self.dustmap.ebv(self.sources[name]['ra'], self.sources[name]['dec'])
                       if self.dustmap is not None else 0.)
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

    @property
    def dustmap(self):
        """Instance of SFD98 dust map"""
        if self._dustmap is not None:
            return self._dustmap
        elif _HAS_SFDMAP:
            if self.sfd_dir is None:
                self._dustmap = sfdmap.SFDMap()
            else:
                self._dustmap = sfdmap.SFDMap(self.sfd_dir)
            return self._dustmap
        else:
            return None
        
