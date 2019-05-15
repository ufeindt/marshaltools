#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# collection of funcions related to the marshall gci scripts. 
#

import requests, json, os, time
import numpy as np
import concurrent.futures
from astropy.time import Time
import astropy.units as u

import logging
logging.basicConfig(level = logging.INFO)
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

MARSHALL_BASE = 'http://skipper.caltech.edu:8080/cgi-bin/growth/'
MARSHALL_SCRIPTS = (
                    'list_programs.cgi', 
                    'list_candidates.cgi',
                    'list_program_sources.cgi',
                    'source_summary.cgi',
                    'print_lc.cgi',
                    'ingest_avro_id.cgi',
                    'save_cand_growth.cgi',
                    'edit_comment.cgi',
                    'update_archived_phot.cgi'
                    )

httpErrors = {
    304: 'Error 304: Not Modified: There was no new data to return.',
    400: 'Error 400: Bad Request: The request was invalid. An accompanying error message will explain why.',
    422: 'Error 422: Invalid Input.',
    403: 'Error 403: Forbidden: The request is understood, but it has been refused. An accompanying error message will explain why',
    404: 'Error 404: Not Found: The URI requested is invalid or the resource requested, such as a category, does not exists.',
    500: 'Error 500: Internal Server Error: Something is broken.',
    503: 'Error 503: Service Unavailable.'
}

SCIENCEPROGRAM_IDS = {
    'AMPEL Test'                                    : 42,
    'Cosmology'                                     : 32,
    'Gravitational Lenses'                          : 43,
    'Correlating Icecube and ZTF'                   : 44,
    'Electromagnetic Counterparts to Neutrinos'     : 25,
    'Test Filter'                                   : 37,
    'Redshift Completeness Factor'                  : 24,
    'Weizmann Test Filter'                          : 45,
    'ZTFBH Offnucear'                               : 47,
    'ZTFBH Nuclear'                                  : 48,
    'Nuclear Transients'                            : 10,
    #'AmpelRapid'                                    : 67
    }

INGEST_PROGRAM_IDS = {                              # TODO add all of them
    'AMPEL Test'                                    : 4,
    'Cosmology'                                     : 5
    }


def requests_retry_session(retries=3, backoff_factor=0.3, status_forcelist=(500, 502, 504), session=None):
    """
        create robust request session. From:
        https://www.peterbe.com/plog/best-practice-with-retries-with-requests
    """
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def growthcgi(scriptname, to_json=True, logger=None, max_attemps=2, **request_kwargs):
    """
    Run one of the growth cgi scripts, check results and return.
    """
    
    # get the logger
    logger = logger if not logger is None else logging.getLogger(__name__)
    
    # check
    if not scriptname in MARSHALL_SCRIPTS:
        raise ValueError("scriptname %s not recognized. Available options are: %s"%
            (scriptname, ", ".join(MARSHALL_SCRIPTS)))
    path = os.path.join(MARSHALL_BASE, scriptname)
    
    # post request to the marshall making several attemps
    n_try, success = 0, False
    while n_try<max_attemps:
        logger.debug('Starting %s post. Attempt # %d'%(scriptname, n_try))
        # set timeout from kwargs or use default
        timeout = request_kwargs.pop('timeout', 60) + (60*n_try-1)
        r = requests_retry_session().post(path, timeout=timeout, **request_kwargs)
        logger.debug('request URL: %s?%s'%(r.url, r.request.body))
        status = r.status_code
        if status != 200:
            try:
                message = httpErrors[status]
            except KeyError as e:
                message = 'Error %d: Undocumented error'%status
            logger.error(message)
        else:
            logger.debug("Successful growth connection.")
            success = True
            break
        n_try+=1
    
    if not success:
        self.logger.error("Failure despite %d attemps!"%max_attemps)
        return None
    
    # parse result to JSON
    if to_json:
        try:
            rinfo =  json.loads(r.text)
        except ValueError as e:
            # No json information returned, usually the status most relevant
            logger.error('No json returned: status %d' % status )
            rinfo =  status
    else:
        rinfo = r.text
    return rinfo

def get_saved_sources(program_id, trange=None, auth=None, logger=None, **request_kwargs):
    """
        get saved sources for a program through the list_program_sources.cgi script.
        Eventually specify a time range. 
        
        Parameters:
        -----------
        
        program_id: `int`
            marshal program ID.
        
        trange: `list` or `tuple` or None
            time constraints for the query in the form of (start_date, end_date). The
            two elements of tis list can be either strings or astropy.time.Time objects.
            if None, try to download all the sources at once.
        
        auth: `list`
            (username, pwd)
        
        Returns:
        --------
            list with the saved sources for the program.
    """
    
    # get the logger
    logger = logger if not logger is None else logging.getLogger(__name__)
    
    # prepare reuqtest data
    req_data = {
                'programidx': program_id,
                'getredshift': 1,
                'getclassification': 1
            }
    
    # eventually add dates there
    if not trange is None:
        
        # format dates to astropy.Time 
        start_date, end_date = trange
        tstart = Time(start_date) if type(start_date) is str else start_date
        tend   = Time(end_date) if type(end_date) is str else end_date
        tstart = tstart.datetime.strftime("%Y-%m-%d %H:%M:%S")
        tend   = tend.datetime.strftime("%Y-%m-%d %H:%M:%S")
        logger.debug("listing saved sources of scienceprogram ID %d for ingested times between %s and %s"%
            (program_id, tstart, tend))
        
        # add date to payload
        req_data.update({
                        'startdate'  : tstart,
                        'enddate'    : tend,
                    })
    else:
        logger.debug("listing saved sources of scienceprogram ID %d"%program_id)
    
    # execute request 
    srcs = growthcgi(
            'list_program_sources.cgi',
            logger=logger,
            auth=auth,
            data=req_data,
            **request_kwargs
            )
    logger.debug("retrieved %d sources"%len(srcs))
    return srcs

def query_scanning_page(
    start_date, end_date, program_name, showsaved="selected", auth=None, 
    logger=None, program_id=None, **request_kwargs):
    """
        return the sources in the scanning page of the given program ingested in the
        marshall between start_date and end_date.
    """
    
    # TODO: the ID used to query the scanning page seems to be different from
    # the one you get from lits programs.
    
    # get the logger
    logger = logger if not logger is None else logging.getLogger(__name__)
    
    # get scienceprogram number
    scienceprogram = program_id
    if program_id is None:
        scienceprogram = SCIENCEPROGRAM_IDS.get(program_name)
        if scienceprogram is None:
            raise KeyError("cannot find scienceprogram number corresponding to program %s. We have: %s"%
                (program_name, repr(SCIENCEPROGRAM_IDS)))
    
    # format dates to astropy.Time 
    tstart = Time(start_date) if type(start_date) is str else start_date
    tend   = Time(end_date) if type(end_date) is str else end_date
    tstart = tstart.datetime.strftime("%Y-%m-%d %H:%M:%S")
    tend   = tend.datetime.strftime("%Y-%m-%d %H:%M:%S")
    logger.debug("querying scanning page of program %s (scienceprogram %d) for ingested times between %s and %s"%
        (program_name, scienceprogram, tstart, tend))
    
    # query and return sources as json
    srcs = growthcgi(
                    'list_candidates.cgi',
                    logger=logger,
                    auth=auth,
                    data={
                        'scienceprogram' : scienceprogram,
                        'startdate'  : tstart,
                        'enddate'    : tend,
                        'showSaved'  : showsaved
                        },
                    **request_kwargs
                )
    
    logger.debug("retrieved %d sources"%len(srcs))
    return srcs


def query_marshal_timeslice(query_func, trange=None, tstep=5*u.day, nworkers=12, max_attemps=2, raise_on_fail=False, logger=None):
    """
        splice up a marhsla query in time so that each request is manageble.
        
        Issue the query function in time slices each of tstep days (optionally
        limiting the global time range) and glue the results together.
        
        The queries are executed in using a thread pool.
        
        Parameters:
        -----------
            
            query_func: `callable`
                query function to be run over the time slices. It must have the signature
                query_func(tstart, tstop) and must return a list.
            
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
        
        Returns:
        --------
            
            list with the glued results from all the time-slice queries
    """
    
    # get the logger
    logger = logger if not logger is None else logging.getLogger(__name__)
    
    # parse time limts
    if not trange is None:
        start_date = trange[0]
        end_date   = trange[1]
    else:
        start_date = "2018-03-01 00:00:00"
        end_date   = (Time.now() +1*u.day).datetime.strftime("%Y-%m-%d %H:%M:%S")
    
    # subdivide the query in time steps
    start, end = Time(start_date), Time(end_date)
    times = np.arange(start, end, tstep).tolist()
    times.append(end)
    logger.info("Querying marshal with %s between %s and %s using dt: %.2f h"%
            (query_func.__name__, start_date, end_date, tstep.to('hour').value))
    
    # create list of time bounds
    tlims = [ [times[it-1], times[it]] for it in range(1, len(times))]
    
    # utility functions for multiprocessing
    def query_func_wrap(tlim): return query_func(tlim[0], tlim[1])
    def threaded_downloads(todo_tlims, candidates):
        """
            download the sources for specified tlims and keep track of what you've done
        """
        
        n_total, n_failed = len(todo_tlims), 0
        with concurrent.futures.ThreadPoolExecutor(max_workers = nworkers) as executor:
            
            jobs = {
                executor.submit(query_func_wrap, tlim): tlim for tlim in todo_tlims}
            
            # inspect completed jobs
            for job in concurrent.futures.as_completed(jobs):
                tlim = jobs[job]
                
                # inspect job result
                try:
                    # collect all the results
                    candids = job.result()
                    candidates += candids
                    logger.debug("Query from %s to %s returned %d candidates. Total: %d"%
                        (tlim[0].iso, tlim[1].iso, len(candids), len(candidates)))
                    # if job is successful, remove the tlim from the todo list
                    todo_tlims.remove(tlim)
                    
                except Exception as e:
                    logger.error("Query from %s to %s generated an exception %s"%
                        (tlim[0].iso, tlim[1].iso, repr(e)))
                    n_failed+=1
        
        # print some info
        logger.debug("jobs are done: total %d, failed: %d"%(n_total, n_failed))
        
        
    # loop through the list of time limits and spread across multiple threads
    start = time.time()
    candidates = []                         # here you collect sources you've successfully downloaded
    n_try, todo_tlims = 0, tlims            # here you keep track of what is done and what is still to be done
    while len(todo_tlims)>0 and n_try<max_attemps:
        logger.debug("Querying the marshal. Iteration number %d: %d jobs to do"%
            (n_try, len(todo_tlims)))
        threaded_downloads(todo_tlims, candidates)
        n_try+=1
    end = time.time()
    
    # notify if it's still not enough
    if len(todo_tlims)>0:
        mssg = "Query for the following time interavals failed:\n"
        for tl in todo_tlims: mssg += "%s %s\n"%(tl[0].iso, tl[1].iso)
        if raise_on_fail:
            raise RuntimeError(mssg)
        else:
            logger.error(mssg)
    
    # check for duplicates
    logger.info("Fetched %d candidates/sources in %.2e sec"%(len(candidates), (end-start)))
    return candidates


def ingest_candidates(
    avro_ids, program_name, program_id, query_program_id, be_anal, 
    max_attempts=3, auth=None, logger=None, **request_kwargs):
    """
        ingest one or more candidate(s) by avro id into the marhsal.
        If needed we can be anal about it and go and veryfy the ingestion.
        avor_ids can be a list with more than one 
    """
    
    # remember the time to be able to go veryfy downloaded candidates
    start_ingestion = Time.now() - 24*u.hour    #TODO: restrict once you are certain it works
    
    # get the logger
    logger = logger if not logger is None else logging.getLogger(__name__)
    
#    # get the program id used by the ingest page
#    ingest_pid = INGEST_PROGRAM_IDS.get(program_name)
#    if ingest_pid is None:
#        raise KeyError("cannot find program %s in SCIENCEPROGRAM_IDS. Availables are: %s"
#            %", ".join(SCIENCEPROGRAM_IDS.keys()))
    
    # apparently the ingestion prefers to use the 'user specific' program id 
    # rather than the other ones.  TODO: figure out if this is a consistent behaviour
    ingest_pid = program_id
    
    # see if you want to ingest just one candidates or a whole bunch of them
    # cast everything to string for consistency and checking
    if type(avro_ids) in [str, int]:
        to_ingest = [str(avro_ids)]
    else:
        to_ingest = [str(aid) for aid in avro_ids]
    logger.info("Trying to ingest %d candidate(s) to to marhsal program %s using ingest ID %d"%
        (len(to_ingest), program_name, ingest_pid))
    
    # If there is nothing to ingest, we are done with no failures :)
    if len(to_ingest) == 0 :
        failed = []
        return failed
    # ingest all the candidates, eventually veryfying and retrying
    n_attempts, failed = 0, []
    while len(to_ingest)>0 and n_attempts < max_attempts:
        
        n_attempts+=1
        logger.debug("attempt number %d of %d."%(n_attempts, max_attempts))
        
        # ingest them
        for avro_id in to_ingest:
            status = growthcgi(
                'ingest_avro_id.cgi',
                logger=logger,
                auth=auth,
                to_json=False,
                data={'avroid': avro_id, 'programidx': str(ingest_pid)},
                **request_kwargs
                )
            logger.debug("Ingesting candidate %s returned %s"%(avro_id, status))
        logger.info("Attempt %d: done ingesting candidates."%n_attempts)
        
        # if you take life easy then it's your problem. We'll exit the loop
        if not be_anal:
            return None
        
        # if you want to be anal about that, go and make sure all the candidates are there
        end_ingestion = Time.now() + 10*u.min
        logger.info("veryfying ingestion looking at candidates ingested between %s and %s"%
                (start_ingestion.iso, end_ingestion.iso))
        done, failed = [], []   # here overwite global one
        try:
            new_candidates = query_scanning_page(
                start_date=start_ingestion.iso, 
                end_date=end_ingestion.iso, 
                program_name=program_name,
                showsaved="selected",
                program_id=query_program_id,
                auth=auth, 
                logger=logger,
                **request_kwargs)
            
            # if you got none it could mean you haven't ingested them.
            # but could also be just a question of time. Death is the only certainty
            if len(new_candidates) == 0:
                logger.warning("attempt # %d. No new candidates, upload seems to have failed."%n_attempts)
                failed = to_ingest
                continue
            
            # see if the avro_id is there (NOTE: assume that the 'candid' in the sources stores the 'avro_id')
            ingested_ids = [str(dd['candid']) for dd in new_candidates]
            for avro_id in to_ingest:
                if avro_id in ingested_ids:
                    done.append(avro_id)
                else:
                    failed.append(avro_id)
            logger.info("attempt # %d. Of the desired candidates %d successfully ingested, %d failed"%
                (n_attempts, len(done), len(failed)))
            
            # remember what is still to be done
            to_ingest = failed
        
        except Exception as e:
            logger.warning("could not query candidate page. Got exception %s"%e)
    
    # return the list of ids that failed consistently after all the attempts
    return failed

