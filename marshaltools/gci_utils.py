#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# collection of funcions related to the marshall gci scripts. 
#

import requests, json, os
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
                    'edit_comment.cgi'
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
    'Nuclear Transients'                            : 10
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
        timeout = request_kwargs.pop('timeout', 30) + (60*n_try-1)
        
        # post the request
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


def query_scanning_page(start_date, end_date, program_name, showsaved="selected", auth=None, logger=None):
    """
        return the sources in the scanning page of the given program ingested in the
        marshall between start_date and end_date.
    """
    
    # get the logger
    logger = logger if not logger is None else logging.getLogger(__name__)
    
    # get scienceprogram number
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
                        }
                )
    
    logger.debug("retrieved %d sources"%len(srcs))
    return srcs


def ingest_candidates(avro_ids, program_name, program_id, be_anal, max_attempts=3, auth=None, logger=None):
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
                data={'avroid': avro_id, 'programidx': str(ingest_pid)}
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
                auth=auth, 
                logger=logger)
            
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

