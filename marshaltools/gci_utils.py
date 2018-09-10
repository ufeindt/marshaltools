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
#logging.getLogger("requests").setLevel(logging.WARNING)
#logging.getLogger("urllib3").setLevel(logging.WARNING)


MARSHALL_BASE = 'http://skipper.caltech.edu:8080/cgi-bin/growth/'
MARSHALL_SCRIPTS = (
                    'list_programs.cgi', 
                    'list_candidates.cgi',
                    'list_program_sources.cgi',
                    'print_lc.cgi'
                    )

httpErrors = {
    304: 'Error 304: Not Modified: There was no new data to return.',
    400: 'Error 400: Bad Request: The request was invalid. An accompanying error message will explain why.',
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
    'Weizmann Test Filter'                          : 45
    }


def growthcgi(scriptname, to_json=True, logger=None, **request_kwargs):
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
    
    # post request to the marshall
    logger.debug('Starting %s post'%(scriptname))
    r = requests.post(path, **request_kwargs)
    logger.debug('request URL: %s?%s'%(r.url, r.request.body))
    status = r.status_code
    if status != 200:
        try:
            message = httpErrors[status]
        except KeyError as e:
            message = 'Error %d: Undocumented error'%status
        logger.error(message)                                                   #TODO: shall we raise the exception we catched?
        return None
    logger.debug("Successful growth connection.")
    
    # parse result to JSON
    if to_json:
        try:
            rinfo =  json.loads(r.text)
        except ValueError as e:                                                 #TODO: shall we raise the exception we catched?
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

