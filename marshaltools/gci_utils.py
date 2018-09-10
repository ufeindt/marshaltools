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
    logger.debug('request URL: %s'%r.url)
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



def get_json_old(start_date, end_date, program_name, prog_id=None):
    """
        given dates as string YYYY-MM-DD hh:mm:ss and program name, 
        get all the transients as a json file
    """
    
    if prog_id is None:
        prog_id = get_program_id(program_name)
    
    start = Time(start_date).datetime.strftime("%Y-%m-%d %H:%M:%S").replace(" ", "+").replace(":", "%3A")
    end = Time(end_date).datetime.strftime("%Y-%m-%d %H:%M:%S").replace(" ", "+").replace(":", "%3A")
    for date in [start, end]:
        date=date.replace(" ", "+").replace(":", "%3A")
    
    query = "&startdate=%s&enddate=%s&scienceprogram=%d&nshow=200"%(start, end, prog_id)

#    print("Getting URL: %s"%query)
    url = list_candidates_url+'?'+query
    return get(url)


#def query_scanning_page(start_date, end_date, program_name, prog_id=None)


def to_df(candid_json):
    """
        from candid_json to dataframe, selecting keys
    """
    my_keys = [
        'classification', 'redshift', 'dec',  'field', 'ra', 'rb', 'rcid', 
        'lastmodified', 'candid', 'can_be_saved_to', 'name', 'programid', 'creationdate']
    
    buff = []
    for candid in candid_json:
        skimmed = dict((k, candid.get(k)) for k in my_keys)
        buff.append(skimmed)
    return pd.DataFrame(buff)


def iterative_get(start_date, end_date, program_name, prog_id=None, tstep=1*u.day):
    """
        get transient for program using time steps between start and end date
    """
    
    if prog_id is None:
        prog_id = get_program_id(program_name)
    
    start, end = Time(start_date), Time(end_date)
    times = np.arange(start, end, tstep).tolist()
    times.append(end)
    print("Getting scanned transient for program %s between %s and %s using dt: %.2f h"%
        (program_name, start_date, end_date, tstep.to('hour').value))
    df_list = []
    for it in range(1, len(times)):
        my_start, my_end = times[it-1], times[it]
        partial_df = to_df(get_json(my_start.iso, my_end.iso, program_name, prog_id))
#        print("got %d transients for program %s from %s to %s"%
#            (len(partial_df), program_name, my_start.iso, my_end.iso))
        if len(partial_df)>200:
#            print("WARNING! reached hard limit on number of entries. Iterating.")
            partial_df = iterative_get(my_start.iso, my_end.iso, program_name, prog_id, tstep=tstep/2.)
        df_list.append(partial_df)
    return df_list

def get_all_scanned_transient(program_name, start_date, end_date, tstep=1*u.day):
    
    prog_id = get_program_id(program_name)
    df_list = iterative_get(start_date, end_date, program_name, prog_id, tstep)
    
    df = pd.concat(df_list)
    print("TOTAL: %d transients for program %s from %s to %s"%
            (len(df), program_name, start_date, end_date))
    out_name = "./dfs/scanned_%s_%s_%s.csv"%(program_name.replace(" ", ""), start_date, end_date)
    df.to_csv(out_name, index=False)
    print ("dataframe saved to %s"%out_name)
    return df



