import time
import marshaltools


prog = marshaltools.ProgramList("AMPEL Test", load_saved=False)
opts = ['None', 'selected', 'notSelected', 'onlySelected', 'onlyNotSelected', 'all']

for showsaved in opts: 
    
    start = time.time()
    res = prog.query_candidate_page(showsaved, '2018-09-06 09:31.09', '2018-09-07 09:31.09')
    end = time.time()
    print ("showsaved: %s. Fetched %d candidates in %.2e sec"%(showsaved, len(res), (end-start)))

