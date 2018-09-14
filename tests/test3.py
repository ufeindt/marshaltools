import time

import marshaltools
prog = marshaltools.ProgramList("AMPEL Test")


opts = ['None', 'selected', 'notSelected', 'onlySelected', 'onlyNotSelected', 'all']

for showsaved in opts: 
    
    start = time.time()
    cand = prog.get_candidates(nworkers=24, showsaved=showsaved)
    end = time.time()
    print ("showsaved: %s. Fetched %d candidates in %.2e sec"%(showsaved, len(cand), (end-start)))
    exit()
