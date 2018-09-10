import time

import marshaltools
prog = marshaltools.ProgramList("AMPEL Test")


#prog.get_candidates()

start = time.time()
cand = prog.get_candidates(nworkers=24)
print ("fetched %d candidates in %.2e sec"%(len(cand), (time.time()-start)))
