# try to ingest some candidates

import astropy.units as u
from astropy.time import Time
import marshaltools


avro_id = '634445152015015010'

# use wisely
#avro_ids = [ 
#    634209464915015007, 634313330115015002, 634242335115015022, 628140190315015023, 
#    634209464415015033, 634258942115015000, 627195522015015005, 634182000215015004, 
#    634306734915015002, 634147162715015055
#    ]

#avro_id = '634209464915015007'
avro_id = '634313330115015002'

prog = marshaltools.ProgramList("AMPEL Test", load_sources=False, load_candidates=False)
#prog = marshaltools.ProgramList("AMPEL Test", load_candidates=True)

prog.ingest_avro(['634242335115015022', '628140190315015023'], be_anal=True)




# ----------------------------------------------------------------------------- #
# manually ingest the package (this workds)
##status = marshaltools.gci_utils.growthcgi(
##                'ingest_avro_id.cgi',
##                auth=(prog.user, prog.passwd),
##                to_json=False,
##                data={'avroid': str(avro_id), 'programidx': 3}
##                )
##print (status)
### see if the source you have is in the candidate page
##start = Time.now()-5*u.min
##end = Time.now()+5*u.min

##cand = prog.query_candidate_page('selected', start, end)
##ingested_ids = [cc['candid'] for cc in cand]
##for c in cand:
##    print (c['candid'])
##print (len(cand))
##print (avro_id in ingested_ids)
##print (type(ingested_ids[0]))
##print (str(ingested_ids[0]) == avro_id)

# ----------------------------------------------------------------------------- #



#res = prog.ingest_avro('627195524515015019', be_anal=True)
#print ("ingestion result:", res)

#res = prog.ingest_avro('634445152015015010', be_anal=True)
#print ("ingestion result:", res)





#res = prog.ingest_avro(['623497692415015010', '627195524515015019'], be_anal=True)
#print ("ingestion result:", res)


