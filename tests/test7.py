# try to ingest

import marshaltools


prog = marshaltools.ProgramList("AMPEL Test", load_sources=False, load_candidates=False)
for pp in prog.program_list:
    print (pp['name'], pp['programidx'])

res = prog.ingest_avro('627195524515015019', be_anal=True)
print ("ingestion result:", res)



res = prog.ingest_avro(['623497692415015010', '627195524515015019'], be_anal=True)
print ("ingestion result:", res)


