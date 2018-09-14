import time

import marshaltools
prog = marshaltools.ProgramList("Cosmology")


summ = prog.source_summary('ZTF18abmjvpb')
#print (summ)

for k, v in summ.items():
    if k!='uploaded_photometry':
        print (k, ":", v)

# first you download them
#prog.get_summaries()

## second time, you simply read them
#prog.get_summaries()

