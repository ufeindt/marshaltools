import time

import marshaltools
prog = marshaltools.ProgramList("Cosmology")

summ = prog.source_summary('ZTF18abmjvpb')
print (summ)

for k, v in summ.items():
    if k!='uploaded_photometry':
        print (k, ":", v)

# first you download them
start = time.time()
prog.get_summaries()
print ("took %.2e sec"%(time.time()-start))

input("press enter to re-read the summaries. This time it will be fast")

## second time, you simply read them
prog.get_summaries()

