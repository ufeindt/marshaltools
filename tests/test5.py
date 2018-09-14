import time

import marshaltools
prog = marshaltools.ProgramList("Cosmology")

# get te summary for one source
summ = prog.source_summary('ZTF18abmjvpb', append=True)


# get the type
t = prog.get_src_key('ZTF18abmjvpb', ['redshift', 'iauname', 'autoannotations'])
print (t)


