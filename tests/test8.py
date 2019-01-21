# try to save some candidates

import marshaltools



cand_str = """ZTF18ablrndb 
ZTF18aawlhkh
ZTF18abporae
ZTF17aabihdn
ZTF18aawlhkh
ZTF18absgvqb
ZTF18ablprcf
ZTF18abufaej
ZTF18absrcps
ZTF18abvfecb
ZTF18abwbtco
ZTF18abxdkni
ZTF18abvrzqj"""
candidates = [x.strip() for x in cand_str.split('\n')]


print (candidates)

prog = marshaltools.ProgramList("AMPEL Test")
#prog.save_sources(candidates[0], programidx=42, save_by='name', max_attempts=3, be_anal=True)



failed = prog.save_sources(candidates[1:], save_by='name', max_attempts=3, be_anal=True)
print (failed)


