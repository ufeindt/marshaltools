import marshaltools


prog = marshaltools.ProgramList('Cosmology')


t = prog.table
name = t['name'][1]
print (t)

print (name)
lc = prog.get_lightcurve(name)

