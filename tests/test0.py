import marshaltools

prog = marshaltools.ProgramList('Cosmology')


# try load sources just for a time range
prog = marshaltools.ProgramList('Cosmology', load_sources=False)
prog.get_saved_sources(trange=('2009-05-18 10:04:09', '2019-05-06 10:04:09'))
