import marshaltools

# test source and some keys to play with
name, src_id = 'ZTF18abmjvpb', 4066
keys = [
					'classification',
					'redshift',
					'uploaded_spectra.observer',
					'autoannotations.username',
					'redshift'
		]

prog = marshaltools.ProgramList("Cosmology", load_sources=True)

# pass the source name
print ("----------------")
out = prog.retrieve_from_src(name, keys)
print (out)

# if you've looked in the summary (as we are doing in this example), the summary 
# is downloaded at the first call, then it is simlpy used. Now it should be faster:
print ("----------------")
out = prog.retrieve_from_src(name, keys)
print (out)

# try with some missing key and default argument. You should see a warning there.
print ("----------------")
keys2 = keys+['fuffa']
out = prog.retrieve_from_src(name, keys2, default="merci")
print (out)


# now try passing the source instead of the name
print ("----------------")
src = prog.find_source(name, include_candidates=False)
out = prog.retrieve_from_src(name, keys, src_dict=src)
print (out)

