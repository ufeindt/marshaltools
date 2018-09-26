import marshaltools

# test source and some keys to play with
name, src_id = 'ZTF18abjyjdz', 3329
keys = [
					'classification',
					'redshift',
					'uploaded_spectra.observer',
					'autoannotations.username',
					'redshift'
		]

prog = marshaltools.ProgramList("AMPEL Test", load_sources=True, load_candidates=True)


# get the source (just to see that it's in the candidates)
#src = prog.find_source(name)
#print (src)



out = prog.retrieve_from_src(name, keys)#, default=None, src_dict=None, append_summary=True, include_candidates=True):
print (out)



## pass the source name
#print ("----------------")
#out = prog.retrieve_from_src(name, keys)
#print (out)

## if you've looked in the summary (as we are doing in this example), the summary 
## is downloaded at the first call, then it is simlpy used. Now it should be faster:
#print ("----------------")
#out = prog.retrieve_from_src(name, keys)
#print (out)

## try with some missing key and default argument. You should see a warning there.
#print ("----------------")
#keys2 = keys+['fuffa']
#out = prog.retrieve_from_src(name, keys2, default="merci")
#print (out)


## now try passing the source instead of the name
#print ("----------------")
#src = prog.find_source(name, include_candidates=False)
#out = prog.retrieve_from_src(name, keys, src_dict=src)
#print (out)

