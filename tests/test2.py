import marshaltools.gci_utils as gci

ss = gci.growthcgi(
                'list_candidates.cgi', 
                logger=None,
                auth=('XX', 'XX'),
                data={
                    'programidx' : 5,
                    'startdate' : '2018-09-06 09:31.09',
                    'enddate' : '2018-09-07 09:31.09'
                    }
                )
print (ss)
