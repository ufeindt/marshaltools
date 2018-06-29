# marshaltools
Tools for grabbing LCs and spectra from the growth marshal

Upon first import you will be asked to enter your marshal username and passwd, which will be saved in your home directory under .growthmarshal.

## Usage
```import masrshaltools

# First create a list of sources in your program, e.g. "Cosmology"
prog = marshaltools.ProgramList('Cosmology')

# Table of SN names, RA and Dec
prog.table

# Download specific LC
lc = prog.get_lightcurve('ZTF18abauprj')

# Table of data as on marshal page (after removing duplicate entries)
lc.table

# Table in sncosmo format
lc.table_sncosmo

# Download all LCs
prog.fetch_all_lightcurves()

# Download all spectra for a specific SN
prog.download_spec('ZTF18abauprj', '/path/to/your/data/ZTF18abauprj.tar.gz'

# Download the spectra of all sources in the program
prog.download_all_specs('/path/to/your/data')
```

## MW dust
The code can use the `sfdmap` package to determine MW E(B-V) at the coordinates of the SN. For this to work you must either set the $SFD_DIR environment variable to where you have the dust maps or use the option `sfd_dir` of `ProgramList`.
