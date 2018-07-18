#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Load ZTF etc filter to sncosmo
"""

import numpy as np
import os
package_path = os.path.dirname(os.path.abspath(__file__))

import sncosmo


def load_filters():
    bandsP48 = {'p48i': 'P48_I.dat',
                'p48r': 'P48_R.dat',
                'p48g': 'P48_g.dat'}

    fileDirectory = 'filters/P48/'
    for bandName, fileName in bandsP48.items():
        filePath = os.path.join(package_path, fileDirectory, fileName)
        if not os.path.exists(filePath):
            raise IOError("No such file: %s" % filePath)
        b = np.loadtxt(filePath)
        band = sncosmo.Bandpass(b[:, 0], b[:, 1], name=bandName)
        sncosmo.registry.register(band, force=True)

    ##########

    bandsP60 = {'p60i': 'iband_eff.dat',
                'p60r': 'rband_eff.dat',
                'p60g': 'gband_eff.dat',
                'p60u': 'uband_eff.dat'}

    fileDirectory = 'filters/SEDm/'
    for bandName, fileName in bandsP60.items():
        filePath = os.path.join(package_path, fileDirectory, fileName)
        if not os.path.exists(filePath):
            raise IOError("No such file: %s" % filePath)
        b = np.loadtxt(filePath)
        band = sncosmo.Bandpass(b[:, 0], b[:, 1], name=bandName)
        sncosmo.registry.register(band, force=True)

    ##########

    bandsUVOT = {'uvotb': 'B_UVOT_synphot.txt',
                'uvotu': 'U_UVOT_synphot.txt',
                'uvotv': 'V_UVOT_synphot.txt',
                'uvm2': 'UVM2_synphot.txt',
                'uvw1': 'UVW1_synphot.txt',
                'uvw2': 'UVW2_synphot.txt'}

    fileDirectory = 'filters/UVOT/'
    for bandName, fileName in bandsUVOT.items():
        filePath = os.path.join(package_path, fileDirectory, fileName)
        if not os.path.exists(filePath):
            raise IOError("No such file: %s" % filePath)
        b = np.loadtxt(filePath)
        band = sncosmo.Bandpass(b[:, 0], b[:, 1], name=bandName)
        sncosmo.registry.register(band, force=True)
