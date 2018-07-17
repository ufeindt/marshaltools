#! /usr/bin/env python
# -*- coding: utf-8 -*-

"""
Class for converting (RA, Dec) <-> (survey field, chip)
"""

import numpy as np
from copy import copy, deepcopy

import os
package_path = os.path.dirname(os.path.abspath(__file__))


_d2r = np.pi / 180

class SurveyFields(object):
    """
    Binner for SurveyField objects
    """

    def __init__(self, ra, dec, ccds, field_id=None,):
        """
        """
        self.ra = np.array(ra)
        self.dec = np.array(dec)
        self.ccds = ccds
        self.ra_range = (np.min([np.min(c_[:,0]) for c_ in ccds]),
                         np.max([np.max(c_[:,0]) for c_ in ccds]))
        self.dec_range = (np.min([np.min(c_[:,1]) for c_ in ccds]),
                          np.max([np.max(c_[:,1]) for c_ in ccds]))
        self.ccd_centers = np.array([[np.mean(ccd[:,k]) for ccd in self.ccds]
                                     for k in range(2)])

        if field_id is None:
            self.field_id = range(len(ra))
        else:
            self.field_id = np.array(field_id, dtype=int)

        self.field_id_index = -999999999 * np.ones(self.field_id.max()+1, dtype=int)
        self.field_id_index[self.field_id] = range(len(self.field_id))

    def coord2field(self, ra, dec, field_id=None):
        """
        Return the lists of fields in which a list of coordinates fall.
        Keep in mind that the fields will likely overlap.
        """
        bo = []
        r_off = []
        d_off = []
        if self.ccds is not None:
            c = []

        if field_id is None:
            field_id = self.field_id

        for f in field_id:
            tmp = self.coord_in_field(f, ra, dec)
            bo.append(tmp['field'])
            r_off.append(tmp['ra_off'])
            d_off.append(tmp['dec_off'])
            if self.ccds is not None:
                c.append(tmp['ccd'])

        # Handle the single coordinate case first
        if type(bo[0]) is np.bool_:
            c = np.array(c)
            return {'field': field_id[np.where(np.array(bo))[0]],
                    'ra_off': np.array(r_off)[~np.isnan(r_off)],
                    'dec_off': np.array(d_off)[~np.isnan(d_off)],
                    'ccd': c[c >= 0]}

        bo = np.array(bo)
        r_off = np.array(r_off)
        d_off = np.array(d_off)

        fields = [field_id[np.where(bo[:,k])[0]]
                  for k in xrange(bo.shape[1])]
        r_out = [np.array(r_off[:,k][~np.isnan(r_off[:,k])])
                 for k in xrange(r_off.shape[1])]
        d_out = [np.array(d_off[:,k][~np.isnan(d_off[:,k])])
                 for k in xrange(d_off.shape[1])]
        c = np.array(c)
        ccds = [np.array(c[:,k][c[:,k] >= 0], dtype=int)
                for k in xrange(c.shape[1])]
        return {'field': fields, 'ra_off': r_out,
                'dec_off': d_out, 'ccd': ccds}

    def field2coord(self, field, ra_off=None, dec_off=None, ccd=None):
        """
        """
        single_val = False
        if type(field) is list:
            field = np.array(field, dtype=int)
        elif type(field) is not np.ndarray:
            field = np.array([field], dtype=int)
            single_val = True

        idx = self.field_id_index[field]
        if ra_off is None and dec_off is None and ccd is None:
            r = self.ra[idx]
            d = self.dec[idx]
        else:
            if ra_off is None and dec_off is None:
                ra_off = np.zeros(len(field))
                dec_off = np.zeros(len(field))
            else:
                if single_val:
                    ra_off = np.array([ra_off])
                    dec_off = np.array([dec_off])
                else:
                    ra_off = np.array(ra_off)
                    dec_off = np.array(dec_off)

            if ccd is not None:
                if single_val:
                    ccd = np.array([ccd], dtype=int)
                else:
                    ccd = np.array(ccd, dtype=int)

            r = np.zeros(len(field))
            d = np.zeros(len(field))

            for  i in idx:
                mask = (idx == i)
                if ccd is not None:
                    c = ccd[mask]
                else:
                    c = None

                r[mask], d[mask] = self.pos2radec(i, ra_off[mask], dec_off[mask], c)

        if single_val:
            return r[0], d[0]
        else:
            return r, d

    def coord_in_field(self, f, ra, dec):
        """
        Check whether coordinates are in the field
        Returns bool if (ra, dec) floats, np.ndarray of bools if (ra, dec) 
        lists or np.ndarrays

        TODO:
        Test various cases of iterables that may break the method
        """
        ra = copy(ra)
        dec = copy(dec)
        i = self.field_id_index[f]


        single_val = False
        if type(ra) is list:
            ra = np.array(ra)
        elif type(ra) is not np.ndarray:
            # Assume it is a float
            ra = np.array([ra])
            single_val = True

        if type(dec) is list:
            dec = np.array(dec)
        elif type(dec) is not np.ndarray:
            # Assume it is a float
            dec = np.array([dec])
            single_val = True

        if len(ra) != len(dec):
            raise ValueError('ra and dec must be of same length')

        ra -= self.ra[i]
        ra1, dec1 = rot_xz_sph(ra, dec, -self.dec[i])
        ra1 *= -np.cos(dec1*_d2r)

        out = np.ones(ra1.shape, dtype=bool)
        out[dec1 < self.dec_range[0]] = False
        out[dec1 > self.dec_range[1]] = False
        out[ra1 < self.ra_range[0]] = False
        out[ra1 > self.ra_range[1]] = False
        ra1[~out] = np.nan
        dec1[~out] = np.nan

        return self._check_ccds_(out, ra1, dec1, single_val)

    def _check_ccds_(self, mask, r, d, single_val=False):
        """
        """
        def _f_edge(x, y, c0, c1):
            return y - c0[1] - (c1[1] - c0[1])/(c1[0] - c0[0]) * (x - c0[0])

        def _f_ccd(x, y, c_):
            return ((_f_edge(x, y, c_[0], c_[3]) > 0) &
                    (_f_edge(x, y, c_[1], c_[2]) < 0) &
                    (_f_edge(y, x, c_[0,::-1], c_[1,::-1]) > 0) &
                    (_f_edge(y, x, c_[3,::-1], c_[2,::-1]) < 0))

        b = np.array([_f_ccd(r[mask], d[mask], ccd)
                      for ccd in self.ccds])
        on_ccd = np.array([np.any(b[:,k]) for k in xrange(b.shape[1])])
        mask[mask] = on_ccd
        n_ccd = -999999999 * np.ones(len(mask), dtype=int)
        n_ccd[mask] = np.array([np.where(b[:,k])[0][0]
                                for k in np.where(on_ccd)[0]], dtype=int)

        r_off = np.nan * np.ones(len(mask))
        d_off = np.nan * np.ones(len(mask))
        r_off[mask] = (r[mask] - self.ccd_centers[0,n_ccd[mask]])
        d_off[mask] = d[mask] - self.ccd_centers[1,n_ccd[mask]]

        if single_val:
            return {'field': mask[0],  'ccd': n_ccd[0],
                    'ra_off': r_off[0], 'dec_off': d_off[0]}
        return {'field': mask,  'ccd': n_ccd,
                'ra_off': r_off, 'dec_off': d_off}

    def pos2radec(self, i, r, d, ccd=None):
        """
        """
        r = copy(r)
        d = copy(d)

        r *= 1
        if self.ccds is not None:
            r += self.ccd_centers[0, ccd]
            d += self.ccd_centers[1, ccd]

        r /= np.cos(d*_d2r)
        r, d = rot_xz_sph(r, d, self.dec[i])
        r += self.ra[i]

        r = ((r + 180) % 360 ) - 180

        return r, d

class ZTFFields(SurveyFields):
    """
    """
    def __init__(self,
                 fields_file=os.path.join(package_path, 'data/ZTF_Fields.txt'),
                 ccd_file=os.path.join(package_path, 'data/ZTF_corners.txt')):
        """
        """
        fields = np.genfromtxt(fields_file, comments='%')

        ccd_corners = np.genfromtxt(ccd_file, skip_header=1)
        ccds = [ccd_corners[np.array([0,1,2,3])+4*k, :2] for k in range(16)]
        super(ZTFFields, self).__init__(fields[:,1], fields[:,2],
                                        ccds, field_id=fields[:,0])

# ============================== #
# = Auxiliary functions        = #
# ============================== #
def cart2sph(vec, cov=None):
    """
    Convert vector in Cartesian coordinates to spherical coordinates 
    (angles in degrees). Convariance matrix can be converted as well
    if it is stated.
    """
    x = vec[0]
    y = vec[1]
    z = vec[2]

    v = np.sqrt(x**2 + y**2 + z**2)
    v_sph = np.array([v, (np.arctan2(y,x) / _d2r + 180) % 360 - 180, 
                          np.arcsin(z/v) / _d2r])

    if cov is None:
        return v_sph
    else:
        jacobian = np.zeros((3,3))
        jacobian[0,0] = x / v
        jacobian[1,0] = - y / (x**2 + y**2)
        jacobian[2,0] = - x * z / (v**2 * np.sqrt(x**2 + y**2))
        jacobian[0,1] = y / v
        jacobian[1,1] = x / (x**2 + y**2)
        jacobian[2,1] = - y * z / (v**2 * np.sqrt(x**2 + y**2))
        jacobian[0,2] = z / v
        jacobian[1,2] = 0
        jacobian[2,2] = np.sqrt(x**2 + y**2) / (v**2)

        cov_sph = (jacobian.dot(cov)).dot(jacobian.T)
        cov_sph[1,1] /= _d2r**2
        cov_sph[2,2] /= _d2r**2
        cov_sph[2,1] /= _d2r**2
        cov_sph[1,2] /= _d2r**2

        cov_sph[0,1] /= _d2r
        cov_sph[0,2] /= _d2r
        cov_sph[1,0] /= _d2r
        cov_sph[2,0] /= _d2r

        return v_sph, cov_sph

def sph2cart(vec, cov=None):
    """
    Convert vector in spherical coordinates (angles in degrees)
    to Cartesian coordinates. Convariance matrix can be converted as well
    if it is stated.
    """
    v = vec[0]
    l = vec[1]*_d2r
    b = vec[2]*_d2r

    v_cart = np.array([v*np.cos(b)*np.cos(l), v*np.cos(b)*np.sin(l),
                       v*np.sin(b)])

    if cov is None:
        return v_cart
    else:
        cov_out = deepcopy(cov)
        cov_out[1,1] *= _d2r**2
        cov_out[2,2] *= _d2r**2
        cov_out[2,1] *= _d2r**2
        cov_out[1,2] *= _d2r**2
        cov_out[0,1] *= _d2r
        cov_out[0,2] *= _d2r
        cov_out[1,0] *= _d2r
        cov_out[2,0] *= _d2r

        jacobian = np.zeros((3,3))
        jacobian[0,0] = np.cos(b) * np.cos(l)
        jacobian[1,0] = np.cos(b) * np.sin(l)
        jacobian[2,0] = np.sin(b)
        jacobian[0,1] = - v * np.cos(b) * np.sin(l)
        jacobian[1,1] = v * np.cos(b) * np.cos(l)
        jacobian[2,1] = 0
        jacobian[0,2] = - v * np.sin(b) * np.cos(l)
        jacobian[1,2] = - v * np.sin(b) * np.sin(l)
        jacobian[2,2] = v * np.cos(b)

        cov_cart = (jacobian.dot(cov_out)).dot(jacobian.T)

        return v_cart, cov_cart

def rot_xz(v, theta):
    """
    Rotate Cartesian vector v by angle theta around axis (0,1,0)
    """
    return np.array([v[0]*np.cos(theta*_d2r) - v[2]*np.sin(theta*_d2r),
                     v[1],
                     v[2]*np.cos(theta*_d2r) + v[0]*np.sin(theta*_d2r)])

def rot_xz_sph(l, b, theta):
    """
    Rotate Spherical coordinate (l,b) by angle theta around axis (0,1,0)
    """
    v_cart = sph2cart([1,l,b])
    v_rot = rot_xz(v_cart, theta)
    return cart2sph(v_rot)[1:]
