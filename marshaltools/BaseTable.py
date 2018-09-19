#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Base class to store marshal logins. Config functions are modified from ztfquery
#

import os, json

_SOURCE = 'ZmV0Y2hsY3M='
_CONFIG_FILE = os.path.join(os.environ.get('HOME'), '.growthmarshal')

def pad(text):
    """ good length password """
    while len(text) % 8 != 0:
        text += ' '
    return text

def encrypt_config():
    """ """
    import getpass
    #des = DES.new(base64.b64decode( _SOURCE ), DES.MODE_ECB)
    out = {}
    out['username'] = input('Enter your GROWTH Marshal username: ')
    out['password'] = getpass.getpass()
    fileout = open(_CONFIG_FILE, "w")
    #fileout.write(des.encrypt(pad(json.dumps(out))))
    fileout.write(json.dumps(out))
    fileout.close()

def decrypt_config():
    """ """
    #des = DES.new(  base64.b64decode( _SOURCE ), DES.MODE_ECB)
    # out = json.loads(des.decrypt(open(_CONFIG_FILE, "rb").read()))
    out = json.load(open(_CONFIG_FILE, "r"))
    return out['username'], out['password']

class BaseTable(object):
    """
    Virtual class that only contains the config loading method
    """
    def _load_config_(self, **kwargs):
        """
        """
        user = kwargs.pop('user', None)
        passwd = kwargs.pop('passwd', None)
        
        
        if user is None or passwd is None:
            if os.path.exists(_CONFIG_FILE):
                user, pw = decrypt_config()
                self.user = user
                self.passwd = pw
            else:
                raise ValueError('Please provide username and password' +
                                 ' as options "user" and "passwd".')
        else:
            self.user = user
            self.passwd = passwd

        return kwargs
