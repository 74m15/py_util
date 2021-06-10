# -*- coding: utf-8 -*-
"""
Created on Sun Jun  6 18:22:48 2021

@author: CAPUANO-P
"""

from base64 import b64encode, b64decode

def security_encode(value):
    return b64encode(b64encode(value))

def security_decode(value):
    return b64decode(b64decode(value))
