# -*- coding: utf-8 -*-
"""
Created on Sun Jun  6 18:22:48 2021

@author: CAPUANO-P
"""

from base64 import b64encode, b64decode

def security_encode(value):
    return str(b64encode(b64encode(value.encode())), encoding="utf-8")

def security_decode(value):
    return str(b64decode(b64decode(value)), encoding="utf-8")

if __name__ == "__main__":
    from getpass import getpass
    from sys import argv
    
    if len(argv) > 1:
        if argv[1] == "encode":
            input = getpass("Enter message to encode: ")
            
            print(f"\n{security_encode(input)}\n")
        elif argv[1] == "decode":
            input = getpass("Enter message to decode: ")
            
            print(f"\n{security_decode(input)}\n")
        else:
            print("Syntax error: use 'encode' or 'decode'")
