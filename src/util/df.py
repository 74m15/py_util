# -*- coding: utf-8 -*-
"""
Created on Tue Feb 26 13:06:31 2019

@author: CAPUANO-P
"""

import operator

from collections import OrderedDict

_OP_MAP = {
    "=" : operator.eq,
    "<" : operator.lt,
    "<=" : operator.le,
    ">" : operator.gt,
    ">=" : operator.ge,
    "!=" : operator.ne
}

def filter_df(data, *conditions):
    
    def check(row, condition):
        if len(condition) == 2:
            return row[condition[0]] == condition[1]
        elif len(condition) == 3:
            op = _OP_MAP.get(condition[1])
            
            if op:
                return op(row[condition[0]], row[condition[2]])
            else:
                raise RuntimeError("Wrong condition: {0}".format(condition))
        else:
            raise RuntimeError("Unexpected condition: {0}".format(condition))
    
    return [row for row in data if all([check(row, condition) for condition in conditions])]

def sort_df(data, *keys):
    
    def key(idx):    
        def inner_key(row):
            return row[idx]
        
        return inner_key
    
    if len(keys) == 0:
        return data
    else:
        tmp = data
        
        for k in keys[-1::-1]:
            if isinstance(k, int):
                tmp = sorted(tmp, key=key(k))
            elif isinstance(k, list) or isinstance(k, tuple):
                if len(k) == 1:
                    tmp = sorted(tmp, key=key(k[0]))
                elif len(k) == 2:
                    tmp = sorted(tmp, key=key(k[0]), reverse=k[1])
                else:
                    raise RuntimeError("Unexpected sort key: {0}".format(k))
            else:
                raise RuntimeError("Unexpected sort key: {0}".format(k))
        
        return tmp

def map_df(data, *maps):
    if len(maps) == 0:
        return data
    else:
        identity = lambda x: x
        
        new_maps = OrderedDict()
        
        for map in maps:
            if isinstance(map, int):
                new_maps[map] = identity
            elif isinstance(map, list) or isinstance(map, tuple):
                if (len(map) == 2):
                    new_maps[map[0]] = map[1]
                else:
                    raise RuntimeError("Unexpected map: {0}".format(map))
            else:
                raise RuntimeError("Unexpected map: {0}".format(map))
        
        return [ [new_maps[k](row[k]) for k in new_maps.keys()] for row in data ]

def select_df(data, *keys):
    if len(keys) == 0:
        return data
    else:
        return [ [row[k] for k in keys] for row in data ]
