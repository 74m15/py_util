#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 11 14:41:53 2017

@author: pasquale
"""

import json
import re

from os import getenv
from urllib.request import urlopen

NO_EVAL         = 0
EVALUATE_SIMPLE = 1
EVALUATE_EVAL   = 2

class WrapIter(object):
    def __init__(self, iter, context, evaluate):
        self._iter = iter
        self.context = context
        self.evaluate = evaluate
    
    def __iter__(self):
        return self
    
    def next(self):
        return self.__next__()
    
    def __next__(self):
        try:
            value = self._iter.__next__()
            
            return Wrap.return_value(value, self.evaluate, self.context)
        except Exception as ex:
            raise StopIteration
        
class Wrap(object):
    
    _default_context = dict()
    
    def __init__(self, obj, context=None, evaluate=NO_EVAL):
        self._obj = obj
        self.context = context or Wrap._default_context
        self.evaluate = evaluate
    
    def bind_context(self, context):
        self.context = context
    
    def get_context(self):
        return self.context
    
    @staticmethod
    def bind_default_context(context):
        Wrap._default_context = context
    
    @staticmethod
    def get_default_context():
        return Wrap._default_context
    
    @staticmethod
    def return_value(value, evaluate, context):
        if (isinstance(value, list) or isinstance(value, dict)):
            return Wrap(value, context, evaluate)
        else:
            if (evaluate and (isinstance(value, str) or isinstance(value, unicode))):
                return evaluate_string(value, context, evaluate == EVALUATE_EVAL)
            else:
                return value
    
    def __getattr__(self, name):
        try:
            evaluate = NO_EVAL
            
            if (name in self._obj):
                value = self._obj.get(name)
            elif ("[{0}]".format(name) in self._obj):
                value = self._obj.get("[{0}]".format(name))
                evaluate = EVALUATE_SIMPLE
            elif ("[[{0}]]".format(name) in self._obj):
                value = self._obj.get("[[{0}]]".format(name))
                evaluate = EVALUATE_EVAL
            elif ("@{0}".format(name) in self._obj):
                return Wrap.load(
                    self["@{0}".format(name)].url, 
                    self["@{0}".format(name)].root)
            else:
                value = None
            
            return Wrap.return_value(value, evaluate, self.context)
        except Exception as ex:
            return None

    def __len__(self):
        return len(self._obj)
    
    def __iter__(self):
        return WrapIter(self._obj.__iter__(), self.context, self.evaluate)
        
    def __getitem__(self, key):
        try:
            value = self._obj[key]
            
            return Wrap.return_value(value, self.evaluate, self.context)
        except Exception as ex:
            return None
    
    def __repr__(self):
        return repr(self._obj)

    def __str__(self):
        return str(self._obj)
    
    def to_json(self):
        return json.dumps(self._obj)
    
    def to_object(self):
        return self._obj
    
    def to_dict(self):
        if (isinstance(self._obj, dict)):
            result = dict()
            
            for k in self._obj.keys():
                if (k[:2] == "[[" and k[-2:] == "]]"):
                    key = k[2:-2]
                    
                    result[key] = getattr(self, key)
                elif (k[:1] == "[" and k[-1:] == "]"):
                    key = k[1:-1]
                    
                    result[key] = getattr(self, key)
                else:
                    result[k] = getattr(self, k)
            
            return result
        else:
            raise TypeError("Wrapped object is not a dictionary")

    def to_list(self):
        if (isinstance(self._obj, list)):
            result = list()
            
            for i in range(len(self._obj)):
                result.append(self[i])
            
            return result
        else:
            raise TypeError("Wrapped object is not a list")
    
    @staticmethod
    def prepare_context(args):
        context = dict()
        
        for arg in args:
            if (re.match("[A-Za-z0-9_]+=.+", arg)):
                match = re.match("([A-Za-z0-9_]+)=(.+)", arg)
                
                context[match.group(1)] = match.group(2)
            else:
                context[arg] = True
        
        return context
    
    
    @classmethod
    def load(cls, url, root=None, context=None, evaluate=NO_EVAL):
        connection = urlopen(url)
        
        with connection:
            data = cls(json.load(connection), context, evaluate)
        
        connection.close()
        
        if root:
            return eval(f"data.{root}")
        else:
            return data


def evaluate_string(pattern, context, evaluate=True):
    
    def replace_context(match):
        var_name = re.search("\$\{([A-Za-z0-9_]+)\}", match.group(0)).group(1)
        
        return str(context.get(var_name))
    
    def replace_env(match):
        var_name = re.search("\$\[([A-Za-z0-9_]+)\]", match.group(0)).group(1)
        
        return str(getenv(var_name))
    
    s_context = re.sub("\$\{[A-Za-z0-9_]+\}", replace_context, pattern)
    s = re.sub("\$\[[A-Za-z0-9_]+\]", replace_env, s_context)
    
    if (evaluate):
        return eval(s)
    else:
        return s
