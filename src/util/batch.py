#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jan 13 16:22:52 2018

@author: pasquale
"""

import importlib
import json
import logging
import logging.config
import re
import sys
import traceback

from argparse import ArgumentParser, REMAINDER

from .common import Wrap


class Batch(object):
    
    SYS_runtime_id = "SYS.runtime_id"
    
    def __init__(self, config, batch_name, batch, args):
        self.name = config.name
        self.config = config
        self.logger = logging.getLogger(config.logger)
        
        self._init_log()
        
        self.context = self._prepare_context(args)
        self.config.bind_context(self.context)
        self.batch = batch(config.batch_config[batch_name], self.context, self.logger)
    
    
    def _prepare_context(self, args):
        context = dict()
        
        for arg in args:
            if (re.match("[A-Za-z0-9_]+=.+", arg)):
                match = re.match("([A-Za-z0-9_]+)=(.+)", arg)
                
                context[match.group(1)] = match.group(2)
            else:
                context[arg] = True
        
        for (k,v) in context.items():
            self.logger.debug("Context: \"{0}\" : \"{1}\"".format(k, v))
        
        return context
    
    
    def _init_log(self):
        self.logger.info("")
        self.logger.info("")
        self.logger.info("<<<<<-----")
        self.logger.info("Batch \"{0}\" starting NOW".format(self.name))
        self.logger.info("----->>>>>")
        self.logger.info("")
        self.logger.info("")
    
    
    def __enter__(self):
        self.logger.debug("Entering Context Manager...")
        
        return self
    
    
    def execute(self):
        try:
            self.logger.debug("Running batch...")
            
            rc = self.batch.execute()
            
            self.logger.debug("Batch executed properly. Exit code is '{0}'".format(rc))
            
            return rc if rc else 0
        except Exception as ex:
            self.logger.error("Batch execution error: {0} \"{1}\"".format(type(ex), ex))
            traceback.print_exc()
            
            return 1
    
    
    def __exit__(self, *args):
        self.logger.debug("Exiting Context Manager... (args={0})".format(args))
    
    
    @staticmethod 
    def validate(json):
        return json.get("name") is not None
    
    
    @classmethod
    def create(cls):
        parser = ArgumentParser()
        
        parser.add_argument("-l", "--log-config", help="logger config file", type=str, default="logging.conf", dest="log_config_file")
        parser.add_argument("-c", "--config", help="JSON config file", type=str, default="config.json", dest="json_config_file")
        parser.add_argument("-b", "--batch", help="Batch job class", type=str, required=True, dest="batch_class")
        parser.add_argument("args", nargs=REMAINDER)
        
        opts = Wrap(vars(parser.parse_args()))
        
        config = None
        
        # load new logger configuration from file
        if (opts.log_config_file is not None):
            try:
                logging.debug(
                    "Loading new logger configuration from file '{0}'".format(opts.log_config_file))
                
                # apply new configuration
                logging.config.fileConfig(opts.log_config_file)
                logging.info(
                    "New logger configuration applied from file '{0}'".format(opts.log_config_file))
            except Exception as ex:
                logging.error(
                    "Cannot apply new logger configuration from file '{0}'".format(opts.log_config_file))
                
                raise RuntimeError(
                    "Configuration error: Cannot apply new logger configuration from file '{0}'".format(opts.log_config_file), ex)
        
        # load JSON configuration
        if (opts.json_config_file is not None):
            try:
                with open(opts.json_config_file, "rt") as f:
                    config = json.load(f)
                    
                    if (not cls.validate(config)):
                        logging.error(
                            "Configuration not valid from file '{0}'".format(opts.json_config_file))
                        
                        raise RuntimeError(
                            "Configuration error: Configuration not valid from file '{0}'".format(opts.json_config_file))
            except Exception as ex:
                logging.error(
                    "Cannot load configuration from file '{0}'".format(opts.json_config_file))
                
                raise RuntimeError(
                    "Configuration error: Cannot load configuration from file '{0}'".format(opts.json_config_file), ex)
        
         # load Batch job class
        if (opts.batch_class is not None):
            try:
                dot = opts.batch_class.rfind(".")
                
                assert dot >= 0
                
                module_name = opts.batch_class[0:dot]
                class_name = opts.batch_class[(dot+1):]
                
                logging.debug("Loading class '{0}.{1}'".format(module_name, class_name))
            
                logging.debug("Loading module '{0}'...".format(module_name))
                module = importlib.import_module(module_name)
                
                logging.debug("Loading class '{0}'...".format(class_name))
                batch_class = eval(f"module.{class_name}")
            except Exception as ex:
                logging.error(
                    "Cannot load batch class '{0}".format(opts.batch_class))
                
                raise RuntimeError(
                    "Configuration error: Cannot load batch class '{0}'".format(opts.batch_class), ex)
        else:
            raise RuntimeError("Configuration error: Batch class undeclared")
                        
        return cls(Wrap(config), opts.batch_class, batch_class, opts.args.to_object())

if (__name__ == "__main__"):
    logging.info("Program starting...")
    
    batch = Batch.create()
    
    if (batch is None):
        logging.fatal("Cannot start program. Abnormal termination!")
        
        sys.exit(1)
    else:
        logging.info("Program starting...")
        
        with batch as b:
            exit_code = b.execute()
        
        logging.info("Exit code: {0}".format(exit_code))
        
        sys.exit(exit_code)
