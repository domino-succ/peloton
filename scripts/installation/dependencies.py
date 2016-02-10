#!/usr/bin/env python
# encoding: utf-8

## ==============================================
## GOAL : Install Dependencies
## ==============================================

import sys
import shlex
import shutil
import tempfile
import os
import time
import logging
import argparse
import pprint
import numpy
import re
import fnmatch
import string
import subprocess
import tempfile

## ==============================================
## LOGGING CONFIGURATION
## ==============================================

LOG = logging.getLogger(__name__)
LOG_handler = logging.StreamHandler()
LOG_formatter = logging.Formatter(
    fmt='%(asctime)s [%(funcName)s:%(lineno)03d] %(levelname)-5s: %(message)s',
    datefmt='%m-%d-%Y %H:%M:%S'
)
LOG_handler.setFormatter(LOG_formatter)
LOG.addHandler(LOG_handler)
LOG.setLevel(logging.INFO)

## ==============================================
## CONFIGURATION
## ==============================================

my_env = os.environ.copy()
FILE_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.join(os.path.dirname(FILE_DIR), os.pardir)
THIRD_PARTY_DIR = os.path.join(ROOT_DIR, "third_party")

NVML_DIR = os.path.join(THIRD_PARTY_DIR, "nvml")
NANOMSG_DIR = os.path.join(THIRD_PARTY_DIR, "nanomsg")

## ==============================================
## Utilities
## ==============================================

def exec_cmd(cmd):
    """
    Execute the external command and get its exitcode, stdout and stderr.
    """
    args = shlex.split(cmd)
    verbose = True

    # TRY
    FNULL = open(os.devnull, 'w')
    try:
        if verbose == True:
            subprocess.check_call(args, env=my_env)
        else:
            subprocess.check_call(args, stdout=FNULL, stderr=subprocess.STDOUT, env=my_env)
    # Exception
    except subprocess.CalledProcessError as e:
        print "Command     :: ", e.cmd
        print "Return Code :: ", e.returncode
        print "Output      :: ", e.output
    # Finally
    finally:
        FNULL.close()

def install_dependencies():
    
    print("Path at terminal when executing this file")
    print(os.getcwd() + "\n")

    print("This file path, relative to os.getcwd()")
    print(__file__ + "\n")
    
    print(FILE_DIR)
    print(ROOT_DIR)
    print(NVML_DIR)

    ## ==============================================
    ## NVM Library
    ## ==============================================
    LOG.info("Installing nvml library")
    os.chdir(NVML_DIR)
    cmd = 'make -j4'
    exec_cmd(cmd)
    cmd = 'sudo make install -j4'
    exec_cmd(cmd)
    os.chdir('..')

    LOG.info("Finished installing nvml library")

    ## ==============================================
    ## Nanomsg Library
    ## ==============================================
    LOG.info("Installing nanomsg library")
    os.chdir(NANOMSG_DIR)
    cmd = './configure'
    exec_cmd(cmd)
    cmd = 'make -j4'
    exec_cmd(cmd)
    cmd = 'sudo make install -j4'
    exec_cmd(cmd)
    os.chdir('..')

    LOG.info("Finished installing nanomsg library")

## ==============================================
## MAIN
## ==============================================
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Install Dependencies')

    args = parser.parse_args()

    try:
        prev_dir = os.getcwd()
        
        install_dependencies()

    finally:
        # Go back to prev dir
        os.chdir(prev_dir)

