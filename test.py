#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

#from subprocess import call
import subprocess
import os
import shutil

search_dir = "./tpcc-output"
output_file = "./tpcc.summary"
############################################
# Delete old output file and directory
############################################
if os.path.exists(search_dir):
    shutil.rmtree(search_dir)

if os.path.isfile(output_file):
    os.remove(output_file)

########################################################################################################
#   Thread from 1 to n
#######################################################################################################

for i in range(1, 12):
    cmd = "./src/tpcc -k 0.03 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -q ml -z 1"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

