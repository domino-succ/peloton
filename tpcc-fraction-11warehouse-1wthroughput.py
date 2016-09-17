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

########### literal max single 
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -o -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -o -l -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

##########  literal max pair
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -o -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -o -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()


########### literal sum single
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -l -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

##########  literal sum pair
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

##############################################################

###########  canonical max single
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -o -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -o -l -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

###########  canonical max pair
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -o -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -o -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

############ canonical sum single 
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -l -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

############ canonical sum pair
cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
   cmd = "./src/tpcc -k 0.1 -b 11 -p occ -g co -d 5 -w 11 -n 1 -z hash -c -f -h"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()


########################################################################################################
#   Request speed  10,000 per second
#######################################################################################################

############### literal max single 
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -o -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()

for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -o -l -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

############### literal max pair
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -o -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()
    
for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -o -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

############### literal sum single 
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()
    
for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -l -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

############### literal sum pair 
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()
    
for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

####################################################################################

############### canonical max single
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -o -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()
    
for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -o -l -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

############### canonical max pair
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -o -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()
    
for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -o -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

############### canonical sum single
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -l -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()
    
for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -l -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

############### canonical sum pair
cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -j -h"
pid = subprocess.Popen(cmd, shell=True)
pid.wait()
    
for i in range(1, 11):
    cmd = "./src/tpcc -b 11 -k 0.1 -p occ -g co -d 5 -w 11 -n 1 -v 10000 -z hash -c -f -h"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

result = open(output_file, "a")
files = os.listdir(search_dir)
########################################################################################################
#   Sort all files based on create time
#######################################################################################################
files = [os.path.join(search_dir, f) for f in files]
files.sort(key=lambda x: os.path.getmtime(x))

########################################################################################################
#   Pick out the last line of each file and put it into the summary file
#######################################################################################################
for filename in files:
    fl = filename
    with open(fl, "rb") as f:
        first = f.readline()
        f.seek(-2, 2)
        while f.read(1) != b"\n":
            f.seek(-2, 1)
        last = f.readline()
        print last
        result.write(last)
result.close()
