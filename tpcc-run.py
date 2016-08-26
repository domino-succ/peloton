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
    cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z control"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 12):
    cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z ml"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

####################
for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o -l -j"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o -l"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o -l -c -j"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o -l -c"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()
#####################
for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o -j"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o -c -j"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -o -c"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()
#######################
for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -j"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()
    
for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()

for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -c -j"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()
    
for i in range(1, 12):
   cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z hash -c"
   pid = subprocess.Popen(cmd, shell=True)
   pid.wait()
#######################

for i in range(1, 12):
    cmd = "./src/tpcc -k 0.01 -b" + " " + str(i) + " " + "-p occ -g co -d 5 -w 11 -n 1 -z cluster -m 1 -x 10000"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()


########################################################################################################
#   Warehouse from 1 to n
#######################################################################################################

#for i in range(4, 12):
#    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w" + " " + str(i) + " " + " -n 1 -z control"
#    pid = subprocess.Popen(cmd, shell=True)
#    pid.wait()
#
#for i in range(4, 12):
#    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w" + " " + str(i) + " " + " -n 1 -z ml"
#    pid = subprocess.Popen(cmd, shell=True)
#    pid.wait()
#
##for i in range(4, 12):
##    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w" + " " + str(i) + " " + " -n 1 -z hash -l"
##    pid = subprocess.Popen(cmd, shell=True)
##    pid.wait()
##
##for i in range(4, 12):
##    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w" + " " + str(i) + " " + " -n 1 -z hash"
##    pid = subprocess.Popen(cmd, shell=True)
##    pid.wait()
#
#for i in range(4, 12):
#    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w" + " " + str(i) + " " + " -n 1 -z cluster -m 1 -x 10000"
#    pid = subprocess.Popen(cmd, shell=True)
#    pid.wait()

########################################################################################################
#   Request speed from 1000 to 12000 per second
#######################################################################################################
for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -z control -v" + " " + str(i)
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -z ml -v" + " " + str(i)
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

#############################
for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o -l -j"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o -l"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o -l -c -j"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o -l -c"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()
#####################
for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o -j"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o -c -j"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()
    
for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -o -c"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()
#####################
for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -j"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -c -j"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()
    
for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -v" + " " + str(i) + " " + "-z hash -c"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()
####################
for i in range(1000, 12000, 1000):
    cmd = "./src/tpcc -b 11 -k 0.01 -p occ -g co -d 5 -w 11 -n 1 -z cluster -m 1 -x 10000 -v" + " " + str(i)
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
