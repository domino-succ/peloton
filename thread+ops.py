#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

#from subprocess import call
import subprocess
import os
import shutil

search_dir = "./ycsb-output"
output_file = "./ycsb.summary"
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

for i in range(1, 9):
    cmd = "./src/ycsb -b" + " " + str(i) + " " + "-k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o 2 -q control"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b" + " " + str(i) + " " + "-k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o 2 -q queue"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b" + " " + str(i) + " " + "-k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o 2 -q detect"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b" + " " + str(i) + " " + "-k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o 2 -q ml"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b" + " " + str(i) + " " + "-k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o 2 -q range"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

########################################################################################################
#   Multiple opertions per txn
#######################################################################################################
for i in range(1, 9):
    cmd = "./src/ycsb -b 8 -k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o" + " " + str(i) + " " + "-q control"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b 8 -k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o" + " " + str(i) + " " + "-q queue"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b 8 -k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o" + " " + str(i) + " " + "-q detect"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b 8 -k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o" + " " + str(i) + " " + "-q ml"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

for i in range(1, 9):
    cmd = "./src/ycsb -b 8 -k 10 -u 1 -z 0.99 -p occ -g co -d 5 -w 0 -o" + " " + str(i) + " " + "-q range"
    pid = subprocess.Popen(cmd, shell=True)
    pid.wait()

result = open("./ycsb.summary", "a")
files = os.listdir(search_dir)
files = [os.path.join(search_dir, f) for f in files]
files.sort(key=lambda x: os.path.getmtime(x))

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
