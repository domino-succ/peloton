#!/usr/bin/python2.4
#
# Small script to show PostgreSQL and Pyscopg together
#

#from subprocess import call
import subprocess
import os
import shutil

filename = "./tpcc.summary"
output = "./tpccave.summary"

if os.path.exists(output):
    os.remove(output)

result = open(output, "a")

with open(filename) as inf:
    content = [x.rstrip("\n") for x in inf]
    count = len(content)
    for i in range(0, count, 10):
        print count
        print i
        print "========="
        ### throughput ###
        throughput = [x.split()[0:1] for x in content[i:i+10]]
        elements_t = []
        for item in throughput:
            elements_t.append(float(item[0]))
        ave_throughput = sum(elements_t)/len(elements_t)
        ### abort rate ###
        abort = [x.split()[1:2] for x in content[i:i+10]]
        elements_a = []
        for item in abort:
            elements_a.append(float(item[0]))
        ave_abort = sum(elements_a)/len(elements_a)
        ### delay ###
        delay = [x.split()[2:3] for x in content[i:i+10]]
        elements_d = []
        for item in delay:
            elements_d.append(float(item[0]))
        ave_delay = sum(elements_d)/len(elements_d)
        ### thread ###
        thread = [x.split()[6:7] for x in content[i:i+10]]
        elements_c = []
        for item in thread:
            elements_c.append(float(item[0]))
        ave_thread = sum(elements_c)/len(elements_c)

        result.write(str(ave_throughput) + ' ' + str(ave_abort) + ' '+ str(ave_delay) + ' '+str(ave_thread)+ '\n')

result.close()
