#! /usr/bin/env python
import multiprocessing 
from multiprocessing import Process, Queue

import sys
from scapy.all import *
import subprocess
import pandas 
import numpy
import numpy as np
from itertools import tee, izip

import dns.resolver

#to verify cronjob
#fo = open("/nethome/hramamurthy3/foo.txt", "w")
#print "Hello"
def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)

def worker(in_list, out_queue):
    #invoked in a process, must take url input from list
    #keep result in DataFrame that should be pushed to queue
    #index = list(xrange(len(df1)))
    f_index =0
    index2 = list(xrange(2*len(in_list)))
    
    columns = ['Domain', 'RHIP', 'TTL', 'AS', 'BGP Prefix', 'CC','Registry', 'IP Allocated', 'AS Name']
    df_features = pandas.DataFrame(index = index2, columns = columns)
    df_features[['Domain']] = df_features[['Domain']].astype(str)
    df_features[['RHIP']] = df_features[['RHIP']].astype(str)
    df_features[['TTL']] = df_features[['TTL']].astype(str)
    df_features[['AS']] = df_features[['AS']].astype(str)
    df_features[['BGP Prefix']] = df_features[['BGP Prefix']].astype(str)
    df_features[['CC']] = df_features[['CC']].astype(str)
    df_features[['Registry']] = df_features[['Registry']].astype(str)
    df_features[['IP Allocated']] = df_features[['IP Allocated']].astype(str)
    df_features[['AS Name']] = df_features[['AS Name']].astype(str)
    #print df1
    #print 'process id:',os.getpid()

    #Take input properly and run in loop
    my_pid = os.getpid()
    filename_path = "/nethome/phasthanthar3"
    file_name = filename_path + "features_dns_db_multi%06d.csv"%(os.getpid())
    print file_name
    out_queue.put(file_name)

    for domain in in_list:

        try:
            rhip = dns.resolver.query(domain)
         
            var3 = rhip.rrset.ttl
        except dns.resolver.NXDOMAIN:
            df_features.ix[(f_index), 'Domain'] = domain
            df_features.ix[(f_index), 'RHIP'] = "NXDOMAIN"
            df_features.ix[(f_index), 'TTL'] = "NXDOMAIN"
            df_features.ix[(f_index), 'AS'] ="NXDOMAIN"
            df_features.ix[(f_index), 'BGP Prefix'] = "NXDOMAIN"
            df_features.ix[(f_index), 'CC'] = "NXDOMAIN"
            df_features.ix[(f_index), 'Registry'] = "NXDOMAIN"
            df_features.ix[(f_index), 'IP Allocated'] = "NXDOMAIN"
            f_index = f_index +1
            continue
        except:
            df_features.ix[(f_index), 'Domain'] = domain
            df_features.ix[(f_index), 'RHIP'] = "Null resolver"
            df_features.ix[(f_index), 'TTL'] = "Null resolver"
            df_features.ix[(f_index), 'AS'] ="Null resolver"
            df_features.ix[(f_index), 'BGP Prefix'] = "Null resolver"
            df_features.ix[(f_index), 'CC'] = "Null resolver"
            df_features.ix[(f_index), 'Registry'] = "Null resolver"
            df_features.ix[(f_index), 'IP Allocated'] = "Null resolver"
            f_index = f_index +1
            continue
        as_number = [None]* len(rhip)
        bgp_prefix = [None]* len(rhip)
        country = [None]* len(rhip)
        registry = [None]* len(rhip)
        alloc = [None]* len(rhip)
        as_name = [None]* len(rhip)     
        for i in range(len(rhip)):
             #print str(rhip[i])
             var2 = rhip[i]
            
             string = "whois -h whois.cymru.com -v " + str(rhip[i])
             try:
                 proc = subprocess.Popen(string, shell = True, stdout = subprocess.PIPE)
             except:
                df_features.ix[(f_index), 'Domain'] = domain
                df_features.ix[(f_index), 'RHIP'] = "Null whois"
                df_features.ix[(f_index), 'TTL'] = "Null whois"
                df_features.ix[(f_index), 'AS'] ="Null whois"
                df_features.ix[(f_index), 'BGP Prefix'] = "Null whois"
                df_features.ix[(f_index), 'CC'] = "Null whois"
                df_features.ix[(f_index), 'Registry'] = "Null whois"
                df_features.ix[(f_index), 'IP Allocated'] = "Null from whois"
                f_index = f_index + 1
                df_features.to_csv(file_name, sep = ',')
                continue
             
             output = proc.stdout.read()
       
             mod_out = '\n'.join(output.split('\n')[1:])
             final_out = mod_out.split('|')
             try:
                    temp = '\n'.join(final_out[6].split('\n')[1:])
                    as_number[i] = temp
                    bgp_prefix[i] = final_out[8]
                    country[i] = final_out[9]
                    registry[i] = final_out[10]
                    alloc[i] = final_out[11]
                    as_name[i] = final_out[12]
             except IndexError:
                    continue
             try:      
                    df_features.ix[(f_index+i), 'Domain'] = domain
                    df_features.ix[(f_index+i), 'RHIP'] = str(rhip[i])
                    df_features.ix[(f_index+i), 'TTL'] = str(var3)
                    df_features.ix[(f_index+i), 'AS'] =str(as_number[i])
                    df_features.ix[(f_index+i), 'BGP Prefix'] = str(bgp_prefix[i])
                    df_features.ix[(f_index+i), 'CC'] = str(country[i])
             	    df_features.ix[(f_index+i), 'Registry'] = str(registry[i])
             	    df_features.ix[(f_index+i), 'IP Allocated'] = str(alloc[i])
            	    df_features.ix[(f_index+i), 'AS Name'] = str(as_name[i])
                    #df_features.to_csv(file_name, sep= ',')
             except:
                   continue
        f_index = f_index+i+1
    

    #need to output csv
    df_features.to_csv(file_name, sep= ',')
    #Need to send process ID to access the csv
    


#main starts here
if __name__ == '__main__':
    #filename_path = "/home/pragna/Documents/ECE8813_AdvCompSec/Project/Output_CSV_files/11_17_2015/"
    num_proc = 4
    out_file = "weka_features_final.csv"
    try:
        if sys.argv[1] == "-":
            urls = sys.stdin.readlines()
        else:
            urls = open(sys.argv[1]).readlines()
        if len(sys.argv) >= 3:
            out_file = sys.argv[2]
    except:
        print "Usage: %s <file with URLs to fetch> [<file to write the features>]" % sys.argv[0]
        raise SystemExit


    # Make a queue with just input domains
    in_queue = []
    index =0
    for url in urls:
        url = url.strip()
        if not url or url[0] == "#":
            continue
        index = index + 1
        in_queue.append(url)

    #check args
    assert in_queue, "no URLs given"
    num_urls = len(in_queue)
    print num_urls
    chunksize = int(math.ceil(num_urls / num_proc))

    #use out_queue to get process ids for combining output files
    out_queue =multiprocessing.Queue()
    procs = []
    for i in range(num_proc):
        p = multiprocessing.Process(
                target=worker,
                args=(in_queue[chunksize * i:chunksize * (i + 1)], out_queue ))
        procs.append(p)
        p.start()


    #wait for all workers to finish
    for p in procs:
        p.join()
    print "Join completed"

    list_file_name = [out_queue.get() for p in procs]
    #for p in procs:
    #    list_file_name.append(out_queue.get())
    #    print list_file_name[j]

#starting post processing here
#A list of data frames
df =[]
for file_n in list_file_name:
    #final_file = filename_path + "features_zeus_11_13_multi" + str(proc_id) + ".csv"
    data_frame = pandas.read_csv(file_n)
    df.append(data_frame)

print "concatenating dfs"
df_test = pandas.concat(df)
index = list(xrange(len(df_test)))

#print len(df_test)
#print df1
df_test['Index'] = index
df_test.dropna(inplace = True)
df_test.to_csv( "pDNS_data_collected_test100.csv", sep = ",")
df_test = df_test.set_index('Index')
#print len(df_test)

#df_test.to_csv('test_weka.csv', sep= ',')


columns = ['Domain', 'Distinct IP', 'Distinct BGP Prefix', 'Distinct AS', 'Countries', 'TTL']
df2 = pandas.DataFrame(index= index, columns = columns)
#df1['AS Name'] = df1['AS Name'].str.lstrip('"').str.rstrip('\n"')
#df1['AS Name'] =  df1['AS Name'].map(lambda x: str(x)[:-9])
counter = 0
counter_ip = 1
counter_bgp = 1
counter_as = 1
counter_cc = 1

for (i1,r1), (i2,r2) in pairwise(df_test.iterrows()):

    if r2['Domain'] == r1['Domain']:

        counter += 1
        counter_ip = counter_ip + 1
        
        if r2['AS'] != r1['AS']:
            counter_as = counter_as + 1
            
            
        if r2['BGP Prefix'] != r1['BGP Prefix']:
            counter_bgp = counter_bgp + 1
            
        if r2['CC'] != r1['CC']:
            counter_cc = counter_cc + 1


        continue
        



    if counter != 0:
        df2.ix[i1, 'Domain'] =  r1['Domain']
        df2.ix[i1, 'Distinct IP'] =  counter_ip
        df2.ix[i1, 'Distinct BGP Prefix'] = counter_bgp
        df2.ix[i1, 'Distinct AS'] = counter_as
        df2.ix[i1, 'Countries'] = counter_cc
    
        df2.ix[i1, 'TTL'] = r1['TTL']
        counter = 0
        #continue

    else:
        counter_ip = 1
        counter_bgp = 1
        counter_as = 1
        counter_cc = 1

        df2.ix[i1, 'Domain'] =  r1['Domain']
        df2.ix[i1, 'Distinct IP'] =  counter_ip
        df2.ix[i1, 'Distinct BGP Prefix'] = counter_bgp
        df2.ix[i1, 'Distinct AS'] = counter_as
        df2.ix[i1, 'Countries'] = counter_cc
        df2.ix[i1, 'TTL'] = r1['TTL']

df2.index.name = 'Index'
df2.dropna(inplace = True)
#df2.to_csv("weka_11_15_final.csv", sep = ",")
df2.to_csv( out_file, sep = ",")

