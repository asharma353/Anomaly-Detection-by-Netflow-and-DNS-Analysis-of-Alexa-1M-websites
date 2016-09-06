#! /usr/bin/env python
import multiprocessing 
from multiprocessing import Process
import Queue
import sys
import subprocess 
import numpy
from scapy.all import *
import pandas 

pcap = sys.argv[1]
#outfile = sys.argv[2]


def is_valid_ip(ip):
    octets = ip.split(".")
    if(len(octets) !=4):
        return False
    for x in octets:
	if not x.isdigit():
		return False
	i = int(x)
	if i<0 or i>255:
		return False
    return True

#worker function definition
def worker(in_list):
    f_index =0
    index2 = list(xrange(len(in_list)))

    columns = ['Domain', 'RHIP', 'TTL', 'AS', 'BGP Prefix', 'CC', 'IP Allocated']
    df_features = pandas.DataFrame(index = index2, columns = columns)
    df_features[['Domain']] = df_features[['Domain']].astype(str)
    df_features[['RHIP']] = df_features[['RHIP']].astype(str)
    df_features[['TTL']] = df_features[['TTL']].astype(str)
    df_features[['AS']] = df_features[['AS']].astype(str)
    df_features[['BGP Prefix']] = df_features[['BGP Prefix']].astype(str)
    df_features[['CC']] = df_features[['CC']].astype(str)
    #df_features[['Registry']] = df_features[['Registry']].astype(str)
    df_features[['IP Allocated']] = df_features[['IP Allocated']].astype(str)
    #df_features[['AS Name']] = df_features[['AS Name']].astype(str)
    #define output file name
    file_name = "/nethome/hramamurthy3/csv/features_pcap_29_10k_%06d.csv"%(os.getpid())


    for i in range(len(in_list)):
        domain, ip_addr, ttl = in_list[i]
        as_number = [None]* 1
        bgp_prefix = [None]* 1
        country = [None]* 1
        #registry = [None]* len(rhip)
        alloc = [None]* 1
        as_name = [None]* 1

        string = "whois -h whois.cymru.com -v " + str(ip_addr)
        try:
            proc = subprocess.Popen(string, shell = True, stdout = subprocess.PIPE)
        except:
            df_features.to_csv(filename, sep = ',')
            continue

        output = proc.stdout.read()
        mod_out = '\n'.join(output.split('\n')[1:])
        final_out = mod_out.split('|')
        try:
            temp = '\n'.join(final_out[6].split('\n')[1:])
            as_number[0] = temp
            bgp_prefix[0] = final_out[8]
            country[0] = final_out[9]
            #registry[0] = final_out[10]
            alloc[0] = final_out[11]
            as_name[0] = final_out[12]
        except IndexError:
            continue
        try:    
            df_features.ix[(f_index), 'Domain'] = domain
            df_features.ix[(f_index), 'RHIP'] = str(ip_addr)
            df_features.ix[(f_index), 'TTL'] = str(ttl)
            df_features.ix[(f_index), 'AS'] = str(as_number[0])
            df_features.ix[(f_index), 'BGP Prefix'] = str(bgp_prefix[0])
            df_features.ix[(f_index), 'CC'] = str(country[0])
            #df_features.ix[(f_index), 'Registry'] = str(registry[0])
            df_features.ix[(f_index), 'IP Allocated'] = str(alloc[0])
            #df_features.ix[(f_index), 'AS Name'] = str(as_name[0])
        except:
            continue

        f_index = f_index+1
        #i = i+1

    df_features.to_csv(file_name, sep= ',')
#print "Success", domain



#main starts here
if __name__=='__main__':
    num_conn = 12
    pkts = rdpcap(pcap)
    #index1 = list(xrange(5*len(pkts)))

    #f_index =0
    a= 0
    in_list = []
    for p in pkts:
        #print a
        #a += 1
        if p.haslayer(DNS):
            if (p[DNS].qr == 1L):
                domain = p[DNSQR].qname
                #print domain
                if (p[DNS].ancount != 0):
                    count = p[DNS].ancount
                    #as_number = [None]* count
                    #bgp_prefix = [None]* count
                    #country = [None]* count
                    #registry = [None]* count
                    #alloc = [None]* count
                    #as_name = [None]* count
                    i = 0
                    while i < count:
                        if (is_valid_ip(p[DNSRR][i].rdata)):
                            #print p[DNSRR][i].rdata
                            ip_addr = p[DNSRR][i].rdata
                            ttl = p[DNSRR][i].ttl
                            in_list.append((domain, ip_addr, ttl ))
                            
                        i = i+1

    #Starting the worker processes
    assert in_list, "no URLs given"
    num_urls = len(in_list)
    chunksize = int(math.ceil(num_urls / num_conn))
    procs = []
    for i in range(num_conn):
        p = multiprocessing.Process(
                target=worker,
                args=(in_list[chunksize * i:chunksize * (i + 1)], ))
        procs.append(p)
        p.start()

    #wait for all workers to finish
    for i in range(num_conn):
        p.join()
