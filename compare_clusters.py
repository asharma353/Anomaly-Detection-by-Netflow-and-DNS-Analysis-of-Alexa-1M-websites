import sys
import csv


if(len(sys.argv) <3 ):
    print "Enter atleast one output and one input file"
    exit
else:
    readers = []
    dicts= {}
    dn = []
    k=[]
    v=[]
    i=0
    arguments = sys.argv[2:]
    for x in arguments:
        readers.append(csv.reader(open(x,"rb")))
        i=i+1
    #dictlist = [dict() for x in arguments] 
        
    #print dictlist
    j=0
    for row in readers:
        
        for rows in readers[j]:
            k=rows[2]
            k2=rows[0]
            v=rows[8]
            if k in dicts:
                dicts[k][k2] = v
            else:
                dicts[k]={k2:v} 
                
        j += 1

    #print dicts

    
        
    writer = csv.writer(open(sys.argv[1], 'wb'))
    for key, value in dicts.items():
        if len([item for item in value if item]) > 3:
            a = 0
            flag = 0
            for key1,value1 in value.items():
                #print value
                if a == 0:
                   a = value1
                elif a != value1:
                    flag = 1
                    break
            if flag == 1:
                writer.writerow([key, value])            
            
           
