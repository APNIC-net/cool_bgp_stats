# -*- coding: utf-8 -*-
"""
Created on Mon May 22 16:18:28 2017

@author: sofiasilva
"""
import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler
from time import time
import csv

prefixes_ctl_str = '''LOAD CSV  
                          FROM '{}'
                              HAVING FIELDS 
                              (
                                  prefix,
                                  dateseen [date format 'YYYY-MM-DD']
                                  )
    
                          INTO postgresql://postgres@localhost/sofia?tablename=prefixes
                              TARGET COLUMNS
                              (
                                  prefix,
                                  dateseen
                                  )
     
                          WITH drop indexes, 
                              fields optionally enclosed by '"',  
                              fields escaped by double-quote,  
                              fields terminated by ','  
     
                          SET client_encoding to 'utf-8',  
                             work_mem to '512MB', 
                             maintenance_work_mem to '1GB', 
                             standard_conforming_strings to 'on';'''

asns_ctl_str = '''LOAD CSV  
                      FROM '{}'
                          HAVING FIELDS 
                          (
                              asn,
                              isorigin,
                              dateseen [date format 'YYYY-MM-DD']
                              )

                      INTO postgresql://postgres:@localhost/sofia?tablename=asns
                          TARGET COLUMNS
                          (
                              asn,
                              isorigin,
                              dateseen
                              )
 
                      WITH drop indexes,
                          fields optionally enclosed by '"',  
                          fields escaped by double-quote,  
                          fields terminated by ','  
 
                      SET client_encoding to 'utf-8',  
                          work_mem to '512MB', 
                          maintenance_work_mem to '1GB', 
                          standard_conforming_strings to 'on';'''


def main(argv):
    routing_file = ''
    files_path = ''
    DEBUG = False

    try:
        opts, args = getopt.getopt(argv,"hp:f:D", ['files_path=', 'routingFile=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> -f <readable routing file> [-D]'.format(sys.argv[0])
        print "p: Provide the path to a folder to use to save files."
        print "f: Provide the name of a routing file in readable format."
        print "D: DEBUG mode"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files path> -f <readable routing file> [-D]'.format(sys.argv[0])
            print "p: Provide the path to a folder to use to save files."
            print "f: Provide the name of a routing file in readable format."
            print "D: DEBUG mode"
            sys.exit()
        elif opt == '-p':
            files_path = os.path.abspath(arg)
        elif opt == '-f':
            routing_file = os.path.abspath(arg)
        elif opt == '-D':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
    
    bgp_handler = BGPDataHandler(DEBUG, files_path)
    
    isReadable = True
    RIBfile = False
    COMPRESSED = False
    
    start = time()
    prefixes, originASes, middleASes, routing_date =\
                        bgp_handler.getPrefixesASesAndDate(routing_file, isReadable,\
                                                    RIBfile, COMPRESSED)
    end = time()
    sys.stderr.write('It took {} seconds to get the lists of prefixes, origin ASes and middle ASes.\n'.format(end-start))
    
    start = time()
    prefixes_tuples = zip(prefixes, [routing_date]*len(prefixes))

    prefixes_csv = '{}/prefixes_{}.csv'.format(files_path, routing_date)
    with open(prefixes_csv, 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(prefixes_tuples)
    
    prefixes_ctl = '{}/prefixes_{}.ctl'.format(files_path, routing_date)
    with open(prefixes_ctl, 'wb') as ctl_file:
        ctl_file.write(prefixes_ctl_str.format(prefixes_csv))
    
    end = time()
    sys.stderr.write('It took {} seconds to generate the CSV and CTL files for the insertion of prefixes.\n'.format(end-start))
    
    start = time()
    originASes_tuples = zip(originASes, [True]*len(originASes), [routing_date]*len(originASes))
    
    originASes_csv = '{}/originASes_{}.csv'.format(files_path, routing_date)
    with open(originASes_csv, 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(originASes_tuples)

    originASes_ctl = '{}/originASes_{}.ctl'.format(files_path, routing_date)
    with open(originASes_ctl, 'wb') as ctl_file:
        ctl_file.write(asns_ctl_str.format(originASes_csv))
    
    end = time()
    sys.stderr.write('It took {} seconds to generate the CSV and CTL files for the insertion of origin ASes.\n'.format(end-start))
        
    start = time()
    middleASes_tuples = zip(middleASes, [False]*len(middleASes), [routing_date]*len(middleASes))
    
    middleASes_csv = '{}/middleASes_{}.csv'.format(files_path, routing_date)
    with open(middleASes_csv, 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(middleASes_tuples)

    middleASes_ctl = '{}/middleASes_{}.ctl'.format(files_path, routing_date)
    with open(middleASes_ctl, 'wb') as ctl_file:
        ctl_file.write(asns_ctl_str.format(middleASes_csv))
    
    end = time()
    sys.stderr.write('It took {} seconds to generate the CSV and CTL files for the insertion of middle ASes.\n'.format(end-start))
    
if __name__ == "__main__":
    main(sys.argv[1:])