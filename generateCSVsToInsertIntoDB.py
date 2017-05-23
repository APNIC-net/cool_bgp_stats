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


def main(argv):
    routing_file = ''

    try:
        opts, args = getopt.getopt(argv,"hf:", ['routingFile=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -f <routing file>'.format(sys.argv[0])
        print "Provide the name of a routing file in readable format."
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -f <readable routing file>'.format(sys.argv[0])
            print "Provide the name of a routing file in readable format."
            sys.exit()
        elif opt == '-f':
            routing_file = os.path.abspath(arg)
        else:
            assert False, 'Unhandled option'

    DEBUG = False
    files_path = '/home/sofia/BGP_stats_files'
    
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

    
    # columns=('prefix', 'dateseen')
    
    start = time()
    prefixes_tuples = zip(prefixes, [routing_date]*len(prefixes))
    with open('{}/prefixes_{}.csv'.format(files_path, routing_date), 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(prefixes_tuples)
    end = time()
    sys.stderr.write('It took {} seconds to generate the CSV file for the list of prefixes.\n'.format(end-start))

    
    # columns=('asn', 'isorigin', 'dateseen')
    
    start = time()
    originASes_tuples = zip(originASes, [True]*len(originASes), [routing_date]*len(originASes))
    with open('{}/originASes_{}.csv'.format(files_path, routing_date), 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(originASes_tuples)
    end = time()
    sys.stderr.write('It took {} seconds to generate the CSV file for the list of origin ASes.\n'.format(end-start))


    start = time()
    middleASes_tuples = zip(middleASes, [False]*len(middleASes), [routing_date]*len(middleASes))
    with open('{}/middleASes_{}.csv'.format(files_path, routing_date), 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(middleASes_tuples)
    end = time()
    sys.stderr.write('It took {} seconds to generate the CSV file for the list of middle ASes.\n'.format(end-start))

if __name__ == "__main__":
    main(sys.argv[1:])