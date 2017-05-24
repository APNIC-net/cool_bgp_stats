# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 11:50:52 2017

@author: sofiasilva
"""

import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler
from time import time

def storeReadableFile(routing_file, bgp_handler):
    if routing_file.endswith('readable'):
        RIBfiles = False
        READABLE = True
        COMPRESSED = False
        
        bgp_handler.storeHistoricalDataFromFile(routing_file, READABLE, RIBfiles, COMPRESSED)    
            
def storeReadables(readables_path, dates_inserted, bgp_handler):
    # Available readable files
    readable_files = []
    for readable_file in os.listdir(readables_path):
        if readable_file.endswith('readable'):
            readable_files.append('{}/{}'.format(readables_path, readable_file))
            dates_inserted.append(readable_file.split('.')[0])
    
    RIBfiles = False
    READABLE = True
    COMPRESSED = False
    
    start = time()
    bgp_handler.storeHistoricalData(readable_files, READABLE, RIBfiles, COMPRESSED)
    end = time()
    sys.stderr.write('storeHistoricalData took {} seconds in total\n'.format(end-start))    
    
def storeBGPRibs(archive_folder, dates_inserted, bgp_handler):
    extension = 'bgprib.mrt'
    
    # bgprib.mrt files in the archive folder for which
    # we don't have readable files
    bgprib_files_list = []
    for root, subdirs, files in os.walk(archive_folder):
        for filename in files:
            date = filename.split('.')[0]
            year = int(date.split('-')[0])
            # For paralelization we check for the year of the file, so that
            # different files are processed by different scripts
            if filename.endswith(extension) and date not in dates_inserted and\
                (year == 2006 + int(sys.argv[1]) or year == 2018 - int(sys.argv[1]) or\
                (int(sys.argv[1]) == 1 and year == 2012)):
                bgprib_files_list.append(os.path.join(root, filename))
                dates_inserted.append(date)
    
    RIBfiles = True
    READABLE = False
    COMPRESSED = False
    
    start = time()
    bgp_handler.storeHistoricalData(bgprib_files_list, READABLE, RIBfiles, COMPRESSED)
    end = time()
    sys.stderr.write('It took {} seconds in total to insert the files {} into the DB.\n'.format(end-start, bgprib_files_list))

def storeDumps(archive_folder, dates_inserted, bgp_handler):                                                               
    extension = 'dmp.gz'
    
    # dmp.gz files in the archive folder that
    # haven't been inserted into the Visibility DB yet
    dmp_files_list = []
    for root, subdirs, files in os.walk(archive_folder):
        for filename in files:
            date = filename.split('.')[0]
            year = int(date.split('-')[0])
            if filename.endswith(extension) and\
                date not in dates_inserted and\
                (year == 2006 + int(sys.argv[1]) or year == 2018 - int(sys.argv[1]) or\
                (int(sys.argv[1]) == 1 and year == 2012)):
                dmp_files_list.append(os.path.join(root, filename))
    
    RIBfiles = False
    READABLE = False
    COMPRESSED = True
    
    bgp_handler.storeHistoricalData(dmp_files_list, READABLE, RIBfiles, COMPRESSED)

def main(argv):
    proc_num = -1
    routing_file = ''

    try:
        opts, args = getopt.getopt(argv,"hn:f:", ['procNumber=', 'routingFile=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -n <process number [1-5]> | -f <routing file>'.format(sys.argv[0])
        print "The process number is a number from 1 to 5 that allows the script to process a subset of the available files so that different scripts can process different files."
        print "Use -f option if you just want to insert routing data from a specific readable file."
        print "The file must have the 'readable' extension and be in readable format."
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -n <process number [1-5]> | -f <readable routing file>'.format(sys.argv[0])
            print "The process number is a number from 1 to 5 that allows the script to process a subset of the available files so that different scripts can process different files."
            print "Use -f option if you just want to insert routing data from a specific readable file."
            print "The file must have the 'readable' extension and be in readable format."
            sys.exit()
        elif opt == '-n':
            proc_num = arg
        elif opt == '-f':
            routing_file = os.path.abspath(arg)
        else:
            assert False, 'Unhandled option'
    
    archive_folder = '/data/wattle/bgplog'

    readables_path = '/home/sofia/BGP_stats_files/hist_part{}'.format(proc_num)

    DEBUG = False
    files_path = '/home/sofia/BGP_stats_files'
    
    bgp_handler = BGPDataHandler(DEBUG, files_path)
    
    dates_inserted = []

    if routing_file == '':
        storeReadables(readables_path, dates_inserted, bgp_handler)
        storeBGPRibs(archive_folder, dates_inserted, bgp_handler)
        storeDumps(archive_folder, dates_inserted, bgp_handler)
    else:
        storeReadableFile(routing_file, bgp_handler)
            
if __name__ == "__main__":
    main(sys.argv[1:])