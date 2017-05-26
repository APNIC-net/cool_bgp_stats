# -*- coding: utf-8 -*-
"""
Created on Mon May 22 16:18:28 2017

@author: sofiasilva
"""
import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler
from VisibilityDBHandler import VisibilityDBHandler
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
                          
def generateFilesFromReadables(readables_path, existing_dates, files_path,
                               bgp_handler):    
    for readable_file in os.listdir(readables_path):
        if readable_file.endswith('readable'):
            file_date = bgp_handler.getDateFromFileName(readable_file)
            
            if file_date not in existing_dates:
                generateFilesFromReadableRoutingFile(files_path,
                                                     '{}/{}'.format(readables_path,
                                                                    readable_file),
                                                      bgp_handler)
                existing_dates.add(file_date)
    
    return existing_dates
    
   
def generateFilesFromOtherRoutingFiles(archive_folder, existing_dates,
                                       files_path, bgp_handler, proc_num,
                                       extension, RIBfile, COMPRESSED):
    
    # Routing files in the archive folder for dates that haven't been
    # inserted into the DB yet
    for root, subdirs, files in os.walk(archive_folder):
        for filename in files:
            date = bgp_handler.getDateFromFileName(filename)
        
            # For paralelization we check for the year of the file, so that
            # different files are processed by different scripts
            if filename.endswith(extension) and date not in existing_dates and\
                (date.year == 2006 + proc_num or date.year == 2018 - proc_num or\
                (proc_num == 1 and date.year == 2012)):

                full_filename = os.path.join(root, filename)
                readable_file = bgp_handler.getReadableFile(full_filename,
                                                            False, RIBfile,
                                                            COMPRESSED)

                generateFilesFromReadableRoutingFile(files_path,
                                                     readable_file,
                                                     bgp_handler)
                existing_dates.add(date)

    return existing_dates


def writeCSVandCTLfiles(filename_woExt, tuples, ctl_str):
    csv_filename = '{}.csv'.format(filename_woExt)

    with open(csv_filename, 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(tuples)
    
    ctl_filename = '{}.ctl'.format(filename_woExt)
    
    with open(ctl_filename, 'wb') as ctl_file:
        ctl_file.write(ctl_str.format(csv_filename))

def generateFilesForItem(name, item_list, files_path, routing_date):
    start = time()
    
    if name == 'prefixes':
        tuples = zip(item_list, [routing_date]*len(item_list))
        ctl_str = prefixes_ctl_str
    else:
        tuples = zip(item_list, [True]*len(item_list), [routing_date]*len(item_list))
        ctl_str = asns_ctl_str

    filename_woExt = '{}/{}_{}'.format(name, files_path, routing_date)
    writeCSVandCTLfiles(filename_woExt, tuples, ctl_str)
    
    end = time()
    sys.stdout.write('It took {} seconds to generate the CSV and CTL files for the insertion of {} for {}.\n'.format(end-start, name, routing_date))
    
def generateFilesFromReadableRoutingFile(files_path, routing_file, bgp_handler):
    isReadable = True
    RIBfile = False
    COMPRESSED = False
    
    start = time()
    prefixes, originASes, middleASes, routing_date =\
                        bgp_handler.getPrefixesASesAndDate(routing_file, isReadable,\
                                                    RIBfile, COMPRESSED)
    end = time()
    sys.stdout.write('It took {} seconds to get the lists of prefixes, origin ASes and middle ASes for {}.\n'.format(end-start, routing_date))

    try:
        generateFilesForItem('prefixes', prefixes, files_path, routing_date)
        
    except KeyboardInterrupt:
        sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
        generateFilesForItem('prefixes', prefixes, files_path, routing_date)
        generateFilesForItem('originASes', originASes, files_path, routing_date)
        generateFilesForItem('middleASes', middleASes, files_path, routing_date)
        sys.exit(0)
        
    try:
        generateFilesForItem('originASes', originASes, files_path, routing_date)
        
    except KeyboardInterrupt:
        sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
        generateFilesForItem('originASes', originASes, files_path, routing_date)
        generateFilesForItem('middleASes', middleASes, files_path, routing_date)
        sys.exit(0)
        
    try:
        generateFilesForItem('middleASes', middleASes, files_path, routing_date)
        
    except KeyboardInterrupt:
        sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
        generateFilesForItem('middleASes', middleASes, files_path, routing_date)
        sys.exit(0)
        
    
def main(argv):
    routing_file = ''
    files_path = '/home/sofia/BGP_stats_files'
    readables_path = ''
    archive_folder = '/data/wattle/bgplog'
    proc_num = -1
    DEBUG = False

    try:
        opts, args = getopt.getopt(argv,"hp:A:f:n:D", ['files_path=', 'archive_folder=', 'procNumber=', 'routingFile=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> (-A <archive folder> -n <process number> | -f <readable routing file>) [-D]'.format(sys.argv[0])
        print "p: Provide the path to a folder to use to save files."
        print "A: Provide the path to the folder containing hitorical routing data."
        print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
        print "OR"
        print "f: Provide the name of a routing file in readable format."
        print "D: DEBUG mode"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files path> (-A <archive folder> -n <process number> | -f <readable routing file>) [-D]'.format(sys.argv[0])
            print "p: Provide the path to a folder to use to save files."
            print "A: Provide the path to the folder containing hitorical routing data."
            print "AND"
            print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
            print "OR"
            print "f: Provide the name of a routing file in readable format."
            print "D: DEBUG mode"
            sys.exit()
        elif opt == '-p':
            files_path = os.path.abspath(arg)
        elif opt == '-A':
            archive_folder = os.path.abspath(arg)
        elif opt == '-n':
            proc_num = arg
        elif opt == '-f':
            routing_file = os.path.abspath(arg)
        elif opt == '-D':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
        

    if proc_num == -1:
        if routing_file == '':
            print "If you don't provide the path to a routing file you MUST provide a process number."
            sys.exit(-1)
    else:
        readables_path = '/home/sofia/BGP_stats_files/hist_part{}'.format(proc_num)
    
    bgp_handler = BGPDataHandler(DEBUG, files_path)
    
    if routing_file != '':
        generateFilesFromReadableRoutingFile(files_path, routing_file, bgp_handler)
    else:
        db_handler = VisibilityDBHandler()

        # We just consider a date to be ready if it is present in the DB
        # for prefixes, origin ASes AND middle ASes.
        existing_dates = set(db_handler.getListOfDatesForPrefixes()).\
                            intersection(set(db_handler.getListOfDatesForOriginASes())).\
                            intersection(set(db_handler.getListOfDatesForMiddleASes()))
        
        existing_dates = generateFilesFromReadables(readables_path,
                                                    existing_dates,
                                                    files_path,
                                                    bgp_handler)
        
        existing_dates = generateFilesFromOtherRoutingFiles(archive_folder,
                                                            existing_dates,
                                                            files_path,
                                                            bgp_handler,
                                                            proc_num,
                                                            'bgprib.mrt',
                                                            True, False)
        
        existing_dates = generateFilesFromOtherRoutingFiles(archive_folder,
                                                            existing_dates,
                                                            files_path,
                                                            bgp_handler,
                                                            proc_num,
                                                            'dmp.gz', False,
                                                            True)
                
    
if __name__ == "__main__":
    main(sys.argv[1:])