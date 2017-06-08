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
from datetime import datetime
import csv, re

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
                          
def getDatesOfExistingCSVs(files_path, existing_dates):
    for existing_file in os.listdir(files_path):
        if existing_file.endswith('.csv'):
            existing_dates.add(getDateFromFileName(existing_file))
    
    return existing_dates
            
def generateFilesFromReadables(readables_path, existing_dates, files_path,
                               bgp_handler, output_file):    
    for readable_file in os.listdir(readables_path):
        if readable_file.endswith('readable'):
            file_path = '{}/{}'.format(readables_path, readable_file)
            routing_date = getDateFromFile(file_path, output_file)
            
            if routing_date not in existing_dates:
                generateFilesFromReadableRoutingFile(files_path,
                                                     file_path,
                                                     bgp_handler)
                existing_dates.add(routing_date)
    
    return existing_dates


def getDatesForExistingReadables(files_path):
    readable_dates = set()    
    for root, subdirs, files in os.walk(files_path):
        for filename in files:
            if filename.endswith('readable'):
                readable_dates.add(getDateFromFileName(filename))

    return readable_dates                
   
def generateFilesFromOtherRoutingFiles(archive_folder, readable_dates,
                                       existing_dates, files_path, bgp_handler,
                                       proc_num, extension, output_file):
    
    # Routing files in the archive folder for dates that haven't been
    # inserted into the DB yet
    for root, subdirs, files in os.walk(archive_folder):
        for filename in files:
            date = getDateFromFileName(filename)
        
            # For paralelization we check for the year of the file, so that
            # different files are processed by different scripts
            if filename.endswith(extension) and date not in readable_dates and\
                (date.year == 2006 + proc_num or date.year == 2018 - proc_num or\
                (proc_num == 1 and date.year == 2012)):
                
                full_filename = os.path.join(root, filename)
                readable_file = bgp_handler.getReadableFile(full_filename, False)
                if readable_file == '':
                    with open(output_file, 'a') as output:
                        output.write('Got an empty readable file name for file {}\n'.format(full_filename))
                    continue
                
                readable_dates.add(date)
                
                routing_date = getDateFromFile(readable_file, output_file)
                if routing_date not in existing_dates:
                    generateFilesFromReadableRoutingFile(files_path,
                                                         readable_file,
                                                         bgp_handler)
                    existing_dates.add(routing_date)

    return existing_dates, readable_dates


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
    elif name == 'originASes':
        tuples = zip(item_list, [True]*len(item_list), [routing_date]*len(item_list))
        ctl_str = asns_ctl_str
    else: # name == 'middleASes'
        tuples = zip(item_list, [False]*len(item_list), [routing_date]*len(item_list))
        ctl_str = asns_ctl_str
        
    filename_woExt = '{}/{}_{}'.format(files_path, name, routing_date)
    writeCSVandCTLfiles(filename_woExt, tuples, ctl_str)
    
    end = time()
    sys.stdout.write('It took {} seconds to generate the CSV and CTL files for the insertion of {} for {}.\n'.format(end-start, name, routing_date))
    
def generateFilesFromReadableRoutingFile(files_path, routing_file, bgp_handler):    
    start = time()
    prefixes, originASes, middleASes, routing_date =\
                        bgp_handler.getPrefixesASesAndDate(routing_file)
    end = time()
    sys.stdout.write('It took {} seconds to get the lists of prefixes, origin ASes and middle ASes for {}.\n'.format(end-start, routing_date))

    if routing_date.year == 1970:
        os.remove(routing_file)
        file_date = getDateFromFileName(routing_file)
        routing_file = bgp_handler.getSpecificFilesFromArchive(file_date,
                                                               extension='bgprib.mrt')
        start = time()
        prefixes, originASes, middleASes, routing_date =\
                            bgp_handler.getPrefixesASesAndDate(routing_file)
        end = time()
        sys.stdout.write('It took {} seconds to get the lists of prefixes, origin ASes and middle ASes for {}.\n'.format(end-start, routing_date))

    if len(prefixes) > 0:
        try:
            generateFilesForItem('prefixes', prefixes, files_path, routing_date)
            
        except KeyboardInterrupt:
            sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
            generateFilesForItem('prefixes', prefixes, files_path, routing_date)
            
            if len(originASes) > 0:
                generateFilesForItem('originASes', originASes, files_path, routing_date)
                
            if len(middleASes) > 0:
                generateFilesForItem('middleASes', middleASes, files_path, routing_date)

            sys.exit(0)

    if len(originASes) > 0:
        try:
            generateFilesForItem('originASes', originASes, files_path, routing_date)
            
        except KeyboardInterrupt:
            sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
            generateFilesForItem('originASes', originASes, files_path, routing_date)
            
            if len(middleASes) > 0:
                generateFilesForItem('middleASes', middleASes, files_path, routing_date)

            sys.exit(0)

    if len(middleASes) > 0:        
        try:
            generateFilesForItem('middleASes', middleASes, files_path, routing_date)
            
        except KeyboardInterrupt:
            sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
            generateFilesForItem('middleASes', middleASes, files_path, routing_date)
            sys.exit(0)

# We assume the routing files have routing info for a single date,
# therefore we get the routing date from the first line of the file.
def getDateFromFile(file_path, output_file):        
    with open(file_path, 'rb') as readable_file:
        first_line = readable_file.readline()
        try:
            timestamp = float(first_line.split('|')[1])
        except IndexError:
            timestamp = ''
            for i in range(5):
                line = readable_file.readline()
                try:
                    timestamp = float(line.split('|')[1])
                    break
                except IndexError:
                    continue
            
            if timestamp == '':
                with open(output_file, 'a') as output:
                    output.write('Cannot get date from content of file {}\n'.format(file_path))
                return None
        return datetime.utcfromtimestamp(timestamp).date()
        
def getDateFromFileName(filename):        
    dates = re.findall('(?P<year>[1-2][9,0][0,1,8,9][0-9])[-_]*(?P<month>[0-1][0-9])[-_]*(?P<day>[0-3][0-9])',\
                filename)
                
    if len(dates) > 0:
        file_date = '{}{}{}'.format(dates[0][0], dates[0][1], dates[0][2])
        return datetime.strptime(file_date, '%Y%m%d').date()
    else:
        return None
    
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
            try:
                proc_num = int(arg)
            except ValueError:
                print "The process number MUST be a number."
                sys.exit(-1)
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
    
    output_file = '{}/{}_{}_{}.output'.format(files_path, sys.argv[0], proc_num, datetime.today().date())

    bgp_handler = BGPDataHandler(DEBUG, readables_path)
    
    if routing_file != '':
        generateFilesFromReadableRoutingFile(files_path, routing_file, bgp_handler)
    else:
        db_handler = VisibilityDBHandler()

        # We just consider a date to be ready if it is present in the DB
        # for prefixes, origin ASes AND middle ASes.
        existing_dates = set(db_handler.getListOfDatesForPrefixes()).\
                            intersection(set(db_handler.getListOfDatesForOriginASes())).\
                            intersection(set(db_handler.getListOfDatesForMiddleASes()))
        
        existing_dates = getDatesOfExistingCSVs(files_path, existing_dates)
        
        existing_dates = generateFilesFromReadables(readables_path,
                                                    existing_dates,
                                                    files_path,
                                                    bgp_handler,
                                                    output_file)
        
        readable_dates = getDatesForExistingReadables(readables_path)
        
        existing_dates, readable_dates = generateFilesFromOtherRoutingFiles(
                                            archive_folder,
                                            readable_dates,
                                            existing_dates,
                                            files_path,
                                            bgp_handler,
                                            proc_num,
                                            'bgprib.mrt',
                                            output_file)
        
        existing_dates, readable_dates = generateFilesFromOtherRoutingFiles(
                                            archive_folder,
                                            readable_dates,
                                            existing_dates,
                                            files_path,
                                            bgp_handler,
                                            proc_num,
                                            'dmp.gz',
                                            output_file)

    
if __name__ == "__main__":
    main(sys.argv[1:])