# -*- coding: utf-8 -*-
"""
Created on Thu May 11 09:48:57 2017

@author: sofiasilva
"""

"""
Deaggregation And Stability Evolution

1) Stability of announcements. Measure update rate.
    a) Compare update rate of more specific prefixes to update rate of less specific prefixes. Are more specific prefixes more unstable than less specific prefixes?
    b) Compare update rate of prefixes allocated/assigned in the last two years to update rate of prefixes allocated/assigned more than 2 years ago. Are recently allocated/assigned prefixes more unstable than more mature prefixes?

2) Deaggregation. Count allocated/assigned prefixes that are being deaggregated. Compare probability of deaggregation for prefixes allocated/assigned in the last two years to the probability of deaggregation for prefixes allocated/assigned more than two years ago. Are recently allocated/assigned prefixes more likely to be deaggregated than more mature prefixes?
"""

import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
import sys, getopt
from BGPDataHandler import BGPDataHandler
from StabilityAndDeaggDailyStats import StabilityAndDeagg
from datetime import datetime
from calendar import monthrange

def concatenateFiles(routing_file, v4_routing_file, v6_routing_file):
    with open(v4_routing_file, 'a') as v4_file:
        with open(v6_routing_file, 'r') as v6_file:
            for line in v6_file:
                v4_file.write(line)
                
    os.rename(v4_routing_file, routing_file)
    return routing_file
    
def main(argv):
    files_path = ''
    DEBUG = False
    startDate = None
    endDate = None
    readables_folder = ''
    archive_folder = ''
    es_host = ''
    
    try:
        opts, args = getopt.getopt(argv, "hp:s:e:R:A:DE:", ["files_path=", "startDate=", "endDate=", "readables_folder=", "archive_folder=", "ElasticSearch_host=",])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> -s <Start date> -e <End Date> -R <readables folder> -A <archive folder> [-D] [-E <ElasticSearch host>]'.format(sys.argv[0])
        sys.exit(-1)
        
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statitics about BGP updates and deaggregation in the BGP routing table for a gvien period of time."
            print 'Usage: {} -h | -p <files path> -s <Start date> -e <End Date> -R <readables folder> -A <archive folder> [-D] [-E <ElasticSearch host>]'.format(sys.argv[0])
            print "h: Help"
            print "p: Path to folder in which files will be saved. (MANDATORY)"
            print "s: Start. Initial date for the period of time for which statistics will be computed. (MANDATORY)"
            print "e: End. Final date for the period of time for which statistics will be computed. (MANDATORY)"
            print "R: Readables folder. Path to the folder with readable routing and/or updates files."
            print "A: Archive folder. Path to the folder with historical routing and/or updates data."
            print "D: Debug mode. Use this option if you want the script to run in debug mode."
            print "E: Insert compute statistics into ElasticSearch. The hostname of the ElasticSearch host MUST be provided if this option is used."
            sys.exit(0)
        elif opt == '-p':
            if arg != '':
                files_path = os.path.abspath(arg)
            else:
                print "If option -p is used, the path to a folder in which files will be saved MUST be provided."
                sys.exit(-1)
        elif opt == '-s':
            if arg != '':
                startDate = arg
            else:
                print "Option -s MUST be used providing a start date."
                sys.exit(-1)
        elif opt == '-e':
            if arg != '':
                endDate = arg
            else:
                print "Option -e MUST be used providing an end date."
                sys.exit(-1)
        elif opt == '-R':
            if arg != '':
                readables_folder = os.path.abspath(arg)
            else:
                print "Option -R MUST be used providing the path to a folder with readable routing and/or updates files."
                sys.exit(-1)
        elif opt == '-A':
            if arg != '':
                archive_folder = os.path.abspath(arg)
            else:
                print "Option -A MUST be used providing the path to the archive folder with readable historical routing and/or updates data."
                sys.exit(-1)
        elif opt == '-D':
            DEBUG = True
        elif opt == '-E':
            if arg != '':
                es_host = arg
            else:
                print "If option -E is used, the name of a host running ElasticSearch must be provided."
                sys.exit(-1)
        else:
            assert False, 'Unhandled option'
            
    try:
        if len(startDate) == 4:
            startDate_date = datetime.strptime(startDate, '%Y').date()
        elif len(startDate) == 6:
            startDate_date = datetime.strptime(startDate, '%Y%m').date()
        elif len(startDate) == 8:
            startDate_date = datetime.strptime(startDate, '%Y%m%d').date()
        else:
            print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
            sys.exit()
    except ValueError:
        print "Error when parsing start date.\n"
        print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
        sys.exit()
        
    if len(endDate) == 4:
        endYear = endDate
        endMonth = '12'
        endDay = monthrange(int(endYear), int(endMonth))[1]
    elif len(endDate) == 6:
        endYear = endDate[0:4]
        endMonth = endDate[4:6]
        endDay = monthrange(int(endYear), int(endMonth))[1]
    elif len(endDate) == 8:
        endYear = endDate[0:4]
        endMonth = endDate[4:6]
        endDay = endDate[6:8]
    else:
        print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
        sys.exit()

    try:  
        endDate_date = datetime.strptime('{}{}{}'.format(endYear, endMonth, endDay), '%Y%m%d').date()   
    except ValueError:
        print "Error when parsing end date.\n"
        print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
        sys.exit()
        
    bgp_handler = BGPDataHandler(DEBUG, files_path)

    # We first process the readable files
    readables_dict = bgp_handler.getPathsToHistoricalData_dict(startDate_date,
                                                               endDate_date,
                                                               readables_folder,
                                                               'readable')
    
    # We then complete with the routing files for dates for which we do not
    # have readable files
    all_files_dict = bgp_handler.getPathsToHistoricalData_dict(startDate_date,
                                                               endDate_date,
                                                               archive_folder, '',
                                                               readables_dict)
    
    
    for date in all_files_dict:
        v4_routing_ready = False
        v6_routing_ready = False
        routing_file = ''
        updates_file = ''
        v4_routing_file = ''
        v6_routing_file = ''
        
        if 'readable' in all_files_dict[date]:
            for r_file in all_files_dict[date]['readable']:
                if 'bgpupds' in r_file and updates_file == '':
                    updates_file = r_file
                    
                elif 'bgprib' in r_file and routing_file == '':
                    routing_file = r_file
                    v4_routing_ready = True
                    v6_routing_ready = True
        
                elif 'v6' in r_file and not v6_routing_ready:
                    v6_routing_file = r_file
                    v6_routing_ready = True
                    
                elif not v4_routing_ready:
                    v4_routing_file = r_file
                    v4_routing_ready = True
                    
                if updates_file != '' and (routing_file != '' or\
                    (v4_routing_file != '' and v6_routing_file != '')):
                        break
                    
            if routing_file == '' and v4_routing_file != '' and v6_routing_file != '':
                routing_file = concatenateFiles('{}/{}.readable'.format(\
                                                    files_path, date),
                                                v4_routing_file,
                                                v6_routing_file)
                
            if routing_file != '' and updates_file != '':
                StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path,
                                                           updates_file,
                                                           routing_file, es_host)
                StabilityAndDeagg_inst.computeAndSaveStabilityAndDeaggDailyStats(\
                                                                    bgp_handler)
        
                continue

        if 'bgprib.mrt' in all_files_dict[date] and routing_file == '':
            # There shouldn't be more than one bgprib.mrt file for a specific
            # date. But even if there is, we take the first one in the list.
            routing_file = all_files_dict[date]['bgprib.mrt'][0]
            v4_routing_ready = True
            v6_routing_ready = True
            
        if 'dmp.gz' in all_files_dict[date] and\
            not v4_routing_ready or not v6_routing_ready:
            
            for rou_file in all_files_dict[date]['dmp.gz']:
                if 'v6' in rou_file and not v6_routing_ready:
                    v6_routing_file = rou_file
                    v6_routing_ready = True

                elif not v4_routing_ready:
                    v4_routing_file = rou_file
                    v4_routing_ready = True

                if v4_routing_ready and v6_routing_ready:
                    if not v4_routing_file.endswith('readable'):
                        v4_routing_file = bgp_handler.getReadableFile(v4_routing_file,
                                                                      False)
                    
                    if not v6_routing_file.endswith('readable'):
                        v6_routing_file = bgp_handler.getReadableFile(v6_routing_file,
                                                                      False)
                                                                      
                    routing_file = concatenateFiles('{}/{}.readable'.format(\
                                                        files_path, date),
                                                    v4_routing_file,
                                                    v6_routing_file)
                    break
            
        if 'bgpupds' in all_files_dict[date] and updates_file == '':
            # There shouldn't be more than one bgprib.mrt file for a specific
            # date. But even if there is, we take the first one in the list.
            updates_file = all_files_dict[date]['bgpupds'][0]
        
        if routing_file != '' and updates_file != '':        
            StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path,
                                                       updates_file,
                                                       routing_file, es_host)
                                                       
            StabilityAndDeagg_inst.computeAndSaveStabilityAndDeaggDailyStats(bgp_handler)
        
        else:
            sys.stdout.write('Could not compute Stability and Deaggregation stats due to missing file(s).\n')
            if routing_file == '':
                sys.stdout.write('Routing file for date {} is missing.\n'.format(date))
            if updates_file == '':
                sys.stdout.write('Updates file for date {} is missing.\n'.format(date))
                
if __name__ == "__main__":
    main(sys.argv[1:])
