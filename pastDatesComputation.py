# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 13:22:04 2017

@author: sofiasilva

This script computes statistics for all the past dates for which we have routing and updates data.
"""
import sys, os, getopt
from datetime import date, datetime, timedelta
from RoutingStats import RoutingStats
from DBHandler import DBHandler
from StabilityAndDeaggDailyStats import StabilityAndDeagg
from computeRoutingStats import completeStatsComputation


def concatenateFiles(routing_file, v4_routing_file, v6_routing_file):
    with open(v4_routing_file, 'a') as v4_file:
        with open(v6_routing_file, 'r') as v6_file:
            for line in v6_file:
                v4_file.write(line)
                
    os.rename(v4_routing_file, routing_file)
    return routing_file
    
def getReadableForRoutingFile(routing_file, readables_folder):
    
    readable_filename = '.'.join(routing_file.split('/')[-1].split('.')[0:-1])
    
    readable_file = '{}/{}.readable'.format(readables_folder, readable_filename)
    
    if os.path.exists(readable_file):
        return readable_file
    else:
        return ''

def computationForDate(routing_date, DEBUG, files_path, readables_folder, log):    
    # Computation of routing stats
    log.write('{}: Starting computation of routing stats.\n'.format(datetime.now()))
    log.write('{}: Initializing variables and classes.\n'.format(datetime.now()))
    
    KEEP = False
    EXTENDED = True
    del_file = '{}/extended_apnic_{}.txt'.format(files_path, date.today())
    startDate_date = ''
    INCREMENTAL = False
    final_existing_date = ''
    prefixes_stats_file = ''
    ases_stats_file = ''
    TEMPORAL_DATA = True
    routingStatsObj = RoutingStats(files_path, DEBUG, KEEP, EXTENDED,
                                        del_file, startDate_date, routing_date,
                                        routing_date, INCREMENTAL,
                                        final_existing_date, prefixes_stats_file,
                                        ases_stats_file, TEMPORAL_DATA)
                                        
    db_handler = DBHandler('')
    available_routing_files = db_handler.getPathsToRoutingFilesForDate(routing_date)
    db_handler.close()
    
    if 'bgprib.mrt' in available_routing_files:
        # If there is a bgprib file available, I check whether I already have
        # the readable file for it in the readables folder
        routing_file = available_routing_files['bgprib.mrt']
        readable_routing_file = getReadableForRoutingFile(routing_file,
                                                          readables_folder)
        
        # If I do, I use it
        if readable_routing_file != '':
            routing_file = readable_routing_file
        
        # If I don't, I will use the bgprib.mrt file
    
    elif 'dmp.gz' in available_routing_files and 'v6.dmp.gz' in available_routing_files:
        # If there is not bgprib.mrt file available, but the two dmp files
        # (for v4 and v6) are available, I use them
        dmp_file = available_routing_files['dmp.gz']
        # If I already have a readable file, I use it
        readable_dmp = getReadableForRoutingFile(dmp_file, readables_folder)
        if readable_dmp == '':
            readable_dmp = routingStatsObj.bgp_handler.getReadableFile(dmp_file,
                                                                       False)

        v6dmp_file = available_routing_files['v6.dmp.gz']
        # If I already have a readable file, I use it
        readable_v6dmp = getReadableForRoutingFile(v6dmp_file, readables_folder)
        if readable_v6dmp == '':
            readable_v6dmp = routingStatsObj.bgp_handler.getReadableFile(v6dmp_file,
                                                                         False)
         
        routing_file = concatenateFiles('{}/{}_v4andv6.dmp.readable'\
                                            .format(files_path, routing_date),
                                            readable_dmp, readable_v6dmp)
    elif 'dmp.gz' in available_routing_files:
        # If there is only one of the dmp files available, I will work with it
        # but I'll print a message to the log
        log.write('Only the dmp.gz file is available for date {}. Computing stats only for IPv4.\n'.format(routing_date))
        
        dmp_file = available_routing_files['dmp.gz']
        # If I already have a readable file, I use it
        readable_dmp = getReadableForRoutingFile(dmp_file, readables_folder)

        if readable_dmp != '':
            routing_file = readable_dmp
        else:
            routing_file = dmp_file
            
    elif 'v6.dmp.gz' in available_routing_files:
        # If there is only one of the dmp files available, I will work with it
        # but I'll print a message to the log
        log.write('Only the v6.dmp.gz file is available for date {}. Computing stats only for IPv6.\n'.format(routing_date))
        
        v6dmp_file = available_routing_files['v6.dmp.gz']
        # If I already have a readable file, I use it
        readable_v6dmp = getReadableForRoutingFile(v6dmp_file, readables_folder)
        
        if readable_v6dmp != '':
            routing_file = readable_v6dmp
        else:
            routing_file = v6dmp_file
    
    else:
        # This should never happen. At least one routing file for a date must be available
        log.write('No routing file is available for date {}\n'.format(routing_date))
        return False
        
    log.write('{}: Loading structures.\n'.format(datetime.now()))
                                                                         
    loaded = routingStatsObj.bgp_handler.loadStructuresFromRoutingFile(routing_file)
    
    if loaded:
        loaded = routingStatsObj.bgp_handler.loadUpdatesDF(routing_date)
    
    if not loaded:
        log.write('{}: Data structures not loaded! Aborting.\n'.format(datetime.now()))
        sys.exit()
    
    dateStr = 'Delegated_BEFORE{}'.format(routing_date)
    dateStr = '{}_AsOf{}'.format(dateStr, routing_date)
    
    file_name = '%s/routing_stats_%s' % (files_path, dateStr)
    
    log.write('{}: Starting actual computation of routing stats.\n'.format(datetime.now()))
    es_host = 'twerp.rand.apnic.net'
    completeStatsComputation(routingStatsObj, files_path, file_name, dateStr,
                                 TEMPORAL_DATA, es_host, prefixes_stats_file,
                                 ases_stats_file)
                                 
    # Computation of stats about updates rate and probability of deaggregation
    log.write('{}: Starting to compute stats about the updates rates and the probability of deaggregation.\n'.format(datetime.now()))
                   
    StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path, routing_date,
                                               routing_file, es_host)
    
    StabilityAndDeagg_inst.computeAndSaveStabilityAndDeaggDailyStats()
    
    return True

def getCompleteDatesSet(proc_num):
    yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                        4:[2014, 2015], 5:[2016, 2017]}
    initial_date = date(yearsForProcNums[proc_num][0], 1, 1)
    final_date = date(yearsForProcNums[proc_num][-1], 12, 31)

    yesterday = date.today() - timedelta(1)
    if final_date > yesterday:
        final_date = yesterday
        
    numOfDays = (final_date - initial_date).days
    return set([final_date - timedelta(days=x) for x in range(0, numOfDays)])
    
def main(argv):
    DEBUG = False
    files_path = '/home/sofia/past_dates_computation'

    try:
        opts, args = getopt.getopt(argv,"hp:n:D", ['files_path=', 'procNumber=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files_path> -n <process number> [-D]'.format(sys.argv[0])
        print "p: Files path. Path to a folder in which generated files will be saved."
        print "n: Provide a process number from 1 to 5, which allows the script to compute stats for a subset of the past dates."
        print "D: DEBUG mode"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files_path> -n <process number> [-D]'.format(sys.argv[0])
            print "p: Files path. Path to a folder in which generated files will be saved."
            print "n: Provide a process number from 1 to 5, which allows the script to compute stats for a subset of the past dates."
            print "D: DEBUG mode"
            sys.exit()
        elif opt == '-p':
            if arg != '':
                files_path = os.path.abspath(arg)
        elif opt == '-n':
            try:
                proc_num = int(arg)
                
                if proc_num < 1 or proc_num > 5:
                    print "The process number MUST be between 1 and 5."
                    sys.exit(-1)
                    
            except ValueError:
                print "The process number MUST be a number."
                sys.exit(-1)
        elif opt == '-D':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
    
    log_file = '{}/pastDatesComputation_{}_{}.log'.format(files_path, proc_num, date.today())
    log = open(log_file, 'w')
    
    readables_folder = '/home/sofia/BGP_stats_files/hist_part{}'.format(proc_num)

    dates_set = getCompleteDatesSet(proc_num)
    
    for past_date in dates_set:
        computationForDate(past_date, DEBUG, files_path, readables_folder, log)
        
    log.close()