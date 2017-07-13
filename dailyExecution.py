# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 15:43:32 2017

@author: sofiasilva

This script inserts visibility, routing and updates data into the DB.
Then computes daily statistics about the update rate and the probability of deaggregation .
Then instantiates the BulkWHOISParser class in order to download the bulk WHOIS
files and load all the needed structures for the OrgHeuristics.
Finally, it computes the statistics about routing for yesterday.
"""
import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from datetime import date, datetime
from dailyInsertion import insertionForDate
from BulkWHOISParser import BulkWHOISParser
from BGPDataHandler import BGPDataHandler
from RoutingStats import RoutingStats
from StabilityAndDeaggDailyStats import StabilityAndDeagg
from computeRoutingStats import computeAndSavePerPrefixStats, computeAndSavePerASStats

def computeStatsForDate(date_to_work_with, files_path, routing_file, ROUTING,
                        STABILITY, DEAGG_PROB, BulkWHOIS, ELASTIC):
    DEBUG = False
    
    sys.stdout.write('{}: Initializing variables and classes.\n'.format(datetime.now()))
    
    if ELASTIC:
        es_host = 'twerp.rand.apnic.net'
    else:
        es_host = ''
        
    bgp_handler = BGPDataHandler(DEBUG, files_path)

    if ROUTING:        
        KEEP = False
        EXTENDED = True
        del_file = '{}/extended_apnic_{}.txt'.format(files_path, date.today())
        startDate_date = ''
        INCREMENTAL = False
        final_existing_date = ''
        dateStr = 'Delegated_BEFORE{}'.format(date_to_work_with)
        dateStr = '{}_AsOf{}'.format(dateStr, date_to_work_with)
        file_name = '{}/RoutingStats_{}'.format(files_path, dateStr)
        prefixes_stats_file = '{}_prefixes.csv'.format(file_name)
        ases_stats_file = '{}_asns.csv'.format(file_name)
        TEMPORAL_DATA = True
        routingStatsObj = RoutingStats(files_path, DEBUG, KEEP, EXTENDED,
                                            del_file, startDate_date, date_to_work_with,
                                            date_to_work_with, INCREMENTAL,
                                            final_existing_date, prefixes_stats_file,
                                            ases_stats_file, TEMPORAL_DATA)
    
    sys.stdout.write('{}: Loading structures.\n'.format(datetime.now()))
    
    loaded = True
    
    if ROUTING or DEAGG_PROB:
        loaded = bgp_handler.loadStructuresFromRoutingFile(routing_file)
    else:
        bgp_handler.routingDate = date_to_work_with
    
    if loaded and (ROUTING or STABILITY):
        loaded = bgp_handler.loadUpdatesDFs(bgp_handler.routingDate)
    
    if not loaded:
        sys.stdout.write('{}: Data structures not loaded! Aborting.\n'.format(datetime.now()))
        sys.exit()
    
    if STABILITY or DEAGG_PROB:
        # Computation of stats about updates rate and probability of deaggregation
        sys.stdout.write('{}: Starting to compute stats about the updates rates and the probability of deaggregation.\n'.format(datetime.now()))
        
        StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path, ELASTIC,
                                                   bgp_handler)
    
    fork1_pid = os.fork()
    
    if fork1_pid == 0:
        # If we are in the child process of the first fork, we fork again
        fork2_pid = os.fork()
        if fork2_pid == 0:
            if STABILITY:
                # If we are in the child process of the second fork, we compute some stats
                StabilityAndDeagg_inst.computeAndSaveStabilityDailyStats()
            sys.exit(0)
        else:
            if DEAGG_PROB:
                # If we are in the parent process of the second fork, we compute some other stats
                StabilityAndDeagg_inst.computeAndSaveDeaggDailyStats()
            os.waitpid(fork2_pid, 0)
            sys.exit(0)
    else:
        if ROUTING:
            if BulkWHOIS:
                # If we are in the parent process of the first fork, we call BulkWHOISParser
                # Instantiation of the BulkWHOISParser class
                sys.stdout.write('{}: Executing BulkWHOISParser.\n'.format(datetime.now()))
                BulkWHOISParser(files_path, DEBUG)
        
            # Computation of routing stats
            sys.stdout.write('{}: Starting computation of routing stats.\n'.format(datetime.now()))
            
            # and then we fork again
            fork3_pid = os.fork()
            
            if fork3_pid == 0:
                # If we are in the child process of the third fork,
                # we compute stats for prefixes
                computeAndSavePerPrefixStats(files_path, file_name, dateStr,
                                             routingStatsObj, bgp_handler,
                                             prefixes_stats_file,
                                             TEMPORAL_DATA, es_host)
                sys.exit(0)
        
            else:
                # If we are in the parent process of the third fork,
                # we compute stats for ASes
                computeAndSavePerASStats(files_path, file_name, dateStr,
                                         routingStatsObj, bgp_handler,
                                         ases_stats_file, TEMPORAL_DATA, es_host)
                os.waitpid(fork3_pid, 0)
        
        os.waitpid(fork1_pid, 0)
    
    
    sys.stdout.write('{}: Cleaning up.\n'.format(datetime.now()))
    
    # TODO Uncomment when we are sure everything is OK
#    sys.stdout.write('{}: Removing readable file {}.\n'.format(datetime.now(), readable_routing_file))
#    os.remove(readable_routing_file)

def main(argv):
    date_to_work_with = ''
    ROUTING = False
    STABILITY = False
    DEAGG_PROB = False
    ELASTIC = False

    try:
        opts, args = getopt.getopt(argv,"hd:RSDE", ['date_to_work_with=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -d <Date to work with> [-R] [-S] [-D] [-E]'.format(sys.argv[0])
        print "d: Date of the files whose data you want to be inserted into the DB. Format YYYYMMDD."
        print "The data from the files in /data/wattle/bgplog/YYYY/MM/DD will be inserted into the DB."
        print "If this option is not used, insertions of the data from the files in the folder corresponding to today will be inserted into the DB."
        print "R: Routing. Compute statistics about routing (for prefixes and ASes)."
        print "S: Stability. Compute statistics about stability (update rate)."
        print "D: Deaggregation probability. Compute statistics about the probability of deaggregation for each routed prefix."
        print "E: elasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -d <Date to work with> [-R] [-S] [-D] [-E]'.format(sys.argv[0])
            print "d: Date of the files whose data you want to be inserted into the DB. Format YYYYMMDD."
            print "The data from the files in /data/wattle/bgplog/YYYY/MM/DD will be inserted into the DB."
            print "If this option is not used, insertions of the data from the files in the folder corresponding to today will be inserted into the DB."
            print "R: Routing. Compute statistics about routing (for prefixes and ASes)."
            print "S: Stability. Compute statistics about stability (update rate)."
            print "D: Deaggregation probability. Compute statistics about the probability of deaggregation for each routed prefix."
            print "E: elasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
            sys.exit()
        elif opt == '-d':
            if arg != '':
                try:
                    date_to_work_with = date(int(arg[0:4]),
                                             int(arg[4:6]),
                                             int(arg[6:8]))
                except ValueError:
                    print "If you use the option -D you MUST provide a date in format YYYYMMDD."
                    sys.exit(-1)
            else:
                print "If you use the option -D you MUST provide a date in format YYYYMMDD."
                sys.exit(-1)
        elif opt == '-R':
            ROUTING = True
        elif opt == '-S':
            STABILITY = True
        elif opt == '-D':
            DEAGG_PROB = True
        elif opt == '-E':
            ELASTIC = True
        else:
            assert False, 'Unhandled option'
    
    if date_to_work_with == '':
        date_to_work_with = date.today()
        
    readable_routing_file = readable_routing_file = insertionForDate(date_to_work_with)

    files_path = '/home/sofia/daily_execution'

    # We call this function with BulkWHOIS = True because we want the Bulk WHOIS
    # data to be updated daily.
    # When this function is called by pastDatesComputation, it is called with
    # BulkWHOIS = False
    computeStatsForDate(date_to_work_with, files_path, readable_routing_file,
                        ROUTING, STABILITY, DEAGG_PROB, True, ELASTIC)

if __name__ == "__main__":
    main(sys.argv[1:])