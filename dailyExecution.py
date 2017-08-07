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
from datetime import date, datetime, timedelta
from dailyInsertion import insertionForDate
from BulkWHOISParser import BulkWHOISParser
from BGPDataHandler import BGPDataHandler
from RoutingStats import RoutingStats
from StabilityAndDeaggDailyStats import StabilityAndDeagg
from ElasticSearchImporter import ElasticSearchImporter
from computeRoutingStats import computeAndSavePerPrefixStats, computeAndSavePerASStats


def computeRouting(date_to_work_with, files_path, DEBUG, BulkWHOIS,
                   bgp_handler, es_host):
    KEEP = False
    EXTENDED = True
    del_file = '{}/extended_apnic_{}.txt'.format(files_path, date.today())
    startDate_date = ''
    dateStr = 'Delegated_BEFORE{}'.format(date_to_work_with)
    dateStr = '{}_AsOf{}'.format(dateStr, date_to_work_with)
    file_name = '{}/RoutingStats/RoutingStats_{}'.format(files_path, dateStr)
    prefixes_stats_file = '{}_prefixes.csv'.format(file_name)
    ases_stats_file = '{}_asns.csv'.format(file_name)
    TEMPORAL_DATA = True
    routingStatsObj = RoutingStats(files_path, DEBUG, KEEP, EXTENDED,
                                        del_file, startDate_date, date_to_work_with,
                                        date_to_work_with, TEMPORAL_DATA)
                                        
    if es_host != '':
        esImporter = ElasticSearchImporter(es_host)

    if BulkWHOIS:
        # If we are in the parent process of the first fork, we call BulkWHOISParser
        # Instantiation of the BulkWHOISParser class
        sys.stdout.write('{}: Executing BulkWHOISParser.\n'.format(datetime.now()))
        BulkWHOISParser(files_path, DEBUG)

    # Computation of routing stats
    sys.stdout.write('{}: Starting to compute routing stats.\n'.format(datetime.now()))
    
    # and then we fork again
    fork3_pid = os.fork()
    
    if fork3_pid == 0:
        # If we are in the child process of the third fork,
        # we compute stats for prefixes
        sys.stdout.write('{}: Starting to compute routing stats for prefixes.\n'.format(datetime.now()))
        
        if not os.path.exists(prefixes_stats_file):
            routingStatsObj.writeStatsFileHeader(routingStatsObj.allVar_pref, prefixes_stats_file)

            delegatedNetworks = routingStatsObj.del_handler.delegated_df[\
                            (routingStatsObj.del_handler.delegated_df['resource_type'] == 'ipv4') |\
                            (routingStatsObj.del_handler.delegated_df['resource_type'] == 'ipv6')]
    
            # TODO Remove after debugging
            delegatedNetworks = delegatedNetworks[0:10]
            
            computeAndSavePerPrefixStats(routingStatsObj, bgp_handler,
                                         delegatedNetworks, prefixes_stats_file,
                                         TEMPORAL_DATA, files_path, dateStr,
                                         es_host, esImporter)
        sys.exit(0)

    else:
        # If we are in the parent process of the third fork,
        # we compute stats for ASes
        if not os.path.exists(ases_stats_file):
            routingStatsObj.writeStatsFileHeader(routingStatsObj.allVar_ases, ases_stats_file)

            expanded_del_asns_df = routingStatsObj.del_handler.getExpandedASNsDF()
            
            # TODO Remove after debugging
            expanded_del_asns_df = expanded_del_asns_df[0:10]
        
            computeAndSavePerASStats(routingStatsObj, bgp_handler,
                                     expanded_del_asns_df, ases_stats_file,
                                     TEMPORAL_DATA, es_host, esImporter)
        
        os.waitpid(fork3_pid, 0)
        
        
def computeStatsForDate(date_to_work_with, files_path, routing_file, ROUTING,
                        STABILITY, DEAGG_PROB, BulkWHOIS, ELASTIC):
    DEBUG = False
    
    sys.stdout.write('{}: Initializing variables and classes.\n'.format(datetime.now()))
    
    if ELASTIC:
        es_host = 'twerp.rand.apnic.net'
    else:
        es_host = ''
        
    bgp_handler = BGPDataHandler(DEBUG, files_path)
    
    sys.stdout.write('{}: Loading structures.\n'.format(datetime.now()))
    
    loaded = True
    
    if ROUTING or DEAGG_PROB:
        if routing_file == '':
            routing_file = BGPDataHandler.getRoutingFileForDate(date_to_work_with,
                                                                files_path,
                                                                DEBUG)
            
            if routing_file == '':
                sys.stderr.write("{}: No routing file available for date {}\n".format(datetime.now(), date_to_work_with))
                loaded = False
            else:
                loaded = bgp_handler.loadStructuresFromRoutingFile(routing_file)
        else:
            loaded = bgp_handler.loadStructuresFromRoutingFile(routing_file)
    else:
        bgp_handler.routingDate = date_to_work_with
    
    if loaded and (ROUTING or STABILITY):
        loaded = bgp_handler.loadUpdatesDFs(bgp_handler.routingDate)
    
    if not loaded:
        sys.stdout.write('{}: Data structures not loaded! Aborting.\n'.format(datetime.now()))
        sys.exit()
    else:
        sys.stdout.write('{}: Data structures loaded successfully!\n'.format(datetime.now()))

    
    if STABILITY or DEAGG_PROB:
        # Computation of stats about updates rate and probability of deaggregation
        sys.stdout.write('{}: Instantiating the StabilityAndDeagg class.\n'.format(datetime.now()))
        
        StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path, ELASTIC,
                                                   bgp_handler)
    
    if (STABILITY or DEAGG_PROB) and ROUTING:
        fork1_pid = os.fork()
    
        if fork1_pid == 0:
            if STABILITY and DEAGG_PROB:
                # If we are in the child process of the first fork, we fork again
                fork2_pid = os.fork()
                if fork2_pid == 0:
                    # If we are in the child process of the second fork, we compute some stats
                    sys.stdout.write('{}: Starting to compute update rates.\n'.format(datetime.now()))
                    StabilityAndDeagg_inst.computeAndSaveStabilityDailyStats()                        
                    sys.exit(0)
                    
                else:
                    # If we are in the parent process of the second fork, we compute some other stats
                    sys.stdout.write('{}: Starting to compute statistics about deaggregation.\n'.format(datetime.now()))
                    StabilityAndDeagg_inst.computeAndSaveDeaggDailyStats()                                            
                    os.waitpid(fork2_pid, 0)
                    sys.exit(0)
                    
            elif STABILITY:
                sys.stdout.write('{}: Starting to compute update rates.\n'.format(datetime.now()))
                StabilityAndDeagg_inst.computeAndSaveStabilityDailyStats()                    
                sys.exit(0)
                
            elif DEAGG_PROB:
                sys.stdout.write('{}: Starting to compute statistics about deaggregation.\n'.format(datetime.now()))
                StabilityAndDeagg_inst.computeAndSaveDeaggDailyStats()
                sys.exit(0)
                
        else:
            computeRouting(date_to_work_with, files_path, DEBUG, BulkWHOIS,
                           bgp_handler, es_host)
            
            os.waitpid(fork1_pid, 0)

    elif ROUTING:
        computeRouting(date_to_work_with, files_path, DEBUG, BulkWHOIS,
                           bgp_handler, es_host)
    
    elif STABILITY and DEAGG_PROB:
        fork2_pid = os.fork()

        if fork2_pid == 0:
            # If we are in the child process of the second fork, we compute some stats
            sys.stdout.write('{}: Starting to compute update rates.\n'.format(datetime.now()))
            StabilityAndDeagg_inst.computeAndSaveStabilityDailyStats()
            sys.exit(0)
            
        else:
            # If we are in the parent process of the second fork, we compute some other stats
            sys.stdout.write('{}: Starting to compute statistics about deaggregation.\n'.format(datetime.now()))
            StabilityAndDeagg_inst.computeAndSaveDeaggDailyStats()
            os.waitpid(fork2_pid, 0)

    elif STABILITY:
        sys.stdout.write('{}: Starting to compute update rates.\n'.format(datetime.now()))
        StabilityAndDeagg_inst.computeAndSaveStabilityDailyStats()
        
    elif DEAGG_PROB:
        sys.stdout.write('{}: Starting to compute statistics about deaggregation.\n'.format(datetime.now()))
        StabilityAndDeagg_inst.computeAndSaveDeaggDailyStats()

# TODO Uncomment when we are sure everything is OK                        
#    sys.stdout.write('{}: Cleaning up.\n'.format(datetime.now()))
    
#    sys.stdout.write('{}: Removing readable file {}.\n'.format(datetime.now(), readable_routing_file))
#    os.remove(readable_routing_file)

def main(argv):
    ROUTING = False
    STABILITY = False
    DEAGG_PROB = False
    ELASTIC = False

    try:
        opts, args = getopt.getopt(argv,"hRSDE", [])
    except getopt.GetoptError:
        print 'Usage: {} -h | [-R] [-S] [-D] [-E]'.format(sys.argv[0])
        print "The data from the files in /data/wattle/bgplog/YYYY/MM/DD will be inserted into the DB, being YYYYMMDD the date of today"
        print "In order to be sure all the needed data is available, statistics will be computed for yesterday."
        print "R: Routing. Compute statistics about routing (for prefixes and ASes)."
        print "S: Stability. Compute statistics about stability (update rate)."
        print "D: Deaggregation probability. Compute statistics about the probability of deaggregation for each routed prefix."
        print "E: elasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print "The data from the files in /data/wattle/bgplog/YYYY/MM/DD will be inserted into the DB, being YYYYMMDD the date of today"
            print "In order to be sure all the needed data is available, statistics will be computed for yesterday."
            print "R: Routing. Compute statistics about routing (for prefixes and ASes)."
            print "S: Stability. Compute statistics about stability (update rate)."
            print "D: Deaggregation probability. Compute statistics about the probability of deaggregation for each routed prefix."
            print "E: elasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
            sys.exit()
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
        
    insertionForDate(date.today())

    files_path = '/home/sofia/BGP_stats_files'

    # We call this function with BulkWHOIS = True because we want the Bulk WHOIS
    # data to be updated daily.
    # When this function is called by pastDatesComputation, it is called with
    # BulkWHOIS = False
    computeStatsForDate(date.today() - timedelta(1), files_path, '', ROUTING, STABILITY,
                        DEAGG_PROB, True, ELASTIC)

if __name__ == "__main__":
    main(sys.argv[1:])