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
from DelegatedHandler import DelegatedHandler
from BulkWHOISParser import BulkWHOISParser
from BGPDataHandler import BGPDataHandler
from RoutingStats import RoutingStats
from StabilityAndDeaggDailyStats import StabilityAndDeagg
from ElasticSearchImporter import ElasticSearchImporter
from computeRoutingStats import partialPrefixStats, partialASesStats
from multiprocessing.pool import Pool
from contextlib import closing


def computeRouting(date_to_work_with, numOfProcs, files_path, DEBUG, BulkWHOIS,
                   bgp_handler, del_handler, es_host):

    dateStr = 'Delegated_BEFORE{}'.format(date_to_work_with)
    dateStr = '{}_AsOf{}'.format(dateStr, date_to_work_with)
    file_name = '{}/RoutingStats/RoutingStats_{}'.format(files_path, dateStr)
    TEMPORAL_DATA = True
    routingStatsObj = RoutingStats(files_path, TEMPORAL_DATA)
                                        
    if es_host != '':
        esImporter = ElasticSearchImporter(es_host)
    else:
        esImporter = None

    if BulkWHOIS:
        # If we are in the parent process of the first fork, we call BulkWHOISParser
        # Instantiation of the BulkWHOISParser class
        sys.stdout.write('{}: Executing BulkWHOISParser.\n'.format(datetime.now()))
        BulkWHOISParser(files_path, DEBUG)

    # Computation of routing stats
    sys.stdout.write('{}: Starting to compute routing stats.\n'.format(datetime.now()))
    
    fork_pid = os.fork()
    
    if fork_pid == 0:
        # If we are in the child process of the third fork,
        # we compute stats for prefixes
        sys.stdout.write('{}: Starting to compute routing stats for prefixes.\n'.format(datetime.now()))
        
        delegatedNetworks = del_handler.delegated_df[\
                                (del_handler.delegated_df['resource_type'] == 'ipv4') |\
                                (del_handler.delegated_df['resource_type'] == 'ipv6')].reset_index()

        # TODO Remove after debugging
        delegatedNetworks = delegatedNetworks[0:10]
        prefixes_stats_file = '{}_prefixes.csv'.format(file_name)
        partialPrefixStats({'routingStatsObj' : routingStatsObj,
                            'bgp_handler' : bgp_handler,
                            'files_path' : files_path,
                            'delegatedNetworks' : delegatedNetworks,
                            'fullASN_df' : del_handler.fullASN_df,
                            'prefixes_stats_file' : prefixes_stats_file,
                            'TEMPORAL_DATA' : TEMPORAL_DATA,
                            'dateStr': dateStr,
                            'es_host' : es_host,
                            'esImporter' : esImporter})
        
        
#        pref_parts_size = int(round(float(delegatedNetworks.shape[0])/numOfProcs))
#
#        argsDicts = []
#        pref_pos = 0
#        
#        for i in range(numOfProcs+1):
#            partial_pref_stats_file = '{}_prefixes_{}.csv'.format(file_name, i)
#            if not os.path.exists(partial_pref_stats_file):
#                routingStatsObj.writeStatsFileHeader(routingStatsObj.allVar_pref,
#                                                     partial_pref_stats_file)
#    
#                argsDicts.append({'routingStatsObj' : routingStatsObj,
#                                    'bgp_handler' : bgp_handler,
#                                    'files_path' : files_path,
#                                    'delegatedNetworks' : delegatedNetworks[pref_pos:pref_pos+pref_parts_size],
#                                    'fullASN_df' : del_handler.fullASN_df,
#                                    'prefixes_stats_file' : partial_pref_stats_file,
#                                    'TEMPORAL_DATA' : TEMPORAL_DATA,
#                                    'dateStr' : dateStr,
#                                    'es_host' : es_host,
#                                    'esImporter' : esImporter})
#
#                pref_pos = pref_pos + pref_parts_size
#                
#        with closing(Pool(numOfProcs)) as pref_pool:
#            pref_pool.map(partialPrefixStats, argsDicts)
#            pref_pool.terminate()
#            
        sys.exit(0)

    else:
        # If we are in the parent process of the third fork,
        # we compute stats for ASes
        expanded_del_asns_df = del_handler.getExpandedASNsDF()
    
        # TODO Remove after debugging
        expanded_del_asns_df = expanded_del_asns_df[0:10]
#        ases_stats_file = '{}_ases.csv'.format(file_name)
#        partialASesStats({'routingStatsObj' : routingStatsObj,
#                         'bgp_handler' : bgp_handler,
#                         'expanded_ases_df' : expanded_del_asns_df,
#                         'ases_stats_file' : ases_stats_file,
#                         'TEMPORAL_DATA' : TEMPORAL_DATA,
#                         'dateStr' : dateStr,
#                         'es_host' : es_host,
#                         'esImporter' : esImporter})

        ases_parts_size = int(round(float(expanded_del_asns_df.shape[0])/numOfProcs))
    
        argsDicts = []
        ases_pos = 0
        
        for i in range(numOfProcs+1):
            partial_ases_stats_file = '{}_ases_{}.csv'.format(file_name, i)
            if not os.path.exists(partial_ases_stats_file):
                routingStatsObj.writeStatsFileHeader(routingStatsObj.allVar_ases,
                                                     partial_ases_stats_file)
                
                argsDicts.append({'routingStatsObj' : routingStatsObj,
                                     'bgp_handler' : bgp_handler,
                                     'expanded_ases_df' : expanded_del_asns_df[ases_pos:ases_pos+ases_parts_size],
                                     'ases_stats_file' : partial_ases_stats_file,
                                     'TEMPORAL_DATA' : TEMPORAL_DATA,
                                     'dateStr' : dateStr,
                                     'es_host' : es_host,
                                     'esImporter' : esImporter})
                
                ases_pos = ases_pos + ases_parts_size

        with closing(Pool(numOfProcs)) as ases_pool:
            ases_pool.map(partialASesStats, argsDicts)
            ases_pool.terminate()
            
        os.waitpid(fork_pid, 0)
        
        
        
def computeStatsForDate(date_to_work_with, numOfProcs, files_path, routing_file,
                        del_handler, ROUTING, STABILITY, DEAGG_PROB, BulkWHOIS,
                        ELASTIC):
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
            computeRouting(date_to_work_with, numOfProcs, files_path, DEBUG,
                           BulkWHOIS, bgp_handler, del_handler, es_host)
            
            os.waitpid(fork1_pid, 0)

    elif ROUTING:
        computeRouting(date_to_work_with, numOfProcs, files_path, DEBUG,
                       BulkWHOIS, bgp_handler, del_handler, es_host)
    
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
    numOfProcs = 1
    ROUTING = False
    STABILITY = False
    DEAGG_PROB = False
    ELASTIC = False

    try:
        opts, args = getopt.getopt(argv,"hRSDE", [])
    except getopt.GetoptError:
        print 'Usage: {} -h | [-n <num of processes>] [-R] [-S] [-D] [-E]'.format(sys.argv[0])
        print "The data from the files in /data/wattle/bgplog/YYYY/MM/DD will be inserted into the DB, being YYYYMMDD the date of today"
        print "In order to be sure all the needed data is available, statistics will be computed for yesterday."
        print "n = Number of parallel processes for computation of routing stats."
        print "The main process will be forked so that one process takes care of the computation of stats about BGP updates, another one takes care of the computation of stats about deaggregation and another one takes care of the computation of routing stats."
        print "Besides, the process in charge of computing routing stats will be forked so that one subprocess takes care of the computation of stats for prefixes and another one takes care of the computation of stats for ASes."
        print "Finally, each of these subprocesses will be divided into a pool of n threads in order to compute the stats in parallel for n subsets of prefixes/ASes."
        print "Therefore, if the three flags R, S and D are used there will be (2 + 2*n) parallel processes in total."
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
        elif opt == '-n':
            try:
                numOfProcs = int(arg)
            except ValueError:
                print "The number of processes MUST be a number!"
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
        
    insertionForDate(date.today())

    files_path = '/home/sofia/BGP_stats_files'
    
    yesterday = date.today() - timedelta(1)
    
    DEBUG = False
    EXTENDED = True
    del_file = '{}/delegated_extended_apnic_{}.txt'.format(files_path, date.today())
    INCREMENTAL = False
    final_existing_date = ''
    KEEP = False
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL,
                                   final_existing_date, KEEP)
                                   
    del_handler.delegated_df = del_handler.delegated_df[del_handler.delegated_df['date'] <= yesterday] 

    # We call this function with BulkWHOIS = True because we want the Bulk WHOIS
    # data to be updated daily.
    # When this function is called by pastDatesComputation, it is called with
    # BulkWHOIS = False
    computeStatsForDate(yesterday, numOfProcs, files_path, '', del_handler,
                        ROUTING, STABILITY, DEAGG_PROB, True, ELASTIC)

if __name__ == "__main__":
    main(sys.argv[1:])