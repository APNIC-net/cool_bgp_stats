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
from DBHandler import DBHandler
from netaddr import IPNetwork
import pandas as pd
import subprocess, shlex
from datetime import datetime, date, timedelta
from ElasticSearchImporter import ElasticSearchImporter
import updatesStats_ES_properties
import deaggStats_ES_properties

class StabilityAndDeagg:
    def __init__(self, DEBUG, files_path, routing_date, routing_file, es_host):
        self.DEBUG = DEBUG
        self.files_path = files_path
        self.routing_date = routing_date
        self.routing_file = routing_file
        self.es_host = es_host
     
    @staticmethod
    def getDelegationDate(prefix):
        cmd = shlex.split("origindate -d ',' -f 1")
        cmd_echo = shlex.split("echo {}".format(prefix))
        p = subprocess.Popen(cmd_echo, stdout=subprocess.PIPE)
        output = subprocess.check_output(cmd, stdin=p.stdout)
        return datetime.strptime(output.split(',')[1], '%Y%m%d').date()
    
    @staticmethod
    def concatenateFiles(routing_file, v4_routing_file, v6_routing_file):
        with open(v4_routing_file, 'a') as v4_file:
            with open(v6_routing_file, 'r') as v6_file:
                for line in v6_file:
                    v4_file.write(line)
                    
        os.rename(v4_routing_file, routing_file)
        return routing_file
        
    def computeUpdatesStats(self, updates_df, stats_file):
        if updates_df.shape[0] > 0:
            for prefix, prefix_subset in updates_df.groupby('prefix'):
                del_date = self.getDelegationDate(prefix)
                network = IPNetwork(prefix)
                for update_date, date_subset in prefix_subset.groupby('update_date'):
                    del_age = (update_date - del_date).days
                    numOfAnn = len(date_subset[date_subset['upd_type'] == 'A']['prefix'].tolist())
                    numOfWith = len(date_subset[date_subset['upd_type'] == 'W']['prefix'].tolist())
                    
                    with open(stats_file, 'a') as s_file:
                        s_file.write('{}|{}|{}|{}|{}|{}|{}|{}\n'.format(prefix, del_date,
                                                                        update_date, del_age,
                                                                        network.version,
                                                                        network.prefixlen,
                                                                        numOfAnn, numOfWith))
    
    def computeDeaggregationStats(self, bgp_handler, stats_file):    
        for prefix, prefix_subset in bgp_handler.bgp_df.groupby('prefix'):
            del_date = self.getDelegationDate(prefix)
            
            network = IPNetwork(prefix)
            if network.version == 4:
                prefixes_radix = bgp_handler.ipv4Prefixes_radix
            else:
                prefixes_radix = bgp_handler.ipv6Prefixes_radix
                
            isRoot = False
            isRootDeagg = False
            
            # If the list of covering prefixes in the Radix tree has only 1 prefix,
            # it is the prefix itself, therefore the prefix is a root prefix
            if len(prefixes_radix.search_covering(prefix)) == 1:
                isRoot = True
        
                # If the list of covered prefix includes more prefixes than the prefix
                # itself, then the root prefix is being deaggregated.
                if len(bgp_handler.ipv4Prefixes_radix.search_covered(prefix)) > 1:
                    isRootDeagg = True
            
            with open(stats_file, 'a') as s_file:
                s_file.write('{}|{}|{}|{}|{}|{}\n'.format(prefix, del_date,
                                                            bgp_handler.routingDate,
                                                            (bgp_handler.routingDate -\
                                                            del_date).days,
                                                            isRoot, isRootDeagg))
    
    @staticmethod
    def generateJSONfile(stats_file):
        stats_df = pd.read_csv(stats_file, sep = ',')
        json_filename = '{}.json'.format(stats_file.split('.')[0])
        stats_df.to_json(json_filename, orient='index')
        sys.stderr.write("Stats saved to JSON and CSV files successfully!\n")
        sys.stderr.write("Files generated:\n{}\n\nand\n\n{}\n".format(stats_file,
                                                                    json_filename))
        return stats_df
    
    def importStatsIntoElasticSearch(self, stats_df, stats_name, es_properties):
        esImporter = ElasticSearchImporter(self.es_host)
        esImporter.createIndex(es_properties.mapping, es_properties.index_name)
        numOfDocs = esImporter.ES.count(es_properties.index_name)['count']
    
        stats_df = stats_df.fillna(-1)
        
        bulk_data, numOfDocs = esImporter.prepareData(stats_df,
                                                      es_properties.index_name,
                                                      es_properties.doc_type,
                                                      numOfDocs,
                                                      es_properties.unique_index)
                                            
        dataImported = esImporter.inputData(es_properties.index_name, bulk_data, numOfDocs)
    
        if dataImported:
            sys.stderr.write("Stats about {} were saved to ElasticSearch successfully!\n".format(stats_name))
        else:
            sys.stderr.write("Stats about {} could not be saved to ElasticSearch.\n".format(stats_name))
    
    def computeAndSaveStabilityAndDeaggDailyStats(self):
        bgp_handler = BGPDataHandler(self.DEBUG, self.files_path)

        if self.routing_file != '':
            routing_file = self.routing_file
        else:
            db_handler = DBHandler('')
            available_routing_files = db_handler.getPathsToRoutingFilesForDate(self.routing_date)
            db_handler.close()
            
            if 'bgprib.mrt' in available_routing_files:
                routing_file = available_routing_files['bgprib.mrt']
                
            elif 'dmp.gz' in available_routing_files and 'v6.dmp.gz' in available_routing_files:
                readable_v4 = bgp_handler.getReadableFile(available_routing_files['dmp.gz'])
                readable_v6 = bgp_handler.getReadableFile(available_routing_files['v6.dmp.gz'])
                routing_file = '{}/{}_v4andv6.readable'.format(self.files_path,
                                                                self.routing_date)
                                                                
                routing_file = self.concatenateFiles(routing_file,
                                                     readable_v4,
                                                     readable_v6)
                
            elif 'dmp.gz' in available_routing_files:
                routing_file = available_routing_files['dmp.gz']
                
            elif 'v6.dmp.gz' in available_routing_files:
                routing_file = available_routing_files['v6.dmp.gz']
                
            else:
                # This should never happen
                print 'No routing file for date {}\n'.format(self.routing_date)
                routing_file = ''
            
        loaded = bgp_handler.loadStructuresFromRoutingFile(routing_file)

        if loaded:
            loaded = bgp_handler.loadUpdatesDF(bgp_handler.routingDate)
            
            updates_stats_file = '{}/updatesStats_{}.csv'.format(self.files_path,
                                                                 self.routing_date)
            with open(updates_stats_file, 'w') as u_file:
                u_file.write('prefix|del_date|updates_date|del_age|ip_version|prefLength|numOfAnnouncements|numOfWithdraws\n')
                
            self.computeUpdatesStats(bgp_handler.updates_df, updates_stats_file)
            
            updates_stats_df = self.generateJSONfile(updates_stats_file)
            
            if self.es_host != '':
                self.importStatsIntoElasticSearch(updates_stats_df, 'BGP updates',
                                                  self.es_host, updatesStats_ES_properties)

            if bgp_handler.routingDate is not None:
                deagg_stats_file = '{}/deaggStats_{}.csv'.format(self.files_path,
                                                                 self.routing_date)
                                                                 
                with open(deagg_stats_file, 'w') as d_file:
                    d_file.write('prefix|del_date|routing_date|del_age|isRoot|isRootDeagg\n')
                    
                self.computeDeaggregationStats(bgp_handler, deagg_stats_file)
                
                deagg_stats_df = self.generateJSONfile(deagg_stats_file)
                
                if self.es_host != '':
                    self.importStatsIntoElasticSearch(deagg_stats_df, 'deaggregation',
                                                      self.es_host, deaggStats_ES_properties)
        
            else:
                sys.stdout.write('Stats about deaggregation from routing file {} not computed due to file being empty.\n'.format(self.routing_file))

def main(argv):
    DEBUG = False
    files_path = '/home/sofia/BGP_stats_files'
    routing_date = None
    routing_file = ''
    es_host = ''
    
    # For DEBUG
    DEBUG = True
    files_path = '/Users/sofiasilva/BGP_files'
    routing_date = date.today()
    
    try:
        opts, args = getopt.getopt(argv, "hp:r:R:DE:", ["files_path=",
                                                        "routing_file=",
                                                      "Routing_date=",
                                                      "ElasticSearch_host=",])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> -r <routing file> -R <Routing date> [-D] [-E <ElasticSearch host>]'.format(sys.argv[0])
        sys.exit(-1)
        
    for opt, arg in opts:
        if opt == '-h':
            print "This script loads two tables with data about daily BGP updates and deaggregation in the BGP routing table."
            print 'Usage: {} -h | -p <files path> -r <routing file> -R <Routing date> [-D] [-E <ElasticSearch host>]'.format(sys.argv[0])
            print "h: Help"
            print "p: Path to folder in which files will be saved. (MANDATORY)"
            print "r: Path to routing file from which stats will be computed."
            print "R: Routing date for the stats to be computed in format YYYYMMDD. If not provided, stats will be computed for the day before today."
            print "D: Debug mode. Use this option if you want the script to run in debug mode."
            print "E: Insert compute statistics into ElasticSearch. The hostname of the ElasticSearch host MUST be provided if this option is used."
        elif opt == '-p':
            if arg != '':
                files_path = os.path.abspath(arg)
            else:
                print "If option -p is used, the path to a folder in which files will be saved MUST be provided."
                sys.exit(-1)
        elif opt == '-r':
            if arg != '':
                routing_file = os.path.abspath(arg)
            else:
                print "If option -r is used, the path to a routing file from which stats can be computed MUST be provided."
                sys.exit(-1)
        elif opt == '-R':
            if arg != '':
                routing_date = datetime.strptime(arg, '%Y%m%d')
            else:
                print "If option -R is used, the routing date for which stats will be computed MUST be provided."
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
            
    if routing_date is None and routing_file == '':
        routing_date = date.today() - timedelta(1)
    
    StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path, routing_date,
                                               routing_file, es_host)

    StabilityAndDeagg_inst.computeAndSaveStabilityAndDeaggDailyStats()    
        
if __name__ == "__main__":
    main(sys.argv[1:])