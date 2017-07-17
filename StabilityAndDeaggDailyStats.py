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
from netaddr import IPNetwork
import pandas as pd
import subprocess, shlex
from datetime import datetime, date, timedelta
from BGPDataHandler import BGPDataHandler
from ElasticSearchImporter import ElasticSearchImporter
import updatesStats_ES_properties
import deaggStats_ES_properties
import pickle
import radix

class StabilityAndDeagg:
    def __init__(self, DEBUG, files_path, ELASTIC, bgp_handler):
        self.DEBUG = DEBUG
        self.files_path = files_path
        
        if ELASTIC:
            self.es_host = 'twerp.rand.apnic.net'
        else:
            self.es_host = ''
            
        # bgp_handler MUST be an instance of the BGPDataHandler class and have
        # all the structures loaded
        self.bgp_handler = bgp_handler
        
        self.prefixes_data_pkl = './prefixes_datta.pkl'
        if os.path.exists(self.prefixes_data_pkl):
            self.prefixes_data = pickle.load(open(self.prefixes_data_pkl, "rb"))
        else:
            self.prefixes_data = radix.Radix()
        
    def getDelegationDate(self, prefix):
        pref_node = self.prefixes_data.search_exact(prefix)
        if pref_node is not None and 'del_date' in pref_node.data:
            del_date = pref_node.data['del_date']
        else:
            cmd = shlex.split("origindate -d ',' -f 1")
            cmd_echo = shlex.split("echo {}".format(prefix))
            p = subprocess.Popen(cmd_echo, stdout=subprocess.PIPE)
            output = subprocess.check_output(cmd, stdin=p.stdout)
            p.kill()
            del_date = datetime.strptime(output.split(',')[1].strip(), '%Y%m%d').date()

            if pref_node is None:
                pref_node = self.prefixes_data.add(prefix)
                
            pref_node.data['del_date'] = del_date
        
        return del_date
        
    def getDelegationCC(self, prefix):
        pref_node = self.prefixes_data.search_exact(prefix)
        if pref_node is not None and 'cc' in pref_node.data:
            cc = pref_node.data['cc']
        else:
            cmd = shlex.split("origincc -d ',' -f 1")
            cmd_echo = shlex.split("echo {}".format(prefix))
            p = subprocess.Popen(cmd_echo, stdout=subprocess.PIPE)
            output = subprocess.check_output(cmd, stdin=p.stdout)
            p.kill()
            cc = output.split(',')[1].strip()

            if pref_node is None:
                pref_node = self.prefixes_data.add(prefix)
                
            pref_node.data['cc'] = cc
        
        return cc

    @staticmethod        
    def getDelegationDates_fromFile(prefixes_file):
        cmd = shlex.split("origindate -d ',' -f 1")
        output = subprocess.check_output(cmd, stdin=open(prefixes_file, 'r'))
        del_dates_df = pd.Series(output.split('\n')).str.rsplit(',', expand=True) 
        del_dates_df.columns = ['prefix', 'del_date', 'first_ip', 'count']
        return del_dates_df

    @staticmethod
    def getDelegationCCs_fromFile(prefixes_file):
        cmd = shlex.split("origincc -d ',' -f 1")
        output = subprocess.check_output(cmd, stdin=open(prefixes_file, 'r'))
        del_cc_df = pd.Series(output.split('\n')).str.rsplit(',', expand=True)
        del_cc_df.columns = ['prefix', 'cc']
        return del_cc_df
        

    def computeUpdatesStats(self, updates_df, stats_file):
        if updates_df.shape[0] > 0:
            prefixes_file = '/tmp/prefixes.txt'
            with open(prefixes_file, 'w') as output_file:
                output_file.write('\n'.join(set(updates_df['prefix'].tolist())))

            del_dates_df = self.getDelegationDates_fromFile(prefixes_file)
            updates_df = pd.merge(updates_df, del_dates_df, how='left', on='prefix')
            del_cc_df = self.getDelegationCCs_fromFile(prefixes_file)
            updates_df = pd.merge(updates_df, del_cc_df, how='left', on='prefix')
            
            os.remove(prefixes_file)
            
            updates_df['del_age'] = updates_df.apply(lambda row:(row['update_date'] - datetime.strptime(row['del_date'], '%Y%m%d').date()).days, axis=1)
            
            updates_df.to_csv(stats_file, header=True, index=False, columns=[
                                                                       'prefix',
                                                                       'del_date',
                                                                       'cc',
                                                                       'update_date',
                                                                       'del_age',
                                                                       'ip_version',
                                                                       'preflen',
                                                                       'upd_type',
                                                                       'updates_count'])
                                                                         
#            with open(self.prefixes_data_pkl, 'wb') as f:
#                pickle.dump(self.prefixes_data, f, pickle.HIGHEST_PROTOCOL)
            
            return True
        else:
            return False
    
    def computeDeaggregationStats(self, bgp_handler, stats_file): 
        if bgp_handler.bgp_df.shape[0] > 0:        
            for prefix, prefix_subset in bgp_handler.bgp_df.groupby('prefix'):
                pref_node = self.prefixes_data.search_exact(prefix)
    
                if pref_node is not None:
                    del_date = pref_node.data['del_date']
                    cc = pref_node.data['cc']
                else:
                    del_date = self.getDelegationDate(prefix)
                    cc = self.getDelegationCC(prefix)
                    pref_node = self.prefixes_data.add(prefix)
                    pref_node.data['del_date'] = del_date
                    pref_node.data['cc'] = cc
                        
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
                    s_file.write('{}|{}|{}|{}|{}|{}|{}\n'.format(prefix, del_date, cc,
                                                                bgp_handler.routingDate,
                                                                (bgp_handler.routingDate -\
                                                                del_date).days,
                                                                isRoot, isRootDeagg))
            
            with open(self.prefixes_data_pkl, 'wb') as f:
                pickle.dump(self.prefixes_data, f, pickle.HIGHEST_PROTOCOL)
        
            return True
        else:
            return False
            
            
    @staticmethod
    def generateJSONfile(stats_file):
        stats_df = pd.read_csv(stats_file, sep = ',')
        json_filename = '{}.json'.format(stats_file.split('.')[0])
        stats_df.to_json(json_filename, orient='index')
        sys.stderr.write("Stats saved to JSON and CSV files successfully!\n")
        sys.stderr.write("Files generated:\n{}\n\nand\n\n{}\n".format(stats_file,
                                                                    json_filename))
        return stats_df
    
    @staticmethod
    def importStatsIntoElasticSearch(es_host, stats_df, stats_name, es_properties):
        esImporter = ElasticSearchImporter(es_host)
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
    
    def computeAndSaveStabilityDailyStats(self):           
        updates_stats_file = '{}/updatesStats_{}.csv'.format(self.files_path,
                                                             self.bgp_handler.routingDate)
        if not os.path.exists(updates_stats_file):
            computed = self.computeUpdatesStats(self.bgp_handler.updates_prefixes,
                                                updates_stats_file)
            
            if computed:
                updates_stats_df = self.generateJSONfile(updates_stats_file)
            
                if self.es_host != '':
                    self.importStatsIntoElasticSearch(self.es_host,
                                                      updates_stats_df,
                                                      'BGP updates',
                                                      updatesStats_ES_properties)


    def computeAndSaveDeaggDailyStats(self):
        if self.bgp_handler.routingDate is not None:
            deagg_stats_file = '{}/deaggStats_{}.csv'.format(self.files_path,
                                                             self.bgp_handler.routingDate)
            
            if not os.path.exists(deagg_stats_file):
                with open(deagg_stats_file, 'w') as d_file:
                    d_file.write('prefix|del_date|CC|routing_date|del_age|isRoot|isRootDeagg\n')
                    
                computed = self.computeDeaggregationStats(self.bgp_handler,
                                                          deagg_stats_file)

                if computed:                
                    deagg_stats_df = self.generateJSONfile(deagg_stats_file)
                    
                    if self.es_host != '':
                        self.importStatsIntoElasticSearch(self.es_host,
                                                          deagg_stats_df,
                                                          'deaggregation',
                                                          deaggStats_ES_properties)


def main(argv):
    DEBUG = False
    files_path = '/home/sofia/BGP_stats_files'
    routing_date = None
    routing_file = ''
    DEAGG = False
    STABILITY = False
    ELASTIC = False
    
    try:
        opts, args = getopt.getopt(argv, "hp:r:R:SDdE", ["files_path=",
                                                        "routing_file=",
                                                      "Routing_date=",])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> -r <routing file> -R <Routing date> [-S] [-D] [-d] [-E]'.format(sys.argv[0])
        sys.exit(-1)
        
    for opt, arg in opts:
        if opt == '-h':
            print "This script loads two tables with data about daily BGP updates and deaggregation in the BGP routing table."
            print 'Usage: {} -h | -p <files path> -r <routing file> -R <Routing date> [-S] [-D] [-d] [-E]'.format(sys.argv[0])
            print "h: Help"
            print "p: Path to folder in which files will be saved. (MANDATORY)"
            print "r: Path to routing file from which stats will be computed."
            print "R: Routing date for the stats to be computed in format YYYYMMDD. If not provided, stats will be computed for the day before today."
            print "S: Stability. Compute statistics about update rates."
            print "D: Deaggregation. Compute statistics about probability of deaggregation."
            print "d: Debug mode. Use this option if you want the script to run in debug mode."
            print "E: Insert compute statistics into ElasticSearch."
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
                routing_date = datetime.strptime(arg, '%Y%m%d').date()
            else:
                print "If option -R is used, the routing date for which stats will be computed MUST be provided."
                sys.exit(-1)
        elif opt == '-D':
            DEAGG = True
        elif opt == '-S':
            STABILITY = True
        elif opt == '-d':
            DEBUG = True
        elif opt == '-E':
            ELASTIC = True
        else:
            assert False, 'Unhandled option'
    
    if DEAGG or STABILITY:
        if routing_date is None and routing_file == '':
            routing_date = date.today() - timedelta(1)
        
        bgp_handler = BGPDataHandler(DEBUG, files_path)
        
        if routing_file != '':
            loaded = bgp_handler.loadStructuresFromRoutingFile(routing_file)
    
            if loaded:
                loaded = bgp_handler.loadUpdatesDFs(bgp_handler.routingDate)
        else:
            loaded = bgp_handler.loadStructuresFromArchive(routing_date)
        
        if not loaded:
            sys.stdout.write('{}: Data structures not loaded! Aborting.\n'.format(datetime.now()))
            sys.exit()
            
        StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path, ELASTIC,
                                                   bgp_handler)
        
        if STABILITY:
            StabilityAndDeagg_inst.computeAndSaveStabilityDailyStats()
            
        if DEAGG:
            StabilityAndDeagg_inst.computeAndSaveDeaggDailyStats()    
        
if __name__ == "__main__":
    main(sys.argv[1:])