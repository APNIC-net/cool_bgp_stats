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
from netaddr import IPNetwork
import pandas as pd
import subprocess, shlex
from datetime import datetime

def getDelegationDate(prefix):
    cmd = shlex.split("origindate -d ',' -f 1")
    cmd_echo = shlex.split("echo {}".format(prefix))
    p = subprocess.Popen(cmd_echo, stdout=subprocess.PIPE)
    output = subprocess.check_output(cmd, stdin=p.stdout)
    return datetime.strptime(output.split(',')[1], '%Y%m%d').date()
    
def getUpdatesCountsDF(updates_df):
    updates_df['update_date'] = updates_df.apply(lambda row:\
                                            datetime.utcfromtimestamp(\
                                            row['timestamp']).date(),\
                                            axis=1)
                                                
    updates_counts_df = pd.DataFrame(columns=['prefix', 'del_date', 'updates_date',
                                             'del_age', 'ip_version', 'prefLength',
                                             'numOfAnnouncements', 'numOfWithdraws'])
                                             
    for prefix, prefix_subset in updates_df.groupby('prefix'):
        del_date = getDelegationDate(prefix)
        network = IPNetwork(prefix)
        for update_date, date_subset in prefix_subset.groupby('update_date'):
            del_age = (update_date - del_date).days
            numOfAnn = len(date_subset[date_subset['upd_type'] == 'A']['prefix'].tolist())
            numOfWith = len(date_subset[date_subset['upd_type'] == 'W']['prefix'].tolist())
            
            updates_counts_df.loc[updates_counts_df.shape[0]] = [prefix, del_date,
                                                                update_date, del_age,
                                                                network.version,
                                                                network.prefixlen,
                                                                numOfAnn, numOfWith]
    return updates_counts_df

def getDeaggregationDF(bgp_handler):
    deaggregation_DF = pd.DataFrame(columns=['prefix', 'del_date', 'routing_date',
                                             'del_age', 'isRoot', 'isRootDeagg'])
    
    for prefix, prefix_subset in bgp_handler.bgp_df.groupby('prefix'):
        del_date = getDelegationDate(prefix)
        
        network = IPNetwork(prefix)
        if network.version == 4:
            prefixes_radix = bgp_handler.ipv4Prefixes_radix
        else:
            prefixes_radix = bgp_handler.ipv6Prefixes_radix
            
        # If the list of covering prefixes in the Radix tree has only 1 prefix,
        # it is the prefix itself, therefore the prefix is a root prefix
        if len(prefixes_radix.search_covering(prefix)) == 1:
            isRoot = True
    
            # If the list of covered prefix includes more prefixes than the prefix
            # itself, then the root prefix is being deaggregated.
            if len(bgp_handler.ipv4Prefixes_radix.search_covered(prefix)) > 1:
                isRootDeagg = True
        else:
            isRoot = False
            isRootDeagg = False
        
        deaggregation_DF.loc[deaggregation_DF.shape[0]] = [prefix, del_date,
                                                            bgp_handler.routingDate,
                                                            (bgp_handler.routingDate -\
                                                            del_date).days,
                                                            isRoot, isRootDeagg]
    
    return deaggregation_DF

def main(argv):
    DEBUG = False
    files_path = ''
    routing_file = ''
    updates_file = ''
    
    # For DEBUG
    DEBUG = True
    files_path = '/Users/sofiasilva/BGP_files'
    routing_file = '/Users/sofiasilva/BGP_files/2017-05-01.bgprib.readable' 
    updates_file = '/Users/sofiasilva/BGP_files/2017-05-01_test.bgpupd.readable'
 
    
    try:
        opts, args = getopt.getopt(argv, "hp:D", ["files_path="])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> -R <Routing file> -U <BGP Updates file> [-D]'.format(sys.argv[0])
        sys.exit(-1)
        
    for opt, arg in opts:
        if opt == '-h':
            print "This script loads two tables with data about daily BGP updates and deaggregation in the BGP routing table."
            print 'Usage: {} -h | -p <files path> -R <Routing file> -U <BGP Updates file> [-D]'.format(sys.argv[0])
            print "h: Help"
            print "p: Path to folder in which files will be saved. (MANDATORY)"
            print "R: Path to the routing file to be used. Can be an mrt or a dmp file. Can be a compressed file with extension .gz (MANDATORY)"
            print "U: Path to the file with BGP updates to be used. Must be a bgpupd.mrt file. Can be a compressed file with extension .gz (MANDATORY)"
            print "D: Debug mode. Use this option if you want to script to run in debug mode."
        elif opt == '-p':
            if arg != '':
                files_path = os.path.abspath(arg)
            else:
                print "If option -p is used, the path to a folder in which files will be saved MUST be provided."
                sys.exit(-1)
        elif opt == '-R':
            if arg != '':
                routing_file = os.path.abspath(arg)
            else:
                print "If option -R is used, the path to the routing file to be used MUST be provided."
                sys.exit(-1)
        elif opt == '-U':
            if arg != '':
                updates_file = os.path.abspath(arg)
            else:
                print "If option -U is used, the path to the file with BGP updates to be used MUST be provided."
                sys.exit(-1)
        elif opt == '-D':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
    
    if files_path == '' or routing_file == '' or updates_file == '':
        print "You MUST provide the corresponding paths using options -p, -R and -U."
        sys.exit(-1)
        
    bgp_handler = BGPDataHandler(DEBUG, files_path)

    bgp_handler.loadStructuresFromUpdatesFile(updates_file)
    updates_count_df = getUpdatesCountsDF(bgp_handler.updates_df)
    
    bgp_handler.loadStructuresFromRoutingFile(routing_file)
    deaggregation_df = getDeaggregationDF(bgp_handler)
    
    # TODO Generate csv and json files
    # TODO Insert both DataFrames into ElasticSearch for aggregation and summarization (compute annual averages)
        
if __name__ == "__main__":
    main(sys.argv[1:])

