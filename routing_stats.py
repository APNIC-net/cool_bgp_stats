#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import os
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from DelegatedHandler import DelegatedHandler
from BGPDataHandler import BGPDataHandler
import ipaddress
import pandas as pd
#import numpy as np
import pytricia
import datetime

# This function computes statistics for each delegated block
# and for each aggregated block resulting from summarizing multiple delegations
# to the same organization.
# Returns a DataFrame with the computed statistics and
# a PyTricia with the routed blocks covering each delegated or aggregated block.
def computePerPrefixStats(bgp_handler, del_handler):     
    orgs_aggr_networks = del_handler.getDelAndAggrNetworks()
    
    bgp_data = bgp_handler.bgp_data
    prefixes_indexes_pyt = bgp_handler.prefixes_indexes_pyt
#    ASes_prefixes_dic = bgp_handler.ASes_prefixes_dic
    
    # TODO Stats related to prefix/originAS (?)
    # Refactor prefix/origin-as pairs to generate data which is tagged by member, economy, region
    
    del_routed_pyt = pytricia.PyTricia()
    
    for i in orgs_aggr_networks.index:
        net = orgs_aggr_networks.ix[i]['ip_block']
        network = ipaddress.ip_network(unicode(net, "utf-8"))
        ips_delegated = network.num_addresses
        del_routed = []
        
        for routed_net in prefixes_indexes_pyt:
            routed_network = ipaddress.ip_network(unicode(routed_net, "utf-8"))
            if(network.overlaps(routed_network)):
                del_routed.append(routed_network)
        
        # TODO Check if prefix and origin AS were delegated to the same organization

        # TODO Both for visibility and for deaggregation, consider the case in which
        # a delegated block is covered by more than one overlapping announces.
        # For visibility, we cannot count those IP addresses more than once. (OK. Already considered.)
        # For deaggregation, how should this be computed? (Read below)

        # From http://irl.cs.ucla.edu/papers/05-ccr-address.pdf

        # Covering prefixes:
        # Based on their relations to the corresponding
        # allocated address blocks, covering prefixes can be categorized into three
        # classes: allocation intact, aggregation over multiple allocations,
        # or fragments from a single allocation.         
        
        # Covered prefixes: (Usually due to traffic engineering)
        # Our first observation about covered prefixes is that they
        # show up and disappear in the routing table more frequently
        # than the covering prefixes. To show this, we compare the
        # routing prefixes between the beginning and end of each 2-
        # month interval and count the following four events: (1) a
        # covered prefix at the beginning remains unchanged at the
        # end of the interval, (2) a covered prefix at the beginning
        # disappears at the end, but its address space is covered by
        # some other prefix(es), (3) a new covered prefix is advertised
        # at the end, and (4) a covered prefix at the beginning disappears
        # before the end and its address space is no longer
        # covered in the routing table.
        
        # We classify covered prefixes into four classes based on their advertisement
        # paths relative to that of their corresponding covering
        # prefixes, with two of them further classified into sub-classes.
        # * Same origin AS, same AS path (SOSP)
        # * Same origin AS, different paths (SODP) (Types 1 and 2)
        # * Different origin ASes, same path (DOSP)
        # * Different origin ASes, different paths (DODP) (Types 1, 2 and 3)
        
        # From http://www.eecs.qmul.ac.uk/~steve/papers/JSAC-deaggregation.pdf

        # For deaggregation:
        # • Lonely: a prefix that does not overlap with any other prefix.
        # • Top: a prefix that covers one or more smaller prefix blocks,
        # but is not itself covered by a less specific.
        # • Deaggregated: a prefix that is covered by a less specific prefix,
        # and this less specific is originated by the same AS as the deaggregated prefix.
        # • Delegated: a prefix that is covered by a less specific, and this
        # less specific is not originated by the same AS as the delegated prefix.
        # Deaggregation factor of an AS to be the ratio between the number
        # of announced prefixes and the number of allocated address blocks

        # For visibility:
        # • Only root: The complete allocated address block (called
        # “root prefix”) is announced and nothing else.
        # • root/MS-complete: The root prefix and at least two subprefixes
        # are announced. The set of all sub-prefixes spans
        # the whole root prefix.
        # • root/MS-incomplete: The root prefix and at least one subprefix
        # is announced. Together, the set of announced subprefixes
        # does not cover the root prefix.
        # • no root/MS-complete: The root prefix is not announced.
        # However, there are at least two sub-prefixes which together
        # cover the complete root prefix.
        # • no root/MS-incomplete: The root prefix is not announced.
        # There is at least one sub-prefix. Taking all sub-prefixes
        # together, they do not cover the complete root prefix.

        # From https://labs.ripe.net/Members/ggm/reducing-the-bgp-table-size-a-fairy-tale

        # delegated (i.e. there is a less specific originated another ASN)
        # deaggregated (i.e. there is a less specific originated from the same ASN).
        
        del_routed_pyt[net] = del_routed
        
#        deaggregation = float(np.nan)
        routed_count = len(del_routed)

        ips_routed = 0

        if routed_count > 0: # block is being announced, at least partially
            originASes = set()
            for routed_block in del_routed:
                originAS = bgp_data.ix[prefixes_indexes_pyt[routed_net], 'originAS'] # TODO Test this
                originASes.add(originAS)

            if len(originASes) > 1:
                orgs_aggr_networks.ix[i, 'multiple_originASes'] = True

            aggregated_routed = [ipaddr for ipaddr in\
                            ipaddress.collapse_addresses(del_routed)]
            # ips_routed is obtained from the summarized routed blocks
            # so that IPs contained in overlapping announcements are not
            # counted more than once
            for aggr_r in aggregated_routed:
                ips_routed += aggr_r.num_addresses
                
#            aggregated_count = float(len(aggregated_routed))
#            deaggregation = (1 - (aggregated_count/routed_count))*100
        
        visibility = (ips_routed*100)/ips_delegated

        orgs_aggr_networks.ix[i, 'visibility'] = visibility
#        orgs_aggr_networks.ix[i, 'deaggregation'] = deaggregation

    return orgs_aggr_networks, del_routed_pyt
        

def main(argv):
    
    urls_file = './BGPoutputs.txt'
    RIBfile = True
    files_path = ''
    routing_file = ''
    KEEP = False
    COMPUTE = True 
    DEBUG = False
    EXTENDED = False
    year = ''
    del_file = ''
    INCREMENTAL = False
    stats_file = ''
    final_existing_date = ''
    fromFiles = False
    bgp_data_file = ''
    prefixes_indexes_file = ''
    ASes_prefixes_file = ''
    
    #For DEBUG
#    files_path = '/Users/sofiasilva/BGP_files'
#    routing_file = '/Users/sofiasilva/BGP_files/bview.20170112.0800.gz'
#    KEEP = True
#    EXTENDED = True
#    del_file = '/Users/sofiasilva/BGP_files/extended_apnic_20170201.txt'

    
    
    try:
        opts, args = getopt.getopt(argv, "hp:u:or:knyd:ei:b:x:a:", ["files_path=", "urls_file=", "routing_file=", "delegated_file=", "stats_file=", "bgp_data_file=", "prefiXes_ASes_file=", "ASes_prefixes_file="])
    except getopt.GetoptError:
        print 'Usage: routing_stats.py -h | -p <files path> [-u <urls file> [-o]] [-r <routing file>] [-k] [-n] [-y <year>] [-d <delegated file>] [-e] [-i <stats file>] [-b <bgp_data file> -x <prefiXes_indexes file> -a <ASes_prefixes file>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes routing statistics from files containing Internet routing data and a delegated file."
            print 'Usage: routing_stats.py -h | -p <files path> [-u <urls file> [-o]] [-r <routing file>] [-k] [-n] [-y <year>] [-d <delegated file>] [-e] [-i <stats file>] [-b <bgp_data file> -x <prefiXes_indexes file> -a <ASes_prefixes file>]'
            print 'h = Help'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print 'u = URLs file. File which contains a list of URLs of the files to be downloaded.'
            print "If not provided, the script will try to use ./BGPoutputs.txt"
            print 'All the URLs must point either to RIB files or to files containing "show ip bgp" outputs.'
            print 'If the URLs point to files containing "show ip bgp" outputs, the "-o" option must be used to specify this.'
            print 'o = URLs in the URLs file point to files containing "show ip bgp" outputs.'
            print 'r = Use already downloaded Internet Routing data file.'
            print 'k = Keep downloaded Internet routing data file.'
            print 'n = No computation. If this option is used, statistics will not be computed, just the dictionaries with prefixes/origin ASes will be created and saved to disk.'
            print 'y = Year to compute statistics for. If a year is not provided, statistics will be computed for all the available years.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            print "b = BGP_data file. Path to pickle file containing bgp_data DataFrame."
            print "x = prefiXes_indexes file. Path to pickle file containing prefiXes_indexes PyTricia."
            print "a = ASes_prefixes file. Path to pickle file containing ASes_prefixes dictionary."
            print "If you want to work with BGP data from files, the three options -b, -x and -a must be used."
            print "If not, none of these three options should be used."
            sys.exit()
        elif opt == '-u':
            urls_file = arg
        elif opt == '-o':
            RIBfile = False
        elif opt == '-r':
            routing_file = arg
        elif opt == '-k':
            KEEP = True
        elif opt == '-n':
            COMPUTE = False
        elif opt == '-y':
            year = int(arg)
        elif opt == '-d':
            DEBUG = True
            del_file = arg
        elif opt == '-e':
            EXTENDED = True
        elif opt == '-p':
            files_path = arg.rstrip('/')
        elif opt == '-i':
            INCREMENTAL = True
            stats_file = arg
        elif opt == '-b':
            bgp_data_file = arg
            fromFiles = True
        elif opt == '-x':
            prefixes_indexes_file = arg
            fromFiles = True
        elif opt == '-a':
            ASes_prefixes_file = arg
            fromFiles = True
        else:
            assert False, 'Unhandled option'
        
    if files_path == '':
        print "You must provide the path to a folder to save files."
        sys.exit() 
        
    # If files_path does not exist, we create it
    if not os.path.exists(files_path):
        os.makedirs(files_path)
                
    if DEBUG and del_file == '':
        print "If you choose to run in DEBUG mode you must provide the path to\
                    a delegated file that has already been downloaded."
        sys.exit()
        
    if INCREMENTAL:
        existing_stats_df = pd.DataFrame()

        if stats_file == '':
            print "If option -i is used, a statistics file MUST be provided."
            sys.exit()
        else:
            existing_stats_df = pd.read_csv(stats_file, sep = ',')
            final_existing_date = max(existing_stats_df['Date'])
            # Remove stats for final existing date in case the stats for that day were incomplete
            # Stats for that day will be computed again
            existing_stats_df = existing_stats_df[existing_stats_df['Date'] != final_existing_date]

    if fromFiles and (bgp_data_file == '' or prefixes_indexes_file == '' or ASes_prefixes_file== ''):
        print "If you want to work with BGP data from files, the three options -b, -x and -a must be used."
        print "If not, none of these three options should be used."
        sys.exit()
        
    today = datetime.date.today().strftime('%Y%m%d')
    
    if not DEBUG:

        if EXTENDED:
            del_file = '%s/extended_apnic_%s.txt' % (files_path, today)
        else:
            del_file = '%s/delegated_apnic_%s.txt' % (files_path, today)
        
    bgp_data = BGPDataHandler(urls_file, files_path, routing_file, KEEP, RIBfile, bgp_data_file, prefixes_indexes_file, ASes_prefixes_file)
    
    if COMPUTE: 
        del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year)
        prefixes_Stats, routed_pyt = computePerPrefixStats(bgp_data, del_handler)
        
    else:
       bgp_data.saveDataToFiles(files_path)
        
        
if __name__ == "__main__":
    main(sys.argv[1:])
