#! /usr/bin/python2.7 
# -*- coding: utf8 -*-


import sys, getopt, os
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')

from DelegatedHandler import DelegatedHandler
from BGPDataHandler import BGPDataHandler
import ipaddress
import pandas as pd
#import numpy as np
#import pytricia
import datetime

# This function returns a list of prefixes less specific than the one provided
# that are included in the keys of the PyTricia
def getRoutedParentAndGrandparents(prefix, pyt):
    # Get the key in the PyTricia corresponding to the prefix
    # key_pref is prefix if prefix is in keys
    # or the longest prefix match in keys
    key_pref = pyt.get_key(prefix) 
    
    net_less_specifics = []
    
    if key_pref is not None:
        if key_pref != prefix:
            net_less_specifics.append(key_pref)
            
        prefix = key_pref
        net_parent = pyt.parent(prefix)
        if net_parent is not None:
            net_less_specifics.append(net_parent)
            granpa = pyt.parent(net_parent)
                    
            while granpa is not None:
                net_less_specifics.append(granpa)
                granpa = pyt.parent(granpa)
        
    return net_less_specifics

# This function returns a list of prefixes more specific than the one provided
# that are included in the keys of the PyTricia
def getRoutedChildren(prefix, pyt, longest_pref):
    more_specifics = []
   
    if pyt.has_key(prefix): # Exact match        
        # We return a list of the children of the prefix in the PyTricia
        # including the prefix itself if it is routed as-is
        more_specifics.append(prefix)
        more_specifics.extend(pyt.children(prefix))
        return more_specifics
                
    else: # If net is not in the PyTricia keys
        # we cannot use the children method for it
        # so we get the corresponding key (longest prefix match)
        key_pref = pyt.get_key(prefix)
        prefix_network = ipaddress.ip_network(unicode(prefix, 'utf-8'))

        # If there is no corresponding key
        # it means there is no less specific prefix being routed
        if key_pref is None:
            # so we have to look for children of the prefix's subnets
            if prefix_network.prefixlen < longest_pref:
                immediate_subnets = list(prefix_network.subnets())
                more_specifics.extend(getRoutedChildren(str(immediate_subnets[0]), pyt, longest_pref))
                more_specifics.extend(getRoutedChildren(str(immediate_subnets[1]), pyt, longest_pref))
            
        else:
            # If there is a corresponding key, we can use the children function
            # to get the key's children
            key_children = pyt.children(key_pref)
            # but then we have to check whether these children are also
            # subnets of the given prefix
            for child in key_children:
                child_network = ipaddress.ip_network(unicode(child, 'utf-8'))
                if child_network.subnet_of(prefix_network):
                    more_specifics.append(child)
                    
        return more_specifics
    
# This function returns a set with all the origin ASes for a specific prefix
# seen in the given BGP data
def getOriginASesForBlock(prefix, indexes_pyt, bgp_data):
    originASes = set()
    for index in indexes_pyt[prefix]:
        originAS = bgp_data.ix[index, 'ASpath'].split(' ')[-1]
        originASes.add(originAS)
    return originASes
    
# This function computes statistics for each delegated block
# and for each aggregated block resulting from summarizing multiple delegations
# to the same organization.
# Returns a DataFrame with the computed statistics and
# a PyTricia with the routed blocks covering each delegated or aggregated block.
# The key is a string representing a block, the value is a list of IPNetwroks
def computePerPrefixStats(bgp_handler, del_handler):
    # Obtain a DataFrame with all the delegated blocks and all the aggregated
    # block resulting from summarizing multiple delegations
    # to the same organization
    orgs_aggr_networks = del_handler.getDelAndAggrNetworks()
    
    # We add columns to store as boolean variables the concepts defined in
    # http://irl.cs.ucla.edu/papers/05-ccr-address.pdf
    # See description below
    
    new_cols1 = ['isCovering', 'allocIntact', 'aggrRouted', 'fragmentsRouted', 'isCovered', 'SOSP', 'SODP1', 'SODP2', 'DOSP', 'DODP1', 'DODP2', 'DODP3']

    # We add columns to store as boolean variables the concepts defined in
    # http://www.eecs.qmul.ac.uk/~steve/papers/JSAC-deaggregation.pdf
    # See description below

    new_cols2 = ['isLonely', 'isTop', 'isDeaggregated', 'isDelegated', 'onlyRoot', 'root_MScompl', 'root_MSincompl', 'noRoot_MScompl', 'noRoot_MSincompl']

    new_cols = new_cols1 + new_cols2
    
    for new_col in new_cols:
        orgs_aggr_networks.loc[:, new_col] =\
            pd.Series([False]*orgs_aggr_networks.shape[0],\
            index=orgs_aggr_networks.index)
    
    # Obtain BGP data
    bgp_data = bgp_handler.bgp_data
    ipv4_prefixes_indexes_pyt = bgp_handler.ipv4_prefixes_indexes_pyt
    ipv6_prefixes_indexes_pyt = bgp_handler.ipv6_prefixes_indexes_pyt

    del_routed = dict()
    # Using dict instead of PyTricia due to issues accessing PyTricia values
    # Depending on what is stored as a value and how that value is stored,
    # there are some issues when trying to access the value by key
    # By now we use a dict
    # If we want better perfomance for prefix lookups, PyTricia should be used
    
    # For each delegated or aggregated block
    for i in orgs_aggr_networks.index:
        net = orgs_aggr_networks.ix[i]['ip_block']
        network = ipaddress.ip_network(unicode(net, "utf-8"))
        ips_delegated = network.num_addresses

        if network.version == 4:
            prefixes_indexes_pyt = ipv4_prefixes_indexes_pyt
            longest_pref = bgp_handler.ipv4_longest_pref
        
        if network.version == 6:
            prefixes_indexes_pyt = ipv6_prefixes_indexes_pyt
            longest_pref = bgp_handler.ipv6_longest_pref
                
        net_less_specifics = []
        net_more_specifics = []
        # Find routed blocks related to the delegated or aggregated block 
        if len(prefixes_indexes_pyt.keys()) > 0:       
            net_less_specifics = getRoutedParentAndGrandparents(net, prefixes_indexes_pyt)
            net_more_specifics = getRoutedChildren(net, prefixes_indexes_pyt, longest_pref)          

        if len(net_more_specifics) > 0 or len(net_less_specifics) > 0:
            del_routed[net] = dict()
            del_routed[net]['more_specifics'] = net_more_specifics
            del_routed[net]['less_specifics'] = net_less_specifics
              
        
        # TODO Check if prefix and origin AS were delegated to the same organization
        
        # Based on http://irl.cs.ucla.edu/papers/05-ccr-address.pdf

        # TODO Compute Usage Latency
        # We define usage latency of an allocated address block as
        # the time interval between the allocation time and the first
        # time a part of, or the entire, allocated block shows up in
        # the BGP routing table

        # TODO Analyze stability of visibility
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
        
    
        # Covering prefixes: isCovering = (len(more_specifics) > 0)
        #   * allocation intact: allocIntact = (aggregated == False and block in more_specifics)
        #   * aggregation over multiple allocations: aggrRouted = (??)
        #   * fragments from a single allocation: fragmentsRouted = (aggregated == False and len(more_specifics - block) > 0)
        
        # Covered prefixes: isCovered = (len(less_specifics) > 0)
        # We classify covered prefixes into four classes based on their advertisement
        # paths relative to that of their corresponding covering
        # prefixes, with two of them further classified into sub-classes.
        # Corresponding covering prefixes -> less_specifics
        #   * Same origin AS, same AS path (SOSP) 
        #   * Same origin AS, different paths (SODP) (Types 1 and 2)
        #   * Different origin ASes, same path (DOSP)
        #   * Different origin ASes, different paths (DODP) (Types 1, 2 and 3)

            
        if del_routed.has_key(net):
            # block is being announced, at least partially

            if net in del_routed[net]['more_specifics']:
                more_specifics_wo_block = del_routed[net]['more_specifics'].remove(net)
            
            # We summarize the more specific routed blocks without the block itself
            # to get the maximum aggregation possible of the more specifics
            aggr_less_spec = [ipaddr for ipaddr in\
                                ipaddress.collapse_addresses(more_specifics_wo_block)]
           
            # If there is at least one less specific block being routed
            # the block is covered and therefore its visibility is 100 %
            if len(del_routed[net]['less_specifics']) > 0:
                orgs_aggr_networks.ix[i, 'isCovered'] = True 
                visibility = 100
            
                # TODO Compute 'SOSP', 'SODP1', 'SODP2', 'DOSP', 'DODP1', 'DODP2', 'DODP3'
                
                less_spec_originASes = set()
                for less_spec in del_routed[net]['less_specifics']:
                    less_spec_originASes.add(getOriginASesForBlock(less_spec, prefixes_indexes_pyt, bgp_data))
                    
                blockOriginASes = getOriginASesForBlock(net, prefixes_indexes_pyt, bgp_data)
                if len(blockOriginASes) > 1:
                    orgs_aggr_networks.ix[i, 'multiple_originASes'] = True
                if len(less_spec_originASes.intersection(blockOriginASes)) > 0:    
                    # • Deaggregated: a prefix that is covered by a less specific prefix,
                    # and this less specific is originated by the same AS as the deaggregated prefix.
                    orgs_aggr_networks.ix[i, 'isDeaggregated'] = True
                else:
                     # • Delegated: a prefix that is covered by a less specific, and this
                    # less specific is not originated by the same AS as the delegated prefix.
                    orgs_aggr_networks.ix[i, 'isDelegated'] = True

            # If there are no less specific blocks being routed
            else:
                # If the list of more specific blocks being routed includes
                # the block itself
                if net in del_routed[net]['more_specifics']:
                    # The block is 100 % visible
                    visibility = 100
                # If the block itself is not being routed, we have to compute
                # the visibility based on the more specific block being routed
                else:
                    # ips_routed is obtained from the summarized routed blocks
                    # so that IPs contained in overlapping announcements are not
                    # counted more than once
                    ips_routed = 0            
                    for aggr_r in aggr_less_spec:
                        ips_routed += aggr_r.num_addresses
                                            
                    # The visibility of the block is the percentaje of IPs
                    # that are visible
                    visibility = (ips_routed*100)/ips_delegated
                    
                # If there are more specific blocks being routed apart from
                # the block itself, taking into account we are under the case
                # of the block not having less specific blocks being routed,
                if len(more_specifics_wo_block) > 0:
                    # The block is a Top prefix  
                    # • Top: a prefix that covers one or more smaller prefix blocks,
                    # but is not itself covered by a less specific.
                    orgs_aggr_networks.ix[i, 'isTop'] = True
                
                # If the list of more specific blocks being routed only
                # includes the block itself, taking into account we are 
                # under the case of the block not having less specific
                # blocks being routed,
                else:
                    # the block is a Lonely prefix
                    # • Lonely: a prefix that does not overlap with any other prefix.
                    orgs_aggr_networks.ix[i, 'isLonely'] = True
   
            # If there are more specific blocks being routed
            # the block is a covering prefix
            if len(more_specifics_wo_block) > 0:
                orgs_aggr_networks.ix[i, 'isCovering'] = True
            
           # TODO how do we compute aggrRouted? (aggregation over multiple allocations: aggrRouted = (??))
        
            # If the block is not the result of aggregating multiple delegated blocks           
            if not orgs_aggr_networks.ix[i, 'aggregated']:
                # If there are more specific blocks being routed apart from
                # the block itself
                if len(more_specifics_wo_block) > 0:
                    orgs_aggr_networks.ix[i, 'fragmentsRouted'] =  True

                # Independently of the block being a covering or a covered prefix,
                # if the delegated block (not aggregated) is being announced as-is,
                # allocIntact is True
                if net in del_routed[net]['more_specifics']:
                    orgs_aggr_networks.ix[i, 'allocIntact'] = True
                
                # If there are no less specific blocks being routed and the list
                # of more specific blocks being routed only contains the block
                # itself
                if orgs_aggr_networks.ix[i, 'isCovered'] == False and del_routed[net]['more_specifics'] == [net]:
                    orgs_aggr_networks.ix[i, 'onlyRoot'] = True
                    # This variable is similar to the variable isLonely but can
                    # only be True for not aggregated blocks 
                
                if net in del_routed[net]['more_specifics']:
                    if len(del_routed[net]['more_specifics']) >= 3 and net in aggr_less_spec:
                        # • root/MS-complete: The root prefix and at least two subprefixes
                        # are announced. The set of all sub-prefixes spans
                        # the whole root prefix.
                        orgs_aggr_networks.ix[i, 'root_MScompl'] = True
                    if len(del_routed[net]['more_specifics']) >= 2 and net not in aggr_less_spec:
                        # • root/MS-incomplete: The root prefix and at least one subprefix
                        # is announced. Together, the set of announced subprefixes
                        # does not cover the root prefix.
                        orgs_aggr_networks.ix[i, 'root_MSincompl'] = True
                else:
                    if len(del_routed[net]['more_specifics']) >= 2 and net in aggr_less_spec:
                        # • no root/MS-complete: The root prefix is not announced.
                        # However, there are at least two sub-prefixes which together
                        # cover the complete root prefix.
                        orgs_aggr_networks.ix[i, 'noRoot_MScompl'] = True
                    if len(del_routed[net]['more_specifics']) >= 1 and net not in aggr_less_spec:
                        # • no root/MS-incomplete: The root prefix is not announced.
                        # There is at least one sub-prefix. Taking all sub-prefixes
                        # together, they do not cover the complete root prefix.
                        orgs_aggr_networks.ix[i, 'noRoot_MSincompl'] = True
        
            orgs_aggr_networks.ix[i, 'visibility'] = visibility

    return orgs_aggr_networks, del_routed

# This function determines whether the allocated ASNs are active
# either as middle AS, origin AS or both
# Returns dictionary with an ASN as key and a dictionary containing:
# * a numeric variable (numOfPrefixesPropagated) specifying the number of prefixes propagated by the AS
# (BGP announcements for which the AS appears in the middle of the AS path)
# * a numeric variable (numOfPrefixesOriginated) specifying the number of prefixes originated by the AS
def computeASesStats(bgp_handler, del_handler):
    expanded_del_asns_df = del_handler.getExpandedASNsDF() # TODO Debug this!!
    # For some reason the function is returning a DataFrame with only one row
    # It is something with the use of self
    ASes_originated_prefixes_dic = bgp_handler.ASes_originated_prefixes_dic
    ASes_propagated_prefixes_dic = bgp_handler.ASes_propagated_prefixes_dic
    
    statsForASes = dict()
    
    for asn in expanded_del_asns_df['initial_resource']:
        statsForASes[asn] = dict()
        try:
            statsForASes[asn]['numOfPrefixesOriginated'] = len(ASes_originated_prefixes_dic[asn])
        except KeyError:
            statsForASes[asn]['numOfPrefixesOriginated'] = 0
       
        try:
            statsForASes[asn]['numOfPrefixesPropagated'] = len(ASes_propagated_prefixes_dic[asn])
        except KeyError:
            statsForASes[asn]['numOfPrefixesPropagated'] = 0
       
    return statsForASes
    
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
    month = ''
    day = ''
    del_file = ''
    INCREMENTAL = False
    stats_file = ''
    final_existing_date = ''
    fromFiles = False
    bgp_data_file = ''
    ipv4_prefixes_indexes_file = ''
    ipv6_prefixes_indexes_file = ''
    ASes_originated_prefixes_file = ''
    ASes_propagated_prefixes_file = ''
    
    

#For DEBUG
    files_path = '/Users/sofiasilva/BGP_files'
    routing_file = '/Users/sofiasilva/BGP_files/bgptable.txt'
    KEEP = True
    RIBfile = False
    DEBUG = True
    EXTENDED = True
    del_file = '/Users/sofiasilva/BGP_files/extended_apnic_20170216.txt'
  
    
    try:
        opts, args = getopt.getopt(argv, "hp:u:or:kny:m:D:d:ei:b:4:6:a:s:", ["files_path=", "urls_file=", "routing_file=", "year=", "month=", "day=", "delegated_file=", "stats_file=", "bgp_data_file=", "IPv4_prefixes_ASes_file=", "IPv6_prefixes_ASes_file=", "ASes_originated_prefixes_file=", "ASes_propagated_prefixes_file="])
    except getopt.GetoptError:
        print 'Usage: routing_stats.py -h | -p <files path> [-u <urls file> [-o]] [-r <routing file>] [-k] [-n] [-y <year> [-m <month> [-D <day>]]] [-d <delegated file>] [-e] [-i <stats file>] [-b <bgp_data file> -4 <IPv4 prefixes_indexes file> -6 <IPv6 prefixes_indexes file> -a <ASes_originated_prefixes file> -s <ASes_propagated_prefixes file>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes routing statistics from files containing Internet routing data and a delegated file."
            print 'Usage: routing_stats.py -h | -p <files path> [-u <urls file> [-o]] [-r <routing file>] [-k] [-n] [-y <year> [-m <month> [-D <day>]]] [-d <delegated file>] [-e] [-i <stats file>] [-b <bgp_data file> -4 <IPv4 prefixes_indexes file> -6 <IPv6 prefixes_indexes file> -a <ASes_originated_prefixes file> -s <ASes_propagated_prefixes file>]'
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
            print 'm = Month of Year to compute statistics for. This option can only be used if a year is also provided.'
            print 'D = Day of Month to compute statistics for. This option can only be used if a year and a month are also provided.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            print "b = BGP_data file. Path to pickle file containing bgp_data DataFrame."
            print "4 = IPv4 prefixes_indexes file. Path to pickle file containing IPv4 prefixes_indexes PyTricia."
            print "6 = IPv6 prefixes_indexes file. Path to pickle file containing IPv6 prefixes_indexes PyTricia."
            print "a = ASes_originated_prefixes file. Path to pickle file containing ASes_originated_prefixes dictionary."
            print "s = ASes_propagated_prefixes file. Path to pickle file containing ASes_propagated_prefixes dictionary."
            print "If you want to work with BGP data from files, the three options -b, -x, -a and -s must be used."
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
        elif opt == '-m':
            month = int(arg)
        elif opt == '-D':
            day = int(arg)
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
        elif opt == '-4':
            ipv4_prefixes_indexes_file = arg
            fromFiles = True
        elif opt == '-6':
            ipv6_prefixes_indexes_file = arg
            fromFiles = True
        elif opt == '-a':
            ASes_originated_prefixes_file = arg
            fromFiles = True
        elif opt == '-s':
            ASes_propagated_prefixes_file = arg
            fromFiles = True
        else:
            assert False, 'Unhandled option'
        
    if year == '' and (month != '' or (month == '' and day != '')):
        print 'If you provide a month, you must also provide a year.'
        print 'If you provide a day, you must also provide a month and a year.'
        sys.exit()
   
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

    if fromFiles and (bgp_data_file == '' or ipv4_prefixes_indexes_file == '' or\
        ipv6_prefixes_indexes_file == '' or ASes_originated_prefixes_file == '' or\
        ASes_propagated_prefixes_file == ''):
        print "If you want to work with BGP data from files, the three options -b, -x, -a and -s must be used."
        print "If not, none of these three options should be used."
        sys.exit()
        
    today = datetime.date.today().strftime('%Y%m%d')
    
    if not DEBUG:

        if EXTENDED:
            del_file = '%s/extended_apnic_%s.txt' % (files_path, today)
        else:
            del_file = '%s/delegated_apnic_%s.txt' % (files_path, today)

   
    bgp_handler = BGPDataHandler(urls_file, files_path, routing_file, KEEP, RIBfile,\
                    bgp_data_file, ipv4_prefixes_indexes_file, ipv6_prefixes_indexes_file,\
                    ASes_originated_prefixes_file, ASes_propagated_prefixes_file)
    
    if COMPUTE: 
        del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL,\
                        final_existing_date, year, month, day)
        prefixes_Stats, routed_pyt = computePerPrefixStats(bgp_handler, del_handler)
        statsForASes = computeASesStats(bgp_handler, del_handler)
        # TODO Save Stats and routed prefixes to files and ElasticSearch
        
    else:
       bgp_handler.saveDataToFiles(files_path)
        
        
if __name__ == "__main__":
    main(sys.argv[1:])
