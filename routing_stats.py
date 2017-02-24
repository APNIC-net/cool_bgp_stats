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
import copy

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
    
# This function returns the origin AS for a specific prefix
# as seen in the given BGP data
def getOriginASForBlock(prefix, indexes_pyt, bgp_data):
    originASes = set()
    for index in indexes_pyt[prefix]:
        originAS = bgp_data.ix[index, 'ASpath'].split(' ')[-1]
        originASes.add(originAS)
    
    if len(originASes) > 1:
        print "Found prefix originated by more than one AS (%s)" % prefix
        # TODO Analyze these special cases
    if len(originASes) > 0:
        return list(originASes)[0]
    else:
        return None

# This function returns a set with all the AS paths for a specific prefix
# seen in the given BGP data    
def getASpathsForBlock(prefix, indexes_pyt, bgp_data):
    ASpaths = set()
    for index in indexes_pyt[prefix]:
        ASpaths.add(bgp_data.ix[index, 'ASpath'])
    
    return ASpaths
    
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
        
    # We add columns to store as boolean variables the concepts defined in [1]
    
    new_cols1 = ['isCovering', 'allocIntact', 'maxAggrRouted', 'fragmentsRouted',\
                'isCovered_Level1', 'isCovered_Level2plus',\
                'SOSP', 'SODP1', 'SODP2', 'DOSP', 'DODP1', 'DODP2', 'DODP3']

    # The concept of 'aggregation over multiple allocations' defined in the paper
    # cited above is also slightly modified to suit our needs.
    # (Note that we use 'delegation' instead of 'allocation' as it is a more
    # general term.)
    # In the paper each announcement is analyzed. While here we analyze each delegated
    # block or each block resulting from the summarization of multiple delegations.
    # We don't analyze all the possible summarizations of multiple delegations,
    # but just the maximum aggregation possible. This is why we use a variable
    # maxAggrRouted. This variale will be true if there is an announcement for
    # a block resulting from the maximum aggregation possible of multiple delegations.
    
    # We add columns to store as boolean variables some of the concepts defined in [2]
    # Prefixes classified as Top in this paper are the prefixes classified as Covering
    # in [1]. Prefixes classified as Deaggregated in [2] are those classified as
    # SOSP or SODP in [1]. Prefixes classified as Delegated in [2] are those
    # classified as DOSP or DODP in [1]
    # We just use the concepts defined in [2] that are not redundant with the
    # concepts defined in [1]

    new_cols2 = ['isLonely', 'onlyRoot', 'root_MScompl', 'root_MSincompl',\
                    'noRoot_MScompl', 'noRoot_MSincompl']

    # we also add a column to store info about the prefix being originated by an AS
    # that was delegated to an organization that is not the same that received
    # the delegation of the block
    new_cols = new_cols1 + new_cols2 + ['originatedByDiffOrg']
    
    for new_col in new_cols:
        orgs_aggr_networks.loc[:, new_col] =\
            pd.Series([False]*orgs_aggr_networks.shape[0],\
            index=orgs_aggr_networks.index)
    
    # Obtain BGP data
    bgp_data = bgp_handler.bgp_data
    ipv4_prefixes_indexes_pyt = bgp_handler.ipv4_prefixes_indexes_pyt
    ipv6_prefixes_indexes_pyt = bgp_handler.ipv6_prefixes_indexes_pyt
    
    # Obtain ASN delegation data
    asn_del = del_handler.delegated_df[del_handler.delegated_df['resource_type'] == 'asn']

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
              
                
        # Based on [1]

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
          
        visibility = -1
        
        if del_routed.has_key(net):
            # block is being announced, at least partially
            
            more_specifics_wo_block = copy.copy(del_routed[net]['more_specifics'])
            
            # If the block is being announced as-is            
            if net in del_routed[net]['more_specifics']:
                # we get a list of the more specifics not including the block itself
                more_specifics_wo_block.remove(net)
            
            if len(more_specifics_wo_block) > 0:     
                # We summarize the more specific routed blocks without the block itself
                # to get the maximum aggregation possible of the more specifics
                aggr_less_spec = [ipaddr for ipaddr in
                                    ipaddress.collapse_addresses(
                                    [ipaddress.ip_network(unicode(ip_net, 'utf-8'))
                                    for ip_net in more_specifics_wo_block])]
           
            # If there is at least one less specific block being routed
            # the block is covered and therefore its visibility is 100 %
            if len(del_routed[net]['less_specifics']) > 0:
                visibility = 100
                if len(del_routed[net]['less_specifics']) == 1:
                    orgs_aggr_networks.ix[i, 'isCovered_Level1'] = True
                else:
                    orgs_aggr_networks.ix[i, 'isCovered_Level2plus'] = True

                # If apart from having less specific blocks being routed,
                # the block itself is being routed
                if net in del_routed[net]['more_specifics']:
                    # we classify the prefix based on its advertisement paths
                    # paths relative to that of their corresponding covering
                    # prefix
      
                    # We get the origin AS for the block
                    blockOriginAS = getOriginASForBlock(net, prefixes_indexes_pyt, bgp_data)
                    
                    # and the set of AS paths for the block
                    blockASpaths = getASpathsForBlock(net, prefixes_indexes_pyt, bgp_data)

                    # The corresponding covering prefix is the last prefix in the
                    # list of less specifics
                    coveringPref = del_routed[net]['less_specifics'][-1]
    
                    coveringPrefOriginAS = getOriginASForBlock(coveringPref,\
                                                prefixes_indexes_pyt, bgp_data)
    
                    coveringPrefASpaths = getASpathsForBlock(coveringPref,\
                                                prefixes_indexes_pyt, bgp_data)               
              
                    if blockOriginAS == coveringPref_originAS:
                        if len(blockASpaths) == 1:
                            if blockASpaths.issubset(coveringPrefASpath):
                                orgs_aggr_networks.ix[i, 'SOSP'] = True
                            else:
                                orgs_aggr_networks.ix[i, 'SODP2'] = True
                        else: # len(blockASpaths) >= 2
                            if coveringPrefASpaths.issubset(blockASpaths):
                            # TODO Check with Geoff which one I should use
                            # What if there is more than one AS path for the
                            # covering prefix?
                            # if len(coveringPrefASpaths.intersection(blockASpaths)) >= 1:
                                orgs_aggr_networks.ix[i, 'SODP1'] = True
                                    
                    else:
                        if len(blockASpaths) == 1:
                            # TODO Check if blockOriginAS is customer of
                            # coveringPrefOriginAS (check with Geoff)
                            # TODO Check with Geoff if this condition is correct
                            blockASpath_woOrigin = ' '.join(list(blockASpaths)[0].split(' ')[0:-1])
                            if blockASpath_woOrigin in coveringPrefASpaths:
                                orgs_aggr_networks.ix[i, 'DOSP'] = True
                        
                            if not blockASpaths.issubset(coveringPrefASpaths):
                                orgs_aggr_networks.ix[i, 'DODP1'] = True
                        else: # len(blockASpaths) >= 2
                            blockASpaths_woOrigin = set()
                            for ASpath in blockASpaths:
                                blockASpaths_woOrigin.add(' '.join(ASpath.split(' ')[0:-1]))
                            
                            if coveringPrefASpaths.issubset(blockASpaths_woOrigin):
                            # TODO Check with Geoff which one I should use
                            # What if there is more than one AS path for the
                            # covering prefix?
                            # if len(coveringPrefASpaths.intersection(blockASpaths_woOrigin)) >= 1:
                                orgs_aggr_networks.ix[i, 'DODP2'] = True
                          
                            # TODO Ask Geoff about this
                            # Origin AS for covered prefix advertises two or more prefixes                             
                            if ??:
                                orgs_aggr_networks.ix[i, 'DODP3'] = True
                            
            # If there are no less specific blocks being routed
            else:
                # If the list of more specific blocks being routed includes
                # the block itself
                if net in del_routed[net]['more_specifics']:
                    # The block is 100 % visible
                    visibility = 100
                    
                    # If the list of more specific blocks being routed only
                    # includes the block itself, taking into account we are 
                    # under the case of the block not having less specific
                    # blocks being routed,
                    if del_routed[net]['more_specifics'] == 1:
                        # the block is a Lonely prefix
                        # • Lonely: a prefix that does not overlap
                        # with any other prefix.
                        orgs_aggr_networks.ix[i, 'isLonely'] = True
                        
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
                    # The block is a Covering prefix  
                    orgs_aggr_networks.ix[i, 'isCovering'] = True

            # If there are more specific blocks being routed 
            if len(more_specifics_wo_block) > 0:
                orgs_aggr_networks.ix[i, 'fragmentsRouted'] =  True
                    
            # If the block is not the result of aggregating multiple delegated blocks
            # (the block was delegated as-is)
            if not orgs_aggr_networks.ix[i, 'aggregated']: 

                # Independently of the block being a covering or a covered prefix,
                # if the delegated block (not aggregated) is being announced as-is,
                # allocIntact is True
                if net in del_routed[net]['more_specifics']:
                    orgs_aggr_networks.ix[i, 'allocIntact'] = True
                    
                    # We check if the prefix and its origin AS were delegated
                    # to the same organization
                    # This not necessarily works correctly as delegations made
                    # by a NIR do not appear in the delegated file
                    # TODO Should we get the organization holding a specific
                    # resource from WHOIS?
                    originASorg = asn_del[(pd.to_numeric(asn_del['initial_resource']) <= int(asn)) &
                                        (pd.to_numeric(asn_del['initial_resource'])+
                                        pd.to_numeric(asn_del['count'])>int(asn))]['opaque_id'].get_values()
                    if len(originASorg) == 1:
                            originASorg = originAS org[0]
                        else:
                            originASorg = 'UNKNOWN Org'
                
                    prefixOrg = orgs_aggr_networks.ix[i, 'opaque_id']
    
                    # If the prefix is being originated by an AS delegated to
                    # a different organization from the organization that
                    # received the delegation of the block                 
                    if originASorg != prefixOrg:
                        orgs_aggr_networks.ix[i, 'originatedByDiffOrg'] = True
                
                # If there are no less specific blocks being routed and the list
                # of more specific blocks being routed only contains the block
                # itself
                if len(del_routed[net]['less_specifics']) == 0 and del_routed[net]['more_specifics'] == [net]:
                    orgs_aggr_networks.ix[i, 'onlyRoot'] = True
                    # This variable is similar to the variable isLonely but can
                    # only be True for not aggregated blocks 
                
                if net in del_routed[net]['more_specifics']:
                    if len(del_routed[net]['more_specifics']) >= 3 and net in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • root/MS-complete: The root prefix and at least two subprefixes
                        # are announced. The set of all sub-prefixes spans
                        # the whole root prefix.
                        orgs_aggr_networks.ix[i, 'root_MScompl'] = True
                    if len(del_routed[net]['more_specifics']) >= 2 and net not in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • root/MS-incomplete: The root prefix and at least one subprefix
                        # is announced. Together, the set of announced subprefixes
                        # does not cover the root prefix.
                        orgs_aggr_networks.ix[i, 'root_MSincompl'] = True
                else:
                    if len(del_routed[net]['more_specifics']) >= 2 and net in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • no root/MS-complete: The root prefix is not announced.
                        # However, there are at least two sub-prefixes which together
                        # cover the complete root prefix.
                        orgs_aggr_networks.ix[i, 'noRoot_MScompl'] = True
                    if len(del_routed[net]['more_specifics']) >= 1 and net not in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • no root/MS-incomplete: The root prefix is not announced.
                        # There is at least one sub-prefix. Taking all sub-prefixes
                        # together, they do not cover the complete root prefix.
                        orgs_aggr_networks.ix[i, 'noRoot_MSincompl'] = True
                        
            # if the block is the result of an aggregation of multiple delegations
            else:
                # and the block itself is being routed
                if net in del_routed[net]['more_specifics']:
                    # the maximum aggregation over multiple delegations is being routed
                    orgs_aggr_networks.ix[i, 'maxAggrRouted'] = True
        
        orgs_aggr_networks.ix[i, 'visibility'] = visibility

    return orgs_aggr_networks, del_routed

# This function determines whether the allocated ASNs are active
# either as middle AS, origin AS or both
# Returns dictionary with an ASN as key and a dictionary containing:
# * a numeric variable (numOfPrefixesPropagated) specifying the number of prefixes propagated by the AS
# (BGP announcements for which the AS appears in the middle of the AS path)
# * a numeric variable (numOfPrefixesOriginated) specifying the number of prefixes originated by the AS
def computeASesStats(bgp_handler, del_handler):
    expanded_del_asns_df = del_handler.getExpandedASNsDF() 
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

# [1] http://irl.cs.ucla.edu/papers/05-ccr-address.pdf
# [2] http://www.eecs.qmul.ac.uk/~steve/papers/JSAC-deaggregation.pdf