#! /usr/bin/python2.7 
# -*- coding: utf8 -*-


import sys, getopt, os, bz2
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
from DelegatedHandler import DelegatedHandler
from BGPDataHandler import BGPDataHandler
import ipaddress
import pandas as pd
import numpy as np
import datetime
import copy
import radix


# This function downloads information about relationships between ASes inferred
# by CAIDA and stores it in a dictionary in which all the active ASes appear as keys
# and the value is another dictionary that also has an AS as key and a string
# as value specifying whether it is a P2P, a P2C or a C2P relationship
# The serial variable must be 1 or 2 depending on CAIDAS's data to be used
def getASrelInfo(serial, files_path, KEEP):
    
    folder_url = 'http://data.caida.org/datasets/as-relationships/serial-%s/' % serial
    index_file = '%s/CAIDA_index.txt' % files_path
    get_file(folder_url, index_file)
    
    with open(index_file, 'r') as index:
        maxDate = 0
        for line in index.readlines():
            href_pos = line.find('href')
            if href_pos != -1 and line.find('as-rel') != -1:
                try:
                    date_pos = href_pos+6
                    date = int(line[date_pos:date_pos+8])
                    if date > maxDate:
                        maxDate = date
                except ValueError:
                    continue
            
    index.close()
    if not KEEP:
        os.remove(index_file)
    
    if serial == 1:
        as_rel_url = '%s%s.as-rel.txt.bz2' % (folder_url, maxDate)
        as_rel_file = '%s/CAIDA_ASrel_%s.txt.bz2' % (files_path, maxDate)
    elif serial == 2:
        as_rel_url = '%s%s.as-rel2.txt.bz2' % (folder_url, maxDate)
        as_rel_file = '%s/CAIDA_ASrel2_%s.txt.bz2' % (files_path, maxDate)

    get_file(as_rel_url, as_rel_file)
    
    ASrels = dict()
    
    print 'Working with file %s\n' % as_rel_file
    
    with bz2.BZ2File(as_rel_file, 'rb') as as_rel:
        for line in as_rel.readlines():
            if not line.startswith('#'):
                line_parts = line.split('|')
                as1 = int(line_parts[0])
                as2 = int(line_parts[1])
                rel_type = line_parts[2]
                if rel_type == '0':
                    rel_type = 'P2P'
                elif rel_type == '-1':
                    rel_type = 'P2C'
                
                if as1 not in ASrels:
                    ASrels[as1] = dict()
                if as2 not in ASrels[as1]:
                    ASrels[as1][as2] = rel_type

                if rel_type == 'P2C':
                    rel_type = 'C2P'

                if as2 not in ASrels:
                    ASrels[as2] = dict()
                if as2 not in ASrels[as2]:
                    ASrels[as2][as1] = rel_type
                    
    as_rel.close()
    if not KEEP:
        os.remove(as_rel_file)
        
    return ASrels
    
# This function computes statistics for each delegated block
# and for each aggregated block resulting from summarizing multiple delegations
# to the same organization.
# Returns a DataFrame with the computed statistics and
# a Radix with the routed blocks covering each delegated or aggregated block.
# The data dictionary of each node in the Radix contains two keys:
# * more_specifics - the value for this key is a list of more specific blocks
# being routed (including the block itself if it is being routed as-is)
# * less_specifics - the value for this key is a list of less specific blocks
# being routed
def computePerPrefixStats(bgp_handler, del_handler, ASrels):
    # Obtain a DataFrame with all the delegated blocks and all the aggregated
    # block resulting from summarizing multiple delegations
    # to the same organization
    orgs_aggr_networks = del_handler.getDelAndAggrNetworks()
        
    # We add columns to store as boolean variables the concepts defined in [1]
    
    new_cols1 = ['isCovering', 'allocIntact', 'maxAggrRouted', 'fragmentsRouted',\
                'isCovered_Level1', 'isCovered_Level2plus',\
                'SOSP', 'SODP1', 'SODP2', 'DOSP', 'DODP1', 'DODP2', 'DODP3']

    # The concept of 'aggregation over multiple allocations' defined in the paper
    # cited above is slightly modified to suit our needs.
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
    
    # Obtain ASN delegation data
    asn_del = del_handler.delegated_df[del_handler.delegated_df['resource_type'] == 'asn']

    
    del_routed = radix.Radix()
    
    # For each delegated or aggregated block
    for i in orgs_aggr_networks.index:
        net = orgs_aggr_networks.ix[i]['ip_block']
        network = ipaddress.ip_network(unicode(net, "utf-8"))
        ips_delegated = network.num_addresses
   
        # Find routed blocks related to the delegated or aggregated block 
        net_less_specifics = bgp_handler.getRoutedParentAndGrandparents(network)
        net_more_specifics = bgp_handler.getRoutedChildren(network)

        if len(net_more_specifics) > 0 or len(net_less_specifics) > 0:
            routed_node = del_routed.add(net)
            routed_node.data['more_specifics'] = net_more_specifics
            routed_node.data['less_specifics'] = net_less_specifics
              

        # For not aggregated blocks (blocks that were delegated as-is) we compute
        # their Usage Latency and Stability of Visibility
        
        # Based on [1]
        # We define usage latency of an allocated address block as
        # the time interval between the allocation time and the first
        # time a part of, or the entire, allocated block shows up in
        # the BGP routing table
        
        #TODO define Stability of Visibility
        
        # Our first observation about covered prefixes is that they
        # show up and disappear in the routing table more frequently
        # than the covering prefixes. 
        
        # To show this, we compare the
        # routing prefixes between the beginning and end of each 2-
        # month interval and count the following four events: (1) a
        # covered prefix at the beginning remains unchanged at the
        # end of the interval, (2) a covered prefix at the beginning
        # disappears at the end, but its address space is covered by
        # some other prefix(es), (3) a new covered prefix is advertised
        # at the end, and (4) a covered prefix at the beginning disappears
        # before the end and its address space is no longer
        # covered in the routing table.
        
        if not orgs_aggr_networks.ix[i, 'aggregated']: 
            # Usage Latency computation
            del_date = del_handler.getDelegationDate(network)
            
            first_seen = bgp_handler.getDateFirstSeen(network)
                        
            if first_seen is not None and del_date is not None:
                orgs_aggr_networks.ix[i, 'UsageLatency'] = (first_seen-del_date).days
            else:
                orgs_aggr_networks.ix[i, 'UsageLatency'] = float('inf')
      
            # Intact Allocation Usage Latency computation
            # We compute the number of days between the date the block
            # was delegated and the date the block as-is was first seen
            first_seen_intact = bgp_handler.getDateFirstSeenIntact(network)
                        
            if first_seen_intact is not None and del_date is not None:
                orgs_aggr_networks.ix[i, 'UsageLatencyAllocIntact'] = (first_seen_intact-del_date).days
            else:
                orgs_aggr_networks.ix[i, 'UsageLatencyAllocIntact'] = float('inf')
        
            if del_date is not None:

                # Stability of Visibility analysis
                periodsSeen = bgp_handler.getPeriodsSeenFragments(network)
                
                # TODO Compute average, standard deviation, min and max
                # amongst all fragments for daysUsable, daysUsed, daysSeen,
                # avgPeriodsLength, stdPeriodLength, minPeriodLength and
                # maxPeriodLength
                
                # TODO Compute visibility per period
                
                # TODO Anything else?

                # Intact Allocation Stability of Visibility analysis
                periodsIntact = bgp_handler.getPeriodsSeenIntact(network)
                numOfPeriods = len(periodsIntact)
                
                if numOfPeriods > 0:
                    today = datetime.date.today().strftime('%Y%m%d')
                    daysUsable = (today-del_date).days
                    
                    last_seen_intact = bgp_handler.getDateLastSeenIntact(network)
                    daysUsed= (last_seen_intact-first_seen_intact).days
                    daysSeen = bgp_handler.getTotalDaysSeenIntact(network)
                                    
                    periodsLengths = []
                    for period in periodsIntact:
                        periodsLengths.append(\
                            (datetime.datetime.strptime(str(period[1]), '%Y%m%d') -\
                            datetime.datetime.strptime(str(period[0]), '%Y%m%d')).days)
                    
                    avgPeriodLength = np.average(periodsLengths)
                    stdPeriodLength = np.std(periodsLengths)
                    minPeriodLength = np.min(periodsLengths)
                    maxPeriodLength = np.max(periodsLengths)
                

          
        visibility = -1
        
        routed_node = del_routed.search_exact(net)
        if routed_node is not None:
            # block is currently being announced, at least partially
        
            # We get the origin AS for the block
            # If the block is not being routed as-is (net is not in the more
            # specifics list), blockOriginAS is None
            blockOriginAS = bgp_handler.getOriginASForBlock(network)
            
            more_specifics_wo_block = copy.copy(routed_node.data['more_specifics'])
            
            # If the block is being announced as-is            
            if net in routed_node.data['more_specifics']:
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
            if len(routed_node.data['less_specifics']) > 0:
                visibility = 100
                if len(routed_node.data['less_specifics']) == 1:
                    orgs_aggr_networks.ix[i, 'isCovered_Level1'] = True
                else:
                    orgs_aggr_networks.ix[i, 'isCovered_Level2plus'] = True

                # If apart from having less specific blocks being routed,
                # the block itself is being routed
                if net in routed_node.data['more_specifics']:
                    # we classify the prefix based on its advertisement paths
                    # paths relative to that of their corresponding covering
                    # prefix
                    
                    # We get the set of AS paths for the block
                    blockASpaths = bgp_handler.getASpathsForBlock(network)

                    # The corresponding covering prefix is the last prefix in the
                    # list of less specifics
                    coveringPref = routed_node.data['less_specifics'][-1]
                    coveringNet = ipaddress.ip_network(unicode(coveringPref, 'utf-8'))
                    coveringPrefOriginAS = bgp_handler.getOriginASForBlock(coveringNet)
                    coveringPrefASpaths = bgp_handler.getASpathsForBlock(coveringNet)       
              
                    if blockOriginAS == coveringPrefOriginAS:
                        if len(blockASpaths) == 1:
                            if blockASpaths.issubset(coveringPrefASpaths):
                                orgs_aggr_networks.ix[i, 'SOSP'] = True
                            else:
                                orgs_aggr_networks.ix[i, 'SODP2'] = True
                        else: # len(blockASpaths) >= 2
                            if len(coveringPrefASpaths.intersection(blockASpaths)) > 0 and\
                                len(blockASpaths.difference(coveringPrefASpaths)) > 0:
                                orgs_aggr_networks.ix[i, 'SODP1'] = True
                                    
                    else:
                        if len(blockASpaths) == 1:
                       
                            if blockOriginAS in ASrels and\
                                coveringPrefOriginAS in ASrels[blockOriginAS] and\
                                ASrels[blockOriginAS][coveringPrefOriginAS] == 'C2P':

                                blockASpath_woOrigin = ' '.join(list(blockASpaths)[0].split(' ')[0:-1])
                                if blockASpath_woOrigin in coveringPrefASpaths:
                                    orgs_aggr_networks.ix[i, 'DOSP'] = True
                        
                            if not blockASpaths.issubset(coveringPrefASpaths):
                                orgs_aggr_networks.ix[i, 'DODP1'] = True
                        else: # len(blockASpaths) >= 2
                            blockASpaths_woOrigin = set()
                            for ASpath in blockASpaths:
                                blockASpaths_woOrigin.add(' '.join(ASpath.split(' ')[0:-1]))
                            
                            if len(coveringPrefASpaths.intersection(blockASpaths_woOrigin)) > 0 and\
                                len(blockASpaths_woOrigin.difference(coveringPrefASpaths)) > 0:
                                orgs_aggr_networks.ix[i, 'DODP2'] = True
                          
                            if len(coveringPrefASpaths.intersection(blockASpaths)) == 0:
                                # TODO Ask Geoff about this
                                # Origin AS for covered prefix and Origin AS for
                                # covering prefix have a common customer?
                                # Origin AS for covered prefix advertises two or more prefixes 
                                orgs_aggr_networks.ix[i, 'DODP3'] = True
                            
            # If there are no less specific blocks being routed
            else:
                # If the list of more specific blocks being routed includes
                # the block itself
                if net in routed_node.data['more_specifics']:
                    # The block is 100 % visible
                    visibility = 100
                    
                    # If the list of more specific blocks being routed only
                    # includes the block itself, taking into account we are 
                    # under the case of the block not having less specific
                    # blocks being routed,
                    if len(routed_node.data['more_specifics']) == 1:
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
                if net in routed_node.data['more_specifics']:
                    orgs_aggr_networks.ix[i, 'allocIntact'] = True
                    
                    # We check if the prefix and its origin AS were delegated
                    # to the same organization
                    # This not necessarily works correctly as delegations made
                    # by a NIR do not appear in the delegated file
                    # TODO Should we get the organization holding a specific
                    # resource from WHOIS?
                    # https://www.apnic.net/about-apnic/whois_search/about/rdap/
                    originASorg = asn_del[(pd.to_numeric(asn_del['initial_resource']) <= int(blockOriginAS)) &
                                        (pd.to_numeric(asn_del['initial_resource'])+
                                        pd.to_numeric(asn_del['count'])>int(blockOriginAS))]['opaque_id'].get_values()
                    if len(originASorg) == 1:
                        originASorg = originASorg[0]
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
                if len(routed_node.data['less_specifics']) == 0 and routed_node.data['more_specifics'] == [net]:
                    orgs_aggr_networks.ix[i, 'onlyRoot'] = True
                    # This variable is similar to the variable isLonely but can
                    # only be True for not aggregated blocks 
                
                elif net in routed_node.data['more_specifics']:
                    if len(routed_node.data['more_specifics']) >= 3 and net in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • root/MS-complete: The root prefix and at least two subprefixes
                        # are announced. The set of all sub-prefixes spans
                        # the whole root prefix.
                        orgs_aggr_networks.ix[i, 'root_MScompl'] = True
                    elif len(routed_node.data['more_specifics']) >= 2 and net not in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • root/MS-incomplete: The root prefix and at least one subprefix
                        # is announced. Together, the set of announced subprefixes
                        # does not cover the root prefix.
                        orgs_aggr_networks.ix[i, 'root_MSincompl'] = True
                else:
                    if len(routed_node.data['more_specifics']) >= 2 and net in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • no root/MS-complete: The root prefix is not announced.
                        # However, there are at least two sub-prefixes which together
                        # cover the complete root prefix.
                        orgs_aggr_networks.ix[i, 'noRoot_MScompl'] = True
                    elif len(routed_node.data['more_specifics']) >= 1 and net not in [str(ip_net) for ip_net in aggr_less_spec]:
                        # • no root/MS-incomplete: The root prefix is not announced.
                        # There is at least one sub-prefix. Taking all sub-prefixes
                        # together, they do not cover the complete root prefix.
                        orgs_aggr_networks.ix[i, 'noRoot_MSincompl'] = True
                        
            # if the block is the result of an aggregation of multiple delegations
            else:
                # and the block itself is being routed
                if net in routed_node.data['more_specifics']:
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
    urls_provided = False
    RIBfiles = True
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
    ipv4_prefixesDates_file = ''
    ipv6_prefixesDates_file = ''
    archive_folder = '' 
    extension = ''
    COMPRESSED = False
    startDate = ''

#For DEBUG
    files_path = '/Users/sofiasilva/BGP_files'
#    routing_file = '/Users/sofiasilva/BGP_files/bgptable.txt'
    KEEP = True
#    RIBfiles = False
    DEBUG = True
    EXTENDED = True
    del_file = '/Users/sofiasilva/BGP_files/extended_apnic_20170216.txt'
    archive_folder = '/Users/sofiasilva/BGP_files'
    extension = 'bgprib.mrt'
#    COMPRESSED = True
#    COMPUTE = False    
    
    try:
        opts, args = getopt.getopt(argv, "hp:u:r:H:E:I:ockny:m:D:d:ei:b:4:6:a:s:F:S:", ["files_path=", "urls_file=", "routing_file=", "Historcial_data_folder=", "Extension=", "InitialDate=", "year=", "month=", "day=", "delegated_file=", "stats_file=", "bgp_data_file=", "IPv4_prefixes_ASes_file=", "IPv6_prefixes_ASes_file=", "ASes_originated_prefixes_file=", "ASes_propagated_prefixes_file=", "ipv4_prefixesDates_file=", "ipv6_prefixesDates_file="])
    except getopt.GetoptError:
        print 'Usage: routing_stats.py -h | -p <files path> [-u <urls file> | -r <routing file> | -H <Historical data folder> -E <extension> [-I <Initial date>]] [-o] [-c] [-k] [-n] [-y <year> [-m <month> [-D <day>]]] [-d <delegated file>] [-e] [-i <stats file>] [-b <bgp_data file> -4 <IPv4 prefixes_indexes file> -6 <IPv6 prefixes_indexes file> -a <ASes_originated_prefixes file> -s <ASes_propagated_prefixes file>] [-F <ipv4_prefixesDates file>] [-S <ipv6_prefixesDates file>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes routing statistics from files containing Internet routing data and a delegated file."
            print 'Usage: routing_stats.py -h | -p <files path> [-u <urls file> | -r <routing file> | -H <Historical data folder> -E <extension> [-I <Initial date>]] [-o] [-c] [-k] [-n] [-y <year> [-m <month> [-D <day>]]] [-d <delegated file>] [-e] [-i <stats file>] [-b <bgp_data file> -4 <IPv4 prefixes_indexes file> -6 <IPv6 prefixes_indexes file> -a <ASes_originated_prefixes file> -s <ASes_propagated_prefixes file>] [-F <ipv4_prefixesDates file>] [-S <ipv6_prefixesDates file>]'
            print 'h = Help'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print 'u = URLs file. File which contains a list of URLs of the files to be downloaded.'
            print 'All the URLs must point either to RIB files or to files containing "show ip bgp" outputs.'
            print 'If the URLs point to files containing "show ip bgp" outputs, the "-o" option must be used to specify this.'
            print 'r = Use already downloaded Internet Routing data file.'
            print "H = Historical data. Instead of processing a single file, process the routing data contained in the archive folder provided."
            print "E = Extension. If you use the -H option you MUST also use the -E option to provide the extension of the files in the archive you want to work with."
            print "I = Incremental dates. If you use this option you must provide a start date for the period of time for which you want to get the dates in which the prefixes were seen."
            print "If you also use the -F or the -S option to provide an existing prefixesDates file, the new dates will be added to the existing dates in the Radix."
            print "If none of the three options -u, -r or -H are provided, the script will try to work with routing data from URLs included ./BGPoutputs.txt"
            print 'o = The routing data to be processed is in the format of "show ip bgp" outputs.'
            print 'c = Compressed. The files containing routing data are compressed.'
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
            print "4 = IPv4 prefixes_indexes file. Path to pickle file containing IPv4 prefixes_indexes Radix."
            print "6 = IPv6 prefixes_indexes file. Path to pickle file containing IPv6 prefixes_indexes Radix."
            print "a = ASes_originated_prefixes file. Path to pickle file containing ASes_originated_prefixes dictionary."
            print "s = ASes_propagated_prefixes file. Path to pickle file containing ASes_propagated_prefixes dictionary."
            print "If you want to work with BGP data from files, the three options -b, -x, -a and -s must be used."
            print "If not, none of these four options should be used."
            print "F = IPv4 (Four) prefixesDates file. Path to pickle file containing prefixesDates Radix with the dates in which each IPv4 prefix was seen."
            print "S = IPv6 (Six) prefixesDates file. Path to pickle file containing prefixesDates Radix with the dates in which each IPv6 prefix was seen."
            sys.exit()
        elif opt == '-u':
            urls_file = arg
            urls_provided = True
        elif opt == '-o':
            RIBfiles = False
        elif opt == '-c':
            COMPRESSED = True
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
            files_path = os.path.abspath(arg.rstrip('/'))
        elif opt == '-i':
            INCREMENTAL = True
            stats_file = arg
        elif opt == '-b':
            bgp_data_file = arg
            fromFiles = True
        elif opt == '-4':
            ipv4_prefixes_indexes_file = os.path.abspath(arg)
            fromFiles = True
        elif opt == '-6':
            ipv6_prefixes_indexes_file = os.path.abspath(arg)
            fromFiles = True
        elif opt == '-a':
            ASes_originated_prefixes_file = os.path.abspath(arg)
            fromFiles = True
        elif opt == '-s':
            ASes_propagated_prefixes_file = os.path.abspath(arg)
            fromFiles = True
        elif opt == '-H':
            archive_folder = os.path.abspath(arg.rstrip('/'))
        elif opt == '-E':
            extension = arg
        elif opt == '-I':
            startDate = int(arg)
        elif opt == '-F':
            ipv4_prefixesDates_file = os.path.abspath(arg)
        elif opt == '-S':
            ipv6_prefixesDates_file = os.path.abspath(arg)
        else:
            assert False, 'Unhandled option'
            
    if urls_provided and (routing_file != '' or archive_folder != '') or\
        routing_file != '' and (urls_provided or archive_folder != '') or\
        archive_folder != '' and (urls_provided or routing_file != ''):

        print "You MUST NOT use more than one of the -u, -r and -H options."
        sys.exit()
        
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
        
    if archive_folder != '' and extension == '':
        print "If you use the -H option you MUST also use the -E option to provide the extension of the files in the archive you want to work with."
        sys.exit()

                
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


    bgp_handler = BGPDataHandler(DEBUG, files_path, KEEP, RIBfiles, COMPRESSED)

    loaded = False 
    
    if ipv4_prefixesDates_file != '' or ipv6_prefixesDates_file != '':
        bgp_handler.loadPrefixDatesFromFiles(ipv4_prefixesDates_file, ipv6_prefixesDates_file)    
    
    if fromFiles:
        loaded = bgp_handler.loadStructuresFromFiles(bgp_data_file, ipv4_prefixes_indexes_file,\
                                ipv6_prefixes_indexes_file, ASes_originated_prefixes_file,\
                                ASes_propagated_prefixes_file)
    else:
        if routing_file == '' and archive_folder == '':
            loaded = bgp_handler.loadStructuresFromURLSfile(urls_file)
        elif routing_file != '':
            loaded = bgp_handler.loadStructuresFromRoutingFile(routing_file)
        else: # archive_folder not null
            loaded = bgp_handler.loadStructuresFromArchive(archive_folder, extension, startDate)
    
    if not loaded:
        print "Data structures not loaded!\n"
        sys.exit()
        
    if COMPUTE: 
        del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL,\
                        final_existing_date, year, month, day)

        ASrels = getASrelInfo(serial=2, files_path=files_path, KEEP=KEEP)

        prefixes_Stats, routed_radix = computePerPrefixStats(bgp_handler, del_handler, ASrels)
        statsForASes = computeASesStats(bgp_handler, del_handler)
        # TODO Save Stats and routed prefixes to files and ElasticSearch
        
    else:
       bgp_handler.saveDataToFiles()
        
        
if __name__ == "__main__":
    main(sys.argv[1:])

# [1] http://irl.cs.ucla.edu/papers/05-ccr-address.pdf
# [2] http://www.eecs.qmul.ac.uk/~steve/papers/JSAC-deaggregation.pdf