#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import os, subprocess, shlex
import re
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
from DelegatedHandler import DelegatedHandler
import ipaddress
import pandas as pd
import numpy as np
import pickle
import pytricia

# For some reason in my computer os.getenv('PATH') differs from echo $PATH
# /usr/local/bin is not in os.getenv('PATH')
bgpdump = '/usr/local/bin/bgpdump'

   
def downloadAndUnzip(url, files_path, routing_file, KEEP):
    if routing_file == '':
        routing_file = '%s/%s' % (files_path, url.split('/')[-1])
        get_file(url, routing_file)
    
    if KEEP:
        cmd = 'gunzip -k %s' % routing_file
        #  GUNZIP
        #  -k --keep            don't delete input files during operation
    else:
        cmd = 'gunzip %s' % routing_file
        
    subprocess.call(shlex.split(cmd))    
    
    return '.'.join(routing_file.split('.')[:-1]) # Path to decompressed file
    
def decodeAndParse(decomp_file_name, KEEP):
    readable_file_name = '%s.readable' % decomp_file_name    
    cmd = shlex.split('%s -m -O %s %s' % (bgpdump, readable_file_name, decomp_file_name))
#    cmd = shlex.split('bgpdump -m -O %s %s' % (readable_file_name, decomp_file_name))   
    
    #  BGPDUMP
    #  -m         one-line per entry with unix timestamps
    #  -O <file>  output to <file> instead of STDOUT
    subprocess.call(cmd)
    
    # For DEBUG
#    readable_file_name = '%s.test' % readable_file_name
    
    readable_file_obj = open(readable_file_name, 'r')

    prefixes_ASes_pyt = pytricia.PyTricia()
    ASes_prefixes_dic = dict()
    
    for line in readable_file_obj.readlines():
        pattern = re.compile("^TABLE_DUMP.?\|\d+\|B\|(.+?)\|.+?\|(.+?)\|(.+?)\|(.+?)\|.+")
        s = pattern.search(line)
    	
        if s:
#            peer = s.group(1)
            prefix = s.group(2)
            path = s.group(3)
            originAS = path.split(' ')[-1]
#            origin = s.group(4)
            if prefixes_ASes_pyt.has_key(prefix): 
                if originAS not in prefixes_ASes_pyt[prefix]:
                    prefixes_ASes_pyt[prefix].append(originAS)
            else:
                prefixes_ASes_pyt[prefix] = [originAS]
                
            if originAS in ASes_prefixes_dic.keys():
                if prefix not in ASes_prefixes_dic[originAS]:
                    ASes_prefixes_dic[originAS].append(prefix)
            else:
                ASes_prefixes_dic[originAS] = [prefix]
    readable_file_obj.close()
    
    if not KEEP:
        try:
            os.remove(decomp_file_name)
            os.remove(readable_file_name)
        except OSError:
            pass
    
    return prefixes_ASes_pyt, ASes_prefixes_dic
    
def getDelAndAggrNetworks():
    # For DEBUG
    DEBUG = False
    EXTENDED = True
    del_file = '/Users/sofiasilva/BGP_files/extended_apnic_20170201.txt'
    INCREMENTAL = False
    final_existing_date = ''
    year = ''
    
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year)

    delegated_df_copy = del_handler.delegated_df.reset_index()
    
    ipv4_cidr_del_df = pd.DataFrame(columns=delegated_df_copy.columns)
    
    # For IPv4 the 'count' column includes the number of IP addresses delegated
    # but it not necessarily corresponds to a CIDR block.
    # Therefore we convert each row to the corresponding CIDR block or blocks,
    # now using the 'count' column to save the prefix length instead of the number of IPs.
    for index, row in delegated_df_copy[delegated_df_copy['resource_type'] == 'ipv4'].iterrows():
        initial_ip = ipaddress.ip_address(unicode(row['initial_resource'], "utf-8"))
        count = int(row['count'])
        final_ip = initial_ip + count - 1
        
        cidr_networks = [ipaddr for ipaddr in ipaddress.summarize_address_range(\
                            ipaddress.IPv4Address(initial_ip),\
                            ipaddress.IPv4Address(final_ip))]
        
        for net in cidr_networks:
            ipv4_cidr_del_df.loc[ipv4_cidr_del_df.shape[0]] = [row['index'],\
                                                                row['registry'],\
                                                                row['cc'],\
                                                                row['resource_type'],\
                                                                str(net.network_address),\
                                                                int(net.prefixlen),\
                                                                row['date'],\
                                                                '%s_cidr' % row['status'],\
                                                                row['opaque_id'],\
                                                                row['region']]       
    
    orgs_aggr_networks = pd.DataFrame(columns=['opaque_id',\
                                                'cc',\
                                                'region',\
                                                'ip_block',\
                                                'aggregated',\
                                                'visibility',\
                                                'deaggregation',\
                                                'multiple_originASes'])
                                                
    ipv6_subset = delegated_df_copy[delegated_df_copy['resource_type'] == 'ipv6']
    ip_subset = pd.concat([ipv4_cidr_del_df, ipv6_subset])

    orgs_groups = ip_subset.groupby(ip_subset['opaque_id'])
    
    for org in list(set(ip_subset['opaque_id'])):
        org_subset = orgs_groups.get_group(org)

        del_networks_list = []
        del_networks_info = pytricia.PyTricia()

        for index, row in org_subset.iterrows():
            ip_block = u'%s/%s' % (row['initial_resource'], int(row['count']))
            cc = row['cc']
            region = row['region']
            del_networks_info[str(ip_block)] = {'cc':cc, 'region':region}

            orgs_aggr_networks.loc[orgs_aggr_networks.shape[0]] = [org, cc, region, str(ip_block), False, 0, 0, False]
            del_networks_list.append(ipaddress.ip_network(ip_block))
     
        aggregated_networks = [ipaddr for ipaddr in ipaddress.collapse_addresses(del_networks_list)]
        
        for aggr_net in aggregated_networks:
            ccs = set()
            regions = set()
            for del_block in del_networks_list:
                del_block_str = str(del_block)
                if aggr_net.supernet_of(del_block):
                    ccs.add(del_networks_info[del_block_str]['cc'])
                    regions.add(del_networks_info[del_block_str]['region'])
            orgs_aggr_networks.loc[orgs_aggr_networks.shape[0]] = [org, list(ccs), list(regions), str(aggr_net), True, 0, 0, False]
    
    return orgs_aggr_networks

def computeRoutingStats(url, files_path, routing_file, KEEP):
    decomp_file_name = downloadAndUnzip(url, files_path, routing_file, KEEP)
   
    orgs_aggr_networks = getDelAndAggrNetworks()
    
    prefixes_ASes_pyt, ASes_prefixes_dic = decodeAndParse(decomp_file_name, KEEP)
    
    # TODO Stats related to prefix/originAS (?)
    # Refactor prefix/origin-as pairs to generate data which is tagged by member, economy, region
    
    del_routed_pyt = pytricia.PyTricia()
    
    for i in orgs_aggr_networks.index:
        net = orgs_aggr_networks.ix[i]['ip_block']
        network = ipaddress.ip_network(unicode(net, "utf-8"))
        ips_delegated = network.num_addresses
        del_routed = []
        
        for routed_net in prefixes_ASes_pyt:
            routed_network = ipaddress.ip_network(unicode(routed_net, "utf-8"))
            if(network.overlaps(routed_network)):
                del_routed.append(routed_network)
        
        # TODO Check if prefix and origin AS were delegated to the same organization

        # TODO Both for visibility and for deaggregation, consider the case in which
        # a delegated block is covered by more than one overlapping announces.
        # For visibility, we cannot count those IP addresses more than once.
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
        
        del_routed_pyt[net] = del_routed
        
        deaggregation = float(np.nan)
        routed_count = len(del_routed)

        ips_routed = 0

        if routed_count > 0: # block is being announced, at least partially
            originASes = set()
            for routed_block in del_routed:
                originAS = prefixes_ASes_pyt[str(routed_block)]
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
                
            aggregated_count = float(len(aggregated_routed))
            deaggregation = (1 - (aggregated_count/routed_count))*100
        
        visibility = (ips_routed*100)/ips_delegated

        orgs_aggr_networks.ix[i, 'visibility'] = visibility
        orgs_aggr_networks.ix[i, 'deaggregation'] = deaggregation
                    
                        
    orgs_stats = dict()
    
    for org in list(set(orgs_aggr_networks['opaque_id'])):
        org_rows = orgs_aggr_networks[orgs_aggr_networks['opaque_id'] == org]
        orgs_stats[org] = dict()
        delegated_blocks = list(org_rows['ip_block'])
        orgs_stats[org]['del_blocks_aggr'] = delegated_blocks
        routed_blocks = []
        for d in delegated_blocks:
            routed_blocks.extend(del_routed_pyt[d])
            orgs_stats[org]['routed_blocks'] = routed_blocks
        orgs_stats[org]['visibility'] = np.mean(org_rows['visibility'])
        deagg_list = list(org_rows['deaggregation'])
        orgs_stats[org]['avg_deaggregation'] = np.mean(np.ma.masked_array(deagg_list, np.isnan(deagg_list)))

    return orgs_stats
        

def main(argv):
    
    urls_file = './Collectors.txt'     # TODO modificar URL para usar colector de APNIC
    files_path = ''
    routing_file = ''
    KEEP = False
    COMPUTE = True
    
    #For DEBUG
#    files_path = '/Users/sofiasilva/BGP_files'
#    routing_file = '/Users/sofiasilva/BGP_files/bview.20170112.0800.gz'
#    KEEP = True

    
    try:
        opts, args = getopt.getopt(argv, "hu:r:knp:", ["urls_file=", "routing_file=", "files_path="])
    except getopt.GetoptError:
        print 'Usage: downloadAndProcess.py -h | [-u <urls file>] [-r <routing file>] [-k] [-n] -p <files path>'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script downloads a file with Internet routing data, uncompresses it and decodes it using BGPDump"
            print 'Usage: downloadAndProcess.py -h | [-u <urls file>] [-r <routing file>] [-k] [-n] -p <files path>'
            print 'h = Help'
            print 'u = URLs file. File which contains a list of URLs of the files to be downloaded.'
            print "If not provided, the script will try to use ./Collectors.txt"
            print 'r = Use already downloaded Internet Routing data file.'
            print 'k = Keep downloaded Internet routing data file.'
            print 'n = No computation. If this option is used, statistics will not be computed, just the dictionaries with prefixes/origin ASes will be created and saved to disk.'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            sys.exit()
        elif opt == '-u':
            urls_file = arg
        elif opt == '-r':
            routing_file = arg
        elif opt == '-k':
            KEEP = True
        elif opt == '-n':
            COMPUTE = False
        elif opt == '-p':
            files_path = arg
        else:
            assert False, 'Unhandled option'
        
    if files_path == '':
        print "You must provide the path to a folder to save files."
        sys.exit()
    
    if COMPUTE:    
        if routing_file == '':
            urls_file_obj = open(urls_file, 'r')
        
            for line in urls_file_obj:
                sys.stderr.write("Starting to work with %s" % line)
                computeRoutingStats(line.strip(), files_path, routing_file, KEEP)           
            
            urls_file_obj.close()
            
        else:
            computeRoutingStats('', files_path, routing_file, KEEP)
    else:
        if routing_file == '':
            urls_file_obj = open(urls_file, 'r')
        
            for line in urls_file_obj:
                sys.stderr.write("Starting to work with %s" % line)
                decomp_file_name = downloadAndUnzip(line.strip(), files_path, routing_file, KEEP)
                prefixes_ASes_pyt, ASes_prefixes_dic = decodeAndParse(decomp_file_name, KEEP)

            urls_file_obj.close()
            
        else:
            decomp_file_name = downloadAndUnzip('', files_path, routing_file, KEEP)
            prefixes_ASes_pyt, ASes_prefixes_dic = decodeAndParse(decomp_file_name, KEEP)
            
        with open('%s/prefixes_ASes.pkl' % files_path, 'wb') as f:
            pickle.dump(prefixes_ASes_pyt, f, pickle.HIGHEST_PROTOCOL)
            
        with open('%s/ASes_prefixes.pkl' % files_path, 'wb') as f:
            pickle.dump(ASes_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
        
if __name__ == "__main__":
    main(sys.argv[1:])
