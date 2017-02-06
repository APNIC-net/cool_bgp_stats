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

    prefixes_dic = dict()
    
    for line in readable_file_obj.readlines():
        pattern = re.compile("^TABLE_DUMP.?\|\d+\|B\|(.+?)\|.+?\|(.+?)\|(.+?)\|(.+?)\|.+")
        s = pattern.search(line)
    	
        if s:
#            peer = s.group(1)
            prefix = s.group(2)
            path = s.group(3)
            originAS = path.split(' ')[-1]
#            origin = s.group(4)
            if prefix in prefixes_dic.keys():
                if originAS not in prefixes_dic[prefix]:
                    prefixes_dic[prefix].extend([originAS])
            else:
                prefixes_dic[prefix] = [originAS]

    readable_file_obj.close()
    
    if not KEEP:
        try:
            os.remove(decomp_file_name)
            os.remove(readable_file_name)
        except OSError:
            pass
    
    return prefixes_dic
    
def getAggregatedNetworks(del_df, orgs_aggr_networks):
    orgs_groups = del_df.groupby(del_df['opaque_id'])
    
    for org in list(set(del_df['opaque_id'])):
        org_subset = orgs_groups.get_group(org)

        networks_list = []        
        for index, row in org_subset.iterrows():
            networks_list.append(ipaddress.ip_network(u'%s/%s' % (row['initial_resource'], int(row['count']))))
        
        aggregated_networks = [ipaddr for ipaddr in ipaddress.collapse_addresses(networks_list)]
        
        for aggr_net in aggregated_networks:
            orgs_aggr_networks.loc[orgs_aggr_networks.shape[0]] = [org, str(aggr_net), [], 0, 0]
    
    return orgs_aggr_networks

def computeRoutingStats(url, files_path, routing_file, KEEP):
    decomp_file_name = downloadAndUnzip(url, files_path, routing_file, KEEP)
    prefixes_ASes_dic = decodeAndParse(decomp_file_name)
    
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
                                                                net.network_address,\
                                                                net.prefixlen,\
                                                                row['date'],\
                                                                '%s_cidr' % row['status'],\
                                                                row['opaque_id'],\
                                                                row['region']]       
    
    orgs_aggr_networks = pd.DataFrame(columns=['opaque_id', 'del_network', 'visibility', 'deaggregation'])
    orgs_aggr_networks = getAggregatedNetworks(ipv4_cidr_del_df, orgs_aggr_networks)
    ipv6_subset = delegated_df_copy[delegated_df_copy['resource_type'] == 'ipv6']
    orgs_aggr_networks = getAggregatedNetworks(ipv6_subset, orgs_aggr_networks)
    
    del_routed_dic = dict()
    
    for del_net in orgs_aggr_networks['del_network']:
        del_network = ipaddress.ip_network(unicode(del_net, "utf-8"))
        ips_delegated = del_network.num_addresses
        ips_routed = 0
        del_routed_dic[del_net] = []
        
        for routed_net in prefixes_ASes_dic.keys():
            routed_network = ipaddress.ip_network(unicode(routed_net, "utf-8"))
            if(del_network.overlaps(routed_network)):
                del_routed_dic[del_net].append(routed_network)
                ips_routed += routed_network.num_addresses
        
        visibility = (ips_routed*100)/ips_delegated

        deaggregation = float(np.nan)
        routed_count = len(del_routed_dic[del_net])
        if routed_count > 0:               
            aggregated_count = float(len([ipaddr for ipaddr in\
                            ipaddress.collapse_addresses(del_routed_dic[del_net])]))
            deaggregation = (1 - (aggregated_count/routed_count))*100
        
        curr_index = np.flatnonzero(orgs_aggr_networks['del_network'] == del_net)[0]
        orgs_aggr_networks.ix[curr_index, 'visibility'] = visibility
        orgs_aggr_networks.ix[curr_index, 'deaggregation'] = deaggregation
                    
                        
    orgs_stats = dict()
    
    for org in list(set(orgs_aggr_networks['opaque_id'])):
        org_rows = orgs_aggr_networks[orgs_aggr_networks['opaque_id'] == org]
        orgs_stats[org] = dict()
        delegated_blocks = list(org_rows['del_network'])
        orgs_stats[org]['del_blocks_aggr'] = delegated_blocks
        routed_blocks = []
        for d in delegated_blocks:
            routed_blocks.extend(del_routed_dic[d])
            orgs_stats[org]['routed_blocks'] = routed_blocks
        orgs_stats[org]['visibility'] = np.mean(org_rows['visibility'])
        deagg_list = list(org_rows['deaggregation'])
        orgs_stats[org]['avg_deaggregation'] = np.mean(np.ma.masked_array(deagg_list, np.isnan(deagg_list)))

    return orgs_stats
        

def main(argv):
    
    urls_file = ''
    files_path = ''
    routing_file = ''
    KEEP = False
    
    #For DEBUG
    files_path = '/Users/sofiasilva/BGP_files'
    urls_file = './Collectors.txt'     # TODO modificar URL para usar colector de APNIC
    routing_file = '/Users/sofiasilva/BGP_files/bview.20170112.0800.gz'
    KEEP = True

    
    try:
        opts, args = getopt.getopt(argv, "hu:r:kp:", ["urls_file=", "routing_file=", "files_path="])
    except getopt.GetoptError:
        print 'Usage: downloadAndProcess.py -h | -u <urls file> [-r <routing file>] [-k] -p <files path>'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script downloads a file with Internet routing data, uncompresses it and decodes it using BGPDump"
            print 'Usage: downloadAndProcess.py -h | -u <urls file> [-k] -p <files path>'
            print 'h = Help'
            print 'u = URLs file. File which contains a list of URLs of the files to be downloaded.'
            print 'r = Use already downloaded Internet Routing data file.'
            print 'k = Keep downloaded Internet routing data file.'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            sys.exit()
        elif opt == '-u':
            urls_file = arg
        elif opt == '-r':
            routing_file = arg
        elif opt == '-k':
            KEEP = True
        elif opt == '-p':
            files_path = arg
        else:
            assert False, 'Unhandled option'
            
    if urls_file == '':
        print "You must provide the path to a file with the URLs of the files to be downloaded."
        sys.exit()
        
    if files_path == '':
        print "You must provide the path to a folder to save files."
        sys.exit()
    
    if routing_file == '':
        urls_file_obj = open(urls_file, 'r')
    
        for line in urls_file_obj:
            sys.stderr.write("Starting to work with %s" % line)
            computeRoutingStats(line.strip(), files_path, routing_file, KEEP)           
        
        urls_file_obj.close()
        
    else:
        computeRoutingStats('', files_path, routing_file, KEEP)
    
        
if __name__ == "__main__":
    main(sys.argv[1:])
