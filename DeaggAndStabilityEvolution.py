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
from BGPDataHandler import BGPDataHandler
#from DelegatedHandler import DelegatedHandler
import radix

DEBUG = False
#del_file = '/Users/sofiasilva/BGP_files/extended_apnic_20170328.txt'
#EXTENDED = True
#del_startDate = '20000101'
#del_endDate = '20000131'
#INCREMENTAL = False
#final_existing_date = ''
files_path = '/home/sofia/BGP_stats_files'
KEEP = True

# TODO Para mi projecto, analizar solo los bloques del delegated file
#del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file,
#                                                del_startDate, del_endDate, INCREMENTAL,
#                                                final_existing_date, KEEP)

archive_folder = '/home/sofia/BGP_stats_files'

bgp_handler = BGPDataHandler(DEBUG, files_path, KEEP)

# TODO Repetir luego con ext = '' para las fechas faltantes para que se complete con los arhivos para los que no tengo readable
routing_files_dict = dict()
ext = 'readable'
routing_files_dict = bgp_handler.getPathsToHistoricalData_dict('', '',
                                                          archive_folder, ext,
                                                          routing_files_dict)

daily_stats = dict()

for date in routing_files_dict:
    daily_stats[date] = dict()
    daily_stats[date]['v4_deagg'] = 0
    daily_stats[date]['v6_deagg'] = 0
    
    ipv4_radix = radix.Radix()
    ipv6_radix = radix.Radix()
    
    date_v4_ready = False
    date_v6_ready = False
    for r_file in routing_files_dict[date]:
        if not date_v4_ready or not date_v6_ready:
            if r_file.endswith('bgprib.mrt'):
                date_v4_ready = True
                date_v6_ready = True
                isReadable = False
                RIBfile = True
                COMPRESSED = False
            elif r_file.endswith('bgprib.readable'):
                date_v4_ready = True
                date_v6_ready = True
                isReadable = True
                RIBfile = False
                COMPRESSED = False
            elif r_file.endswith('v6.readable'):
                date_v6_ready = True
                isReadable = True
                RIBfile = False
                COMPRESSED = False
            elif r_file.endswith('readable'):
                date_v4_ready = True
                isReadable = True
                RIBfile = False
                COMPRESSED = False                
            elif r_file.endswith('v6.dmp.gz'):
                date_v6_ready = True
                isReadable = False
                RIBfile = False
                COMPRESSED = True
            elif r_file.endswith('dmp.gz'):
                date_v4_ready = True
                isReadable = False
                RIBfile = False
                COMPRESSED = True
                
            ipv4_radix, ipv6_radix = bgp_handler.completeRadixesFromRoutingFile(
                                                    r_file, isReadable,
                                                    RIBfile, COMPRESSED,
                                                    ipv4_radix, ipv6_radix)

            if date_v4_ready and date_v6_ready:
                break
            
    daily_stats[date]['v4_numOfPrefs'] = len(ipv4_radix.prefixes())
    daily_stats[date]['v6_numOfPrefs'] = len(ipv6_radix.prefixes())
    
    for ipv4_node in ipv4_radix:
        if len(ipv4_radix.search_covered(ipv4_node.prefix)) > 1:
            daily_stats[date]['v4_deagg'] += 1
    
    for ipv6_node in ipv6_radix:
        if len(ipv6_radix.search_covered(ipv6_node.prefix)) > 1:
            daily_stats[date]['v6_deagg'] += 1

for date in daily_stats:
    print 'Date: {} - Total number of IPv4 Prefixes: {} - Number of deaggregated IPv4 Prefixes: {} - Total number of IPv6 Prefixes: {} - Number of deaggregated IPv6 Prefixes: {}'.format(date, daily_stats[date]['v4_numOfPrefs'], daily_stats[date]['v4_deagg'], daily_stats[date]['v6_numOfPrefs'], daily_stats[date]['v6_deagg'])
            
# TODO Debug