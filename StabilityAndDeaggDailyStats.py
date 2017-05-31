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
from netaddr import IPNetwork
import pandas as pd

DEBUG = True
files_path = '/Users/sofiasilva/BGP_files'
 
routing_file = '/Users/sofiasilva/BGP_files/2017-05-01.bgprib.readable' 
updates_file = '/Users/sofiasilva/BGP_files/2017-05-01_test.bgpupd.readable'
 
bgp_handler = BGPDataHandler(DEBUG, files_path)

#MRTfile = True
#COMPRESSED = False
#readable_updates = bgp_handler.getReadableFile(updates_file, False, MRTfile, COMPRESSED)
readable_updates = updates_file
updates_df = bgp_handler.processReadableUpdatesDF(readable_updates, None)

updateRates_DF = pd.DataFrame(columns=['routing_date', 'prefLength',
                                       'numOfAnnouncements', 'numOfWithdraws'])

for prefLength, prefLength_subset in updates_df.groupby('prefLength'):
    for routing_date, date_subset in prefLength_subset.groupby('routing_date'):
        updateRates_DF.loc[updateRates_DF.shape[0]] = [routing_date,
                                                        prefLength,
                                                        sum(date_subset['numOfAnnouncements']),
                                                        sum(date_subset['numOfWithdraws'])]

READABLE = True
MRTfile = False
COMPRESSED = False
bgp_handler.loadStructuresFromRoutingFile(routing_file, READABLE, MRTfile, COMPRESSED)

deaggregation_DF = pd.DataFrame(columns=['prefix', 'del_date', 'routing_date',
                                          'isRoot', 'isRootDeagg'])

for prefix, prefix_subset in bgp_handler.bgp_df.groupby('prefix'):
#    del_date = ??
    # TODO Obtener delegation date
    # Preguntar a Geoff cómo invocar origindate desde python
    
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
    
    deaggregation_DF.iloc[deaggregation_DF.shape[0]] = [prefix, del_date,
                                                        bgp_handler.routingDate,
                                                        isRoot, isRootDeagg]

# TODO DEBUG