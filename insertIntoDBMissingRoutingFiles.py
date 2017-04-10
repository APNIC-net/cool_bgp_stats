# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 11:50:52 2017

@author: sofiasilva
"""

import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler


# Dates of readable files already inserted into the Visibility DB
readable_files_dates = []
for readable_file in os.listdir('/home/sofia/BGP_stats_files/historical_files'):
    if readable_file.endswith('readable'):
        readable_files_dates.append(readable_file.split('.')[0])

for readable_file in os.listdir('/home/sofia/BGP_stats_files/historical_files2'):
    if readable_file.endswith('readable'):
        readable_files_dates.append(readable_file.split('.')[0])
        
archive_folder = '/data/wattle/bgplog'
extension = 'bgprib.mrt'

# bgprib.mrt files in the archive folder that
# haven't been inserted into the Visibility DB yet
bgprib_files_list = []
for root, subdirs, files in os.walk(archive_folder):
    for filename in files:
        date = filename.split('.')[0]
        if filename.endswith(extension) and date not in readable_files_dates:
            bgprib_files_list.append(os.path.join(root, filename))
            readable_files_dates.append(date)

DEBUG = False
files_path = '/home/sofia/BGP_stats_files/historical_files2'
KEEP = True

bgp_handler = BGPDataHandler(DEBUG, files_path, KEEP)

RIBfiles = True
READABLE = False
COMPRESSED = False

bgp_handler.storeHistoricalData(bgprib_files_list, True, READABLE, RIBfiles, COMPRESSED)
                                                                   
extension = 'dmp.gz'

# dmp.gz files in the archive folder that
# haven't been inserted into the Visibility DB yet
dmp_files_list = []
for root, subdirs, files in os.walk(archive_folder):
    for filename in files:
        if filename.endswith(extension) and filename.split('.')[0] not in readable_files_dates:
            dmp_files_list.append(os.path.join(root, filename))

RIBfiles = False
READABLE = False
COMPRESSED = True

bgp_handler.storeHistoricalData(dmp_files_list, True, READABLE, RIBfiles, COMPRESSED)