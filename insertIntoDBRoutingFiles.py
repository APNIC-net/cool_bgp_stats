# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 11:50:52 2017

@author: sofiasilva
"""

import os, sys
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler

readables_path = '/home/sofia/BGP_files/hist_part{}'.format(sys.argv[1])

DEBUG = False
files_path = readables_path
KEEP = True

bgp_handler = BGPDataHandler(DEBUG, files_path, KEEP)

# Available readable files
readable_files = []
dates_inserted = []
for readable_file in os.listdir(readables_path):
    if readable_file.endswith('readable'):
        readable_files.append('{}/{}'.format(readables_path, readable_file))
        dates_inserted.append(readable_file.split('.')[0])

RIBfiles = False
READABLE = True
COMPRESSED = False

bgp_handler.storeHistoricalData(readable_files, True, READABLE, RIBfiles, COMPRESSED)

archive_folder = '/data/wattle/bgplog'
extension = 'bgprib.mrt'

# bgprib.mrt files in the archive folder for which
# we don't have readable files
bgprib_files_list = []
for root, subdirs, files in os.walk(archive_folder):
    for filename in files:
        date = filename.split('.')[0]
        year = int(date.split('-')[0])
        # For paralelization we check for the year of the file, so that
        # different files are processed by different scripts
        if filename.endswith(extension) and date not in dates_inserted and\
            (year == 2006 + int(sys.argv[1]) or year == 2018 - int(sys.argv[1]) or\
            (int(sys.argv[1]) == 1 and year == 2012)):
            bgprib_files_list.append(os.path.join(root, filename))
            dates_inserted.append(date)

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
        year = int(date.split('-')[0])
        if filename.endswith(extension) and\
            filename.split('.')[0] not in dates_inserted and\
            (year == 2006 + int(sys.argv[1]) or year == 2018 - int(sys.argv[1]) or\
            (int(sys.argv[1]) == 1 and year == 2012)):
            dmp_files_list.append(os.path.join(root, filename))

RIBfiles = False
READABLE = False
COMPRESSED = True

bgp_handler.storeHistoricalData(dmp_files_list, True, READABLE, RIBfiles, COMPRESSED)