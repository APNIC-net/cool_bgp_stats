# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 15:43:32 2017

@author: sofiasilva

This script inserts visibility, routing and updates data into the DB.
Then instantiates the BulkWHOISParser class in order to download the bulk WHOIS
files and load all the needed structures for the OrgHeuristics.
Finally, it computes the statistics about routing for yesterday.
"""
import os, sys
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from datetime import date, timedelta, datetime
import shlex, subprocess
from BulkWHOISParser import BulkWHOISParser
from BGPDataHandler import BGPDataHandler
from RoutingStats import RoutingStats
from computeRoutingStats import completeStatsComputation
from generateCSVsToInsertIntoDB import generateFilesFromRoutingFile
from generateCSVsToInsertUpdatesIntoDB import generateCSVFromUpdatesFile


DEBUG = False
files_path = '/home/sofia/daily_execution'
archive_folder = '/data/wattle/bgplog'

today = date.today()
date_to_work_with = today - timedelta(1)
log_file = '{}/dailyInsertionsFor{}.log'.format(files_path, date_to_work_with)
log = open(log_file, 'w')

# Insertion of visibility, routing and updates data into the DB
log.write('{}: Starting generating CSV files for insertion of visibility, routing and updates data into the DB.\n'.format(datetime.now()))

bgp_handler = BGPDataHandler(DEBUG, files_path)

file_name = '{}/{}/{}/{}/{}-{}-{}'.format(archive_folder,
                                                date_to_work_with.year,
                                                date_to_work_with.strftime('%m'),
                                                date_to_work_with.strftime('%d'),
                                                date_to_work_with.year,
                                                date_to_work_with.strftime('%m'),
                                                date_to_work_with.strftime('%d'))

bgprib_file = '{}.bgprib.mrt'.format(file_name)

dates_ready, visibility_csvs = generateFilesFromRoutingFile(files_path,
                                                           bgprib_file,
                                                           bgp_handler,
                                                           'visibility',
                                                           dict(), log_file,
                                                           archive_folder)

dates_ready, routing_bgprib_csv = generateFilesFromRoutingFile(files_path,
                                                               bgprib_file,
                                                               bgp_handler,
                                                               'routing',
                                                               dict(), log_file,
                                                               archive_folder)

dmp_file = '{}.dmp.gz'.format(file_name)

dates_ready, routing_dmp_csv = generateFilesFromRoutingFile(files_path,
                                                            dmp_file,
                                                            bgp_handler,
                                                            'routing',
                                                            dict(), log_file,
                                                            archive_folder)

v6dmp_file = '{}.v6.dmp.gz'.format(file_name)

dates_ready, routing_v6dmp_csv = generateFilesFromRoutingFile(files_path,
                                                              v6dmp_file,
                                                              bgp_handler,
                                                              'routing',
                                                              dict(),
                                                              log_file,
                                                              archive_folder)

updates_file = '{}.bgpupd.mrt'.format(file_name)

updates_csv = generateCSVFromUpdatesFile(updates_file, files_path, bgp_handler,
                                         log_file)
                                         
log.write('{}: Finished generating CSV files. Starting generating SQL file for insertion of visibility, routing and updates data into the DB.\n'.format(datetime.now()))

sql_file = '{}/dailyInsertion_{}.sql'.format(files_path, date_to_work_with)

with open(sql_file, 'w') as sql_f:
    sql_f.write("\connect sofia;\n")
    sql_f.write("copy prefixes (prefix, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[0]))
    sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[1]))
    sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[2]))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_bgprib_csv[0]))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_dmp_csv[0]))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_v6dmp_csv[0]))
    sql_f.write('''copy updates (update_date, update_time, upd_type,
                            bgp_neighbor, peeras, prefix, source_file) from
                            '{}' WITH (FORMAT csv);\n'''.format(updates_csv))

log.write('{}: SQL file generated. Inserting into the DB.\n'.format(datetime.now()))

cmd = shlex.split('psql -U postgres -f {}'.format(sql_file))
subprocess.call(cmd)

# Instantiation of the BulkWHOISParser class
log.write('{}: Executing BulkWHOISParser.\n'.format(datetime.now()))
BulkWHOISParser(files_path, DEBUG)

# Computation of routing stats
log.write('{}: Starting computation of routing stats.\n'.format(datetime.now()))
log.write('{}: Initializing variables and classes.\n'.format(datetime.now()))

KEEP = False
EXTENDED = True
del_file = '{}/extended_apnic_{}.txt'.format(files_path, today)
startDate_date = ''
INCREMENTAL = False
final_existing_date = ''
prefixes_stats_file = ''
ases_stats_file = ''
TEMPORAL_DATA = True
routingStatsObj = RoutingStats(files_path, DEBUG, KEEP, EXTENDED,
                                    del_file, startDate_date, date_to_work_with,
                                    date_to_work_with, INCREMENTAL,
                                    final_existing_date, prefixes_stats_file,
                                    ases_stats_file, TEMPORAL_DATA)

log.write('{}: Loading structures.\n'.format(datetime.now()))
                            
readable_routing_file = '{}/{}-{}-{}.bgprib.readable'.format(date_to_work_with.year,
                                                            date_to_work_with.strftime('%m'),
                                                            date_to_work_with.strftime('%d'))
                                                            
loaded = routingStatsObj.bgp_handler.loadStructuresFromRoutingFile(readable_routing_file)

if loaded:
    loaded = routingStatsObj.bgp_handler.loadUpdatesDF(routingStatsObj.bgp_handler.routingDate)

if not loaded:
    log.write('{}: Data structures not loaded! Aborting.\n'.format(datetime.now()))
    sys.exit()

dateStr = 'Delegated_BEFORE{}'.format(date_to_work_with)
dateStr = '{}_AsOf{}'.format(dateStr, date_to_work_with)

file_name = '%s/routing_stats_%s' % (files_path, dateStr)
es_host = 'twerp.rand.apnic.net'

log.write('{}: Starting actual computation of routing stats.\n'.format(datetime.now()))
completeStatsComputation(routingStatsObj, files_path, file_name, dateStr,
                             TEMPORAL_DATA, es_host, prefixes_stats_file,
                             ases_stats_file)


log.write('{}: Cleaning up.\n'.format(datetime.now()))

log.write('{}: Removing readable file {}.\n'.format(datetime.now(), readable_routing_file))
os.remove(readable_routing_file)

log.write('{}: Removing visibility CSVs {}.\n'.format(datetime.now(), visibility_csvs))
for csv in visibility_csvs:
    os.remove(csv)

log.write('{}: Removing BGPRIB routing CSV.\n'.format(datetime.now(), routing_bgprib_csv))
os.remove(routing_bgprib_csv)
log.write('{}: Removing DMP routing CSV.\n'.format(datetime.now(), routing_dmp_csv))
os.remove(routing_dmp_csv)
log.write('{}: Removing v6 DMP routing CSV.\n'.format(datetime.now(), routing_v6dmp_csv))
os.remove(routing_v6dmp_csv)
log.write('{}: Removing updates CSV.\n'.format(datetime.now(), updates_csv))
os.remove(updates_csv)
log.write('{}: Removing SQL file.\n'.format(datetime.now(), sql_file))
os.remove(sql_file)