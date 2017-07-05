# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 15:43:32 2017

@author: sofiasilva

This script inserts visibility, routing and updates data into the DB.
Then computes daily statistics about the update rate and the probability of deaggregation .
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
from StabilityAndDeaggDailyStats import StabilityAndDeagg
from computeRoutingStats import computeAndSavePerPrefixStats, computeAndSavePerASStats
from generateCSVsToInsertIntoDB import generateFilesFromRoutingFile
from generateCSVsToInsertUpdatesIntoDB import generateCSVFromUpdatesFile


DEBUG = False
files_path = '/home/sofia/daily_execution'
archive_folder = '/data/wattle/bgplog'

today = date.today()
date_to_work_with = today - timedelta(1)
log_file = '{}/dailyExecution_{}.log'.format(files_path, date_to_work_with)

def concatenateFiles(routing_file, v4_routing_file, v6_routing_file):
    with open(v4_routing_file, 'a') as v4_file:
        with open(v6_routing_file, 'r') as v6_file:
            for line in v6_file:
                v4_file.write(line)
                
    os.rename(v4_routing_file, routing_file)
    return routing_file
    

# Insertion of visibility, routing and updates data into the DB
sys.stdout.write('{}: Starting generating CSV files for insertion of visibility, routing and updates data into the DB.\n'.format(datetime.now()))

bgp_handler = BGPDataHandler(DEBUG, files_path)

routing_files = dict()

file_name = '{}/{}/{}/{}/{}-{}-{}'.format(archive_folder,
                                                date_to_work_with.year,
                                                date_to_work_with.strftime('%m'),
                                                date_to_work_with.strftime('%d'),
                                                date_to_work_with.year,
                                                date_to_work_with.strftime('%m'),
                                                date_to_work_with.strftime('%d'))

bgprib_file = '{}.bgprib.mrt'.format(file_name)

if os.path.exists(bgprib_file):
    routing_files['bgprib'] = bgprib_file
    
    dates_ready, visibility_csvs = generateFilesFromRoutingFile(files_path,
                                                               bgprib_file,
                                                               bgp_handler,
                                                               'visibility',
                                                               dict(), log_file,
                                                               archive_folder,
                                                               DEBUG)
    
    dates_ready, routing_bgprib_csv = generateFilesFromRoutingFile(files_path,
                                                                   bgprib_file,
                                                                   bgp_handler,
                                                                   'routing',
                                                                   dict(), log_file,
                                                                   archive_folder,
                                                                   DEBUG)    

dmp_file = '{}.dmp.gz'.format(file_name)

if os.path.exists(dmp_file):
    routing_files['dmp'] = dmp_file
    
    dates_ready, routing_dmp_csv = generateFilesFromRoutingFile(files_path,
                                                                dmp_file,
                                                                bgp_handler,
                                                                'routing',
                                                                dict(), log_file,
                                                                archive_folder,
                                                                DEBUG)

v6dmp_file = '{}.v6.dmp.gz'.format(file_name)

if os.path.exists(v6dmp_file):
    routing_files['v6.dmp'] = v6dmp_file
    dates_ready, routing_v6dmp_csv = generateFilesFromRoutingFile(files_path,
                                                                  v6dmp_file,
                                                                  bgp_handler,
                                                                  'routing',
                                                                  dict(),
                                                                  log_file,
                                                                  archive_folder,
                                                                  DEBUG)

if 'bgprib' not in routing_files:
    readable_v4 = ''
    if 'dmp' in routing_files:
        readable_v4 = BGPDataHandler.getReadableFile(routing_files['dmp'],
                                                     False, files_path, DEBUG)
    
    readable_v6 = ''
    if 'v6.dmp' in routing_files:
        readable_v6 = BGPDataHandler.getReadableFile(routing_files['v6.dmp'],
                                                     False, files_path, DEBUG)
    readable_complete = ''
                     
    if readable_v4 != '' and readable_v6 != '':
        readable_complete = concatenateFiles('{}/routing_file_{}.readable'\
                                                .format(date_to_work_with),
                                             readable_v4, readable_v6)
    elif readable_v4 != '':
        readable_complete = readable_v4
    elif readable_v6 != '':
        readable_complete = readable_v6

    if readable_complete != '':
       dates_ready, visibility_csvs = generateFilesFromRoutingFile(files_path,
                                                                   readable_complete,
                                                                   bgp_handler,
                                                                   'visibility',
                                                                   dict(), log_file,
                                                                   archive_folder,
                                                                   DEBUG)
                                                                   
updates_file = '{}.bgpupd.mrt'.format(file_name)

if os.path.exists(updates_file):
    updates_csv = generateCSVFromUpdatesFile(updates_file, files_path, files_path,
                                             DEBUG, log_file)
                                         
sys.stdout.write('{}: Finished generating CSV files. Starting generating SQL file for insertion of visibility, routing and updates data into the DB.\n'.format(datetime.now()))

sql_file = '{}/dailyInsertion_{}.sql'.format(files_path, date_to_work_with)

with open(sql_file, 'w') as sql_f:
    sql_f.write("\connect sofia;\n")
    sql_f.write("copy prefixes (prefix, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[0]))
    sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[1]))
    sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[2]))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_bgprib_csv[0]))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_dmp_csv[0]))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_v6dmp_csv[0]))
    # If it is the first day of the year
    if date_to_work_with.month == 1 and date_to_work_with.day == 1:
        # We need to create the partition for the new year and its indexes
        sql_f.write('''CREATE TABLE updates_y{} (
                        CHECK ( update_date >= DATE '{}-01-01' AND 
                        update_date <= DATE '{}-12-31' )
                        ) INHERITS (updates);\n'''.format(date_to_work_with.year,
                                                        date_to_work_with.year))
                                                        
        sql_f.write('''CREATE INDEX updates_y{}_update_date 
                        ON updates_y{} (update_date);\n'''\
                        .format(date_to_work_with.year))

        sql_f.write('''CREATE INDEX updates_y{}_update_date_upd_type 
                        ON updates_y{} (update_date, upd_type);\n'''\
                        .format(date_to_work_with.year))
                        
        # and we need to update the updates_insert_trigger function
        sql_f.write('''create or replace function updates_insert_trigger()\n
                        returns trigger as $$\n
                        begin\n''')
                        
        sql_f.write('''IF ( NEW.update_date >= DATE '{}-01-01' 
                        AND NEW.update_date <= '{}-12-31' ) 
                        THEN INSERT INTO updates_y{} VALUES (NEW.*);\n'''\
                        .format(date_to_work_with.year, date_to_work_with.year,
                                date_to_work_with.year))
                                
        for year in range(date_to_work_with.year-1, 2006, -1):
            sql_f.write('''ELSIF ( NEW.update_date >= DATE '{}-01-01' 
                            AND NEW.update_date <= '{}-12-31' ) 
                            THEN INSERT INTO updates_y{} VALUES (NEW.*);\n'''\
                            .format(year, year, year))

        sql_f.write('''ELSE\n
                        RAISE EXCEPTION 'Date out of range. 
                        Fix the updates_insert_trigger() function!';\n
                        END IF;\n
                        RETURN NULL;\n
                        END;\n
                        $$\n
                        LANGUAGE plpgsql;\n''')

    # If the CSV file corresponds to the first day of the year or to
    # the last day of the year, we insert into the master table (updates table)
    # and let the updates_insert_trigger function redirect the insertions
    # to the right partition, as the CSV file may contain rows for a different year.
    if date_to_work_with.month == 1 and date_to_work_with.day == 1 or\
        date_to_work_with.month == 12 and date_to_work_with.day == 31:

        sql_f.write('''copy updates (update_date, update_time, upd_type, 
                                bgp_neighbor, peeras, prefix, source_file) from 
                                '{}' WITH (FORMAT csv);\n'''.format(updates_csv))

    # else we insert into the right partition for better efficiency
    else:
        sql_f.write('''copy updates_y{} (update_date, update_time, upd_type, 
                        bgp_neighbor, peeras, prefix, source_file) from 
                        '{}' WITH (FORMAT csv);\n'''\
                        .format(date_to_work_with.year, updates_csv))

sys.stdout.write('{}: SQL file generated. Inserting into the DB.\n'.format(datetime.now()))

cmd = shlex.split('psql -U postgres -f {}'.format(sql_file))
subprocess.call(cmd)

readable_routing_file = '{}/{}-{}-{}.bgprib.readable'.format(files_path,
                                                            date_to_work_with.year,
                                                            date_to_work_with.strftime('%m'),
                                                            date_to_work_with.strftime('%d'))
 
sys.stdout.write('{}: Initializing variables and classes.\n'.format(datetime.now()))

es_host = 'twerp.rand.apnic.net'
KEEP = False
EXTENDED = True
del_file = '{}/extended_apnic_{}.txt'.format(files_path, today)
startDate_date = ''
INCREMENTAL = False
final_existing_date = ''
dateStr = 'Delegated_BEFORE{}'.format(date_to_work_with)
dateStr = '{}_AsOf{}'.format(dateStr, date_to_work_with)
file_name = '{}/RoutingStats_{}'.format(files_path, dateStr)
prefixes_stats_file = '{}_prefixes.csv'.format(file_name)
ases_stats_file = '{}_asns.csv'.format(file_name)
TEMPORAL_DATA = True
routingStatsObj = RoutingStats(files_path, DEBUG, KEEP, EXTENDED,
                                    del_file, startDate_date, date_to_work_with,
                                    date_to_work_with, INCREMENTAL,
                                    final_existing_date, prefixes_stats_file,
                                    ases_stats_file, TEMPORAL_DATA)

sys.stdout.write('{}: Loading structures.\n'.format(datetime.now()))
                                                                     
loaded = routingStatsObj.bgp_handler.loadStructuresFromRoutingFile(readable_routing_file)

if loaded:
    loaded = routingStatsObj.bgp_handler.loadUpdatesDF(routingStatsObj.bgp_handler.routingDate)

if not loaded:
    sys.stdout.write('{}: Data structures not loaded! Aborting.\n'.format(datetime.now()))
    sys.exit()

# Computation of stats about updates rate and probability of deaggregation
sys.stdout.write('{}: Starting to compute stats about the updates rates and the probability of deaggregation.\n'.format(datetime.now()))

StabilityAndDeagg_inst = StabilityAndDeagg(DEBUG, files_path, es_host,
                                           routingStatsObj.bgp_handler)

fork1_pid = os.fork()

if fork1_pid == 0:
    # If we are in the child process of the first fork, we fork again
    fork2_pid = os.fork()
    if fork2_pid == 0:
        # If we are in the child process of the second fork, we compute some stats
        StabilityAndDeagg_inst.computeAndSaveStabilityDailyStats()
        sys.exit(0)
    else:
        # If we are in the parent process of the second fork, we compute some other stats
        StabilityAndDeagg_inst.computeAndSaveDeaggDailyStats()
        os.waitpid(fork2_pid)
        sys.exit(0)
else:
    # If we are in the parent process of the first fork, we call BulkWHOISParser
    # Instantiation of the BulkWHOISParser class
    sys.stdout.write('{}: Executing BulkWHOISParser.\n'.format(datetime.now()))
    BulkWHOISParser(files_path, DEBUG)

    # Computation of routing stats
    sys.stdout.write('{}: Starting computation of routing stats.\n'.format(datetime.now()))
    
    # and then we fork again
    fork3_pid = os.fork()
    
    if fork3_pid == 0:
        # If we are in the child process of the third fork,
        # we compute stats for prefixes
        computeAndSavePerPrefixStats(files_path, file_name, dateStr,
                                     routingStatsObj, prefixes_stats_file,
                                     TEMPORAL_DATA, es_host)
        sys.exit(0)

    else:
        # If we are in the parent process of the third fork,
        # we compute stats for ASes
        computeAndSavePerASStats(files_path, file_name, dateStr, routingStatsObj,
                                 ases_stats_file, TEMPORAL_DATA, es_host)
        os.waitpid(fork3_pid)
    
    os.waitpid(fork1_pid)


sys.stdout.write('{}: Cleaning up.\n'.format(datetime.now()))

sys.stdout.write('{}: Removing readable file {}.\n'.format(datetime.now(), readable_routing_file))
os.remove(readable_routing_file)

sys.stdout.write('{}: Removing visibility CSVs {}.\n'.format(datetime.now(), visibility_csvs))
for csv in visibility_csvs:
    os.remove(csv)

sys.stdout.write('{}: Removing BGPRIB routing CSV.\n'.format(datetime.now(), routing_bgprib_csv))
os.remove(routing_bgprib_csv)
sys.stdout.write('{}: Removing DMP routing CSV.\n'.format(datetime.now(), routing_dmp_csv))
os.remove(routing_dmp_csv)
sys.stdout.write('{}: Removing v6 DMP routing CSV.\n'.format(datetime.now(), routing_v6dmp_csv))
os.remove(routing_v6dmp_csv)
sys.stdout.write('{}: Removing updates CSV.\n'.format(datetime.now(), updates_csv))
os.remove(updates_csv)
sys.stdout.write('{}: Removing SQL file.\n'.format(datetime.now(), sql_file))
os.remove(sql_file)