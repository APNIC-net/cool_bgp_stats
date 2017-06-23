# -*- coding: utf-8 -*-
"""
Created on Fri Jun 23 15:43:32 2017

@author: sofiasilva

Goal: To insert visibility, routing and updates data into the DB daily.
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from datetime import date, timedelta
from glob import glob
import shlex, subprocess
from BGPDataHandler import BGPDataHandler
from generateCSVsToInsertIntoDB import generateFilesFromRoutingFile
from generateCSVsToInsertUpdatesIntoDB import generateCSVFromUpdatesFile


DEBUG = False
files_path = '/home/sofia/BGP_stats_files/daily_insertions'
archive_folder = '/data/wattle/bgplog'
bgp_handler = BGPDataHandler(DEBUG, files_path)
date_to_insert = date.today() - timedelta(1)
log_file = '{}/dailyInsertionsFor{}.log'.format(files_path, date_to_insert)

file_name = '{}/{}/{}/{}/{}-{}-{}'.format(archive_folder,
                                                date_to_insert.year,
                                                date_to_insert.month,
                                                date_to_insert.day,
                                                date_to_insert.year,
                                                date_to_insert.month,
                                                date_to_insert.day)

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

for readable_file in glob('{}/*.readable'.format(files_path)):
    os.remove(readable_file)

sql_file = '{}/dailyInsertion_{}.sql'.format(files_path, date_to_insert)

with open(sql_file, 'w') as sql_f:
    sql_f.write("\connect sofia;\n")
    sql_f.write("copy prefixes (prefix, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[0]))
    sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[1]))
    sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[2]))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_bgprib_csv))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_dmp_csv))
    sql_f.write("copy routing_data (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_v6dmp_csv))
    sql_f.write('''copy updates (update_date, update_time, upd_type,
                            bgp_neighbor, peeras, prefix, source_file) from
                            '{}' WITH (FORMAT csv);\n'''.format(updates_csv))

cmd = shlex.split('psql -U postgres -f {}'.format(sql_file))
subprocess.call(cmd)