# -*- coding: utf-8 -*-
"""
Created on Fri Jul  7 15:08:50 2017

@author: sofiasilva
"""
import sys, os, getopt
import shlex, subprocess
from datetime import datetime, date, timedelta
from BGPDataHandler import BGPDataHandler
from generateCSVsToInsertIntoDB import generateFilesFromRoutingFile
from generateCSVsToInsertUpdatesIntoDB import generateCSVFromUpdatesFile


def concatenateFiles(routing_file, v4_routing_file, v6_routing_file):
    with open(v4_routing_file, 'a') as v4_file:
        with open(v6_routing_file, 'r') as v6_file:
            for line in v6_file:
                v4_file.write(line)
                
    os.rename(v4_routing_file, routing_file)
    return routing_file

def insertionForDate(date_to_work_with):
    DEBUG = False
    files_path = '/home/sofia/daily_execution'
    archive_folder = '/data/wattle/bgplog'
    log_file = '{}/dailyInsertion_{}.log'.format(files_path, date_to_work_with)
    
    # Insertion of visibility, routing and updates data into the DB
    sys.stdout.write('{}: Starting generating CSV files for insertion of visibility, routing and updates data into the DB.\n'.format(datetime.now()))
    
    bgp_handler = BGPDataHandler(DEBUG, files_path)
        
    file_name = '{}/{}/{}/{}/{}-{}-{}'.format(archive_folder,
                                                    date_to_work_with.year,
                                                    date_to_work_with.strftime('%m'),
                                                    date_to_work_with.strftime('%d'),
                                                    date_to_work_with.year,
                                                    date_to_work_with.strftime('%m'),
                                                    date_to_work_with.strftime('%d'))
    
    # We first insert the files available in the current folder of the archive
    # into the archive_index table
    bgprib_file = '{}.bgprib.mrt'.format(file_name)
    if os.path.exists(bgprib_file):       
        dates_ready, routing_bgprib_csv = generateFilesFromRoutingFile(files_path,
                                                                       bgprib_file,
                                                                       bgp_handler,
                                                                       'routing',
                                                                       dict(), log_file,
                                                                       archive_folder,
                                                                       DEBUG)    
    
    dmp_file = '{}.dmp.gz'.format(file_name)
    if os.path.exists(dmp_file):
        dates_ready, routing_dmp_csv = generateFilesFromRoutingFile(files_path,
                                                                    dmp_file,
                                                                    bgp_handler,
                                                                    'routing',
                                                                    dict(), log_file,
                                                                    archive_folder,
                                                                    DEBUG)
    
    v6dmp_file = '{}.v6.dmp.gz'.format(file_name)
    if os.path.exists(v6dmp_file):
        dates_ready, routing_v6dmp_csv = generateFilesFromRoutingFile(files_path,
                                                                      v6dmp_file,
                                                                      bgp_handler,
                                                                      'routing',
                                                                      dict(),
                                                                      log_file,
                                                                      archive_folder,
                                                                      DEBUG)

    # For visibility data, we insert data about the day before because the mrt file is generated the day after
    day_before = date_to_work_with - timedelta(1)
    
    routing_file = BGPDataHandler.getRoutingFileForDate(day_before)

    dates_ready, visibility_csvs = generateFilesFromRoutingFile(files_path,
                                                               routing_file,
                                                               bgp_handler,
                                                               'visibility',
                                                               dict(), log_file,
                                                               archive_folder,
                                                               DEBUG)
                                                               
    updates_file = '{}.bgpupd.mrt'.format(file_name)
    
    if not os.path.exists(updates_file):
        updates_file = '{}/{}/{}/{}/{}-{}-{}.log.gz'.format(archive_folder,
                                                            day_before.year,
                                                            day_before.strftime('%m'),
                                                            day_before.strftime('%d'),
                                                            day_before.year,
                                                            day_before.strftime('%m'),
                                                            day_before.strftime('%d'))

    updates_csv = generateCSVFromUpdatesFile(updates_file, files_path, files_path,
                                             DEBUG, log_file)
                                             
    sys.stdout.write('{}: Finished generating CSV files. Starting generating SQL file for insertion of visibility, routing and updates data into the DB.\n'.format(datetime.now()))
    
    sql_file = '{}/dailyInsertion_{}.sql'.format(files_path, date_to_work_with)
    
    with open(sql_file, 'w') as sql_f:
        sql_f.write("\connect sofia;\n")
        sql_f.write("copy prefixes (prefix, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[0]))
        sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[1]))
        sql_f.write("copy asns (asn, isorigin, dateseen) from '{}' WITH (FORMAT csv);\n".format(visibility_csvs[2]))
        sql_f.write("copy archive_index (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_bgprib_csv[0]))
        sql_f.write("copy archive_index (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_dmp_csv[0]))
        sql_f.write("copy archive_index (routing_date, extension, file_path) from '{}' WITH (FORMAT csv);\n".format(routing_v6dmp_csv[0]))
        # If day_before is the first day of the year
        # (we consider day_beofre because the bgpupd.mrt file contains data for
        # the day before the date in its name)
        if day_before.month == 1 and day_before.day == 1:
            # We need to create the partition for the new year and its indexes
            sql_f.write('''CREATE TABLE updates_y{} (
                            CHECK ( update_date >= DATE '{}-01-01' AND 
                            update_date <= DATE '{}-12-31' )
                            ) INHERITS (updates);\n'''.format(day_before.year,
                                                            day_before.year))
                                                            
            sql_f.write('''CREATE INDEX updates_y{}_update_date 
                            ON updates_y{} (update_date);\n'''\
                            .format(day_before.year))
                            
            # and we need to update the updates_insert_trigger function
            sql_f.write('''create or replace function updates_insert_trigger()\n
                            returns trigger as $$\n
                            begin\n''')
                            
            sql_f.write('''IF ( NEW.update_date >= DATE '{}-01-01' 
                            AND NEW.update_date <= '{}-12-31' ) 
                            THEN INSERT INTO updates_y{} VALUES (NEW.*);\n'''\
                            .format(day_before.year, day_before.year,
                                    day_before.year))
                                    
            for year in range(day_before.year-1, 2006, -1):
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
    
        # If day_before is the first or the last day of the year,
        # we insert into the master table (updates table)
        # and let the updates_insert_trigger function redirect the insertions
        # to the right partition, as the CSV file may contain rows for a different year.
        if day_before.month == 1 and day_before.day == 1 or\
            day_before.month == 12 and day_before.day == 31:
    
            sql_f.write('''copy updates (update_date, update_time, upd_type, 
                                    bgp_neighbor, peeras, prefix, source_file) from 
                                    '{}' WITH (FORMAT csv);\n'''.format(updates_csv))
    
        # else we insert into the right partition for better efficiency
        else:
            sql_f.write('''copy updates_y{} (update_date, update_time, upd_type, 
                            bgp_neighbor, peeras, prefix, source_file) from 
                            '{}' WITH (FORMAT csv);\n'''\
                            .format(day_before.year, updates_csv))
    
    sys.stdout.write('{}: SQL file generated. Inserting into the DB.\n'.format(datetime.now()))
    
    cmd = shlex.split('psql -U postgres -f {}'.format(sql_file))
    subprocess.call(cmd)
    
    sys.stdout.write('{}: Cleaning up.\n'.format(datetime.now()))
    
    sys.stdout.write('{}: Removing visibility CSVs {}.\n'.format(datetime.now(), visibility_csvs))
    for csv in visibility_csvs:
        os.remove(csv)
        
    sys.stdout.write('{}: Removing BGPRIB routing CSV {}.\n'.format(datetime.now(), routing_bgprib_csv))
    os.remove(routing_bgprib_csv)
    sys.stdout.write('{}: Removing DMP routing CSV {}.\n'.format(datetime.now(), routing_dmp_csv))
    os.remove(routing_dmp_csv)
    sys.stdout.write('{}: Removing v6 DMP routing CSV {}.\n'.format(datetime.now(), routing_v6dmp_csv))
    os.remove(routing_v6dmp_csv)
    sys.stdout.write('{}: Removing updates CSV {}.\n'.format(datetime.now(), updates_csv))
    os.remove(updates_csv)
    sys.stdout.write('{}: Removing SQL file {}.\n'.format(datetime.now(), sql_file))
    os.remove(sql_file)
    
def main(argv):
    date_to_work_with = ''

    try:
        opts, args = getopt.getopt(argv,"hD:", ['date_to_work_with=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -D <Date to work with>'.format(sys.argv[0])
        print "D: Date of the files whose data you want to be inserted into the DB. Format YYYYMMDD."
        print "The data from the files in /data/wattle/bgplog/YYYY/MM/DD will be inserted into the DB."
        print "If this option is not used, insertions of the data from the files in the folder corresponding to today will be inserted into the DB."
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -D <Date to work with>'.format(sys.argv[0])
            print "D: Date of the files whose data you want to be inserted into the DB. Format YYYYMMDD."
            print "The data from the files in /data/wattle/bgplog/YYYY/MM/DD will be inserted into the DB."
            print "If this option is not used, insertions of the data from the files in the folder corresponding to today will be inserted into the DB."
            sys.exit()
        elif opt == '-D':
            if arg != '':
                try:
                    date_to_work_with = date(int(arg[0:4]),
                                             int(arg[4:6]),
                                             int(arg[6:8]))
                except ValueError:
                    print "If you use the option -D you MUST provide a date in format YYYYMMDD."
                    sys.exit(-1)
            else:
                print "If you use the option -D you MUST provide a date in format YYYYMMDD."
                sys.exit(-1)
        else:
            assert False, 'Unhandled option'
    
    if date_to_work_with == '':
        date_to_work_with = date.today()
    
    insertionForDate(date_to_work_with)
    
if __name__ == "__main__":
    main(sys.argv[1:])