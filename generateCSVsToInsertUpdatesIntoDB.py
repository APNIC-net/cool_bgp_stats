# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 17:43:13 2017

@author: sofiasilva
"""
import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler
import gzip
import shlex, subprocess
from datetime import datetime, date, timedelta
import pandas as pd

yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                    4:[2014, 2015], 5:[2016, 2017]}
                        
def generateCSVFromUpdatesFile(updates_file, files_path, readables_path, DEBUG,
                               output_file):
                                   
    sys.stdout.write('Starting to work with file {}\n'.format(updates_file))
    
    filename = updates_file.split('/')[-1]
    csv_file = '{}/{}.csv'.format(files_path, filename)
    
    if os.path.exists(csv_file):
        with open(output_file, 'a') as output:
            output.write('CSV file for updates file {} already exists.\n'.format(updates_file))
        return 'already_existed'
    
    if updates_file.endswith('log.gz'):
        unzipped_file = '{}/{}'.format(files_path, filename[:-3])
        
        if not os.path.exists(unzipped_file):
            with gzip.open(updates_file, 'rb') as gzip_file,\
                open(unzipped_file, 'wb') as output:
                try:
                    output.write(gzip_file.read())
                except IOError:
                    with open(output_file, 'a') as output:
                        output.write('IOError unzipping file {}\n'.format(updates_file))
                    return ''
                    
        filtered_file = '{}.filtered'.format(unzipped_file)
        
        if not os.path.exists(filtered_file):
            with open(filtered_file, 'w') as filtered:
                cmd = shlex.split('grep debugging {}'.format(unzipped_file))
                p = subprocess.Popen(cmd, stdout=subprocess.PIPE)
                
                cmd2 = shlex.split('grep rcvd')
                p2 = subprocess.Popen(cmd2, stdin=p.stdout, stdout=filtered)
                p2.communicate()
                p.kill()
        
        announcements_file = '{}.announcements'.format(unzipped_file)
        
        if not os.path.exists(announcements_file):
            with open(announcements_file, 'w') as announcements_f:
                cmd = shlex.split("grep -v withdrawn {}".format(filtered_file))
                p = subprocess.Popen(cmd, stdout=announcements_f)
                p.communicate()

        withdrawals_file = '{}.withdrawals'.format(unzipped_file)
        
        if not os.path.exists(withdrawals_file):
            with open(withdrawals_file, 'w') as withdrawals_f:
                cmd = shlex.split("grep withdrawn {}".format(filtered_file))
                p = subprocess.Popen(cmd, stdout=withdrawals_f)
                p.communicate()
                
#       2015/08/01 00:01:31 debugging: BGP: 202.12.28.1 rcvd UPDATE about 199.60.233.0/24 -- withdrawn
        # We first get a TextFileReader to read the file in chunks (in case it is too big)
        withdrawals_reader = pd.read_csv(withdrawals_file, iterator=True,
                                         chunksize=1000, header=None, sep=' ',
                                         index_col=False, usecols=[0,1,4,8],
                                         names=['update_date',
                                                'update_time',
                                                'bgp_neighbor',
                                                'prefix'])
        
        # We then put the chunks into a single DataFrame
        withdrawals_df = pd.concat(withdrawals_reader, ignore_index = True)
                                        
        withdrawals_df['upd_type'] = 'W'
        withdrawals_df['peerAS'] = -1
        withdrawals_df['source_file'] = updates_file
        
        withdrawals_df.to_csv(csv_file, header=False, index=False, quoting=2,
                          columns=['update_date', 'update_time', 'upd_type',
                                   'bgp_neighbor', 'peerAS', 'prefix', 'source_file'])
                
        with open(announcements_file, 'rb+') as announcements_f, open(csv_file, 'a') as csv_f:
            update_date = ''
            update_time = ''
            bgp_neighbor = ''
            peerAS = -1
            prefixes = []
            
            for line in announcements_f:
                if 'flapped' in line:
                    continue
                
                line_parts = line.strip().split()

                # If a new announcement starts
#               2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd UPDATE w/ attr: nexthop 64.71.180.177, origin i, path 6939 3491 12389 57617
#               2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 91.106.234.0/24
#               2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 37.1.77.0/24
#               2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 37.1.64.0/20
#               2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 91.106.232.0/21
                if 'UPDATE' in line:                   
                    # If we were processing another announcement, we write it
                    # to the csv file
                    if len(prefixes) > 0:
                        for prefix in prefixes:
                            csv_f.write('"{}","{}","{}","{}",{},"{}","{}"\n'\
                                        .format(update_date, update_time,
                                                'A', bgp_neighbor, peerAS,
                                                prefix, updates_file))
                    
                    update_date = line_parts[0]
                    update_time = line_parts[1]
                    bgp_neighbor = line_parts[4]
                    if 'path' in line:
                        peerAS = line.split('path')[1].split()[0]
                        
                        if '.' in peerAS:
                            left, right= peerAS.split('.')
                            peerAS = int(left) * 65536 + int(right)
                        else:
                            peerAS = int(peerAS)
                    else:
                        peerAS = -1
                    prefixes = []
                                             
                else:
                    prefixes.append(line_parts[6].replace('...duplicate', ''))

            # We have to write to the csv file the last announcement            
            if len(prefixes) > 0:
                for prefix in prefixes:
                    csv_f.write('"{}","{}","{}","{}",{},"{}","{}"\n'\
                                .format(update_date, update_time,
                                        'A', bgp_neighbor, peerAS,
                                        prefix, updates_file))
        os.remove(unzipped_file)
        os.remove(filtered_file)
        os.remove(announcements_file)
        os.remove(withdrawals_file)

    elif updates_file.endswith('bgpupd.mrt'):
        readable_file = BGPDataHandler.getReadableFile(updates_file, False,
                                                       readables_path, DEBUG)
        
        readable_woSTATE = '{}.woSTATE'.format(readable_file)
        if not os.path.exists(readable_woSTATE):
            with open(readable_woSTATE, 'w') as woSTATE:
                cmd = shlex.split('grep -v STATE {}'.format(readable_file))
                p = subprocess.Popen(cmd, stdout=woSTATE)
                p.communicate()
                
        readable_announcements = '{}.announcements'.format(readable_file)
        if not os.path.exists(readable_announcements):
            with open(readable_announcements, 'w') as announcements:
                cmd = shlex.split('grep \'|A|\' {}'.format(readable_woSTATE))
                p = subprocess.Popen(cmd, stdout=announcements)
                p.communicate()

        announcements_df = getDF(readable_announcements, 'A', updates_file)

        readable_withdrawals = '{}.withdrawals'.format(readable_file)
        if not os.path.exists(readable_withdrawals):
            with open(readable_withdrawals, 'w') as withdrawals:
                cmd = shlex.split('grep \'|W|\' {}'.format(readable_woSTATE))
                p = subprocess.Popen(cmd, stdout=withdrawals)
                p.communicate()
                
        withdrawals_df = getDF(readable_withdrawals, 'W', updates_file)
        
        updates_df = pd.concat([announcements_df, withdrawals_df])
        
        updates_df.to_csv(csv_file, header=False, index=False, quoting=2,
                          columns=['update_date', 'update_time', 'upd_type',
                                   'bgp_neighbor', 'peerAS', 'prefix', 'source_file'])
    
        os.remove(readable_file)
        os.remove(readable_woSTATE)
        os.remove(readable_announcements)
        os.remove(readable_withdrawals)
        
    return csv_file
    
def getDF(filtered_file, upd_type, updates_file):
    # We first get a TextFileReader to read the file in chunks (in case it is too big)
    file_reader = pd.read_csv(filtered_file, iterator=True, chunksize=1000,
                              header=None, sep='|', index_col=False,
                              usecols=[1,3,4,5], names=['timestamp',
                                                        'bgp_neighbor',
                                                        'peerAS',
                                                        'prefix'])

    # We then put the chunks into a single DataFrame                                    
    df_from_file = pd.concat(file_reader, ignore_index=True)
        
    if df_from_file.shape[0] > 0:
        df_from_file['upd_type'] = upd_type
        df_from_file['source_file'] = updates_file
        df_from_file['update_datetime'] = df_from_file.apply(lambda row:
                                            datetime.utcfromtimestamp(
                                            row['timestamp'])\
                                            .strftime('%Y/%m/%d %H:%M:%S'),
                                            axis=1)
                                            
        datetime_parts = df_from_file.update_datetime.str.rsplit(' ', n=1, expand=True)
        df_from_file['update_date'] = datetime_parts[0]
        df_from_file['update_time'] = datetime_parts[1]

        df_from_file.dropna(subset=['bgp_neighbor', 'peerAS', 'prefix'], how='all')
    
    return df_from_file
  
def getCompleteDatesSet(proc_num):
    if yearsForProcNums[proc_num][0] == 2007:
        initial_date = date(yearsForProcNums[proc_num][0], 6, 11)
    else:
        initial_date = date(yearsForProcNums[proc_num][0], 1, 1)

    final_date = date(yearsForProcNums[proc_num][-1], 12, 31)

    if final_date > date.today():
        final_date = date.today()

    numOfDays = (final_date - initial_date).days + 1

    return set([final_date - timedelta(days=x) for x in range(0, numOfDays)])
                    
                        
def main(argv):
    files_path = '/home/sofia/BGP_stats_files'
    archive_folder = '/data/wattle/bgplog'
    proc_num = -1
    updates_file = ''
    DEBUG = False
    
    
    try:
        opts, args = getopt.getopt(argv,"hp:A:n:f:D", ['files_path=', 'archive_folder=', 'procNumber=', 'updatesFile=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> (-A <archive folder> -n <process number> | -f <updates file>) [-D]'.format(sys.argv[0])
        print "p: Provide the path to a folder to use to save files."
        print "A: Provide the path to the folder containing hitorical routing data."
        print "AND"
        print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
        print "OR"
        print "f: Provide the path to an updates file (bgpupd.mrt or log.gz)."
        print "D: DEBUG mode"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files path> (-A <archive folder> -n <process number> | -f <updates file>) [-D]'.format(sys.argv[0])
            print "p: Provide the path to a folder to use to save files."
            print "A: Provide the path to the folder containing hitorical routing data."
            print "AND"
            print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
            print "OR"
            print "f: Provide the path to an updates file."
            print "D: DEBUG mode"
            sys.exit()
        elif opt == '-p':
            files_path = os.path.abspath(arg)
        elif opt == '-A':
            archive_folder = os.path.abspath(arg)
        elif opt == '-n':
            try:
                proc_num = int(arg)
            except ValueError:
                print "The process number MUST be a number between 1 and 5."
                sys.exit(-1)
        elif opt == '-f':
            updates_file = os.path.abspath(arg)
        elif opt == '-D':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
    
    if proc_num == -1:
        if updates_file == '':
            print "If you don't provide the path to an updates file you MUST provide a process number."
            sys.exit(-1)
        else:
            file_date = BGPDataHandler.getDateFromFileName(updates_file)
            
            for i in range(1, 6):
                if file_date.year in yearsForProcNums[i]:
                    proc_num = i
                    break
                
            if proc_num == -1:
                print "Updates file corresponds to a date that is out of the considered range of years."
                sys.exit(-1)
            
    readables_path = '/home/sofia/BGP_stats_files/readable_updates{}'.format(proc_num)

    output_file = '{}/CSVgeneration_updates_{}_{}.output'.format(files_path, proc_num, datetime.today().date())
        
    if updates_file != '':
        generateCSVFromUpdatesFile(updates_file, files_path, readables_path,
                                   DEBUG, output_file)
    else:
        datesSet = getCompleteDatesSet(proc_num)

	# We have to skip all the dates between Nov 19th 2016 and Jan 23rd 2017
	# because all of them are contained in the bgpup fil for Nov 19th 2016
	# This file is too big to be loaded in memory, therefore it was divided
	# into smaller pieces and processed separately
        initial_date = date(2016, 11, 19)
        final_date = date(2017, 1, 23)
        numOfDays = (final_date - initial_date).days + 1
        skip_set =  set([final_date - timedelta(days=x) for x in range(0, numOfDays)])

        for date_to_process in datesSet:
            bgpupd_date = date_to_process + timedelta(days=1)
            
            if bgpupd_date in skip_set:
                continue
 
            updates_file = '{}/{}/{}/{}/{}-{}-{}.bgpupd.mrt'.format(archive_folder,
                                                                    bgpupd_date.year,
                                                                    bgpupd_date.strftime('%m'),
                                                                    bgpupd_date.strftime('%d'),
                                                                    bgpupd_date.year,
                                                                    bgpupd_date.strftime('%m'),
                                                                    bgpupd_date.strftime('%d'))
            
            if not os.path.exists(updates_file):
                log_file = '{}/{}/{}/{}/{}-{}-{}.log.gz'.format(archive_folder,
                                                                    date_to_process.year,
                                                                    date_to_process.strftime('%m'),
                                                                    date_to_process.strftime('%d'),
                                                                    date_to_process.year,
                                                                    date_to_process.strftime('%m'),
                                                                    date_to_process.strftime('%d'))
                with open(output_file, 'a') as output:
                    output.write('{} not present. Looking for log file {}\n'.format(updates_file, log_file))
            
                updates_file = log_file
                
                if not os.path.exists(updates_file):
                    with open(output_file, 'a') as output:
                        output.write('Updates file not available for date {}\n'.format(date_to_process))
                        continue
            
            csv_file = generateCSVFromUpdatesFile(updates_file, files_path,
                                                  readables_path, DEBUG,
                                                  output_file)

            with open(output_file, 'a') as output:        
                if csv_file == '':
                    output.write('CSV file for date {} could not be generated.\n'.format(date_to_process))
                elif csv_file != 'already_existed':
                    output.write('CSV file {} generated for date {}.\n'.format(csv_file, date_to_process))
        
if __name__ == "__main__":
    main(sys.argv[1:])