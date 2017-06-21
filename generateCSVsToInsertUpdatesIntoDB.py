# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 17:43:13 2017

@author: sofiasilva
"""
import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler
import gzip
import re, shlex, subprocess
from datetime import datetime, date, timedelta

yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                    4:[2014, 2015], 5:[2016, 2017]}
                        
def generateCSVFromUpdatesFile(updates_file, files_path, bgp_handler, output_file):
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
    
        with open(filtered_file, 'rb+') as log, open(csv_file, 'a') as csv_f:
            announcement_in_progress = False
            update_date = ''
            update_time = ''
            upd_type = ''
            bgp_neighbor = ''
            peerAS = -1
            prefixes = []
            
            for line in log:
                line_parts = line.strip().split()
                update_date = line_parts[0]
                update_time = line_parts[1]
                bgp_neighbor = line_parts[4]

                if 'withdrawn' in line or 'UPDATE' in line:
                    if announcement_in_progress:
                        for prefix in prefixes:
                            csv_f.write('"{}","{}","{}",{},"{}","{}"\n'\
                                        .format(update_date, update_time,
                                                upd_type, bgp_neighbor, peerAS,
                                                prefix, updates_file))
                                             
                        announcement_in_progress = False
                    
                    if 'withdrawn' in line:
                        upd_type = 'W'
#                    2015/08/01 00:01:31 debugging: BGP: 202.12.28.1 rcvd UPDATE about 199.60.233.0/24 — withdrawn
                        prefix = line_parts[8]
                        peerAS = -1
                        csv_f.write('"{}","{}","{}",{},"{}","{}"\n'\
                                    .format(update_date, update_time, upd_type,
                                            bgp_neighbor, peerAS, prefix,
                                            updates_file))
                                            
                    else: # 'UPDATE' in line                                
#                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd UPDATE w/ attr: nexthop 64.71.180.177, origin i, path 6939 3491 12389 57617
#                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 91.106.234.0/24
#                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 37.1.77.0/24
#                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 37.1.64.0/20
#                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 91.106.232.0/21
                        announcement_in_progress = True
                        upd_type = 'A'
                        peerAS = line.split('path')[1].split()[0]
                elif announcement_in_progress:
                    prefixes.append(line_parts[6])

    elif updates_file.endswith('bgpupd.mrt'):
        readable_file = bgp_handler.getReadableFile(updates_file, False)
        
        with open(readable_file, 'r') as mrt, open(csv_file, 'a') as csv_f:
            for line in mrt:
                if 'STATE' not in line:
                    # BGP4MP|1493535309|A|202.12.28.1|4777|146.226.224.0/19|4777 2500 2500 2500 7660 22388 11537 20080 20243|IGP|202.12.28.1|0|0||NAG||
                    # BGP4MP|1493514069|W|202.12.28.1|4777|208.84.36.0/23

                    line_parts = line.strip().split('|')
                    update_datetime = datetime.utcfromtimestamp(float(line_parts[1]))
                    update_date = update_datetime.date().strftime('%Y/%m/%d')
                    update_time = update_datetime.time().strftime('%H:%M:%S')
                    upd_type = line_parts[2]
                    bgp_neighbor = line_parts[3]
                    peerAS = long(line_parts[4])
                    prefix = line_parts[5]

                    csv_f.write('"{}","{}","{}",{},"{}","{}"\n'\
                                .format(update_date, update_time, upd_type,
                                        bgp_neighbor, peerAS, prefix,
                                        updates_file))

        
    return csv_file
  
def getCompleteDatesSet(proc_num):
    initial_date = date(yearsForProcNums[proc_num][0], 1, 1)
    final_date = date(yearsForProcNums[proc_num][-1], 12, 31)
    numOfDays = (final_date - initial_date).days
    return set([final_date - timedelta(days=x) for x in range(0, numOfDays)])

        
def getDateFromFileName(filename):        
    dates = re.findall('(?P<year>[1-2][9,0][0,1,8,9][0-9])[-_]*(?P<month>[0-1][0-9])[-_]*(?P<day>[0-3][0-9])',\
                filename)
                
    if len(dates) > 0:
        file_date = '{}{}{}'.format(dates[0][0], dates[0][1], dates[0][2])
        return datetime.strptime(file_date, '%Y%m%d').date()
    else:
        return None
                    
                        
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
            file_date = getDateFromFileName(updates_file)
            
            for i in range(1, 6):
                if file_date.year in yearsForProcNums[i]:
                    proc_num = i
                    break
                
            if proc_num == -1:
                print "Updates file corresponds to a date that is out of the considered range of years."
                sys.exit(-1)
            
    readables_path = '/home/sofia/BGP_stats_files/readable_updates{}'.format(proc_num)
    
    output_file = '{}/CSVgeneration_updates_{}_{}.output'.format(files_path, proc_num, datetime.today().date())
    
    bgp_handler = BGPDataHandler(DEBUG, readables_path)
    
    if updates_file != '':
        generateCSVFromUpdatesFile(updates_file, files_path, bgp_handler,
                                   output_file)
    else:
        datesSet = getCompleteDatesSet(proc_num)
        
        for date_to_process in datesSet:
            bgpupd_date = date_to_process + timedelta(days=1)
            
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
            
            csv_file = generateCSVFromUpdatesFile(updates_file, files_path, bgp_handler,
                                                  output_file)

            with open(output_file, 'a') as output:        
                if csv_file == '':
                    output.write('CSV file for date {} could not be generated.\n'.format(date_to_process))
                elif csv_file != 'already_existed':
                    output.write('CSV file {} generated for date {}.\n'.format(csv_file, date_to_process))
        
if __name__ == "__main__":
    main(sys.argv[1:])