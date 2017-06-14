# -*- coding: utf-8 -*-
"""
Created on Wed Jun 14 17:43:13 2017

@author: sofiasilva
"""
import os, sys, getopt
import gzip
from datetime import datetime

'''
Buscar bgpupd.mrt primero
si no hay, usar archivo .log.gz

Update:
en bgpupd file
BGP4MP|1493535309|A|202.12.28.1|4777|146.226.224.0/19|4777 2500 2500 2500 7660 22388 11537 20080 20243|IGP|202.12.28.1|0|0||NAG||


Withdraw:
en bgpupd file
BGP4MP|1493514069|W|202.12.28.1|4777|208.84.36.0/23

updates table:
update_date date not null,
upd_type char not null,
bgp_neighbor ip address not null,
peerAS long,
prefix ip network not null
'''

def writeUpdateToCSV(update_date, upd_type, bgp_neighbor, peerAS, prefix):
    # TODO Implement
    return None

def generateCSVFromUpdatesFile(updates_file, files_path):
    if updates_file.endswith('log.gz'):
        unzipped_file = '{}/{}'.format(files_path, updates_file.split('/')[-1][:-3])
            
        with gzip.open(updates_file, 'rb') as gzip_file,\
            open(unzipped_file, 'wb') as output:
            try:
                output.write(gzip_file.read())
            except IOError:
                sys.stderr.write('IOError unzipping file {}\n'.format(updates_file))
                return ??
        
        with open(unzipped_file, 'r') as log:
            announcement_in_progress = False
            update_date = None
            upd_type = ''
            bgp_neighbor = ''
            peerAS = -1
            prefixes = []
            
            for line in log:
                if 'debugging' in line:
                    line_parts = line.strip().split()
                    update_date = datetime.strptime(line_parts[0], '%Y/%m/%d')
                    bgp_neighbor = line_parts[4]

                    if 'withdrawn' in line or 'UPDATE' in line:
                        if announcement_in_progress:
                            for prefix in prefixes:
                                writeUpdateToCSV(update_date, upd_type,
                                                 bgp_neighbor, peerAS, prefix)
                            announcement_in_progress = False
                        
                        if 'withdrawn' in line:
                            upd_type = 'W'
    #                    2015/08/01 00:01:31 debugging: BGP: 202.12.28.1 rcvd UPDATE about 199.60.233.0/24 — withdrawn
                            prefix = line_parts[8]
                            peerAS = -1
                            writeUpdateToCSV(update_date, upd_type, bgp_neighbor,
                                             peerAS, prefix)
                        else: # 'UPDATE' in line                                
    #                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd UPDATE w/ attr: nexthop 64.71.180.177, origin i, path 6939 3491 12389 57617
    #                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 91.106.234.0/24
    #                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 37.1.77.0/24
    #                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 37.1.64.0/20
    #                        2015/08/01 00:01:26 debugging: BGP: 64.71.180.177 rcvd 91.106.232.0/21
                            announcement_in_progress = True
                            upd_type = 'A'
                            peerAS = long(line_parts[14])
                    elif announcement_in_progress:
                        prefixes.append(line_parts[6])
    return ??
    # TODO Pensar si voy a llevar una lista de las fechas para las que ya procesé archivo de updates
                        
                        
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
        readables_path = '/home/sofia/BGP_stats_files/hist_part{}'.format(proc_num)
    
    output_file = '{}/CSVgeneration_updates_{}_{}.output'.format(files_path, proc_num, datetime.today().date())
    
    if updates_file != '':
        generateCSVFromUpdatesFile(updates_file, files_path)
        
        
if __name__ == "__main__":
    main(sys.argv[1:])