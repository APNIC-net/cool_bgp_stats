# -*- coding: utf-8 -*-
"""
Created on Wed Jun 28 13:22:04 2017

@author: sofiasilva

This script computes statistics for all the past dates for which we have routing and updates data.
"""
import sys, os, getopt
from datetime import date, datetime, timedelta
from dailyExecution import computeStatsForDate
from BGPDataHandler import BGPDataHandler
from DBHandler import DBHandler

yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                        4:[2014, 2015], 5:[2016, 2017]}

def concatenateFiles(routing_file, v4_routing_file, v6_routing_file):
    with open(v4_routing_file, 'a') as v4_file:
        with open(v6_routing_file, 'r') as v6_file:
            for line in v6_file:
                v4_file.write(line)
                
    os.rename(v4_routing_file, routing_file)
    return routing_file
    
def getReadableForRoutingFile(routing_file, readables_folder):
    
    readable_filename = '.'.join(routing_file.split('/')[-1].split('.')[0:-1])
    
    readable_file = '{}/{}.readable'.format(readables_folder, readable_filename)
    
    if os.path.exists(readable_file):
        return readable_file
    else:
        return ''

def computationForDate(routing_date, DEBUG, files_path, readables_folder,
                       ROUTING, STABILITY, DEAGG_PROB, ELASTIC):    

    routing_file = ''
    
    if ROUTING or DEAGG_PROB:
        sys.stdout.write('{}: Looking for routing file to be used.\n'.format(datetime.now()))
                                            
        db_handler = DBHandler('')
        available_routing_files = db_handler.getPathsToRoutingFilesForDate(routing_date)
        db_handler.close()
        
        if 'bgprib.mrt' in available_routing_files:
            # If there is a bgprib file available, I check whether I already have
            # the readable file for it in the readables folder
            routing_file = available_routing_files['bgprib.mrt']
            readable_routing_file = getReadableForRoutingFile(routing_file,
                                                              readables_folder)
            
            # If I do, I use it
            if readable_routing_file != '':
                routing_file = readable_routing_file
            
            # If I don't, I will use the bgprib.mrt file
        
        elif 'dmp.gz' in available_routing_files and 'v6.dmp.gz' in available_routing_files:
            # If there is not bgprib.mrt file available, but the two dmp files
            # (for v4 and v6) are available, I use them
            dmp_file = available_routing_files['dmp.gz']
            # If I already have a readable file, I use it
            readable_dmp = getReadableForRoutingFile(dmp_file, readables_folder)
            if readable_dmp == '':
                readable_dmp = BGPDataHandler.getReadableFile(dmp_file, False,
                                                              files_path, DEBUG)
    
            v6dmp_file = available_routing_files['v6.dmp.gz']
            # If I already have a readable file, I use it
            readable_v6dmp = getReadableForRoutingFile(v6dmp_file, readables_folder)
            if readable_v6dmp == '':
                readable_v6dmp = BGPDataHandler.getReadableFile(v6dmp_file, False,
                                                                files_path, DEBUG)
             
            routing_file = concatenateFiles('{}/{}_v4andv6.dmp.readable'\
                                                .format(files_path, routing_date),
                                                readable_dmp, readable_v6dmp)
        elif 'dmp.gz' in available_routing_files:
            # If there is only one of the dmp files available, I will work with it
            # but I'll print a message to the log
            sys.stdout.write('Only the dmp.gz file is available for date {}. Computing stats only for IPv4.\n'.format(routing_date))
            
            dmp_file = available_routing_files['dmp.gz']
            # If I already have a readable file, I use it
            readable_dmp = getReadableForRoutingFile(dmp_file, readables_folder)
    
            if readable_dmp != '':
                routing_file = readable_dmp
            else:
                routing_file = dmp_file
                
        elif 'v6.dmp.gz' in available_routing_files:
            # If there is only one of the dmp files available, I will work with it
            # but I'll print a message to the log
            sys.stdout.write('Only the v6.dmp.gz file is available for date {}. Computing stats only for IPv6.\n'.format(routing_date))
            
            v6dmp_file = available_routing_files['v6.dmp.gz']
            # If I already have a readable file, I use it
            readable_v6dmp = getReadableForRoutingFile(v6dmp_file, readables_folder)
            
            if readable_v6dmp != '':
                routing_file = readable_v6dmp
            else:
                routing_file = v6dmp_file
        
        else:
            # This should never happen. At least one routing file for a date must be available
            sys.stdout.write('No routing file is available for date {}\n'.format(routing_date))
            return False
            
        sys.stdout.write('{}: Working with routing file {}\n'.format(datetime.now(), routing_file))
    
    computeStatsForDate(routing_date, files_path, routing_file, ROUTING,
                        STABILITY, DEAGG_PROB, False, ELASTIC)

    # Lo llamamos con BulkWHOIS = False porque no es necesario que cada vez parsee el Bulk WHOIS
    # Las estructuras disponibles van a estar actualizadas porque BulkWHOISParser se va a instanciar a diario
    
    return True

def getCompleteDatesSet(proc_num):
    if proc_num == 1:
        initial_date = date(2007, 6, 11)
    else:
        initial_date = date(yearsForProcNums[proc_num][0], 1, 1)
        
    final_date = date(yearsForProcNums[proc_num][-1], 12, 31)

    yesterday = date.today() - timedelta(1)
    if final_date > yesterday:
        final_date = yesterday
        
    numOfDays = (final_date - initial_date).days
    return set([final_date - timedelta(days=x) for x in range(0, numOfDays)])

def getCompleteDatesSetForYear(year):
    if year == 2007:
        initial_date = date(2007, 6, 11)
    else:
        initial_date = date(year, 1, 1)
        
    final_date = date(year, 12, 31)

    yesterday = date.today() - timedelta(1)
    if final_date > yesterday:
        final_date = yesterday
        
    numOfDays = (final_date - initial_date).days
    return set([final_date - timedelta(days=x) for x in range(0, numOfDays)])
    
def main(argv):
    DEBUG = False
    files_path = '/home/sofia/past_dates_computation'
    ROUTING = False
    STABILITY = False
    DEAGG_PROB = False
    year = 2017
    proc_num = -1
    ELASTIC = False
    stats_date = None

    try:
        opts, args = getopt.getopt(argv,"hp:n:y:d:RSDE", ['files_path=', 'procNumber=', 'year=', 'date=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files_path> (-n <process number> | -y <year> | -d <date>) [-R] [-S] [-D] [-E]'.format(sys.argv[0])
        print "p: Files path. Path to a folder in which generated files will be saved."
        print "n: Process number from 1 to 5, which allows the script to compute stats for a subset of the past dates."
        print "y: Year. Year for which you want the stats to be computed."
        print "d: date: Date for which you want the stats to be computed. Format: YYYYMMDD"
        print "R: Routing stats. Use this option if you want the routing stats to be computed."
        print "S: Stability stats. Use this option if you want the stats about update rates to be computed."
        print "D: Deaggregation probability stats. Use this option if you want the stats about probability of deaggregation  to be computed."
        print "E: ElasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files_path> (-n <process number> | -y <year> | -d <date>) [-R] [-S] [-D] [-E]'.format(sys.argv[0])
            print "p: Files path. Path to a folder in which generated files will be saved."
            print "n: Provide a process number from 1 to 5, which allows the script to compute stats for a subset of the past dates."
            print "y: Year. Year for which you want the stats to be computed."
            print "d: date: Date for which you want the stats to be computed. Format: YYYYMMDD"
            print "R: Routing stats. Use this option if you want the routing stats to be computed."
            print "S: Stability stats. Use this option if you want the stats about update rates to be computed."
            print "D: Deaggregation probability stats. Use this option if you want the stats about probability of deaggregation  to be computed."
            print "E: ElasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
            sys.exit()
        elif opt == '-p':
            if arg != '':
                files_path = os.path.abspath(arg)
        elif opt == '-n':
            try:
                proc_num = int(arg)
                
                if proc_num < 1 or proc_num > 5:
                    print "The process number MUST be between 1 and 5."
                    sys.exit(-1)
                    
            except ValueError:
                print "The process number MUST be a number."
                sys.exit(-1)
        elif opt == '-y':
            if arg != '':
                year = int(arg)
            else:
                print "If the -y option is used you MUST provide the year for which you want stats to be computed."
                sys.exit(-1)
        elif opt == '-d':
            if arg != '':
                try:
                    stats_date = datetime.strptime(arg, '%Y%m%d').date()
                except ValueError:
                    print "You MUST use format YYYYMMDD for the date"
                    sys.exit(-1)
            else:
                print "If the -d option is used you MUST provide the date for which you want the stats to be computed."
                sys.exit(-1)
        elif opt == '-R':
            ROUTING = True
        elif opt == '-S':
            STABILITY = True
        elif opt == '-D':
            DEAGG_PROB = True
        elif opt == '-E':
            ELASTIC = True
        else:
            assert False, 'Unhandled option'
    
    if stats_date is not None:
        year = stats_date.year
        
    if proc_num == -1:
        for i in yearsForProcNums:
            if year in yearsForProcNums[i]:
                proc_num = i
        
        if proc_num == -1:
            print "The year provided MUST be between 2007 and the 2017."
            sys.exiT(-1)
        if stats_date is None:   
            dates_set = getCompleteDatesSetForYear(year)
        else:
            dates_set = set([stats_date])
    else:
        dates_set = getCompleteDatesSet(proc_num)

            
    if not ROUTING and not STABILITY and not DEAGG_PROB:
        print "None of the options -R, -S or -D were provided. Exiting without computing any stats."
        sys.exit(0)
    
    readables_folder = '/home/sofia/BGP_stats_files/hist_part{}'.format(proc_num)

    
    for past_date in dates_set:
        computationForDate(past_date, DEBUG, files_path, readables_folder,
                           ROUTING, STABILITY, DEAGG_PROB, ELASTIC)

        
if __name__ == "__main__":
    main(sys.argv[1:])