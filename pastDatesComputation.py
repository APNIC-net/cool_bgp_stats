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
from DelegatedHandler import DelegatedHandler

yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                        4:[2014, 2015], 5:[2016, 2017]}

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
    ROUTING = False
    STABILITY = False
    DEAGG_PROB = False
    year = 2017
    proc_num = -1
    ELASTIC = False
    stats_date = None
    numOfProcs = 1

    try:
        opts, args = getopt.getopt(argv,"hn:y:d:N:RSDE", ['procNumber=', 'year=', 'date=', 'numOfProcs=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | (-n <process number> | -y <year> | -d <date>) [-N <number of processes>] [-R] [-S] [-D] [-E]'.format(sys.argv[0])
        print "n: Process number from 1 to 5, which allows the script to compute stats for a subset of the past dates."
        print "y: Year. Year for which you want the stats to be computed."
        print "d: date: Date for which you want the stats to be computed. Format: YYYYMMDD"
        print "N: Number of parallel processes within computation of routing stats."
        print "The main process will be forked so that one process takes care of the computation of stats about BGP updates, another one takes care of the computation of stats about deaggregation and another one takes care of the computation of routing stats."
        print "Besides, the process in charge of computing routing stats will be forked so that one subprocess takes care of the computation of stats for prefixes and another one takes care of the computation of stats for ASes."
        print "Finally, each of these subprocesses will be divided into a pool of n threads in order to compute the stats in parallel for n subsets of prefixes/ASes."
        print "Therefore, if the three flags R, S and D are used there will be (2 + 2*n) parallel processes in total."
        print "R: Routing stats. Use this option if you want the routing stats to be computed."
        print "S: Stability stats. Use this option if you want the stats about update rates to be computed."
        print "D: Deaggregation probability stats. Use this option if you want the stats about probability of deaggregation  to be computed."
        print "E: ElasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | (-n <process number> | -y <year> | -d <date>) [-N <number of processes>] [-R] [-S] [-D] [-E]'.format(sys.argv[0])
            print "n: Provide a process number from 1 to 5, which allows the script to compute stats for a subset of the past dates."
            print "y: Year. Year for which you want the stats to be computed."
            print "d: date: Date for which you want the stats to be computed. Format: YYYYMMDD"
            print "N: Number of parallel processes within computation of routing stats."
            print "The main process will be forked so that one process takes care of the computation of stats about BGP updates, another one takes care of the computation of stats about deaggregation and another one takes care of the computation of routing stats."
            print "Besides, the process in charge of computing routing stats will be forked so that one subprocess takes care of the computation of stats for prefixes and another one takes care of the computation of stats for ASes."
            print "Finally, each of these subprocesses will be divided into a pool of n threads in order to compute the stats in parallel for n subsets of prefixes/ASes."
            print "Therefore, if the three flags R, S and D are used there will be (2 + 2*n) parallel processes in total."
            print "R: Routing stats. Use this option if you want the routing stats to be computed."
            print "S: Stability stats. Use this option if you want the stats about update rates to be computed."
            print "D: Deaggregation probability stats. Use this option if you want the stats about probability of deaggregation  to be computed."
            print "E: ElasticSearch. Save computed stats to ElasicSearch engine in twerp.rand.apnic.net"
            sys.exit()
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
        elif opt == '-N':
            try:
                numOfProcs = int(arg)
            except ValueError:
                print "The number of processes MUST be a number."
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
    files_path = '/home/sofia/BGP_stats_files'
    
    DEBUG = False
    EXTENDED = True
    del_file = '{}/delegated_extended_{}.txt'.format(files_path, date.today())
    INCREMENTAL = False
    final_existing_date = ''
    KEEP = False
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL,
                                   final_existing_date, KEEP)
                                   
    original_delegated_df = del_handler.delegated_df.copy(deep=True)
    
    for past_date in dates_set:
        sys.stdout.write("{}: Starting to compute stats for {}\n".format(datetime.now(), past_date))
        routing_file = ''
        
        if ROUTING or DEAGG_PROB:
            sys.stdout.write('{}: Looking for routing file to be used.\n'.format(datetime.now()))
                                                
            routing_file = BGPDataHandler.getRoutingFileForDate(past_date,
                                                                files_path,
                                                                False)
            
            if routing_file == '':
                sys.stdout.write('No routing file is available for date {}\n'.format(past_date))
                continue
                
            routing_filename = routing_file.split('/')[-1]
            
            readable_file = '{}/{}'.format(readables_folder, '.'.join(routing_filename.split('.')[0:-1]))
            
            if os.path.exists(readable_file):
                routing_file = readable_file
            
            sys.stdout.write('{}: Working with routing file {}\n'.format(datetime.now(), routing_file))
    
        del_handler.delegated_df = original_delegated_df[original_delegated_df['date'] <= past_date]
        
        computeStatsForDate(past_date, numOfProcs, files_path, routing_file,
                            del_handler, ROUTING, STABILITY, DEAGG_PROB, False,
                            ELASTIC)
        
        # Lo llamamos con BulkWHOIS = False porque no es necesario que cada vez parsee el Bulk WHOIS
        # Las estructuras disponibles van a estar actualizadas porque BulkWHOISParser se va a instanciar a diario
                
if __name__ == "__main__":
    main(sys.argv[1:])