#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from DelegatedHandler import DelegatedHandler
import sys, getopt
import numpy as np
import pandas as pd
import datetime, time
from ElasticSearchImporter import ElasticSearchImporter
   
# This function computes all the statistics for all the dates included in a given
# subset from a DataFrame, which contains info about delegations (delegated_subset),
# for a given combination of Geographic Area (a), Resource Type (r), Status(s)
# and Organization (o)
# The computed statistics are written to the provided file (stats_filename)
def computation_loop(delegated_subset, a, r, s, o, stats_filename):
                   
    # If we are working with a specific resource type, we group the info
    # about delegations just by date
    date_groups =\
            delegated_subset.groupby(delegated_subset['date']\
                                    .map(lambda x: x.strftime('%Y%m%d')))
        
    res_counts = date_groups['ResourceCount'].agg(np.sum)
    space_counts = date_groups['SpaceCount'].agg(np.sum)

    for date, delsInDate in date_groups:
        numOfDelegations = len(delsInDate['OriginalIndex'].unique())
        if r == 'ipv4' or r == 'ipv6':
            numOfResources = len(delsInDate)
            IPCount = res_counts[date]
            IPSpace = space_counts[date]
        else: # r == 'asn'
            numOfResources = res_counts[date]
            # IPCount and IPSpace do not make sense for r = 'asn'
            IPCount = ''
            IPSpace = ''
        
        with open(stats_filename, 'a') as stats_file:
            #Geographic Area,ResourceType,Status,Organization,Date,NumOfDelegations,NumOfResources,IPCount,IPSpace
            stats_file.write('{},{},{},{},{},{},{},{},{}\n'.format(a, r, s, o, date, numOfDelegations, numOfResources, IPCount, IPSpace))

    
    
# This function computes statistis for all the different combinations of
# Organization, Geographic Area, Resource Type and Status
def computeStatistics(del_handler, stats_filename):
    for org, org_df in del_handler.delegated_df.groupby(del_handler.delegated_df['opaque_id']):
            
        for country, area_df in org_df.groupby(org_df['cc']):
            for r, res_df in area_df.groupby(area_df['resource_type']):            
                for s, status_res_df in res_df.groupby(res_df['status']): 
                    computation_loop(status_res_df, country, r, s, org, stats_filename)
        
        for region, area_df in org_df.groupby(org_df['region']):
            for r, res_df in area_df.groupby(area_df['resource_type']):            
                for s, status_res_df in res_df.groupby(res_df['status']): 
                    computation_loop(status_res_df, region, r, s, org, stats_filename)
    

def main(argv):    
    DEBUG = False
    EXTENDED = False
    date = ''
    UNTIL = False
    del_file = ''
    files_path = ''
    INCREMENTAL = False
    stats_file = ''
    host = ''
    KEEP = False
    
    try:
        opts, args = getopt.getopt(argv, "hp:D:Ud:eki:H:", ["files_path=", "Date=", "del_file=", "stats_file=", "ES_host="])
    except getopt.GetoptError:
        print 'Usage: delegatd_stats_v5.py -h | -p <files path [-D <Date>] [-U] [-d <delegated file>] [-e] [-k] [-i <stats file>] [-H <ElasticSearch host>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statistics from a delegated file provided by APNIC"
            print 'Usage: delegatd_stats_v5.py -h | -p <files path [-D <Date>] [-U] [-d <delegated file>] [-e] [-k] [-i <stats file>] [-H <ElasticSearch host>]'
            print 'h = Help'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print 'D = Date in format YYYY or YYYYmm or YYYYmmdd. Date for which or until which to compute stats.'
            print 'U = Until. If the -U option is used the Date provided will be used to filter all the delegations until this date.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "k = Keep. Keep downloaded files."
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            print "H = Host. The host in which Elasticsearch is running and into which the computed stats will be inserted."
            sys.exit()
        elif opt == '-D':
            date = arg
        elif opt == '-U':
            UNTIL = True
        elif opt == '-d':
            DEBUG = True
            del_file = arg
        elif opt == '-e':
            EXTENDED = True
        elif opt == '-k':
            KEEP = True
        elif opt == '-p':
            files_path = arg
        elif opt == '-i':
            INCREMENTAL = True
            stats_file = arg
        elif opt == '-H':
            host = arg
        else:
            assert False, 'Unhandled option'
            
    if date != '' and not (len(date) == 4 or len(date) == 6 or len(date) == 8):
        print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
        sys.exit()
    
    if UNTIL and len(date) != 8:
        print 'If you use the -U option, you MUST provide a full date in format YYYYmmdd'
        sys.exit()

    if DEBUG and del_file == '':
        print "If you choose to run in DEBUG mode you must provide the path to\
                    a delegated file that has already been downloaded."
        sys.exit()
        
    if files_path == '':
        print "You must provide a folder to save files."
        sys.exit()                             
            
    today = datetime.date.today().strftime('%Y%m%d')
    
    if date == '':
        dateStr = 'AllDates'
    elif UNTIL:
        dateStr = 'UNTIL{}'.format(date)
    else:
        dateStr = date
    
    if not DEBUG:
        file_name = '%s/delegated_stats_%s' % (files_path, dateStr)

        if EXTENDED:
            del_file = '%s/extended_apnic_%s.txt' % (files_path, today)
        else:
            del_file = '%s/delegated_apnic_%s.txt' % (files_path, today)
            
    else:
        file_name = '%s/delegated_stats_test_%s' % (files_path, dateStr)

    if INCREMENTAL:
        if stats_file == '':
            print "If option -i is used, a statistics file MUST be provided."
            sys.exit()
        else:
            try:
                existing_stats_df = pd.read_csv(stats_file, sep = ',')
                final_existing_date = str(max(existing_stats_df['Date']))
                del existing_stats_df
            except (ValueError, KeyError):
                final_existing_date = ''
                INCREMENTAL = False

    if not INCREMENTAL:
        stats_file = '{}.csv'.format(file_name)
        final_existing_date = ''
        with open(stats_file, 'w') as csv_file:
            csv_file.write('Geographic Area,ResourceType,Status,Organization,Date,NumOfDelegations,NumOfResources,IPCount,IPSpace\n')
        
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, date, UNTIL,\
                                    INCREMENTAL, final_existing_date, KEEP)
        
    if not del_handler.delegated_df.empty:
        start_time = time.time()
        computeStatistics(del_handler, stats_file)       

        end_time = time.time()
        sys.stderr.write("Stats computed successfully!\n")
        sys.stderr.write("Statistics computation took {} seconds\n".format(end_time-start_time))   

        stats_df = pd.read_csv(stats_file, sep = ',')
        json_filename = '{}.json'.format(file_name)
        stats_df.to_json(json_filename, orient='index')
        sys.stderr.write("Stats saved to JSON file successfully!\n")
        sys.stderr.write("Files generated:\n{}\nand\n{})\n".format(stats_file, json_filename))
        
        if host != '':
            del_stats_index_name = 'delegated_stats_index'
            del_stats_doc_type = 'delegated_stats'
            
            esImporter = ElasticSearchImporter()
            es = esImporter.connect(host)
            numOfDocs = es.count(del_stats_index_name)['count']
            
            if INCREMENTAL:
                plain_df = stats_df[stats_df['Date'] > final_existing_date]
            else:
                plain_df = stats_df
            
            plain_df['GeographicArea'] = plain_df['Geographic Area']
            del plain_df['Geographic Area']
            plain_df = plain_df.fillna(-1)
    
            bulk_data, numOfDocs = esImporter.prepareData(es, plain_df,
                                                          del_stats_index_name,
                                                          del_stats_doc_type,
                                                          numOfDocs)
                                                
            dataImported = esImporter.inputData(es, del_stats_index_name,
                                                bulk_data, numOfDocs)
    
            if dataImported:
                sys.stderr.write("Stats about delegations for the dates {} saved to ElasticSearch successfully!\n".format(dateStr))
            else:
                sys.stderr.write("Stats about delegations for the dates {} could not be saved to ElasticSearch.\n".format(dateStr))
            
if __name__ == "__main__":
    main(sys.argv[1:])