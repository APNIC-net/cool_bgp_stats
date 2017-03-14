#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import numpy as np
import pandas as pd
import datetime, time
import requests
import json
import hashlib
import getpass
# Just for DEBUG
#import os
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from DelegatedHandler import DelegatedHandler
   
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
    
def hashFromColValue(col_value):
    return hashlib.md5(col_value).hexdigest()

# This function saves a data frame with stats into ElasticSearch
def saveDFToElasticSearch(plain_df, user, password):
    es_host = 'localhost'
    index_name = 'delegated_stats'
    index_type = 'id'
    
    # We create an id that is unique for a certain combination of Geographic Area,
    # Resource Type, Status and Organization
    plain_df['multiindex_comb'] = plain_df['GeographicArea'] +\
                                    plain_df['ResourceType'] +\
                                    plain_df['Status'] +\
                                    plain_df['Organization']
                                    
    plain_df['index'] = plain_df['multiindex_comb'].apply(hashFromColValue)
    plain_df['_id'] = plain_df['Date'].astype('str') + '_' + plain_df['index'].astype('str')
    del plain_df['index']
    del plain_df['multiindex_comb']
    # We convert the DataFrame to JSON format    
    df_as_json = plain_df.to_json(orient='records', lines=True)

    final_json_string = ''
    # For each line of the generated json, we add the corresponding header line
    # with meta data
    for json_document in df_as_json.split('\n'):
        jdict = json.loads(json_document)
        # Header line
        metadata = json.dumps({'index': {'_index': index_name,\
                                        '_type': index_type,\
                                        '_id': jdict['_id']}})
        jdict.pop('_id')
        final_json_string += metadata + '\n' + json.dumps(jdict) + '\n'
    
    # We finally post the generated JSON data to ElasticSearch
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    r = requests.post('http://%s:9200/%s/%s/_bulk' % (es_host, index_name, index_type), data=final_json_string, headers=headers, timeout=60) 
    
    if (r.status_code == 401):
        if user == '' and password == '':
            print("Authentication needed. Please enter your username and password")
            user = raw_input("Username: ")
            password = getpass.getpass("Password: ")

        r = requests.post('http://%s:9200/%s/%s/_bulk' %\
                            (es_host, index_name, index_type),\
                            data=final_json_string, headers=headers,\
                            timeout=60, auth=(user, password)) 
    
    return r

def main(argv):    
    DEBUG = False
    EXTENDED = False
    year = ''
    month = ''
    day = ''
    del_file = ''
    files_path = ''
    INCREMENTAL = False
    stats_file = ''
    final_existing_date = ''
    user = ''
    password = ''
    
    try:
        opts, args = getopt.getopt(argv, "hy:m:D:d:ep:i:u:P:", ["year=", "month=", "Day=", "del_file=", "files_path=", "stats_file=", "user=", "password="])
    except getopt.GetoptError:
        print 'Usage: delegatd_stats_v3.py -h | -p <files path [-y <year> [-m <month> [-D <day>]]] [-d <delegated file>] [-e] [-i <stats file>] [-u <ElasticSearch user> -P <ElasticSearch password>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statistics from one of the delegated files provided by the RIRs"
            print 'Usage: delegatd_stats_v3.py -h | -p <files path [-y <year> [-m <month> [-D <day>]]] [-d <delegated file>] [-e] [-i <stats file>] [-u <ElasticSearch user> -P <ElasticSearch password>]'
            print 'h = Help'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print 'y = Year to compute statistics for. If a year is not provided, statistics will be computed for all the available years.'
            print 'm = Month of Year to compute statistics for. This option can only be used if a year is also provided.'
            print 'D = Day of Month to compute statistics for. This option can only be used if a year and a month are also provided.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            print "u = User to save stats to ElasticSearch."
            print "P = Password to save to stats to ElasticSearch."
            sys.exit()
        elif opt == '-y':
            year = int(arg)
        elif opt == '-m':
            month = int(arg)
        elif opt == '-D':
            day = int(arg)
        elif opt == '-d':
            DEBUG = True
            del_file = arg
        elif opt == '-e':
            EXTENDED = True
        elif opt == '-p':
            files_path = arg
        elif opt == '-i':
            INCREMENTAL = True
            stats_file = arg
        elif opt == '-u':
            user = arg
        elif opt == '-P':
            password = arg
        else:
            assert False, 'Unhandled option'
            
    if year == '' and (month != '' or (month == '' and day != '')):
        print 'If you provide a month, you must also provide a year.'
        print 'If you provide a day, you must also provide a month and a year.'
        sys.exit()

    if DEBUG and del_file == '':
        print "If you choose to run in DEBUG mode you must provide the path to\
                    a delegated file that has already been downloaded."
        sys.exit()
        
    if files_path == '':
        print "You must provide a folder to save files."
        sys.exit()                             
            
    today = datetime.date.today().strftime('%Y%m%d')
    
    if year == '':
        yearStr = 'AllDates'
    else:
        yearStr = str(year)
    
    if not DEBUG:
        file_name = '%s/delegated_stats_%s%s%s_%s' % (files_path, yearStr, month, day, today)

        if EXTENDED:
            del_file = '%s/extended_apnic_%s.txt' % (files_path, today)
        else:
            del_file = '%s/delegated_apnic_%s.txt' % (files_path, today)
            
    else:
        file_name = '%s/delegated_stats_test_%s%s%s_%s' % (files_path, yearStr, month, day, today)    

    if INCREMENTAL:
        if stats_file == '':
            print "If option -i is used, a statistics file MUST be provided."
            sys.exit()
        else:
            existing_stats_df = pd.read_csv(stats_file, sep = ',')
            final_existing_date = max(existing_stats_df['Date'])
            del existing_stats_df
    else:
        stats_file = '{}.csv'.format(file_name)
        
        with open(stats_file, 'w') as csv_file:
            csv_file.write('Geographic Area,ResourceType,Status,Organization,Date,NumOfDelegations,NumOfResources,IPCount,IPSpace\n')
        
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year, month, day)
        
    if not del_handler.delegated_df.empty:
        start_time = time.time()
        computeStatistics(del_handler, stats_file)       

        end_time = time.time()
        sys.stderr.write("Stats computed successfully!\n")
        sys.stderr.write("Statistics computation took {} seconds\n".format(end_time-start_time))   
       
    stats_df = pd.read_csv(stats_file, sep = ',')
    json_filename = '{}.json'.format(file_name)
    stats_df.to_json(json_filename, orient='index')
    sys.stderr.write("Stats saved to files successfully!\n")
    sys.stderr.write("({} and {})\n".format(stats_file, json_filename))

    if user != '' and password != '':
        r = saveDFToElasticSearch(stats_df, user, password)
        status_code = r.status_code
        if status_code == 200:
            sys.stderr.write("Stats saved to ElasticSearch successfully!\n")
        else:
            print "Something went wrong when trying to save stats to ElasticSearch.\n"

        
if __name__ == "__main__":
    main(sys.argv[1:])