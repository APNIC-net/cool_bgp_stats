#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from DelegatedHandler import DelegatedHandler
import sys, getopt
import numpy as np
import pandas as pd
import datetime, time
import requests
import json
import hashlib
import getpass
   
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
    date = ''
    UNTIL = False
    del_file = ''
    files_path = ''
    INCREMENTAL = False
    stats_file = ''
    user = ''
    password = ''
    
    try:
        opts, args = getopt.getopt(argv, "hp:D:Ud:ei:u:P:", ["files_path=", "Date=", "del_file=", "stats_file=", "user=", "Password="])
    except getopt.GetoptError:
        print 'Usage: delegatd_stats_v5.py -h | -p <files path [-D <Date>] [-U] [-d <delegated file>] [-e] [-i <stats file>] [-u <ElasticSearch user> -P <ElasticSearch password>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statistics from one of the delegated files provided by the RIRs"
            print 'Usage: delegatd_stats_v5.py -h | -p <files path [-D <Date>] [-U] [-d <delegated file>] [-e] [-i <stats file>] [-u <ElasticSearch user> -P <ElasticSearch password>]'
            print 'h = Help'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print 'D = Date in format YYYY or YYYYmm or YYYYmmdd. Date for which or until which to compute stats.'
            print 'U = Until. If the -U option is used the Date provided will be used to filter all the delegations until this date.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            print "u = User to save stats to ElasticSearch."
            print "P = Password to save to stats to ElasticSearch."
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
        
        with open(stats_file, 'w') as csv_file:
            csv_file.write('Geographic Area,ResourceType,Status,Organization,Date,NumOfDelegations,NumOfResources,IPCount,IPSpace\n')
        
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, date, UNTIL,\
                                    INCREMENTAL, final_existing_date )
        
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
        
        if user != '' and password != '':
            r = saveDFToElasticSearch(stats_df, user, password)
            status_code = r.status_code
            if status_code == 200:
                sys.stderr.write("Stats saved to ElasticSearch successfully!\n")
            else:
                print "Something went wrong when trying to save stats to ElasticSearch.\n"

        
if __name__ == "__main__":
    main(sys.argv[1:])