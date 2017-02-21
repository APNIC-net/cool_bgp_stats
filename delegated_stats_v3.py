#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import numpy as np
import pandas as pd
import datetime
import requests
import json
import hashlib
import getpass
# Just for DEBUG
#import os
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from DelegatedHandler import DelegatedHandler

statistics = ["NumOfDelegations", "NumOfResources", "IPCount", "IPSpace"]
    
# This function initializes the DataFrame that will be used to store the computed statistics
def initializeStatsDF(del_handler, EXTENDED):
    stats_df = pd.DataFrame()

    # For each organization that appears in the del_handler object
    for o in del_handler.orgs:
        # For each resource type
        for r in del_handler.res_types:
            # Set the status variable with its possible values according to the resource type
            if r == 'asn':
                status = del_handler.status_asn
            else: # r == 'ipv4' or r == 'ipv6'
                status = del_handler.status_ip
              
            # The names of the columns that will be part of the index
            index_names = ['GeographicArea',
                           'ResourceType',
                           'Status',
                           'Organization',
                           'Date'
                           ]
            
            # The iterables variable contains all the possible values of each
            # column that is part of the index
            if EXTENDED:
                iterables = [del_handler.orgs_areas[o], [r], status, [o], list(del_handler.dates_range)]
                                
            else:
                iterables = [del_handler.orgs_areas[o], [r], status, 'NA', list(del_handler.dates_range)]
            
            # The columns that will contain the computed statistics
            cols = [            
            # NumOfDelegations counts the number of rows in the delegated file
                   'NumOfDelegations',
            # For IPv4 or IPv6 NumOfResources = NumOfDelegations as IP blocks delegations
            # are shown in the delegated files as one block per line
            # For ASNs NumOfResources counts the number of ASNs assigned which is not
            # necessarily equal to NumOfDelegations as more than one ASN may be assigned
            # as part of a single delegation
                   'NumOfResources',
            # IPCount counts the number of IPs assigned/allocated for IPv4
            # and the number of /56s for IPv6.
            # For ASNs IPCount is nan
                   'IPCount',
            # IPSpace contains the number of /24s assigned/allocated for IPv4
            # and the number of /48s for IPv6.
            # For ASNs IPSpace is nan
                   'IPSpace'                        
                   ]
                
            # An index with all the possible combinations of the values of the index columns
            index = pd.MultiIndex.from_product(iterables, names=index_names)
            # An empty DataFrame with the index we created and the columns for the stats that will be computed
            aux_df = pd.DataFrame(index=index, columns=cols)

            # The new DataFrame is appended to the existing stats DataFrame the function received as a parameter
            stats_df = pd.concat([stats_df, aux_df])

    # We sort the index and return the DataFrame    
    stats_df.sort_index(inplace=True)
    return stats_df

# This function is used to compute the sum of the number of /56 blocks contained
# in blocks with the prefix lengths specified in prefix_length_array
def compute56s(prefix_length_array):
    return sum(pow(2, 56 - prefix_length_array))

# This function is used to compute the sum of the number of /48 blocks contained
# in blocks with the prefix lengths specified in prefix_length_array
def compute48s(prefix_length_array):
    return sum(pow(2, 48 - prefix_length_array))
   
# This function computes all the statistics for all the dates within a dates_range
# from a DataFrame containing info about delegations (delegated_subset),
# for a given combination of Geographic Area (a), Resource Type (r), Status(s)
# and Organization (o)
# The computes statistics are stored in the DataFrame stats_df
def computation_loop(delegated_subset, a, r, s, o, stats_df, dates_range):
                   
  
    # If we are working with a specific resource type, we group the info
    # about delegations just by date
    date_groups =\
            delegated_subset.groupby(delegated_subset['date']\
                                    .map(lambda x: x.strftime('%Y%m%d')))

    # NumOfDelegations computation
    del_counts = date_groups.size()
    del_nonZeroDates = del_counts.index
    
    # NumOfResources computation
    if r == 'asn':
        res_counts = date_groups['count'].agg(np.sum)
        res_nonZeroDates = res_counts.index
    else: # r == 'ipv4' or r == 'ipv6'
        res_nonZeroDates = []

        if r == 'ipv4':
            ipv4_counts = date_groups['count'].agg(np.sum)
            ipv4_counts_nonZeroDates = ipv4_counts.index
        else: # r == 'ipv6'
            ipv6_counts = date_groups['count'].agg(compute56s)
            ipv6_counts_nonZeroDates = ipv6_counts.index
            
            ipv6_space = date_groups['count'].agg(compute48s)
            ipv6_space_nonZeroDates = ipv6_space.index

    for date in dates_range:
        if r == 'ipv4' or r == 'ipv6':
            if date in del_nonZeroDates: 
                stats_df.loc[a, r, s, o, date]['NumOfDelegations'] =\
                                                            del_counts[date]
                stats_df.loc[a, r, s, o, date]['NumOfResources'] =\
                                                            del_counts[date]                            

            if r == 'ipv4':
                if date in ipv4_counts_nonZeroDates:    
                    ipv4_count = ipv4_counts[date]
                    stats_df.loc[a, r, s, o, date]['IPCount'] =\
                                                                ipv4_count
                    stats_df.loc[a, r, s, o, date]['IPSpace'] =\
                                                            ipv4_count/256

            else: # r == 'ipv6'
                if date in ipv6_counts_nonZeroDates:    
                    stats_df.loc[a, r, s, o, date]['IPCount'] =\
                                                                ipv6_counts[date]

                if date in ipv6_space_nonZeroDates:    
                    stats_df.loc[a, r, s, o, date]['IPSpace'] =\
                            ipv6_space[date]

        else: # r == 'asn'
            if date in del_nonZeroDates: 
                stats_df.loc[a, r, s, o, date]['NumOfDelegations'] =\
                                                del_counts[date]

            if date in res_nonZeroDates: 
                stats_df.loc[a, r, s, o, date]['NumOfResources'] =\
                                                res_counts[date]

            # IPCount and IPSpace do not make sense for r = 'asn'
            stats_df.loc[a, r, s, o, date]['IPCount'] = float(np.nan)
            stats_df.loc[a, r, s, o, date]['IPSpace'] = float(np.nan)

    return stats_df
    
# This function computes statistis for all the different combinations of
# Organization, Geographic Area, Resource Type and Status
def computeStatistics(del_handler, stats_df):
    org_groups = del_handler.delegated_df.groupby(del_handler.delegated_df['opaque_id'])
    for o in del_handler.orgs:
        try:
            org_df = org_groups.get_group(o)
        except KeyError:
            continue
            
        country_groups = org_df.groupby(org_df['cc'])
        region_groups = org_df.groupby(org_df['region'])
        
        for a in del_handler.orgs_areas[o]:
            if a.startswith('Reg_'): # a is a region
                try:
                    area_df = region_groups.get_group(a)
                except KeyError:
                    continue
            else: # a is a CC
                try:
                    area_df = country_groups.get_group(a)
                except KeyError:
                    continue
    
            res_groups = area_df.groupby(area_df['resource_type'])
            
            for r in del_handler.res_types:
                try:
                    res_df = res_groups.get_group(r)
                except KeyError:
                    continue
                    
                if r == 'ipv4' or r == 'ipv6':
                    status = del_handler.status_ip

                else: # r == 'asn'
                    status = del_handler.status_asn
    
                status_groups = res_df.groupby(res_df['status'])
                for s in status:
                    try:
                        status_res_df = status_groups.get_group(s)
                    except KeyError:
                        continue
                
                    stats_df = computation_loop(status_res_df, a, r, s, o, stats_df, del_handler.dates_range)
                
    return stats_df
    
def hashFromColValue(col_value):
    return hashlib.md5(col_value).hexdigest()

# This function saves a data frame with stats into ElasticSearch
def saveToElasticSearch(plain_df, user, password):
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
    
    if INCREMENTAL:
        existing_stats_df = pd.DataFrame()

        if stats_file == '':
            print "If option -i is used, a statistics file MUST be provided."
            sys.exit()
        else:
            existing_stats_df = pd.read_csv(stats_file, sep = ',')
            final_existing_date = max(existing_stats_df['Date'])
            # Remove stats for final existing date in case the stats for that day were incomplete
            # Stats for that day will be computed again
            existing_stats_df = existing_stats_df[existing_stats_df['Date'] != final_existing_date]

    today = datetime.date.today().strftime('%Y%m%d')
    
    if not DEBUG:

        if EXTENDED:
            del_file = '%s/extended_apnic_%s.txt' % (files_path, today)
        else:
            del_file = '%s/delegated_apnic_%s.txt' % (files_path, today)
            
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year, month, day)
        
    if not del_handler.delegated_df.empty:
        stats_df = initializeStatsDF(del_handler, EXTENDED)
        sys.stderr.write("Stats Data Frame initialized successfully!\n")
        stats_df = computeStatistics(del_handler, stats_df)
        
        # We remove the rows with more than 2 nan values
        # The rows for which IPSpace and IPCount don't make sense have 2 nan values
        # and we want te keep those
        stats_df = stats_df.dropna(thresh=2)
        
        if INCREMENTAL:
            stats_df = pd.concat([existing_stats_df, stats_df])
    else:
        if INCREMENTAL:
            stats_df = existing_stats_df
        else:
            sys.exit()
            
    sys.stderr.write("Stats computed successfully!\n")
            
    if DEBUG:
        file_name = '%s/delegated_stats_test_%s_%s' % (files_path, year, today)
    else:
        file_name = '%s/delegated_stats_%s_%s' % (files_path, year, today)
    
        
    stats_df.to_csv('%s.csv' % file_name)
    plain_df = stats_df.reset_index()
    plain_df.to_json('%s.json' % file_name, orient='index')
    sys.stderr.write("Stats saved to files successfully!\n")
    sys.stderr.write("(%s.csv and %s.json)" % (file_name, file_name))

    if user != '' and password != '':
        r = saveToElasticSearch(plain_df, user, password)
        status_code = r.status_code
        if status_code == 200:
            sys.stderr.write("Stats saved to ElasticSearch successfully!\n")
        else:
            print "Something went wrong when trying to save stats to ElasticSearch.\n"

        
if __name__ == "__main__":
    main(sys.argv[1:])