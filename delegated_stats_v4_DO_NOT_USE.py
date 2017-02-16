#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import os, sys, getopt
import numpy as np
import pandas as pd
import datetime
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from DelegatedHandler import DelegatedHandler

statistics = ["NumOfDelegations", "NumOfResources", "IPCount", "IPSpace"]

# This function computes 
def compute56s(prefix_length_array):
    return sum(pow(2, 56 - prefix_length_array))

def compute48s(prefix_length_array):
    return sum(pow(2, 48 - prefix_length_array))
    
def updateOrInsertIntoStatsDF(stats_df, area, res_type, status, org, date, stat_name, stat_value):
    
    try:
        #If a row for this combination already exists in the stats DataFrame
        # we update the value of the stat provided        
        stats_df.loc[(area, res_type, status, org, date), stat_name] = stat_value
        return stats_df
        
    except KeyError:
        # If there is still no row for this specific combination of GeographicArea, ResourceType, Status, Organization and Date
        # we insert a new row and set the provided stat with the corresponding value
        if stat_name == 'NumOfDelegations':
            num_del = stat_value
        else:
            num_del = np.nan
        
        if stat_name == 'NumOfResources':
            num_res = stat_value
        else:
            num_res = np.nan
            
        if stat_name == 'IPCount':
            count = stat_value
        else:
            count = np.nan
 
        if stat_name == 'NumOfDelegations':
            space = stat_value
        else:
            space = np.nan
            
        aux_dic = {'GeographicArea':area,\
                    'ResourceType':res_type,\
                    'Status':status,\
                    'Organization':org,\
                    'Date':date,\
                    'NumOfDelegations':num_del,\
                    'NumOfResources':num_res,\
                    'IPCount':count,\
                    'IPSpace':space}
                    
        index_cols = ['GeographicArea',
                 'ResourceType',
                 'Status',
                 'Organization',
                 'Date']
        stat_cols = ['NumOfDelegations',
                 'NumOfResources',
                 'IPCount',
                 'IPSpace']

        # we provide the column names so that the data frame keeps the order of the columns
        aux_df = pd.DataFrame(data=aux_dic, columns=index_cols+stat_cols, index=[0])
        aux_df.set_index(index_cols, inplace=True)
        stats_df = stats_df.append(aux_df)
        
        return stats_df.sort_index()
    
def computation_loop(delegated_subset, a, r, s, o, stats_df, dates_range):
                   
    if r == 'All':
        date_groups =\
            delegated_subset.groupby([delegated_subset['date']\
                                        .map(lambda x: x.strftime('%Y%m%d')),\
                                    delegated_subset['resource_type']])

        del_counts = date_groups.size()
        del_nonZeroDates = del_counts.index.levels[0]

        for date in dates_range:
            if date in del_nonZeroDates:
                try:
                    ipv4_del = del_counts[(date, 'ipv4')]
                except KeyError:
                    ipv4_del = 0
                try:
                    ipv6_del = del_counts[(date, 'ipv6')]
                except KeyError:
                    ipv6_del = 0
                try:
                    asn_del = del_counts[(date, 'asn')]
                except KeyError:
                    asn_del = 0
                
                try:
                    asn_res =\
                            sum(date_groups.get_group((date, 'asn'))['count'])
                except KeyError:
                    asn_res = 0
                    
                del_sum = ipv4_del + ipv6_del + asn_del
                if del_sum > 0:
                    stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                                    'NumOfDelegations', del_sum)
    
                res_sum = ipv4_del + ipv6_del + asn_res
                if res_sum > 0:
                    stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                                        'NumOfResources', res_sum)
                
            # IPCount and IPSpace do not make sense for r = 'All'
            stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                                            'IPCount', np.nan)
            stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                                            'IPSpace', np.nan)

    else: # r != 'All'
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
                    stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'NumOfDelegations', del_counts[date])
                                                                
                    stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'NumOfResources', del_counts[date])
                    
                if r == 'ipv4':
                    if date in ipv4_counts_nonZeroDates:    
                        ipv4_count = ipv4_counts[date]
                        stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'IPCount', ipv4_count)
                          
                        stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'IPSpace', ipv4_count/256)

                else: # r == 'ipv6'
                    if date in ipv6_counts_nonZeroDates:
                        stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                                    'IPCount', ipv6_counts[date])
                    
                    if date in ipv6_space_nonZeroDates:    
                        stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                                    'IPSpace', ipv6_space[date])
                
            else: # r == 'asn'
                if date in del_nonZeroDates: 
                    stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'NumOfDelegations', del_counts[date])
        
                if date in res_nonZeroDates: 
                    stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'NumOfResources', res_counts[date])
                    
                # IPCount and IPSpace do not make sense for r = 'asn'
                stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'IPCount', np.nan)
                stats_df = updateOrInsertIntoStatsDF(stats_df, a, r, s, o, date,\
                                            'IPSpace', np.nan)
                                            
    return stats_df
    
def computeStatistics(del_handler, stats_df):
    org_groups = del_handler.delegated_df.groupby(del_handler.delegated_df['opaque_id'])
    for o in del_handler.orgs:
        if o == 'All':
            org_df = del_handler.delegated_df
        else:
            try:
                org_df = org_groups.get_group(o)
            except KeyError:
                continue
            
        country_groups = org_df.groupby(org_df['cc'])
        region_groups = org_df.groupby(org_df['region'])
        
        for a in del_handler.orgs_areas[o]:
            if a == 'All':
                area_df = org_df
            elif a.startswith('Reg_'): # a is a region
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
                if r == 'All':
                    stats_df = computation_loop(area_df, a, r, 'All', o, stats_df, del_handler.dates_range)
                else:
                    try:
                        res_df = res_groups.get_group(r)
                    except KeyError:
                        continue
                        
                    if r == 'ipv4' or r == 'ipv6':
                        if a == 'All':
                            status = del_handler.status_ip_all
                        else:
                            status = del_handler.status_ip_countries
                    else: # r == 'asn'
                        if a == 'All':
                            status = del_handler.status_asn_all
                        else:
                            status = del_handler.status_asn_countries
        
                    status_groups = res_df.groupby(res_df['status'])
                    for s in status:
                        if s == 'All':
                            status_res_df = res_df
                        else:
                            try:
                                status_res_df = status_groups.get_group(s)
                            except KeyError:
                                continue
                        
                        stats_df = computation_loop(status_res_df, a, r, s, o, stats_df, del_handler.dates_range)
                     
    return stats_df


def main(argv):    
    DEBUG = False
    EXTENDED = False
    year = ''
    del_file = ''
    files_path = ''
    INCREMENTAL = False
    stats_file = ''
    final_existing_date = ''
    stat = ''
    
    try:
        opts, args = getopt.getopt(argv, "hy:d:ep:i:", ["year=", "del_file=", "files_path=", "stats_file="])
    except getopt.GetoptError:
        print 'Usage: delegatd_stats_v3.py -h | -p <files path [-y <year>] [-d <delegated file>] [-e] [-i <stats file>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statistics from one of the delegated files provided by the RIRs"
            print 'Usage: delegatd_stats_v3.py -h | -p <files path [-y <year>] [-d <delegated file>] [-e] [-i <stats file>]'
            print 'h = Help'
            print 'y = Year to compute statistics for. If a year is not provided, statistics will be computed for all the available years.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            sys.exit()
        elif opt == '-y':
            year = int(arg)
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
        else:
            assert False, 'Unhandled option'

    if DEBUG and del_file == '':
        print "If you choose to run in DEBUG mode you must provide the path to\
                    a delegated file that has already been downloaded."
        sys.exit()
        
    if files_path == '':
        print "You must provide a folder to save files."
        sys.exit()
    
    if stat != '' and stat not in statistics:
        print "Wrong statistic provided."
        sys.exit()

    index_cols = ['GeographicArea',
                 'ResourceType',
                 'Status',
                 'Organization',
                 'Date']
    
    stat_cols = ['NumOfDelegations',
                 'NumOfResources',
                 'IPCount',
                 'IPSpace']
        
        
    
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
            
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year)
        
    if not del_handler.delegated_df.empty:
        stats_df = pd.DataFrame(columns=index_cols+stat_cols)
        stats_df.set_index(index_cols, inplace=True)
        sys.stderr.write("Stats Data Frame initialized successfully!\n")
        stats_df = computeStatistics(del_handler, stats_df)
        
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
    stats_df.reset_index().to_json('%s.json' % file_name, orient='index')
    # TODO save to database?
    sys.stderr.write("Stats saved to files successfully!\n")
    sys.stderr.write("(%s.csv and %s.json)" % (file_name, file_name))

        
if __name__ == "__main__":
    main(sys.argv[1:])