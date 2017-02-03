#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import numpy as np
import pandas as pd
import datetime
from DelegatedHandler import DelegatedHandler

statistics = ["NumOfDelegations", "NumOfResources", "IPCount", "IPSpace"]
    
def initializeStatsDF(del_handler, EXTENDED, stat):
    stats_df = pd.DataFrame()

    #Fill stats_df with 0 for all the possible combinations of column values
    for o in del_handler.orgs:
        for r in del_handler.res_types:
            if r == 'All':
                status = ['All']
            elif r == 'asn':
                if o == 'All':
                    status = del_handler.status_asn_all
                elif o == 'NA':
                    status = del_handler.status_notdel
                else:
                    status = del_handler.status_asn_countries
            else: # r == 'ipv4' or r == 'ipv6'
                if o == 'All':
                    status = del_handler.status_ip_all
                elif o == 'NA':
                    status = del_handler.status_notdel
                else:
                    status = del_handler.status_ip_countries
                

            index_names = ['Geographic Area',
                           'ResourceType',
                           'Status',
                           'Organization',
                           'Date'
                           ]
                          
            if EXTENDED:
                iterables = [del_handler.orgs_areas[o], [r], status, [o], list(del_handler.dates_range)]
                                
            else:
                iterables = [del_handler.orgs_areas[o], [r], status, 'NA', list(del_handler.dates_range)]
            
            if stat == '':
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
            # For ASNs IPCount = -1
                                                           'IPCount',
            # IPSpace contains the number of /24s assigned/allocated for IPv4
            # and the number of /48s for IPv6.
            # For ASNs IPSpace = -1
                                                           'IPSpace'                        
                        ]
            else:
                cols = [stat]
                
            index = pd.MultiIndex.from_product(iterables, names=index_names)
            aux_df = pd.DataFrame(index=index, columns=cols)
            aux_df = aux_df.fillna(0)
            stats_df = pd.concat([stats_df, aux_df])
    
    stats_df.sort_index(inplace=True)
    return stats_df

def compute56s(prefix_length_array):
    return sum(pow(2, 56 - prefix_length_array))

def compute48s(prefix_length_array):
    return sum(pow(2, 48 - prefix_length_array))
    
def computation_loop(delegated_subset, a, r, d, o, stats_df, stats_of_interest, dates_range):
                   
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
                    
                if stats_of_interest['NumOfDelegations']:
                    stats_df.loc[a, r, d, o, date]['NumOfDelegations'] =\
                                                ipv4_del + ipv6_del + asn_del
                if stats_of_interest['NumOfResources']:
                    stats_df.loc[a, r, d, o, date]['NumOfResources'] =\
                                                ipv4_del + ipv6_del + asn_res
                
            # IPCount and IPSpace do not make sense for r = 'All'
            if stats_of_interest['IPCount']:
                stats_df.loc[a, r, d, o, date]['IPCount'] = -1
            if stats_of_interest['IPSpace']:                
                stats_df.loc[a, r, d, o, date]['IPSpace'] = -1
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
                    if stats_of_interest['NumOfDelegations']:
                        stats_df.loc[a, r, d, o, date]['NumOfDelegations'] =\
                                                                del_counts[date]
                    if stats_of_interest['NumOfResources']:
                        stats_df.loc[a, r, d, o, date]['NumOfResources'] =\
                                                                del_counts[date]                            
                else:
                    if stats_of_interest['NumOfDelegations']:
                        stats_df.loc[a, r, d, o, date]['NumOfDelegations'] = 0
                    if stats_of_interest['NumOfResources']:
                        stats_df.loc[a, r, d, o, date]['NumOfResources'] = 0 
                    
                if r == 'ipv4':
                    if date in ipv4_counts_nonZeroDates:    
                        ipv4_count = ipv4_counts[date]
                        if stats_of_interest['IPCount']:
                            stats_df.loc[a, r, d, o, date]['IPCount'] =\
                                                                    ipv4_count
                        if stats_of_interest['IPSpace']:
                            stats_df.loc[a, r, d, o, date]['IPSpace'] =\
                                                                ipv4_count/256
                    else:
                        if stats_of_interest['IPCount']:
                            stats_df.loc[a, r, d, o, date]['IPCount'] = 0
                        if stats_of_interest['IPSpace']:
                            stats_df.loc[a, r, d, o, date]['IPSpace'] = 0
                        
                else: # r == 'ipv6'
                    if stats_of_interest['IPCount']:
                        if date in ipv6_counts_nonZeroDates:    
                            stats_df.loc[a, r, d, o, date]['IPCount'] =\
                                                                    ipv6_counts[date]
                        else:
                            stats_df.loc[a, r, d, o, date]['IPCount'] = 0
                    
                    if stats_of_interest['IPSpace']:
                        if date in ipv6_space_nonZeroDates:    
                            stats_df.loc[a, r, d, o, date]['IPSpace'] =\
                                ipv6_space[date]
                        else:
                            stats_df.loc[a, r, d, o, date]['IPSpace'] = 0
                
            else: # r == 'asn'
                if stats_of_interest['NumOfDelegations']:
                    if date in del_nonZeroDates: 
                        stats_df.loc[a, r, d, o, date]['NumOfDelegations'] =\
                                                    del_counts[date]
                    else:
                        stats_df.loc[a, r, d, o, date]['NumOfDelegations'] = 0
        
                if stats_of_interest['NumOfResources']:
                    if date in res_nonZeroDates: 
                        stats_df.loc[a, r, d, o, date]['NumOfResources'] =\
                                                    res_counts[date]
                    else:
                        stats_df.loc[a, r, d, o, date]['NumOfResources'] = 0
                    
                # IPCount and IPSpace do not make sense for r = 'asn'
                if stats_of_interest['IPCount']:
                    stats_df.loc[a, r, d, o, date]['IPCount'] = -1
                if stats_of_interest['IPSpace']:
                    stats_df.loc[a, r, d, o, date]['IPSpace'] = -1

    return stats_df
    
def computeStatistics(del_handler, stats_df, stats_of_interest):
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
                reg = a.split('_')[1]
                try:
                    area_df = region_groups.get_group(reg)
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
                    stats_df = computation_loop(area_df, a, r, 'All', o, stats_df, stats_of_interest, del_handler.dates_range)
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
                        
                        stats_df = computation_loop(status_res_df, a, r, s, o, stats_df, stats_of_interest, del_handler.dates_range)
                    
    # In some cases, the original value of 0 may have persisted but for
    # r == 'All' or r == 'asn' the columns IPCount and IPSpace don't make sense
    if stats_of_interest['IPCount']:
        stats_df.loc[(slice(None), slice('All','asn'), slice(None), slice(None),\
                                                     slice(None)), 'IPCount'] = -1
    if stats_of_interest['IPSpace']:
        stats_df.loc[(slice(None), slice('All','asn'), slice(None), slice(None),\
                                                     slice(None)), 'IPSpace'] = -1
    
    return stats_df


def main(argv):
    global DEBUG, EXTENDED, del_file, files_path
    
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
        opts, args = getopt.getopt(argv, "hy:d:ep:i:t:", ["year=", "del_file=", "files_path=", "stats_file=", "statistic="])
    except getopt.GetoptError:
        print 'Usage: delegatd_stats_v2.py -h | -y <year> [-d <delegated file>] [-e] -p <files path [-i <stats file>] [-t <statistic>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statistics from the delegated files provided by the RIRs"
            print 'Usage: delegatd_stats_v2.py -h | -y <year> [-d <delegated file>] [-e] -p <files path [-i <stats file>] [-t <statistic>]'
            print 'h = Help'
            print 'y = Year to compute statistics for'
            print 'd = DEBUG mode. Provide path to delegated file.'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            print 't = Statistic. Specify statistic to be computed. Choose one from: "NumOfDelegations", "NumOfResources", "IPCount" or "IPSpace".'
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
        elif opt == '-t':
            stat = arg
        else:
            assert False, 'Unhandled option'
            
    if year == '':
        print "You must provide the year for which you want the statistics to\
                be computed."
        sys.exit()

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

    
    if stat == '':
        stats_of_interest = dict([(s, True) for s in statistics])
    else:
        stats_of_interest = dict([(s, False) for s in statistics])
        stats_of_interest[stat] = True
        
        
    
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
        stats_df = initializeStatsDF(del_handler, EXTENDED, stat)
        stats_df = computeStatistics(del_handler, stats_df, stats_of_interest)
 
        if INCREMENTAL:
            stats_df = pd.concat([existing_stats_df, stats_df])
    else:
        if INCREMENTAL:
            stats_df = existing_stats_df
        else:
            sys.exit()
            
    if DEBUG:
        file_name = '%s/delegated_stats_test_%s_%s' % (files_path, year, today)
    else:
        file_name = '%s/delegated_stats_%s_%s' % (files_path, year, today)
    
        
    stats_df.to_csv('%s.csv' % file_name)
    stats_df.reset_index().to_json('%s.json' % file_name, orient='index')
    
        
if __name__ == "__main__":
    main(sys.argv[1:])