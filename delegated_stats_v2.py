#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
import datetime
#from pyjstat import pyjstat
from get_file import get_file

col_names = ''
res_types = ''
status_asn_all = ''
status_asn_countries = ''
status_ip_all = ''
status_ip_countries = ''
status_notdel = ''

summary_records = pd.DataFrame()

dates_range = ''

CCs = []
orgs = []
orgs_countries = dict()

def initialization(DEBUG, EXTENDED):
    global download_url, col_names, res_types, status_asn_all, status_asn_countries,\
            status_ip_all, status_ip_countries, status_notdel
    
    if DEBUG:
        res_types = ['All', 'asn', 'ipv4']
    else:
        res_types = ['All', 'asn', 'ipv4', 'ipv6']    
    
    if EXTENDED:
        download_url = 'ftp://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest'
        col_names = [
                        'registry',
                        'cc',
                        'resource_type',
                        'initial_resource',
                        'count',
                        'date',
                        'status',
                        'opaque_id'
                    ]
    else:
        download_url = 'ftp://ftp.apnic.net/stats/apnic/delegated-apnic-latest'
        col_names = [
                        'registry',
                        'cc',
                        'resource_type',
                        'initial_resource',
                        'count',
                        'date',
                        'status'
                    ]
    
    status_asn_all = ['All', 'alloc-32bits', 'alloc-16bits', 'available', 'reserved']
    status_asn_countries = ['All', 'alloc-32bits', 'alloc-16bits']
    status_ip_all = ['All', 'allocated', 'assigned', 'available', 'reserved']
    status_ip_countries = ['All', 'allocated', 'assigned']
    status_notdel = ['All', 'available', 'reserved']


def getAndTidyData(DEBUG, EXTENDED, INCREMENTAL, final_existing_date, del_file, year):
    delegated_df = pd.DataFrame()
    
    if not DEBUG:
        get_file(download_url, del_file)
        
    delegated_df = pd.read_csv(
                    del_file,
                    sep = '|',
                    header=None,
                    names = col_names,
                    index_col=False,
                    parse_dates=['date'],
                    infer_datetime_format=True,
                    comment='#'
                )
                
                
        
    global summary_records

    for i in range(len(res_types)):
        summary_records.at[i, 'Type'] = res_types[i]

        if i == 0:
            summary_records.at[i, 'count'] =\
                                int(delegated_df.loc[i, 'initial_resource'])
        else:
            summary_records.at[i, 'count'] =\
                                int(delegated_df.loc[i, 'count'])


    total_rows = delegated_df.shape[0]
    delegated_df =\
        delegated_df[int(total_rows-summary_records.loc[0, 'count']):total_rows]
        
    # Just to verify
    num_rows = len(delegated_df)
    if not num_rows == int(summary_records[summary_records['Type'] == 'All']['count']):
        print 'THERE\'S SOMETHING WRONG!'

    for r in res_types[1:4]:
        total = len(delegated_df[delegated_df['resource_type'] == r])
        if not total == int(summary_records[summary_records['Type'] == r]['count']):
            print 'THERE\'S SOMETHING WRONG WITH THE NUMBER OF %s' % r
    
    delegated_df['date'] = pd.to_datetime(delegated_df['date'], format='%Y%m%d')
    
    # We take the subset corresponding to the year of interest
    delegated_df = delegated_df[delegated_df['date'].map(lambda x: x.year) == year]
    
    if DEBUG:
        asn_subset = delegated_df[delegated_df['resource_type']=='asn']
        ipv4_subset = delegated_df[delegated_df['resource_type']=='ipv4']
        delegated_df = pd.concat([asn_subset.head(n=30),ipv4_subset.head(n=30)])
    
    if INCREMENTAL:
        initial_date = final_existing_date
        delegated_df = delegated_df[delegated_df['date'] >= initial_date]

    else:
        initial_date = min(delegated_df['date'])

    final_date = max(delegated_df['date'])

    global dates_range
    dates_range = pd.date_range(start=initial_date, end=final_date, freq='D')
    dates_range = dates_range.strftime('%Y%m%d')
        
    global CCs, orgs, orgs_countries
        
    delegated_df.ix[pd.isnull(delegated_df.cc), 'cc'] = 'XX'
    CCs = list(set(delegated_df['cc'].values))
    CCs.extend(['All'])
    
    delegated_df.ix[pd.isnull(delegated_df.opaque_id), 'opaque_id'] = 'NA'
    orgs = list(set(delegated_df['opaque_id'].values))
    orgs.extend(['All'])
    
    for o in orgs:
        if o == 'All':
            orgs_countries[o] = CCs
        elif o == 'NA':
            orgs_countries[o] = ['All', 'XX']
        else:
            org_countries = list(set(delegated_df[delegated_df['opaque_id'] == o]['cc']))
            org_countries.extend(['All'])
            orgs_countries[o] = org_countries
    
    asn_subset = delegated_df[delegated_df['resource_type']=='asn']
    alloc_asn_subset = asn_subset[asn_subset['status'] == 'allocated']
    indexes_alloc_asns = alloc_asn_subset.index.tolist()
    delegated_df.ix[indexes_alloc_asns, 'status'] = 'alloc-16bits'
    indexes_32bits = alloc_asn_subset.loc[pd.to_numeric(asn_subset['initial_resource']) > 65535,:].index.tolist()
    delegated_df.ix[indexes_32bits, 'status'] = 'alloc-32bits'
    
    return delegated_df
    
    
def initializeStatsDF(EXTENDED):
    stats_df = pd.DataFrame()

    #Fill stats_df with 0 for all the possible combinations of column values
    for o in orgs:
        for r in res_types:
            if r == 'All':
                status = ['All']
            elif r == 'asn':
                if o == 'All':
                    status = status_asn_all
                elif o == 'NA':
                    status = status_notdel
                else:
                    status = status_asn_countries
            else: # r == 'ipv4' or r == 'ipv6'
                if o == 'All':
                    status = status_ip_all
                elif o == 'NA':
                    status = status_notdel
                else:
                    status = status_ip_countries
                

            index_names = ['Country',
                           'ResourceType',
                           'Status',
                           'Organization',
                           'Date'
                           ]
                          
            if EXTENDED:
                iterables = [orgs_countries[o], [r], status, [o], list(dates_range)]
                                
            else:
                iterables = [orgs_countries[o], [r], status, 'NA', list(dates_range)]
            
            index = pd.MultiIndex.from_product(iterables, names=index_names)
            aux_df = pd.DataFrame(index=index, columns=[
            
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
                                    )
            aux_df = aux_df.fillna(0)
            stats_df = pd.concat([stats_df, aux_df])
    
    stats_df.sort_index(inplace=True)
    return stats_df

def compute56s(prefix_length_array):
    return sum(pow(2, 56 - prefix_length_array))

def compute48s(prefix_length_array):
    return sum(pow(2, 48 - prefix_length_array))
    
def computation_loop(delegated_subset, c, r, d, o, stats_df):
                   
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
                                                    
                stats_df.loc[c, r, d, o, date]['NumOfDelegations'] =\
                                        ipv4_del + ipv6_del + asn_del
                
                stats_df.loc[c, r, d, o, date]['NumOfResources'] =\
                                        ipv4_del + ipv6_del + asn_res
                
            # IPCount and IPSpace do not make sense for r = 'All'
            stats_df.loc[c, r, d, o, date]['IPCount'] = -1
            stats_df.loc[c, r, d, o, date]['IPSpace'] = -1
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
                    stats_df.loc[c, r, d, o, date]['NumOfDelegations'] =\
                                                del_counts[date]
                    stats_df.loc[c, r, d, o, date]['NumOfResources'] =\
                                                del_counts[date]                            
                else:
                    stats_df.loc[c, r, d, o, date]['NumOfDelegations'] = 0
                    stats_df.loc[c, r, d, o, date]['NumOfResources'] = 0 
                    
                if r == 'ipv4':
                    if date in ipv4_counts_nonZeroDates:    
                        ipv4_count = ipv4_counts[date]
                        stats_df.loc[c, r, d, o, date]['IPCount'] =\
                            ipv4_count
                        stats_df.loc[c, r, d, o, date]['IPSpace'] =\
                            ipv4_count/256
                    else:
                        stats_df.loc[c, r, d, o, date]['IPCount'] = 0
                        stats_df.loc[c, r, d, o, date]['IPSpace'] = 0
                        
                else: # r == 'ipv6'
                    if date in ipv6_counts_nonZeroDates:    
                        stats_df.loc[c, r, d, o, date]['IPCount'] =\
                            ipv6_counts[date]
                    else:
                        stats_df.loc[c, r, d, o, date]['IPCount'] = 0
                        
                    if date in ipv6_space_nonZeroDates:    
                        stats_df.loc[c, r, d, o, date]['IPSpace'] =\
                            ipv6_space[date]
                    else:
                        stats_df.loc[c, r, d, o, date]['IPSpace'] = 0
                
            else: # r == 'asn'
                if date in del_nonZeroDates: 
                    stats_df.loc[c, r, d, o, date]['NumOfDelegations'] =\
                                                del_counts[date]
                else:
                    stats_df.loc[c, r, d, o, date]['NumOfDelegations'] = 0
        
                if date in res_nonZeroDates: 
                    stats_df.loc[c, r, d, o, date]['NumOfResources'] =\
                                                res_counts[date]
                else:
                    stats_df.loc[c, r, d, o, date]['NumOfResources'] = 0
                    
                # IPCount and IPSpace do not make sense for r = 'asn'
                stats_df.loc[c, r, d, o, date]['IPCount'] = -1
                stats_df.loc[c, r, d, o, date]['IPSpace'] = -1

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
    
    try:
        opts, args = getopt.getopt(argv, "hy:d:ep:i:", ["year=", "del_file=", "files_path=", "stats_file="])
    except getopt.GetoptError:
        print 'Usage: delegatd_stats_v2.py -h | -y <year> [-d <delegated file>] [-e] -p <files path [-i <stats file>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statistics from the delegated files provided by the RIRs"
            print 'Usage: delegatd_stats_v2.py -h | -y <year> [-d <delegated file>] [-e] -p <files path [-i <stats file>]'
            print 'h = Help'
            print 'y = Year to compute statistics for'
            print 'd = DEBUG mode. Provide path to delegated file.'
            print 'e = Use Extended file'
            print "If option -e is used, file must be extended file."
            print "If option -e is not used, file must be delegated file not extended."
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            print "i = Incremental. Compute incremental statistics from existing stats file."
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

    existing_stats_df = pd.DataFrame()
    
    if INCREMENTAL:
        if stats_file == '':
            print "If option -i is used, a statistics file MUST be provided."
            sys.exit()
        else:
            # TODO leer stats_file into existing_stats_df
            final_existing_date = max(existing_stats_df['Date'])
        
    initialization(DEBUG, EXTENDED)

    today = datetime.date.today().strftime('%Y%m%d')
    
    if not DEBUG:

        if EXTENDED:
            del_file = '%s/extended_apnic_%s.txt' % (files_path, today)
        else:
            del_file = '%s/delegated_apnic_%s.txt' % (files_path, today)
            
    delegated_df = getAndTidyData(DEBUG, EXTENDED, INCREMENTAL, final_existing_date,\
                                                    del_file, year)
    stats_df = initializeStatsDF(EXTENDED)
 
    org_groups = delegated_df.groupby(delegated_df['opaque_id'])
    for o in orgs:
        if o == 'All':
            org_df = delegated_df
        else:
            try:
                org_df = org_groups.get_group(o)
            except KeyError:
                continue
            
        country_groups = org_df.groupby(org_df['cc'])
    
        for c in orgs_countries[o]:
            if c == 'All':
                country_df = org_df
            else:
                try:
                    country_df = country_groups.get_group(c)
                except KeyError:
                    continue
    
            res_groups = country_df.groupby(country_df['resource_type'])
            
            for r in res_types:
                if r == 'All':
                    stats_df = computation_loop(country_df, c, r, 'All', o, stats_df)
                else:
                    try:
                        res_df = res_groups.get_group(r)
                    except KeyError:
                        continue
                        
                    if r == 'ipv4' or r == 'ipv6':
                        if c == 'All':
                            status = status_ip_all
                        else:
                            status = status_ip_countries
                    else: # r == 'asn'
                        if c == 'All':
                            status = status_asn_all
                        else:
                            status = status_asn_countries
        
                    status_groups = res_df.groupby(res_df['status'])
                    for s in status:
                        if s == 'All':
                            status_res_df = res_df
                        else:
                            try:
                                status_res_df = status_groups.get_group(s)
                            except KeyError:
                                continue
                        
                        stats_df = computation_loop(status_res_df, c, r, s, o, stats_df)
               

                    
    # In some cases, the original value of 0 may have persisted but for
    # r == 'All' or r == 'asn' the columns IPCount and IPSpace don't make sense
    stats_df.loc[(slice(None), slice('All','asn'), slice(None), slice(None),\
                                                     slice(None)), 'IPCount'] = -1
    stats_df.loc[(slice(None), slice('All','asn'), slice(None), slice(None),\
                                                     slice(None)), 'IPSpace'] = -1
                    
    
    stats_df = pd.concat([existing_stats_df, stats_df])
    
    if DEBUG:
        file_name = '%s/delegated_stats_test_%s_%s' % (files_path, year, today)
    else:
        file_name = '%s/delegated_stats_%s_%s' % (files_path, year, today)
    
        
    stats_df.to_csv('%s.csv' % file_name)
    stats_df.reset_index().to_json('%s.json' % file_name, orient='index')
    
    # TODO Ver de usar esto aunque haya que generar un archivo por estadistica
    # ya que creo que es mas eficiente
    # For JSON-stat format
    # The function to_json_stat needs argument value to know which is the column with values
    # and it accepts only one column with values
    # pyjstat.to_json_stat(stats_df, value='NumOfDelegations', output='dict')


if __name__ == "__main__":
    main(sys.argv[1:])


#TODO luego de tener las estadisticas hasta el momento, tengo que escribir codigo
# que solo lea lo del ultimo dia y lo agregue al data frame de estadisticas
# y luego genere el nuevo JSON
# actualizar las estadisticas existentes que cambien ('All', dia correspondiente, etc.)
