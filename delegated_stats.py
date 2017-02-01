import sys
import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
import datetime
import calendar
#from pyjstat import pyjstat


DEBUG = False
EXTENDED = True

if DEBUG:
    today = '20170127'
else:
    today = datetime.date.today().strftime('%Y%m%d')

project_path = '/Users/sofiasilva/GitHub/cool_bgp_stats'
files_path = '/Users/sofiasilva/BGP_files'


sys.path.append(project_path)
from get_file import get_file

if EXTENDED:
    dest_file = '%s/extended_apnic_%s.txt' % (files_path, today)
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
    dest_file = '%s/delegated_apnic_%s.txt' % (files_path, today)
    download_url = 'https://ftp.apnic.net/stats/apnic/delegated-apnic-latest'
    col_names = [
                    'registry',
                    'cc',
                    'resource_type',
                    'initial_resource',
                    'count',
                    'date',
                    'status'
                ]

if not DEBUG:
    get_file(download_url, dest_file)
    


delegated_df = pd.read_csv(
                dest_file,
                sep = '|',
                header=None,
                names = col_names,
                index_col=False,
                parse_dates=['date'],
                infer_datetime_format=True,
                comment='#'
            )
            
status_asn_all = ['All', 'alloc-32bits', 'alloc-16bits', 'available', 'reserved']
status_asn_countries = ['All', 'alloc-32bits', 'alloc-16bits']
status_ip_all = ['All', 'allocated', 'assigned', 'available', 'reserved']
status_ip_countries = ['All', 'allocated', 'assigned']
status_notdel = ['All', 'available', 'reserved']
            

if DEBUG:
    res_types = ['All', 'asn', 'ipv4']
    granularities = ['All', 'annually'] 

    if EXTENDED:
        delegated_df = pd.concat([delegated_df[10:90], delegated_df[85400:85420], delegated_df[90900:90990]])
    else:
        delegated_df = pd.concat([delegated_df[10:90], delegated_df[10000:10080]])

    initial_date = datetime.datetime\
                        .strptime(str(min(delegated_df[delegated_df['date'] != 'nan']['date'])), '%Y%m%d')
    final_date = datetime.datetime\
                        .strptime(str(max(delegated_df[delegated_df['date'] != 'nan']['date'])), '%Y%m%d')
   
   
else:
    res_types = ['All', 'asn', 'ipv4', 'ipv6']
    granularities = ['All', 'daily', 'weekly', 'monthly', 'annually']

    try:
        initial_date = datetime.datetime\
                                .strptime(str(delegated_df.loc[0, 'count']), '%Y%m%d')
    except ValueError:
        initial_date = ''
    
    try:
        final_date = datetime.datetime\
                                .strptime(str(delegated_df.loc[0, 'date']), '%Y%m%d')
    except ValueError:
        final_date = ''
        
    summary_records = pd.DataFrame()

    for i in range(4):
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

delegated_df['date'] = pd.to_datetime(delegated_df['date'], format='%Y%m%d')

# If start date is not present in version line we take the minimum date present
if initial_date == '':
    initial_date = min(delegated_df['date'])

# If end date is not present in version line we take the maximum date present
if final_date == '':
    final_date = max(delegated_df['date'])
    
delegated_df.ix[pd.isnull(delegated_df.cc), 'cc'] = 'XX'
CCs = list(set(delegated_df['cc'].values))
CCs.extend(['All'])

delegated_df.ix[pd.isnull(delegated_df.opaque_id), 'opaque_id'] = 'NA'
orgs = list(set(delegated_df['opaque_id'].values))
orgs.extend(['All'])

orgs_countries = dict()

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

def compute56s(prefix_length_array):
    return sum(pow(2, 56 - prefix_length_array))

def compute48s(prefix_length_array):
    return sum(pow(2, 48 - prefix_length_array))
    
def computation_loop(delegated_subset, c, r, d, o, g, stats_df):
    if g == 'All':
        try:
            stats_df.loc[c, r, d, o, 'All', 'All']['NumOfDelegations'] =\
                                                        len(delegated_subset)
        except KeyError:
            print 'Error para %s %s %s %s' % (c, r, d, o)

        if r == 'All':
            asn_subset =\
                delegated_subset[delegated_subset['resource_type'] == 'asn']
            ip_subset =\
                delegated_subset[delegated_subset['resource_type'] != 'asn']
            
            stats_df.loc[c, r, d, o, 'All', 'All']['NumOfResources'] =\
                                sum(asn_subset['count']) + len(ip_subset)
                                
            # IPCount and IPSpace do not make sense for r = 'All'
            stats_df.loc[c, r, d, o, 'All', 'All']['IPCount'] = -1
            stats_df.loc[c, r, d, o, 'All', 'All']['IPSpace'] = -1
            
        elif r == 'ipv4' or r == 'ipv6':
            stats_df.loc[c, r, d, o, 'All', 'All']['NumOfResources'] =\
                                                    len(delegated_subset)

            if r == 'ipv4':                    
                total_ips = sum(delegated_subset['count'])
                
                stats_df\
                    .loc[c, r, d, o, 'All', 'All']['IPCount'] = total_ips
                stats_df\
                    .loc[c, r, d, o, 'All', 'All']['IPSpace'] = total_ips/256
            else: # r == 'ipv6'
                stats_df\
                    .loc[c, r, d, o, 'All', 'All']['IPCount'] = sum(pow(2, 56 - delegated_subset['count']))
                stats_df\
                    .loc[c, r, d, o, 'All', 'All']['IPSpace'] = sum(pow(2, 48 - delegated_subset['count']))
                                        
            
        else: # r == 'asn'
            stats_df.loc[c, r, d, o, 'All', 'All']['NumOfResources'] =\
                                            sum(delegated_subset['count'])
                                            
             # IPCount and IPSpace do not make sense for r = 'asn'
            stats_df.loc[c, r, d, o, 'All', 'All']['IPCount'] = -1
            stats_df.loc[c, r, d, o, 'All', 'All']['IPSpace'] = -1
    else: # g != 'All'                   
        if r == 'All':
            if g == 'annually':
                date_groups =\
                    delegated_subset.groupby([delegated_subset['date']\
                                                .map(lambda x: x.year),\
                                            delegated_subset['resource_type']])
            elif g == 'monthly':
                date_groups =\
                    delegated_subset.groupby([delegated_subset['date']\
                                                .map(lambda x: x.strftime('%Y%m')),\
                                            delegated_subset['resource_type']])
            elif g == 'weekly':
                date_groups =\
                    delegated_subset.groupby([delegated_subset['date']\
                                                .map(lambda x: x.strftime('%Y%W')),\
                                            delegated_subset['resource_type']])
            else: # g == 'daily'
                date_groups =\
                    delegated_subset.groupby([delegated_subset['date']\
                                                .map(lambda x: x.strftime('%Y%m%d')),\
                                            delegated_subset['resource_type']])

            del_counts = date_groups.size()
            del_nonZeroDates = del_counts.index.levels[0]

            for date in dates_dic[g]:
                date = int(date)
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
                                                        
                    stats_df.loc[c, r, d, o, g, str(date)]['NumOfDelegations'] =\
                                            ipv4_del + ipv6_del + asn_del
                    
                    stats_df.loc[c, r, d, o, g, str(date)]['NumOfResources'] =\
                                            ipv4_del + ipv6_del + asn_res
                    
                # IPCount and IPSpace do not make sense for r = 'All'
                stats_df.loc[c, r, d, o, g, str(date)]['IPCount'] = -1
                stats_df.loc[c, r, d, o, g, str(date)]['IPSpace'] = -1
        else: # r != 'All'
            if g == 'annually':
                date_groups =\
                    delegated_subset.groupby(delegated_subset['date']\
                                              .map(lambda x: x.year))
            elif g == 'monthly':
                date_groups =\
                    delegated_subset.groupby(delegated_subset['date']\
                                            .map(lambda x: x.strftime('%Y%m')))
            elif g == 'weekly':
                date_groups =\
                    delegated_subset.groupby(delegated_subset['date']\
                                            .map(lambda x: x.strftime('%Y%W')))
            else:
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
        
            for date in dates_dic[g]:
                if r == 'ipv4' or r == 'ipv6':
                    if int(date) in del_nonZeroDates: 
                        stats_df.loc[c, r, d, o, g, date]['NumOfDelegations'] =\
                                                    del_counts[int(date)]
                        stats_df.loc[c, r, d, o, g, date]['NumOfResources'] =\
                                                    del_counts[int(date)]                            
                    else:
                        stats_df.loc[c, r, d, o, g, date]['NumOfDelegations'] = 0
                        stats_df.loc[c, r, d, o, g, date]['NumOfResources'] = 0 
                        
                    if r == 'ipv4':
                        if int(date) in ipv4_counts_nonZeroDates:    
                            ipv4_count = ipv4_counts[int(date)]
                            stats_df.loc[c, r, d, o, g, date]['IPCount'] =\
                                ipv4_count
                            stats_df.loc[c, r, d, o, g, date]['IPSpace'] =\
                                ipv4_count/256
                        else:
                            stats_df.loc[c, r, d, o, g, date]['IPCount'] = 0
                            stats_df.loc[c, r, d, o, g, date]['IPSpace'] = 0
                            
                    else: # r == 'ipv6'
                        if int(date) in ipv6_counts_nonZeroDates:    
                            stats_df.loc[c, r, d, o, g, date]['IPCount'] =\
                                ipv6_counts[int(date)]
                        else:
                            stats_df.loc[c, r, d, o, g, date]['IPCount'] = 0
                            
                        if int(date) in ipv6_space_nonZeroDates:    
                            stats_df.loc[c, r, d, o, g, date]['IPSpace'] =\
                                ipv6_space[int(date)]
                        else:
                            stats_df.loc[c, r, d, o, g, date]['IPSpace'] = 0
                    
                else: # r == 'asn'
                    if int(date) in del_nonZeroDates: 
                        stats_df.loc[c, r, d, o, g, date]['NumOfDelegations'] =\
                                                    del_counts[int(date)]
                    else:
                        stats_df.loc[c, r, d, o, g, date]['NumOfDelegations'] = 0
            
                    if int(date) in res_nonZeroDates: 
                        stats_df.loc[c, r, d, o, g, date]['NumOfResources'] =\
                                                    res_counts[int(date)]
                    else:
                        stats_df.loc[c, r, d, o, g, date]['NumOfResources'] = 0
                        
                    # IPCount and IPSpace do not make sense for r = 'asn'
                    stats_df.loc[c, r, d, o, g, str(date)]['IPCount'] = -1
                    stats_df.loc[c, r, d, o, g, str(date)]['IPSpace'] = -1
    return stats_df


# Just to verify
if not DEBUG:
    num_rows = len(delegated_df)
    if not num_rows == int(summary_records[summary_records['Type'] == 'All']['count']):
      print 'THERE\'S SOMETHING WRONG!'
    
    for r in res_types[1:4]:
      total = len(delegated_df[delegated_df['resource_type'] == r])
      if not total == int(summary_records[summary_records['Type'] == r]['count']):
        print 'THERE\'S SOMETHING WRONG WITH THE NUMBER OF %s' % r

# Create empty Data Frame
stats_df = pd.DataFrame()

dates_dic = dict()

#Fill it with 0 for all the possible combinations of column values
for g in granularities:
    if g == 'daily':
        dates = pd.date_range(start=initial_date, end=final_date, freq='D')
        dates = dates.strftime('%Y%m%d')
    elif g == 'weekly':
        dates = pd.date_range(start=initial_date - datetime.timedelta(weeks=1),\
                # we substract one week from initial day to make sure first week is included
                end=final_date, freq='W')
        dates = dates.strftime('%Y%W')
    elif g == 'monthly':
        dates = pd.date_range(\
            start=datetime.datetime(initial_date.year, initial_date.month, 1, 0, 0),\
            end=datetime.datetime(final_date.year, final_date.month,\
                calendar.monthrange(final_date.year, final_date.month)[1], 0, 0),\
            freq='M')
        dates = dates.strftime('%Y%m')
    elif g == 'annually':
        dates = pd.date_range(start=datetime.datetime(initial_date.year, 1, 1, 0, 0),\
                            end=datetime.datetime(final_date.year, 12, 31, 0, 0),\
                            freq='A')
        dates = dates.strftime('%Y')
    else:
        dates = ['All']

    #Save valid dates for current granularity    
    dates_dic[g] = dates
    
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
                           'Granularity',
                           'Date'
                           ]
                          
            if EXTENDED:
                iterables = [orgs_countries[o], r, status, o, [g], dates]
                                
            else:
                iterables = [orgs_countries[o], r, status, 'NA', [g], dates]
            
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

for g in granularities:
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
    
        for c in CCs:
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
                    stats_df = computation_loop(country_df, c, r, 'All', o, g, stats_df)
                else:
                    try:
                        res_df = res_groups.get_group(r)
                    except KeyError:
                        res_df = pd.DataFrame()
                        
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
        
                    try:
                        status_groups = res_df.groupby(res_df['status'])
                        for s in status:
                            if s == 'All':
                                status_res_df = res_df
                            else:
                                status_res_df = status_groups.get_group(s)
                            
                            stats_df = computation_loop(status_res_df, c, r, s, o, g, stats_df)
        
                    except KeyError:
                        status_res_df = pd.DataFrame()            

                
# In some cases, the original value of 0 may have persisted but for
# r == 'All' or r == 'asn' the columns IPCount and IPSpace don't make sense
stats_df.loc[(slice(None), slice('All','asn'), slice(None), slice(None),\
                                                 slice(None)), 'IPCount'] = -1
stats_df.loc[(slice(None), slice('All','asn'), slice(None), slice(None),\
                                                 slice(None)), 'IPSpace'] = -1
                

if DEBUG:
    file_name = '%s/delegated_stats_test_%s' % (files_path, today)
else:
    file_name = '%s/delegated_stats_%s' % (files_path, today)

    
stats_df.to_csv('%s.csv' % file_name)
stats_df.to_json('%s.json' % file_name)

# For JSON-stat format
# The function to_json_stat needs argument value to know which is the column with values
# and it accepts only one column with values
# pyjstat.to_json_stat(stats_df, value='NumOfDelegations', output='dict')


#TODO luego de tener las estadisticas hasta el momento, tengo que escribir codigo
# que solo lea lo del ultimo dia y lo agregue al data frame de estadisticas
# y luego genere el nuevo JSON
# actualizar las estadisticas existentes que cambien ('All', dia correspondiente, etc.)
