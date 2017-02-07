#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

"""
Created on Fri Feb  3 11:09:35 2017

@author: sofiasilva
"""
import sys
import pandas as pd
from get_file import get_file


class DelegatedHandler:

    AP_regions = ['Eastern Asia', 'Oceania', 'Southern Asia', 'South-Eastern Asia']

    res_types = []
    status_asn_all = []
    status_asn_countries = []
    status_ip_all = []
    status_ip_countries = []
    status_notdel = []
    
    summary_records = pd.DataFrame()
    delegated_df = pd.DataFrame()    
    
    dates_range = ''
    
    CCs = []
    orgs = []
    orgs_areas = dict()

    def __init__(self, DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year):
        
        if DEBUG:
            self.res_types = ['All', 'asn', 'ipv4']
        else:
            self.res_types = ['All', 'asn', 'ipv4', 'ipv6']    
        
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
        
        self.status_asn_all = ['All', 'alloc-32bits', 'alloc-16bits', 'available', 'reserved']
        self.status_asn_countries = ['All', 'alloc-32bits', 'alloc-16bits']
        self.status_ip_all = ['All', 'allocated', 'assigned', 'available', 'reserved']
        self.status_ip_countries = ['All', 'allocated', 'assigned']
        self.status_notdel = ['All', 'available', 'reserved']
        
        self.getAndTidyData(DEBUG, EXTENDED, download_url, del_file, col_names, INCREMENTAL, final_existing_date, year)
        sys.stderr.write("DelegatedHandler instantiated successfully!")
    
    def getAndTidyData(self, DEBUG, EXTENDED, download_url, del_file, col_names, INCREMENTAL, final_existing_date, year):
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
                    
    
        for i in range(len(self.res_types)):
            self.summary_records.at[i, 'Type'] = self.res_types[i]
    
            if i == 0:
                self.summary_records.at[i, 'count'] =\
                                    int(delegated_df.loc[i, 'initial_resource'])
            else:
                self.summary_records.at[i, 'count'] =\
                                    int(delegated_df.loc[i, 'count'])
    
    
        total_rows = delegated_df.shape[0]
        delegated_df =\
            delegated_df[int(total_rows-self.summary_records.loc[0, 'count']):total_rows]
            
        # Just to verify
        num_rows = len(delegated_df)
        if not num_rows == int(self.summary_records[self.summary_records['Type'] == 'All']['count']):
            print 'THERE\'S SOMETHING WRONG!'
    
        for r in self.res_types[1:4]:
            total = len(delegated_df[delegated_df['resource_type'] == r])
            if not total == int(self.summary_records[self.summary_records['Type'] == r]['count']):
                print 'THERE\'S SOMETHING WRONG WITH THE NUMBER OF %s' % r
        
        delegated_df['date'] = pd.to_datetime(delegated_df['date'], format='%Y%m%d')
        
        if year != '':
            # We take the subset corresponding to the year of interest
            delegated_df = delegated_df[delegated_df['date'].map(lambda x: x.year) == year]
        
        if delegated_df.empty:
            return pd.DataFrame()
            
        if DEBUG:
            asn_subset = delegated_df[delegated_df['resource_type']=='asn']
            ipv4_subset = delegated_df[delegated_df['resource_type']=='ipv4']
            delegated_df = pd.concat([asn_subset.head(n=30),ipv4_subset.head(n=30)])
        
        if INCREMENTAL:
            initial_date = final_existing_date
            delegated_df = delegated_df[delegated_df['date'] >= self.initial_date]
    
        else:
            initial_date = min(delegated_df['date'])
    
        final_date = max(delegated_df['date'])
        
        self.dates_range = pd.date_range(start=initial_date, end=final_date, freq='D')
        self.dates_range = self.dates_range.strftime('%Y%m%d')
            
        delegated_df.ix[pd.isnull(delegated_df.cc), 'cc'] = 'XX'
        self.CCs = list(set(delegated_df['cc'].values))    
    
        country_regions = dict()
        
        with open('./Collections.txt', 'r') as coll_file:
            for line in coll_file:
                cc = line.split(',')[1]
                if cc in self.CCs:
                    try:
                        region = line.split('001 World,')[1].split(',')[0][4:]
                        if region not in self.AP_regions:
                            country_regions[cc] = 'Reg_Out of APNIC region'
                        else:
                            country_regions[cc] = 'Reg_%s' % region
                    except IndexError:
                            country_regions[cc] = 'NA'                        
      
    
        country_regions['AP'] = 'AP Region'
        country_regions['XX'] = 'NA'
        
        areas = ['All'] + self.CCs + list(set(country_regions.values()))
        
        delegated_df.ix[pd.isnull(delegated_df.opaque_id), 'opaque_id'] = 'NA'
        self.orgs = list(set(delegated_df['opaque_id'].values))
        self.orgs.extend(['All'])
        
        for o in self.orgs:
            if o == 'All':
                self.orgs_areas[o] = areas
            elif o == 'NA':
                self.orgs_areas[o] = ['All', 'XX']
            else:
                org_countries = list(set(delegated_df[delegated_df['opaque_id'] == o]['cc']))
    
                org_areas = []
                for country in org_countries:
                    if country in country_regions.keys():
                        org_areas.extend([country_regions[country]])
                    else:
                        print 'Unknown CC: %s' % country
                self.orgs_areas[o] = ['All'] + org_countries + org_areas
        
        delegated_df['region'] = delegated_df['cc'].apply(lambda c: country_regions[c])
        
        asn_subset = delegated_df[delegated_df['resource_type']=='asn']
        alloc_asn_subset = asn_subset[asn_subset['status'] == 'allocated']
        indexes_alloc_asns = alloc_asn_subset.index.tolist()
        delegated_df.ix[indexes_alloc_asns, 'status'] = 'alloc-16bits'
        indexes_32bits = alloc_asn_subset.loc[pd.to_numeric(asn_subset['initial_resource']) > 65535,:].index.tolist()
        delegated_df.ix[indexes_32bits, 'status'] = 'alloc-32bits'
        
        self.delegated_df = delegated_df
