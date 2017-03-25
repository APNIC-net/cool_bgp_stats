#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

"""
Created on Fri Feb  3 11:09:35 2017

@author: sofiasilva
"""
import sys
import pandas as pd
from get_file import get_file
import ipaddress
import numpy as np
import radix
import pickle
import datetime


class DelegatedHandler:

    delegated_df = pd.DataFrame()

    def __init__(self, DEBUG, EXTENDED, del_file, date, UNTIL, INCREMENTAL, final_existing_date):
         
        if EXTENDED:
            download_url = 'ftp://ftp.apnic.net/pub/stats/apnic/delegated-apnic-extended-latest'
            col_names = [
                            'registry',
                            'cc',
                            'resource_type',
                            'initial_resource',
                            'count/prefLen',
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
                            'count/prefLen',
                            'date',
                            'status'
                        ]
        
        self.getAndTidyData(DEBUG, EXTENDED, download_url, del_file, col_names,\
                                date, UNTIL, INCREMENTAL, final_existing_date)
        sys.stderr.write("DelegatedHandler instantiated and loaded successfully!\n")
    
    def getAndTidyData(self, DEBUG, EXTENDED, download_url, del_file, col_names,\
                        date, UNTIL, INCREMENTAL, final_existing_date):
                            
        summary_records = pd.DataFrame()
        res_types = ['asn', 'ipv4', 'ipv6']
        
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
                    
    
        for i in range(4):
            if i == 0:
                summary_records.at[i, 'Type'] = 'All'
                summary_records.at[i, 'count'] =\
                                    int(delegated_df.loc[i, 'initial_resource'])
            else:
                summary_records.at[i, 'Type'] = res_types[i-1]
                summary_records.at[i, 'count'] =\
                                    int(delegated_df.loc[i, 'count/prefLen'])
    
    
        total_rows = delegated_df.shape[0]
        delegated_df =\
            delegated_df[int(total_rows-summary_records.loc[0, 'count']):total_rows]
           
        # Just to verify
        num_rows = len(delegated_df)
        if not num_rows == int(summary_records[summary_records['Type'] == 'All']['count']):
            print 'THERE\'S SOMETHING WRONG!'
    
        for r in res_types:
            total = len(delegated_df[delegated_df['resource_type'] == r])
            if not total == int(summary_records[summary_records['Type'] == r]['count']):
                print 'THERE\'S SOMETHING WRONG WITH THE NUMBER OF %s' % r
        
        # Rows for available and reserved space are filtered out as we are not
        # interested in generating stats about this space.
        delegated_df = delegated_df[delegated_df['status'] != 'available']
        delegated_df = delegated_df[delegated_df['status'] != 'reserved']
        
        delegated_df['date'] = pd.to_datetime(delegated_df['date'], format='%Y%m%d')
        
        # We filter out the delegations made today as there could be missing delegations
        # Delegations for today will be considered tomorrow :)
        delegated_df = delegated_df[delegated_df['date'] < datetime.date.today()]
            
        if date != '':  
            try:
                year = int(date[0:4])
            except ValueError:
                year = ''
            try:
                month = int(date[4:6])
            except ValueError:
                month = ''
            try:
                day = int(date[6:8])
            except ValueError:
                day = ''
        else:
            year = ''
            month = ''
            day = ''
        
        if UNTIL:
            delegated_df = delegated_df[delegated_df['date'] <= pd.to_datetime(date)]
            
        elif year != '':
            # We take the subset corresponding to the year of interest
            delegated_df = delegated_df[delegated_df['date'].map(lambda x: x.year) == year]
            
            if month != '':
                # We take the subset corresponding to the month of interest
                delegated_df = delegated_df[delegated_df['date'].map(lambda x: x.month) == month]
    
            if day != '':
                # We take the subset corresponding to the day of interest
                delegated_df = delegated_df[delegated_df['date'].map(lambda x: x.day) == day]        
            
        if delegated_df.empty:
            return pd.DataFrame()
            
        if DEBUG:
            specific_subset = delegated_df[(delegated_df['initial_resource'] == '2001:df2:ce00::') | (delegated_df['initial_resource'] == '2001:df2:ca00::') | (delegated_df['initial_resource'] == '2001:df3:5c00::')]
            asn_subset = delegated_df[delegated_df['resource_type']=='asn']
            ipv4_subset = delegated_df[delegated_df['resource_type']=='ipv4']
            ipv6_subset = delegated_df[delegated_df['resource_type']=='ipv6']
            delegated_df = pd.concat([asn_subset.head(n=30),ipv4_subset.head(n=30), ipv6_subset.head(n=30), specific_subset])
        
        if INCREMENTAL:
            delegated_df = delegated_df[delegated_df['date'] > pd.to_datetime(final_existing_date)]
            if delegated_df.empty:
                return pd.DataFrame()
            
        delegated_df.ix[pd.isnull(delegated_df.cc), 'cc'] = 'XX'
    
        country_regions = pickle.load(open('./CountryRegions.pkl', "rb"))
                
        if EXTENDED:        
            delegated_df.ix[pd.isnull(delegated_df.opaque_id), 'opaque_id'] = 'NA'
            orgs = list(set(delegated_df['opaque_id'].values))
            orgs_areas = dict()
            
            for o in orgs:
                if o == 'NA':
                    orgs_areas[o] = ['XX']
                else:
                    org_countries = list(set(delegated_df[delegated_df['opaque_id'] == o]['cc']))
        
                    org_areas = []
                    for country in org_countries:
                        if country in country_regions.keys():
                            org_areas.extend([country_regions[country]])
                        else:
                            print 'Unknown CC: %s' % country
                    orgs_areas[o] = ['All'] + org_countries + org_areas
        
        delegated_df['region'] = delegated_df['cc'].apply(lambda c: country_regions[c])
        
        asn_subset = delegated_df[delegated_df['resource_type']=='asn']
        alloc_asn_subset = asn_subset[asn_subset['status'] == 'allocated']
        indexes_alloc_asns = alloc_asn_subset.index.tolist()
        delegated_df.ix[indexes_alloc_asns, 'status'] = 'alloc-16bits'
        indexes_32bits = alloc_asn_subset.loc[pd.to_numeric(asn_subset['initial_resource']) > 65535,:].index.tolist()
        delegated_df.ix[indexes_32bits, 'status'] = 'alloc-32bits'
        
        delegated_df['OriginalIndex'] = delegated_df.index
        
        ipv4_cidr_del_df = pd.DataFrame(columns=delegated_df.columns)
        
        # For IPv4, the 'count/prefLen' column includes the number of IP addresses delegated
        # but it not necessarily corresponds to a CIDR block.
        # Therefore we convert each row to the corresponding CIDR block or blocks,
        # now using the 'count' column to save the prefix length instead of the number of IPs.
        for index, row in delegated_df[delegated_df['resource_type'] == 'ipv4'].iterrows():
            initial_ip = ipaddress.ip_address(unicode(row['initial_resource'], "utf-8"))
            count = int(row['count/prefLen'])
            final_ip = initial_ip + count - 1
            
            cidr_networks = [ipaddr for ipaddr in ipaddress.summarize_address_range(\
                                ipaddress.IPv4Address(initial_ip),\
                                ipaddress.IPv4Address(final_ip))]
            
            for net in cidr_networks:
                ipv4_cidr_del_df.loc[ipv4_cidr_del_df.shape[0]] = [row['registry'],\
                                                                    row['cc'],\
                                                                    row['resource_type'],\
                                                                    str(net.network_address),\
                                                                    int(net.prefixlen),\
                                                                    row['date'],\
                                                                    row['status'],\
                                                                    row['opaque_id'],\
                                                                    row['region'],
                                                                    row['OriginalIndex']]
                                                                    
        delegated_df = pd.concat([ipv4_cidr_del_df, delegated_df[\
                                (delegated_df['resource_type'] == 'ipv6') |\
                                (delegated_df['resource_type'] == 'asn')]])
        
        # We change the format for floats to avoid the values of the count column
        # having .0
        pd.options.display.float_format = '{:,.0f}'.format
        
        delegated_df['ResourceCount'] = np.nan        
        delegated_df['SpaceCount'] = np.nan
        delegated_df.loc[delegated_df['resource_type'] == 'ipv6', 'ResourceCount'] =\
                                                pow(2, 56 - delegated_df['count/prefLen'])
        delegated_df.loc[delegated_df['resource_type'] == 'ipv6', 'SpaceCount'] =\
                                                pow(2, 48 - delegated_df['count/prefLen'])
        delegated_df.loc[delegated_df['resource_type'] == 'ipv4', 'ResourceCount'] =\
                                                pow(2, 32 - delegated_df['count/prefLen'])                                                
        delegated_df.loc[delegated_df['resource_type'] == 'ipv4', 'SpaceCount'] =\
                                                pow(2, 24 - delegated_df['count/prefLen'])
        delegated_df.loc[delegated_df['resource_type'] == 'asn', 'ResourceCount'] =\
                                                            delegated_df['count/prefLen']
        
        self.delegated_df = delegated_df
        
    # This function returns a DataFrame with all the blocks delegated and all
    # the blocks delegated to an organization summarized as much as possible
    def getDelAndAggrNetworks(self):
        
        orgs_aggr_networks = pd.DataFrame(columns= ['prefix', 'aggregated', 'del_date',\
                                                    'opaque_id', 'cc', 'region',])
                                                    
        ip_subset = self.delegated_df[\
                    (self.delegated_df['resource_type'] == 'ipv4') |\
                    (self.delegated_df['resource_type'] == 'ipv6')]

        # We group th DataFrame by organization    
        orgs_groups = ip_subset.groupby(ip_subset['opaque_id'])
        
        # For each organization that received at least one delegation of an IP block
        for org, org_subset in orgs_groups:    
            del_networks_list = []
    
            for index, row in org_subset.iterrows():                
                # We save the info about each delegated block 
                # into the DataFrame we will return
                # with False in the aggregated column
                ip_block = '%s/%s' % (row['initial_resource'], int(row['count/prefLen']))              

                orgs_aggr_networks.loc[orgs_aggr_networks.shape[0]] = [ip_block,\
                                                                    False, row['date'],\
                                                                    row['opaque_id'],\
                                                                    row['cc'],\
                                                                    row['region']] 
                
                # We also add the block to a list in order to summarize the delegations                
                del_networks_list.append(ipaddress.ip_network(unicode(ip_block, 'utf-8')))

            # We summarize all the delegated blocks as much as possible         
            aggregated_networks = [ipaddr for ipaddr in ipaddress.collapse_addresses(del_networks_list)]
                        
            # For each aggregated block 
            for aggr_net in aggregated_networks:
                # If the aggregated network already appears in the list of
                # delegated blocks, we do not need to insert it into the DataFrame
                if aggr_net not in del_networks_list:
                    # I check whether the delegated blocks that generated the summarization
                    # were all delegated to the same country and region
                    ccs = set()
                    regions = set()
                    for del_block in del_networks_list:
                        if aggr_net.supernet_of(del_block):
                            row = ip_subset.ix[ip_subset[(ip_subset['initial_resource'] ==\
                                            str(del_block.network_address)) &\
                                            (ip_subset['count/prefLen'] == del_block.prefixlen)].index[0]]
                            ccs.add(row['cc'])
                            regions.add(row['region'])
                            
                    ccs = list(ccs)
                    if len(ccs) == 1:
                        ccs = ccs[0]
                    
                    regions = list(regions)
                    if len(regions) == 1:
                        regions = regions[0]
                    
                    orgs_aggr_networks.loc[orgs_aggr_networks.shape[0]] = [\
                                                                str(aggr_net),\
                                                                True, '', org,\
                                                                ccs, regions,]
        
        return orgs_aggr_networks
        
    # This function returns a Radix with all the blocks delegated.
    # The data dictionary for each delegated prefix contains all the info about
    # the delegation (delegation date, opaque_id, cc, region, status and
    # resource_type (IPv4 or IPv6))
    def getDelegatedNetworksRadix(self):
                                                            
        ip_subset = self.delegated_df[\
                    (self.delegated_df['resource_type'] == 'ipv4') |\
                    (self.delegated_df['resource_type'] == 'ipv6')]

        delegationsRadix = radix.Radix()
        
        for i in ip_subset.index:
            del_row = ip_subset.ix[i]
            pref_node = delegationsRadix.add(network=del_row['initial_resource'],\
                                                masklen=int(del_row['count/prefLen']))

            pref_node.data['del_date'] = del_row['date'].date()
            pref_node.data['opaque_id'] = del_row['opaque_id']
            pref_node.data['cc'] = del_row['cc']
            pref_node.data['region'] = del_row['region']
            pref_node.data['resource_type'] = del_row['resource_type']
            pref_node.data['status'] = del_row['status']
    
        return delegationsRadix
        
    # This function takes a subset of delegated_df for asns
    # and expands it in order to have one ASN per line
    def getExpandedASNsDF(self):
        asn_subset = self.delegated_df[self.delegated_df['resource_type'] == 'asn']
        expanded_df = pd.DataFrame(columns=asn_subset.columns)
        
        # We scan the asn subset row by row
        for index, row in asn_subset.iterrows():
            # If the value in the 'count/prefLen' column is > 1
            # more than one ASN was allocated
            # so we have to expand this row into 'count/prefLen' rows 
            if row['count/prefLen'] > 1:
                first_asn = int(row['initial_resource'])
                allocated_asns = range(first_asn, first_asn + int(row['count/prefLen']))
                
                for asn in allocated_asns:
                    expanded_df.loc[expanded_df.shape[0]] = [row['registry'],\
                                                                row['cc'],\
                                                                row['resource_type'],\
                                                                str(asn),\
                                                                1,\
                                                                row['date'],\
                                                                row['status'],\
                                                                row['opaque_id'],\
                                                                row['region'],\
                                                                row['OriginalIndex'],\
                                                                1, float(np.nan)]

            # If count/prefLen is 1 we just add the row to the expanded DataFrame
            else:
                expanded_df.loc[expanded_df.shape[0]] = row
            
        return expanded_df

    # Given a prefix this function returns the date in which it was delegated.
    # If the prefix is not in the delegated_df DataFrame None is returned
    def getDelegationDateIPBlock(self, network):
        
        subset = self.delegated_df[\
                (self.delegated_df['initial_resource'] == str(network.network_address)) &\
                (self.delegated_df['count/prefLen'] == network.prefixlen)]
        
        if subset.shape[0] > 0:
            row = self.delegated_df.ix[subset.index[0]]
            return row['date'].to_pydatetime().date()
        else:
            return None
