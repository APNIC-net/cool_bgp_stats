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
import pytricia


class DelegatedHandler:

    AP_regions = ['Eastern Asia', 'Oceania', 'Southern Asia', 'South-Eastern Asia']

    res_types = []
    status_asn = []
    status_ip = []
 
    summary_records = pd.DataFrame()
    delegated_df = pd.DataFrame()    
    
    dates_range = ''
    
    CCs = []
    orgs = []
    orgs_areas = dict()

    def __init__(self, DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year, month, day):
        
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
        
        self.status_asn = ['All', 'alloc-32bits', 'alloc-16bits']
        self.status_ip = ['All', 'allocated', 'assigned']
        
        self.getAndTidyData(DEBUG, EXTENDED, download_url, del_file, col_names, INCREMENTAL, final_existing_date, year, month, day)
        sys.stderr.write("DelegatedHandler instantiated successfully!\n")
    
    def getAndTidyData(self, DEBUG, EXTENDED, download_url, del_file, col_names, INCREMENTAL, final_existing_date, year, month, day):
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
        
        # Rows for available and reserved space are filtered out as we are not
        # interested in generating stats about this space.
        delegated_df = delegated_df[delegated_df['status'] != 'available']
        delegated_df = delegated_df[delegated_df['status'] != 'reserved']
        
        delegated_df['date'] = pd.to_datetime(delegated_df['date'], format='%Y%m%d')
        
        if year != '':
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
            asn_subset = delegated_df[delegated_df['resource_type']=='asn']
            ipv4_subset = delegated_df[delegated_df['resource_type']=='ipv4']
            ipv6_subset = delegated_df[delegated_df['resource_type']=='ipv6']
            delegated_df = pd.concat([asn_subset.head(n=30),ipv4_subset.head(n=30), ipv6_subset.head(n=30)])
        
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
        
    # This function returns a DataFrame with all the blocks delegated to an organization
    # and all these delegated blocks summarized as much as possible
    def getDelAndAggrNetworks(self):
        
        self.delegated_df = self.delegated_df.reset_index()
        
        ipv4_cidr_del_df = pd.DataFrame(columns=self.delegated_df.columns)
        
        # For IPv4, the 'count' column includes the number of IP addresses delegated
        # but it not necessarily corresponds to a CIDR block.
        # Therefore we convert each row to the corresponding CIDR block or blocks,
        # now using the 'count' column to save the prefix length instead of the number of IPs.
        for index, row in self.delegated_df[self.delegated_df['resource_type'] == 'ipv4'].iterrows():
            initial_ip = ipaddress.ip_address(unicode(row['initial_resource'], "utf-8"))
            count = int(row['count'])
            final_ip = initial_ip + count - 1
            
            cidr_networks = [ipaddr for ipaddr in ipaddress.summarize_address_range(\
                                ipaddress.IPv4Address(initial_ip),\
                                ipaddress.IPv4Address(final_ip))]
            
            for net in cidr_networks:
                ipv4_cidr_del_df.loc[ipv4_cidr_del_df.shape[0]] = [row['index'],\
                                                                    row['registry'],\
                                                                    row['cc'],\
                                                                    row['resource_type'],\
                                                                    str(net.network_address),\
                                                                    int(net.prefixlen),\
                                                                    row['date'],\
                                                                    '%s_cidr' % row['status'],\
                                                                    row['opaque_id'],\
                                                                    row['region']]       
        
        orgs_aggr_networks = pd.DataFrame(columns=['opaque_id',\
                                                    'cc',\
                                                    'region',\
                                                    'ip_block',\
                                                    'aggregated',\
                                                    'visibility',\
                                                    'deaggregation',\
                                                    'multiple_originASes'])
                                                    
        ipv6_subset = self.delegated_df[self.delegated_df['resource_type'] == 'ipv6']

        # Now we have both IPv4 and IPv6 delegations in CIDR format
        # we put them together in a single DataFrame
        ip_subset = pd.concat([ipv4_cidr_del_df, ipv6_subset])

        # We group th DataFrame by organization    
        orgs_groups = ip_subset.groupby(ip_subset['opaque_id'])
        
        # For each organization that received at least one delegation of an IP block
        for org in list(set(ip_subset['opaque_id'])):
            # we get all the rows corresponding to that org
            org_subset = orgs_groups.get_group(org)
    
            del_networks_list = []
            del_networks_info = pytricia.PyTricia()
    
            for index, row in org_subset.iterrows():
                ip_block = u'%s/%s' % (row['initial_resource'], int(row['count']))
                cc = row['cc']
                region = row['region']
                # We save each delegated block with its country and region to a PyTricia
                del_networks_info[str(ip_block)] = {'cc':cc, 'region':region}
                
                # and insert it into the DataFrame we will return.
                orgs_aggr_networks.loc[orgs_aggr_networks.shape[0]] = [org, cc, region, str(ip_block), False, -1, -1, False]
                # We also add the block to a list in order to summarize the delegations                
                del_networks_list.append(ipaddress.ip_network(ip_block))

            # We summarize all the delegated blocks as much as possible         
            aggregated_networks = [ipaddr for ipaddr in ipaddress.collapse_addresses(del_networks_list)]
            
            # For each aggregated block 
            for aggr_net in aggregated_networks:
                # I check whether the delegated blocks that generated the summarization
                # were all delegated to the same country and region
                ccs = set()
                regions = set()
                for del_block in del_networks_list:
                    del_block_str = str(del_block)
                    if aggr_net.supernet_of(del_block):
                        ccs.add(del_networks_info[del_block_str]['cc'])
                        regions.add(del_networks_info[del_block_str]['region'])
                orgs_aggr_networks.loc[orgs_aggr_networks.shape[0]] = [org, list(ccs), list(regions), str(aggr_net), True, -1, -1, False]
        
        return orgs_aggr_networks
        
    # This function takes a subset of delegated_df for asns
    # and expands it in order to have one ASN per line
    def getExpandedASNsDF(self):
        asn_subset = self.delegated_df[self.delegated_df['resource_type'] == 'asn']
        expanded_df = pd.DataFrame(columns=asn_subset.columns)
        
        # We scan the asn subset row by row
        for index, row in asn_subset.iterrows():
            # If the value in the 'count' column is > 1
            # more than one ASN was allocated
            # so we have to expand this row into 'count' rows 
            if row['count'] > 1:
                first_asn = int(row['initial_resource'])
                allocated_asns = range(first_asn, first_asn + int(row['count']))
                
                for asn in allocated_asns:
                    expanded_df.loc[expanded_df.shape[0]] = [row['index'],\
                                                                row['registry'],\
                                                                row['cc'],\
                                                                row['resource_type'],\
                                                                str(asn),\
                                                                1.0,\
                                                                row['date'],\
                                                                row['status'],\
                                                                row['opaque_id'],\
                                                                row['region']]

            # If count is 1 we just add the row to the expanded DataFrame
            else:
                expanded_df.loc[expanded_df.shape[0]] = row
            
            return expanded_df
            
