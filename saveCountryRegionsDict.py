# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:53:41 2017

@author: sofiasilva
"""
import os
os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
import pickle

CCs = ['BD', 'WF', 'FR', 'MN', 'VG', 'BN', 'DE', 'JP', 'BT', 'HK', 'WS', 'BR', 'BS', 'CO', 'FJ', 'FM', 'BZ', 'TL', 'NL', 'PW', 'LA', 'TV', 'TW', 'NC', 'TR', 'LK', 'NF', 'TO', 'NZ', 'PA', 'PF', 'PG', 'TH', 'NP', 'PK', 'PH', 'NU', 'TK', 'CK', 'GU', 'IR', 'AE', 'CN', 'AF', 'CA', 'NR', 'IM', 'ZA', 'VN', 'AP', 'AS', 'AU', 'VU', 'IO', 'IN', 'NO', 'KP', 'ID', 'ES', 'KE', 'MM', 'MY', 'MO', 'KH', 'MH', 'US', 'MU', 'KR', 'MV', 'MP', 'SC', 'SB', 'SA', 'SG', 'KI', 'SE', 'GB']
AP_regions = ['Eastern Asia', 'Oceania', 'Southern Asia', 'South-Eastern Asia']

country_regions = dict()

with open('./Collections.txt', 'r') as coll_file:
    for line in coll_file:
        cc = line.split(',')[1]
        if cc in CCs:
            try:
                region = line.split('001 World,')[1].split(',')[0][4:]
                if region not in AP_regions:
                    country_regions[cc] = 'Reg_Out of APNIC region'
                else:
                    country_regions[cc] = 'Reg_%s' % region
            except IndexError:
                    country_regions[cc] = 'NA'                        
      
    
    country_regions['AP'] = 'AP Region'
    country_regions['XX'] = 'NA'

with open('./CountryRegions.pkl', 'wb') as f:
    pickle.dump(country_regions, f, pickle.HIGHEST_PROTOCOL)