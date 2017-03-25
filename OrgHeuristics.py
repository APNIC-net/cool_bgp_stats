# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:12:42 2017

@author: sofiasilva
"""
import urllib2, json
from difflib import SequenceMatcher

#APJII (Indonesia) (ID)
#CNNIC (China) (CN)
#IRINN (India) (IN)
#JPNIC (Japan) (JP)
#KISA (Republic of Korea) (KR)
#TWNIC (Taiwan) (TW)
#VNNIC (Viet Nam) (VN)

# NIRs with WHOIS: IRINN,JPNIC,KRNIC,TWNIC

class OrgHeuristics:
    @staticmethod
    def similar(a, b):
        return round(SequenceMatcher(None, a.lower(), b.lower()).ratio(), 1)

    @staticmethod
    def checkIfSameOrg(prefix, asn):
        rdap = 'http://rdap.apnic.net/'
    
#        nirs = {'JPNIC':'JP', 'IDNIC':'ID', 'CNNIC':'CN', 'IRINN':'IN', 'KRNIC':'KR', 'TWNIC':'TW', 'VNNIC':'VN'}
    
        nirs_dict = {'JP':{'nir_name':'JPNIC', 'opaque_id':'A91A7381', 'address': 'Urbannet-Kanda Bldg 4F, 3-6-2 Uchi-Kanda, Chiyoda-ku, Tokyo 101-0047,Japan', 'prefix_example':'1.0.64.0/18', 'asn_example':'18144'},
                    'ID':{'nir_name':'IDNIC', 'opaque_id':'A9186214', 'address': 'Cyber Building 11th Floor', 'prefix_example':'103.15.226.0/24', 'asn_example':'136052'},
                    'CN':{'nir_name': 'CNNIC', 'opaque_id':'A9162E3D', 'address':'Beijing, China', 'prefix_example':'45.121.52.0/22', 'asn_example':'63530'},
                    'IN':{'nir_name':'IRINN', 'opaque_id':'A918EDB2', 'address': 'National Internet Exchange of India,Flat no. 6B,Uppals M6 Plaza', 'prefix_example':'103.52.223.0/24', 'asn_example':'133961'},
                    'KR':{'nir_name':'KRNIC', 'opaque_id':'A9149F3E', 'address': '135 Jungdae-ro Songpa-gu Seoul, Seoul Songpa-gu Jungdae-ro 135', 'prefix_example':'211.190.231.0/24', 'asn_example':'38660'},
                    'TW':{'nir_name':'TWNIC', 'opaque_id':'A91BDB29', 'address': 'Taiwan Network Information Center, 4F-2, No. 9 Sec. 2, Roosevelt Rd., Taipei, Taiwan, 100', 'prefix_example':'43.255.12.0/22', 'asn_example':'131584'},
                    'VN':{'nir_name':'VNNIC', 'opaque_id':'A91A560A', 'address': 'Vietnam Internet Network Information Center, 18 Nguyen Du, Hai Ba Trung, Hanoi', 'prefix_example':'103.205.100.0/22', 'asn_example':'135900'}}                    
            
        url_ip = '{}ip/{}'.format(rdap, prefix)
        r = urllib2.urlopen(url_ip)
        text = r.read()
        pref_obj = json.loads(text)
        
        url_asn = '{}autnum/{}'.format(rdap, asn)
        r = urllib2.urlopen(url_asn)
        text = r.read()
        asn_obj = json.loads(text)
        
        pref_org_possible_names = [pref_obj['name'], pref_obj['remarks'][0]['description'][0]]
        asn_org_possible_names = [asn_obj['name'], asn_obj['remarks'][0]['description'][0]]
    
        pref_country = pref_obj['country']
        asn_country = asn_obj['country']
        
        if pref_country != asn_country:
            return False
        else:
            country = pref_country
        
        for name in pref_org_possible_names:
            if country in nirs_dict and nirs_dict[country]['nir_name'] not in name:
                for asn_name in asn_org_possible_names:
                    if country in nirs_dict and\
                        nirs_dict[country]['nir_name'] not in asn_name or\
                        country not in nirs_dict:
                            if OrgHeuristics.similar(name, asn_name) > 0.5:
                                return True
            
        
        pref_org_addresses = set()
        for ent in pref_obj['entities']:
            if 'vcardArray' in ent:
                if 'label' in ent['vcardArray'][1][3][1]:
                    address = ent['vcardArray'][1][3][1]['label'].replace('\\n', ', ')
                    if country in nirs_dict and address not in nirs_dict[country]['address'] and\
                        OrgHeuristics.similar(address, nirs_dict[pref_country]['address']) < 0.5 or\
                        country not in nirs_dict:
                        pref_org_addresses.add(address)
        
        asn_org_addresses = set()
        for ent in asn_obj['entities']:
            if 'vcardArray' in ent:
                if 'label' in ent['vcardArray'][1][3][1]:
                    address = ent['vcardArray'][1][3][1]['label'].replace('\\n', ', ')
                    if country in nirs_dict and address not in nirs_dict[country]['address'] and\
                        OrgHeuristics.similar(address, nirs_dict[asn_country]['address']) < 0.5 or\
                        country not in nirs_dict:
                        asn_org_addresses.add(address)
        
        for pref_address in pref_org_addresses:
            if country in nirs_dict and pref_address not in nirs_dict[country]['address']:
                for asn_address in asn_org_addresses:
                    if country in nirs_dict and asn_address not in nirs_dict[country]['address'] or\
                        country not in nirs_dict:
                        if OrgHeuristics.similar(pref_address, asn_address) > 0.5:
                            return True
    
        return False