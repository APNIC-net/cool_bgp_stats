# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:12:42 2017

@author: sofiasilva
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
import radix
import pickle
import urllib2, json
from difflib import SequenceMatcher
import subprocess

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
        prefixes_data_file = './PrefixesData.pkl'
        ases_data_file = './ASesData.pkl'
        
        #        nirs = {'JPNIC':'JP', 'IDNIC':'ID', 'CNNIC':'CN', 'IRINN':'IN', 'KRNIC':'KR', 'TWNIC':'TW', 'VNNIC':'VN'}
    
        nirs_dict = {'JP':{'nir_names':['JPNIC', 'Japan Network Information Center'], 'opaque_id':'A91A7381', 'address': 'Urbannet-Kanda Bldg 4F  3-6-2 Uchi-Kanda  Chiyoda-ku  Tokyo 101-0047 Japan', 'prefix_example':'1.0.64.0/18', 'asn_example':'18144'},
                    'ID':{'nir_names':['IDNIC'], 'opaque_id':'A9186214', 'address': 'Cyber Building 11th Floor', 'prefix_example':'103.15.226.0/24', 'asn_example':'136052'},
                    'CN':{'nir_names':['CNNIC'], 'nir_desc':'', 'opaque_id':'A9162E3D', 'address':'Beijing, China', 'prefix_example':'45.121.52.0/22', 'asn_example':'63530'},
                    'IN':{'nir_names':['IRINN'], 'nir_desc':'', 'opaque_id':'A918EDB2', 'address': 'National Internet Exchange of India,Flat no. 6B,Uppals M6 Plaza', 'prefix_example':'103.52.223.0/24', 'asn_example':'133961'},
                    'KR':{'nir_names':['KRNIC'], 'nir_desc':'', 'opaque_id':'A9149F3E', 'address': '135 Jungdae-ro Songpa-gu Seoul, Seoul Songpa-gu Jungdae-ro 135', 'prefix_example':'211.190.231.0/24', 'asn_example':'38660'},
                    'TW':{'nir_names':['TWNIC'], 'nir_desc':'', 'opaque_id':'A91BDB29', 'address': 'Taiwan Network Information Center, 4F-2, No. 9 Sec. 2, Roosevelt Rd., Taipei, Taiwan, 100', 'prefix_example':'43.255.12.0/22', 'asn_example':'131584'},
                    'VN':{'nir_names':['VNNIC'], 'nir_desc':'', 'opaque_id':'A91A560A', 'address': 'Vietnam Internet Network Information Center, 18 Nguyen Du, Hai Ba Trung, Hanoi', 'prefix_example':'103.205.100.0/22', 'asn_example':'135900'}}                    
                
        # whois -h whois.nic.ad.jp "AS 18144"/e
        # whois -h whois.nic.ad.jp "1.0.64.0/18"/e
        # Buscar Organization
        
        # whois.irinn.in NO ACEPTA CONEXIONES EN EL PUERTO 43. Solo por web!!
        # De todas formas, la info que ofrece es la misma que está en whois.apnic.net
        
        # whois -h whois.kisa.or.kr AS38660
        # 'AS Name            :', 'Name               :' y 'Address            :'
        # 'Zip Code           : ', 'Phone              :', 'E-Mail             :'
        # whois -h whois.kisa.or.kr 211.190.231.0
        # 'Organization Name  :', 'Service Name       :', 'Address            :'
        # 'Zip Code           : ', 'Phone              :', 'E-Mail             :'
        # la info que ofrece es la misma que está en whois.apnic.net
        
        # whois -h whois.twnic.net.tw 218.187.128.0
#           Netname: APOL-NET
#           Netblock: 218.187.128.0/20
#        
#           Administrator contact:
#              adm@aptg.com.tw
#        
#           Technical contact:
#              spam@aptg.com.tw
        # Solo funciona con IPv4 (ni con IPv6 ni con ASN)
        # De todas formas la info que ofrece es la misma que está en whois.apnic.net
        
        if os.path.exists(prefixes_data_file):
            prefixesData_radix = pickle.load(open(prefixes_data_file, "rb"))
        else:
            prefixesData_radix = radix.Radix()
        
        if os.path.exists(ases_data_file):
            asesData_dict = pickle.load(open(ases_data_file, "rb"))
        else:
            asesData_dict = dict()
        
        pref_node = prefixesData_radix.search_exact(prefix)
        
        if pref_node is not None:
            pref_org_data = pref_node.data['org_data']
            pref_country = pref_node.data['country']
        else:
            pref_node = prefixesData_radix.add(prefix)
            
            url_ip = '{}ip/{}'.format(rdap, prefix)
            r = urllib2.urlopen(url_ip)
            text = r.read()
            pref_obj = json.loads(text) 

            pref_country = pref_obj['country']
            
            pref_org_data = set()

            pref_name = pref_obj['name']
            
            if pref_country in nirs_dict:
                nir_name_in_pref_name = False
                for nir_name in nirs_dict[pref_country]['nir_names']:
                    if nir_name in pref_name:
                        nir_name_in_pref_name = True
                        break
            if not nir_name_in_pref_name or pref_country not in nirs_dict:
                pref_org_data.add(pref_name)
                
            for remark in pref_obj['remarks']:
                for desc in remark['description']:
                    if pref_country in nirs_dict and\
                        desc not in nirs_dict[pref_country]['address']:
                            nir_name_in_desc = False
                            for nir_name in nirs_dict[pref_country]['nir_names']:
                                if nir_name in desc:
                                    nir_name_in_desc = True
                                    break
                    if not nir_name_in_desc or pref_country not in nirs_dict:
                        pref_org_data.add(desc)
            
            for ent in pref_obj['entities']:
                if 'vcardArray' in ent:
                    if 'label' in ent['vcardArray'][1][3][1]:
                        address = ent['vcardArray'][1][3][1]['label'].replace('\\n', ', ')
                        if pref_country in nirs_dict and address not in nirs_dict[pref_country]['address'] and\
                            OrgHeuristics.similar(address, nirs_dict[pref_country]['address']) < 0.5 or\
                            pref_country not in nirs_dict:
                            pref_org_data.add(address)
            
            whois = False
            for data in pref_org_data:
                if 'whois.nic.ad.jp' in data:
                    whois = True
                    break
            
            if whois:
                cmd =  'whois -h whois.nic.ad.jp "%s"/e' % prefix
                out = subprocess.check_output(cmd, shell=True).split('\n')
                for line in out:
                    if 'Name' in line or 'Organization' in line or '@' in line:
                        pref_org_data.add(line.split(']')[1].lstrip())
            
            pref_node.data['country'] = pref_country
            pref_node.data['org_data'] = pref_org_data
            
            with open(prefixes_data_file, 'wb') as f:
                pickle.dump(prefixesData_radix, f, pickle.HIGHEST_PROTOCOL)

        if asn in asesData_dict:
            asn_org_data = asesData_dict[asn]['org_data']
            asn_country = asesData_dict[asn]['country']
        else:
            asesData_dict[asn] = dict()
            
            url_asn = '{}autnum/{}'.format(rdap, asn)
            r = urllib2.urlopen(url_asn)
            text = r.read()
            asn_obj = json.loads(text)
           
            asn_country = asn_obj['country']
            
            asn_org_data = set()
            
            asn_name = asn_obj['name']
            
            if asn_country in nirs_dict:
                nir_name_in_asn_name = False
                for nir_name in nirs_dict[asn_country]['nir_names']:
                    if nir_name in asn_name:
                        nir_name_in_asn_name = True
                        break
            if not nir_name_in_asn_name or asn_country not in nirs_dict:
                asn_org_data.add(asn_name)

            for remark in asn_obj['remarks']:
                for desc in remark['description']:
                    if asn_country in nirs_dict and\
                        desc not in nirs_dict[asn_country]['address']:
                            nir_name_in_desc = False
                            for nir_name in nirs_dict[asn_country]['nir_names']:
                                if nir_name in desc:
                                    nir_name_in_desc = True
                                    break
                    if not nir_name_in_desc or asn_country not in nirs_dict:
                        asn_org_data.add(desc)
            
            for ent in asn_obj['entities']:
                if 'vcardArray' in ent:
                    if 'label' in ent['vcardArray'][1][3][1]:
                        address = ent['vcardArray'][1][3][1]['label'].replace('\\n', ', ')
                        if asn_country in nirs_dict and address not in nirs_dict[asn_country]['address'] and\
                            OrgHeuristics.similar(address, nirs_dict[asn_country]['address']) < 0.5 or\
                            asn_country not in nirs_dict:
                            asn_org_data.add(address)
            
            whois = False
            for data in asn_org_data:
                if 'whois.nic.ad.jp' in data:
                    whois = True
                    break
                
            if whois:
                cmd =  'whois -h whois.nic.ad.jp "AS %s"/e' % asn
                out = subprocess.check_output(cmd, shell=True).split('\n')
                for line in out:
                    if 'Name' in line or 'Organization' in line:
                        asn_org_data.add(line.split(']')[1].lstrip())     

            asesData_dict[asn]['country'] = asn_country
            asesData_dict[asn]['org_data'] = asn_org_data
            
            with open(ases_data_file, 'wb') as f:
                pickle.dump(asesData_dict, f, pickle.HIGHEST_PROTOCOL)

        if pref_country != asn_country:
            return False
        
        for pref_data in pref_org_data:
            for asn_data in asn_org_data:
                if OrgHeuristics.similar(pref_data, asn_data) > 0.5:
                    return True
    
        return False