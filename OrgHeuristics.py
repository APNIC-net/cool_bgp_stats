# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 12:12:42 2017

@author: sofiasilva
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
import sys
import pickle
import radix
from ipaddress import ip_network
from difflib import SequenceMatcher
import subprocess

class OrgHeuristics:
    nirs_dict = {'JP':{
                    'nir_name':'JPNIC',
                    'descr':['JPNIC', 'Japan Network Information Center'],
                    'itemnames':['JPNIC-NET-JP', 'OCN-JPNIC-JP', 'JPNIC-NET-JP-ERX', 'JPNIC-NET-JP-AS-BLOCK', 'JPNIC-2Byte-ASBLOCK-AP', 'JPNIC-JP-ASN-BLOCK', 'JPNIC-ASBLOCK-AP'],
                    'admin/tech':['JNIC1-AP'],
                    'mntner':'MAINT-JPNIC',
                    'irt':'IRT-JPNIC-JP',
                    'whois':'whois.nic.ad.jp',
                    'opaque_id':'A91A7381',
                    'phones': ['81352972311', '81352972312'],
                    'prefix_example':'1.0.64.0/18', 'asn_example':'18144'
                    },
                'ID':{
                    'nir_name':'IDNIC',
                    'descr':['Indonesia Network Information Center', 'Gedung Cyber Lt.3A', 'Jl. Kuningan Barat No.8', 'Jakarta 12710'],
                    'itemnames':['IDNIC-ID', 'IDNIC-AS-ID'],
                    'admin/tech':['IA55-AP', 'IH123-AP'],
                    'mntner':'MNT-APJII-ID',
                    'irt':'IRT-IDNIC-ID',
                    'opaque_id':'A9186214',
                    'phones': ['622152960634', '622152960635'],
                    'prefix_example':'103.15.226.0/24', 'asn_example':'136052'
                    },
                'CN':{
                    'nir_name':'CNNIC',
                    'descr':['China Internet Network Information Center', 'No.4, Zhongguancun No.4 South Street,', 'Haidian District, Beijing', 'P.O.Box: No.6 Branch-box of No.349 Mailbox, Beijing'],
                    'itemnames':['CNNIC', 'CNNIC-CRITICAL-CN', 'CNNIC-AP'],
                    'admin/tech':['MW1-AP', 'IPAS1-AP'],
                    'mntner':'MAINT-CNNIC-AP',
                    'irt':'IRT-CNNIC-CN',
                    'opaque_id':'A9162E3D',
                    'phones': ['861058813000', '861058812666'],
                    'prefix_example':'45.121.52.0/22', 'asn_example':'63530'
                    },
                'IN':{
                    'nir_name':'IRINN',
                    'mntner':'MAINT-IN-IRINN',
                    'itr':'IRT-IRINN-IN',
                    'opaque_id':'A918EDB2',
                    'phones': ['9127866000'],
                    'prefix_example':'103.52.223.0/24', 'asn_example':'133961'
                    },
                'KR':{
                    'nir_name':'KRNIC',
                    'descr':['KRNIC', 'Korea Network Information Center', '************************************************', 'Allocated to KRNIC Member.', 'If you would like to find assignment', 'information in detail please refer to', 'the KRNIC Whois Database at:', '"http://whois.nida.or.kr/english/index.html"'],
                    'itemnames':['KRNIC-KR', 'KRNIC-NET', 'KRNIC-AS-KR'],
                    'remarks':[' ******************************************', 'KRNIC is the National Internet Registry', 'in Korea under APNIC. If you would like to', 'find assignment information in detail', 'please refer to the KRNIC Whois DB'],
                    'admin/tech':['HM127-AP'],
                    'mntner':'MNT-KRNIC-AP',
                    'irt':'IRT-KRNIC-KR',
                    'whois':'whois.nic.or.kr',
                    'opaque_id':'A9149F3E',
                    'phones': ['8224055118', '8224055118'],
                    'prefix_example':'211.190.231.0/24', 'asn_example':'38660'
                    },
                'TW':{
                    'nir_name':'TWNIC',
                    'descr':['Taiwan Network Information Center', '4F-2, No. 9 Sec. 2, Roosevelt Rd.,', 'Taipei, Taiwan, 100'],
                    'itemnames':['TWNIC-TW', 'TWNIC-CRITICAL-TW', 'TWNIC-AS'],
                    'remarks':['This object can only be updated by TWNIC hostmasters.', 'To update this object, please contact TWNIC.'],
                    'admin/tech':['TWA2-AP'],
                    'mntner':'MAINT-TW-TWNIC',
                    'irt':'IRT-TWNIC-AP',
                    'opaque_id':'A91BDB29',
                    'phones': ['886223411313', '886223968832'],
                    'prefix_example':'43.255.12.0/22', 'asn_example':'131584'
                    },
                'VN':{
                    'nir_name':'VNNIC',
                    'descr':['VNNIC', 'Vietnam Internet network Information Centre', 'Vietnam Internet network Information Centre - VNNIC', 'Vietnam Internet network information center (VNNIC)', 'Branch of VNNIC in Ho Chi Minh City', 'Branch of VNNIC in Da Nang City', '18 Nguyen Du street, Hanoi capital, Vietnam', '18 Nguyen Du street, Hai Ba Trung District, Hanoi', '18 Nguyen Du, Hanoi'],
                    'itemnames':['VNNIC-NET', 'VNNIC-AS-VN', 'VNNIC-AS-AP', 'VNNIC-4-BYTE-AS-VN'],
                    'admin/tech':['ITMG1-AP', 'VI2-AP', 'PXD6-AP'],
                    'mntner':'MAINT-VN-VNNIC',
                    'irt':'IRT-VNNIC-AP',
                    'opaque_id':'A91A560A',
                    'phones': ['84435564944'],
                    'prefix_example':'103.205.100.0/22', 'asn_example':'135900'
                    }
                }  

    def __init__(self, files_path):
        self.bulkWHOIS_data = {'inetnum' : {'file' : '{}/apnic.db.inetnum.pkl'.format(files_path)},
                       'inet6num' : {'file' : '{}/apnic.db.inet6num.pkl'.format(files_path)},
                       'aut-num' : {'file' : '{}/apnic.db.aut-num.pkl'.format(files_path)},
                       'mntner' : {'file' : '{}/apnic.db.mntner.pkl'.format(files_path)},
                       'irt' : {'file' : '{}/apnic.db.irt.pkl'.format(files_path)}}
                       
        for item in self.bulkWHOIS_data:
            try:
                self.bulkWHOIS_data[item]['data'] =\
                    pickle.load(open(self.bulkWHOIS_data[item]['file'], "rb"))
            except IOError:
                sys.stderr.write(IOError.message)
                sys.exit()
        
        self.ipv4_prefixes_data_file = '{}/ipv4_prefixes_filtered_WHOISdata.pkl'.format(files_path)
        
        try:
            self.ipv4_prefixes_filtered_data = pickle.load(open(self.ipv4_prefixes_data_file, 'rb'))
        except IOError:
            self.ipv4_prefixes_filtered_data = radix.Radix()

        self.ipv6_prefixes_data_file = '{}/ipv6_prefixes_filtered_WHOISdata.pkl'.format(files_path)
        
        try:
            self.ipv6_prefixes_filtered_data = pickle.load(open(self.ipv6_prefixes_data_file, 'rb'))
        except IOError:
            self.ipv6_prefixes_filtered_data = radix.Radix()

        self.ases_data_file = '{}/ases_filtered_WHOISdata.pkl'.format(files_path)
        
        try:
            self.ases_filtered_data = pickle.load(open(self.ases_data_file, 'rb'))
        except IOError:
            self.ases_filtered_data = dict()
            
        # Counter to keep track of how many times these heurisitics are applied
        # during an excecution of computeRoutingStats so that we can decide
        # whether it is worth applying the heuristics separately from the stats
        # computation.
        self.invokedCounter = 0
        self.totalTimeConsumed = 0
            
            
    @staticmethod
    def similar(a, b):
        return round(SequenceMatcher(None, a.lower(), b.lower()).ratio(), 1)

    def checkIfSameOrg(self, prefix, asn):    
        network = ip_network(unicode(prefix, "utf-8"))
        
        pref_node = None
        filtered_data = None
        
        if network.version == 4:
            item = 'inetnum'
            # I save the filename so that I can dump the filtered data to the
            # pickles file later on
            prefixes_data_file = self.ipv4_prefixes_data_file
            filtered_data = self.ipv4_prefixes_filtered_data
                
        else:
            item = 'inet6num'
            prefixes_data_file = self.ipv6_prefixes_data_file
            filtered_data = self.ipv6_prefixes_filtered_data

        # I check whether I already have filtered data for the prefix
        pref_node = filtered_data.search_exact(prefix)
        
        if pref_node is not None:
            pref_org_data = pref_node.data
        else:
            # If I don't, I look for data about the prefix in the un-filtered
            # structure with data from Bulk WHOIS
            pref_node = self.bulkWHOIS_data[item]['data'].search_best(prefix)

            # If there is data for the prefix coming from the Bulk WHOIS
            if pref_node is not None:
                # I proceed to filter it
                pref_org_data = self.getDataOfInterest(network, True, pref_node.data)
                
                # I add the prefix to the filtered_data Radix
                if prefix not in filtered_data:
                    pref_node = filtered_data.add(prefix)
                    # and copy all the filtered information
                    for key in pref_org_data:
                        pref_node.data[key] = pref_org_data[key]
                
                # Finally, I dump the updated Radix to a pickle file
                with open(prefixes_data_file, 'wb') as f:
                    pickle.dump(filtered_data, f, pickle.HIGHEST_PROTOCOL)
            else:
                print 'This should never happen'
                # These heuristics are applied to prefixes delegated by APNIC
                # or prefixes related to prefixes delegated by APNIC,
                # therefore we should always be able to find info about
                # the delegation in the Bulk WHOIS (for the exact prefix,
                # for a more specific prefix or for a less specific prefix).

        # Similarly, I look for already filtered data about the ASN
        try:
            asn_org_data = self.ases_filtered_data[asn]
        except KeyError:
            asn_org_data = None
        
        # If I don't have already filtered data for the ASN, I look for info
        # about it in the data coming from the Bulk WHOIS
        if asn_org_data is None:
            try:
                asn_dict = self.bulkWHOIS_data['aut-num']['data'][asn]
                # and proceed to filter it
                asn_org_data = self.getDataOfInterest(asn, False, asn_dict)
                # I add the filtered information about the ASN to the dictionary
                # with filtered data about ASNs
                self.ases_filtered_data[asn] = asn_org_data
            
                # and finally dump the updated dictionary into a pickle file
                with open(self.ases_data_file, 'wb') as f:
                    pickle.dump(self.ases_filtered_data, f, pickle.HIGHEST_PROTOCOL)
        
            except KeyError:
                print 'This should not happen'
                # If the ASN was not delegated by APNIC, we should have obtained
                # UNKNOWN in computeRoutingStats when trying to get the opaque id
                # for the ASN, and we would not have called OrgHeuristics.
                # Therefore, for the ASNs we will be working with, we should
                # always find information about the delegation in the Bulk WHOIS.
                
        return self.comparePrefixAndASNData(pref_org_data, asn_org_data)

    @staticmethod
    def comparePrefixAndASNData(self, pref_org_data, asn_org_data):
        matching_score = 0
        
        for key in pref_org_data.keys():
            if key in asn_org_data:
                for pref_item in pref_org_data[key]:
                    for asn_item in asn_org_data[key]:
                        matching_score += float(self.comparePrefASNField(pref_item, asn_item, key))/min(len(asn_org_data[key]), len(pref_org_data[key]))
        
        return (matching_score >= 100)
        
        # TODO ver si no sería mejor meter la comparación dentro del proceso
        # de filtrado, para evitar iterar dos veces sobre la misma info
    
    @staticmethod
    def comparePrefASNField(self, pref_field, asn_field, field_name):
        # TODO Debug and adjust these values
        scoresDict = {'itemnames': {'score': 100, 'similarity_threshold': 0.8},
                      'remarks/descr': {'score': 50, 'similarity_threshold': 0.5},
                      'phones': {'score': 50, 'similarity_threshold': 1},
                      'emails': {'score': 50, 'similarity_threshold': 1},
                      'admin/tech': {'score': 50, 'similarity_threshold': 1},
                      'mntners': {'score': 50, 'similarity_threshold': 1},
                      'irts': {'score': 50, 'similarity_threshold': 1}}
        
        if self.similar(pref_field, asn_field) > scoresDict[field_name]['similarity_threshold']:
            return scoresDict[field_name]['score']
        else:
            return 0
    
    def isNIRName(self, name):
        for nir_country in self.nirs_dict:
            for nir_name in self.nirs_dict[nir_country]['itemnames']:
                if nir_name == name:
                    return True
        return False
    
    def isSimilarToNIRRemarkDescr(self, comment):
        for nir_country in self.nirs_dict:
            if 'remarks' in self.nirs_dict[nir_country]:
                for nir_remark in self.nirs_dict[nir_country]['remarks']: 
                    if self.similar(comment, nir_remark) > 0.5:
                        return True
            if 'descr' in self.nirs_dict[nir_country]:
                for nir_descr in self.nirs_dict[nir_country]['descr']:
                    if self.similar(comment, nir_descr) > 0.5:
                        return True
        return False
        
    def isNIRPhone(self, phone):
        phone = phone.replace('-', '')
        phone = phone.replace('+', '')
        phone = phone.replace(' ', '')
        for nir_country in self.nirs_dict:
            if 'phones' in self.nirs_dict[nir_country]:
                for nir_phone in self.nirs_dict[nir_country]['phones']:
                    if phone == nir_phone:
                        return True
        return False
    
    def isNIRContact(self, contact):
        for nir_country in self.nirs_dict:
            if 'admin/tech' in self.nirs_dict[nir_country]:
                for nir_contact in self.nirs_dict[nir_country]['admin/tech']:
                    if contact == nir_contact:
                        return True
        return False
    
    def isMntnerOfInterest(self, mntner):
        if mntner == 'APNIC-HM':
            return False

        for nir_country in self.nirs_dict:
            if 'mntner' in self.nirs_dict[nir_country]:
                if mntner == self.nirs_dict[nir_country]['mntner']:
                    return False
        return True
    
    def isIRTofInterest(self, irt):
        for nir_country in self.nirs_dict:
            if 'irt' in self.nirs_dict[nir_country]:
                if irt == self.nirs_dict[nir_country]['irt']:
                    return False
        return True
        
    def filterIRTs(self, data_dict, filtered_data_dict):
        if 'mnt-irt' in data_dict:
            for irt in data_dict['mnt-irt']:
                if self.isIRTofInterest(irt):
                    filtered_data_dict['irts'].append(irt)
                    
                    irt_data = self.bulkWHOIS_data['irt']['data'][irt]
                    for key in irt_data:
                        if key in ['admin-c', 'tech-c']:
                            if not self.isNIRContact(irt_data[key]):
                                filtered_data_dict['admin/tech'].append(irt_data[key])
                        
                        elif key in ['fax-no', 'phone']:
                            if not self.isNIRPhone(irt_data[key]):
                                filtered_data_dict['phones'].append(irt_data[key])
        
                        elif key == 'remarks':
                            if not self.isSimilarToNIRRemarkDescr(irt_data[key]):
                                filtered_data_dict['remarks/descr'].append(irt_data[key])

        return filtered_data_dict
        
    def filterMntners(self, data_dict, filtered_data_dict):
        for mntner_type in ['mnt-by', 'mnt-lower', 'mnt-routes']:
            if mntner_type in data_dict:
                for mntner in data_dict[mntner_type]:
                    if self.isMntnerOfInterest(mntner):
                        filtered_data_dict['mntners'].append(mntner)
                        
                        mntner_data = self.bulkWHOIS_data['mntner']['data'][mntner]
                        for key in mntner_data:
                            if key in ['admin-c', 'tech-c']:
                                if not self.isNIRContact(mntner_data[key]):
                                    filtered_data_dict['admin/tech'].append(mntner_data[key])
                            
                            if key in ['descr', 'remarks']:
                                if not self.isSimilarToNIRsRemarkDescr(mntner_data[key]):
                                    filtered_data_dict['remarks/descr'].append(mntner_data[key])
                            
        return filtered_data_dict
        
    def filterContacts(self, data_dict, filtered_data_dict):
        for contact_type in ['admin-c', 'tech-c']:
            if contact_type in data_dict:
                for contact in data_dict[contact_type]:
                    if not self.isNIRContact(contact):
                        filtered_data_dict['admin/tech'].append(contact)

        return filtered_data_dict
        
    def queryJPNICwhois(self, resource, isNetwork, filtered_data_dict):
        if isNetwork:
            cmd =  'whois -h whois.nic.ad.jp "{}"/e'.format(str(resource))
        else:
            cmd = 'whois -h whois.nic.ad.jp "AS {}"/e'.format(resource)

        out = subprocess.check_output(cmd, shell=True).split('\n')


        for line in out:
            if isNetwork:
                if 'Network Name' in line:
                    net_name = line.split(']')[1].lstrip()

                    if not self.isNIRName(net_name):
                        filtered_data_dict['itemnames'].append(net_name)
            else:
                if 'AS Name' in line:
                    as_name = line.split(']')[1].lstrip()

                    if not self.isNIRName(as_name):
                        filtered_data_dict['itemnames'].append(as_name)

            if 'Organization' in line:
                org_name = line.split(']')[1].lstrip()

                if not self.isSimilarToNIRsRemarkDescr(org_name):
                    filtered_data_dict['remarks/descr'].append(org_name)

        return filtered_data_dict
        
    def queryKRNICwhois(self, resource, isNetwork, filtered_data_dict):
        if isNetwork:
            if resource.version == 4:
                cmd = 'whois -h whois.nic.or.kr {}'.format(str(resource.network_address))
            else:
                cmd = 'whois -h whois.nic.or.kr {}'.format(str(resource))
        else:
            cmd = 'whois -h whois.nic.or.kr AS{}'.format(str(resource))
            
        out = subprocess.check_output(cmd, shell=True).split('\n')
        
        org_name = ''
        serv_name = ''
        as_name = ''
        phone = ''
        email = ''
        
        for line in out:
            content = line.split(':')[1].strip()
            
            # If 'IPv4 Address' or 'IPv6 Address' appears in a line
            # it means a new block of info starts.
            # If it is not the first block of indo, it is a block of info
            # for a more specific prefix,
            # therefore we reset all the variables so that we just keep the
            # info for the most specific prefix.
            if 'IPv4 Address' in line or 'IPv6 Address' in line:
                org_name = ''
                serv_name = ''
                phone = ''
                email = ''
                                
            elif 'Organization Name' in line or line.startswith('Name'):
                org_name = content
            elif 'Service Name' in line:
                serv_name = content
            elif 'AS Name' in line:
                as_name = content
            elif 'Phone' in line:
                phone = content
            elif 'E-Mail' in line:
                email = content
                
        if org_name != '' and not self.isSimilarToNIRsRemarkDescr(org_name):
            filtered_data_dict['remarks/descr'].append(org_name) 
        
        if serv_name != '' and not self.isNIRName(serv_name):
            filtered_data_dict['itemnames'].append(serv_name)
        
        if as_name != '' and not self.isNIRName(as_name):
            filtered_data_dict['itemnames'].append(as_name)
        
        if phone != '' and not self.isNIRPhone(phone):
            filtered_data_dict['phones'].append(phone)
    
        if email != '' and not self.isNIREmail(email):
            filtered_data_dict['emails'].append(email)

        return filtered_data_dict
    
    def filterRemarksDescr(self, data_dict, filtered_data_dict, resource, isNetwork):
        if 'remarks' in data_dict:
            for remark in data_dict['remarks']:
                if not self.isSimilarToNIRsRemarkDescr(remark):
                    filtered_data_dict['remarks/descr'].append(remark)
                    
                if 'whois.nic.ad.jp' in remark:
                    filtered_data_dict = self.queryJPNICwhois(resource,
                                                              isNetwork,
                                                              filtered_data_dict)
    
                elif 'whois.nic.or.kr' in remark or 'whois.krnic.net' in remark\
                    or 'whois.nida.or.kr' in remark:
                    filtered_data_dict = self.queryKRNICwhois(resource,
                                                              isNetwork,
                                                              filtered_data_dict)
      
        if 'descr' in data_dict:      
            for descr in data_dict['descr']:
                if not self.isSimilarToNIRsRemarkDescr(descr):
                    filtered_data_dict['remarks/descr'].append(descr)

        return filtered_data_dict
        
    def filterNames(self, data_dict, filtered_data_dict, isNetwork):
        if isNetwork:
            data_field = 'netname'
        else:
            data_field = 'as-name'
            
        if data_field in data_dict:
            for name in data_dict[data_field]:
                if not self.isNIRName(name):
                    filtered_data_dict['itemnames'].append(name)
        
        return filtered_data_dict
    
    def getDataOfInterest(self, resource, isNetwork, data_dict):
        filtered_data = dict()
        
        filtered_data['itemnames'] = []
        filtered_data = self.filterNames(data_dict, filtered_data, isNetwork)
 
        filtered_data['phones'] = []
        filtered_data['emails'] = []
        filtered_data['remarks/descr'] = []
        filtered_data = self.filterRemarksDescr(data_dict, filtered_data,
                                                resource, isNetwork)        

        filtered_data['admin/tech'] = []
        filtered_data = self.filterContacts(data_dict, filtered_data)
                    
        filtered_data['mntners'] = []
        filtered_data = self.filterMntners(data_dict, filtered_data)
    
        filtered_data['irts'] = []
        filtered_data = self.filterIRTs(data_dict, filtered_data)
    
        return filtered_data

# TODO Hacer DEBUG llamando a checkIfSameOrg para 2001:4428:200:9a::/64 con ASN 18119
org_h = OrgHeuristics('/Users/sofiasilva/Downloads/')
prefix = '2001:4428:200:9a::/64'
asn = 18119
org_h.checkIfSameOrg(prefix, asn)