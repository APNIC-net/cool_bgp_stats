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
from BulkWHOISParser import RangeDict


class OrgHeuristics:
    nirs_dict = {'JP':{
                    'nir_name':'JPNIC',
                    'descr':['JPNIC', 'Japan Network Information Center'],
                    'netnames/as-names':['JPNIC-NET-JP', 'OCN-JPNIC-JP', 'JPNIC-NET-JP-ERX', 'JPNIC-NET-JP-AS-BLOCK', 'JPNIC-2Byte-ASBLOCK-AP', 'JPNIC-JP-ASN-BLOCK', 'JPNIC-ASBLOCK-AP'],
                    'admin/tech':['JNIC1-AP'],
                    'mntner':'MAINT-JPNIC',
                    'irt':'IRT-JPNIC-JP',
                    'whois':'whois.nic.ad.jp',
                    'opaque_id':'A91A7381',
                    'address': 'Urbannet-Kanda Bldg 4F  3-6-2 Uchi-Kanda  Chiyoda-ku  Tokyo 101-0047 Japan',
                    'prefix_example':'1.0.64.0/18', 'asn_example':'18144'
                    },
                'ID':{
                    'nir_name':'IDNIC',
                    'descr':['Indonesia Network Information Center', 'Gedung Cyber Lt.3A', 'Jl. Kuningan Barat No.8', 'Jakarta 12710'],
                    'netnames/as-names':['IDNIC-ID', 'IDNIC-AS-ID'],
                    'admin/tech':['IA55-AP', 'IH123-AP'],
                    'mntner':'MNT-APJII-ID',
                    'irt':'IRT-IDNIC-ID',
                    'opaque_id':'A9186214',
                    'address': 'Cyber Building 11th Floor',
                    'prefix_example':'103.15.226.0/24', 'asn_example':'136052'
                    },
                'CN':{
                    'nir_name':'CNNIC',
                    'descr':['China Internet Network Information Center', 'No.4, Zhongguancun No.4 South Street,', 'Haidian District, Beijing', 'P.O.Box: No.6 Branch-box of No.349 Mailbox, Beijing'],
                    'netnames/as-names':['CNNIC', 'CNNIC-CRITICAL-CN', 'CNNIC-AP'],
                    'admin/tech':['MW1-AP', 'IPAS1-AP'],
                    'mntner':'MAINT-CNNIC-AP',
                    'irt':'IRT-CNNIC-CN',
                    'opaque_id':'A9162E3D',
                    'address':'4, South 4th Street, No.4, Zhongguancun No.4 South Street, Haidian District, Beijing P.O.Box: No.6 Branch-box of No.349 Mailbox, Beijing',
                    'prefix_example':'45.121.52.0/22', 'asn_example':'63530'
                    },
                'IN':{
                    'nir_name':'IRINN',
                    'mntner':'MAINT-IN-IRINN',
                    'itr':'IRT-IRINN-IN',
                    'opaque_id':'A918EDB2',
                    'address': 'National Internet Exchange of India,Flat no. 6B,Uppals M6 Plaza',
                    'prefix_example':'103.52.223.0/24', 'asn_example':'133961'
                    },
                'KR':{
                    'nir_name':'KRNIC',
                    'descr':['KRNIC', 'Korea Network Information Center', '************************************************', 'Allocated to KRNIC Member.', 'If you would like to find assignment', 'information in detail please refer to', 'the KRNIC Whois Database at:', '"http://whois.nida.or.kr/english/index.html"'],
                    'netnames/as-names':['KRNIC-KR', 'KRNIC-NET', 'KRNIC-AS-KR'],
                    'remarks':[' ******************************************', 'KRNIC is the National Internet Registry', 'in Korea under APNIC. If you would like to', 'find assignment information in detail', 'please refer to the KRNIC Whois DB'],
                    'admin/tech':['HM127-AP'],
                    'mntner':'MNT-KRNIC-AP',
                    'irt':'IRT-KRNIC-KR',
                    'whois':'whois.nic.or.kr', # TODO Verificar si es ese o whois.nida.or.kr?
                    'opaque_id':'A9149F3E',
                    'address': '135 Jungdae-ro Songpa-gu Seoul, Seoul Songpa-gu Jungdae-ro 135',
                    'prefix_example':'211.190.231.0/24', 'asn_example':'38660'
                    },
                'TW':{
                    'nir_name':'TWNIC',
                    'descr':['Taiwan Network Information Center', '4F-2, No. 9 Sec. 2, Roosevelt Rd.,', 'Taipei, Taiwan, 100'],
                    'netnames/as-names':['TWNIC-TW', 'TWNIC-CRITICAL-TW', 'TWNIC-AS'],
                    'remarks':['This object can only be updated by TWNIC hostmasters.', 'To update this object, please contact TWNIC.'],
                    'admin/tech':['TWA2-AP'],
                    'mntner':'MAINT-TW-TWNIC',
                    'irt':'IRT-TWNIC-AP',
                    'opaque_id':'A91BDB29',
                    'address': 'Taiwan Network Information Center, 4F-2, No. 9 Sec. 2, Roosevelt Rd., Taipei, Taiwan, 100',
                    'prefix_example':'43.255.12.0/22', 'asn_example':'131584'
                    },
                'VN':{
                    'nir_name':'VNNIC',
                    'descr':['VNNIC', 'Vietnam Internet network Information Centre', 'Vietnam Internet network Information Centre - VNNIC', 'Vietnam Internet network information center (VNNIC)', 'Branch of VNNIC in Ho Chi Minh City', 'Branch of VNNIC in Da Nang City', '18 Nguyen Du street, Hanoi capital, Vietnam', '18 Nguyen Du street, Hai Ba Trung District, Hanoi', '18 Nguyen Du, Hanoi'],
                    'netnames/as-names':['VNNIC-NET', 'VNNIC-AS-VN', 'VNNIC-AS-AP', 'VNNIC-4-BYTE-AS-VN'],
                    'admin/tech':['ITMG1-AP', 'VI2-AP', 'PXD6-AP'],
                    'mntner':'MAINT-VN-VNNIC',
                    'irt':'IRT-VNNIC-AP',
                    'opaque_id':'A91A560A',
                    'address': 'Vietnam Internet Network Information Center, 18 Nguyen Du, Hai Ba Trung, Hanoi',
                    'prefix_example':'103.205.100.0/22', 'asn_example':'135900'
                    }
                }  

# TODO Investigar sobre http://www.sixxs.net/. Qué es? Qué pasa con las asignaciones hechas por SIXXS?

    def __init__(self, files_path):
        self.bulkWHOIS_data = {'inetnum' : {'file' : '{}/apnic.db.inetnum.pkl'.format(files_path)},
                       'inet6num' : {'file' : '{}/apnic.db.inet6num.pkl'.format(files_path)},
                       'aut-num' : {'file' : '{}/apnic.db.aut-num.pkl'.format(files_path)},
                       'as-block' : {'file' : '{}/apnic.db.as-block.pkl'.format(files_path)},
                       'mntner' : {'file' : '{}/apnic.db.mntner.pkl'.format(files_path)},
                       'irt' : {'file' : '{}/apnic.db.irt.pkl'.format(files_path)}}
                       
        for item in self.bulkWHOIS_data:
            try:
                self.bulkWHOIS_data[item]['data'] =\
                    pickle.load(open(self.bulkWHOIS_data[item]['file'], "rb"))
            except IOError:
                sys.stderr.write(IOError.message)
        
        self.ipv4_prefixes_data_file = '{}/ipv4_prefixes_filtered_WHOISdata.pkl'.format(files_path)
        
        try:
            self.ipv4_prefixes_filtered_data = pickle.load(open(self.ipv4_prefixes_data_file, 'rb'))
        except IOError:
            self.ipv4_prefixes_filtered_data = None

        self.ipv6_prefixes_data_file = '{}/ipv6_prefixes_filtered_WHOISdata.pkl'.format(files_path)
        
        try:
            self.ipv6_prefixes_filtered_data = pickle.load(open(self.ipv6_prefixes_data_file, 'rb'))
        except IOError:
            self.ipv6_prefixes_filtered_data = None

        self.ases_data_file = '{}/ases_filtered_WHOISdata.pkl'.format(files_path)
        
        try:
            self.ases_filtered_data = pickle.load(open(self.ases_data_file, 'rb'))
        except IOError:
            self.ases_filtered_data = None
            
            
    @staticmethod
    def similar(a, b):
        return round(SequenceMatcher(None, a.lower(), b.lower()).ratio(), 1)

    def checkIfSameOrg(self, prefix, asn):
        network = ip_network(unicode(prefix, "utf-8"))
        
        pref_node = None
        filtered_data = None
        
        if network.version == 4:
            item = 'inetnum'
            prefixes_data_file = self.ipv4_prefixes_data_file
            
            if self.ipv4_prefixes_filtered_data is None:
                self.ipv4_prefixes_filtered_data = radix.Radix()
                
            filtered_data = self.ipv4_prefixes_filtered_data
                
        else:
            item = 'inet6num'
            prefixes_data_file = self.ipv6_prefixes_data_file
            
            if self.ipv6_prefixes_filtered_data is None:
                self.ipv6_prefixes_filtered_data = radix.Radix()
                
            filtered_data = self.ipv6_prefixes_filtered_data

        pref_node = filtered_data.search_exact(prefix)
        
        if pref_node is not None:
            pref_org_data = pref_node.data
        else:
            pref_node = self.bulkWHOIS_data[item]['data'].search_exact(prefix)
        
            if pref_node is not None:
                pref_org_data = self.getDataOfInterestForPrefix(prefix, pref_node.data)
                
                if prefix not in filtered_data:
                    pref_node = filtered_data.add(prefix)
                    for key in pref_org_data:
                        pref_node.data[key] = pref_org_data[key]
                
                with open(prefixes_data_file, 'wb') as f:
                    pickle.dump(filtered_data, f, pickle.HIGHEST_PROTOCOL)
            else:
                print 'TODO'
                # TODO Decidir qué hacer si no tengo info para el prefijo
                # Buscar un covering? Buscar covered? Buscar best-match?
                # Best match machea con un exact si hay y sino con el más específico que haya?
                # Tal vez es mejor usar search_best desde un principio?
            

        if self.ases_filtered_data is not None:
            try:
                asn_org_data = self.ases_filtered_data[asn]
            except KeyError:
                asn_org_data = None
        else:
            self.ases_filtered_data = dict()
            asn_org_data = None
        
        if asn_org_data is None:
            try:
                asn_dict = self.bulkWHOIS_data['aut-num']['data'][asn]           
                asn_org_data = self.getDataOfInterestForASN(asn_dict)
                self.ases_filtered_data[asn] = asn_org_data
            
                with open(self.ases_data_file, 'wb') as f:
                    pickle.dump(self.ases_filtered_data, f, pickle.HIGHEST_PROTOCOL)
        
            except KeyError:
                print 'TODO'
                # TODO Decidir qué hacer si no tengo info para el asn
                

        # TODO Si tenemos info del prefijo y del asn, comparar
        # TODO Pensar bien qué fields voy a comparar.
        # Hay algunos fields para los que un match de ellos solos no es
        # suficiente y tiene que haber algún otro match. Pensar de ponderar
        # los fields y que se considere la misma org si se suma más de un
        # cierto umbral

    def isInNIRsnames(self, name):
        for nir_country in self.nirs_dict:
            for nir_name in self.nirs_dict[nir_country]['netnames/as-names']:
                if nir_name == name:
                    return True
        return False
    
    def isSimilarToNIRsremarkDescr(self, comment):
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
    
    def isInNIRsContacts(self, contact):
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
    
    def getDataOfInterestForPrefix(self, prefix, pref_dict):
        pref_org_data = dict()
                
        if 'netname' in pref_dict:
            pref_org_data['netnames'] = []
            for name in pref_dict['netname']:
                if not self.isInNIRnames(name):
                    pref_org_data['netnames'].append(name)
        
        pref_org_data['remarks/descr'] = []
        
        if 'remarks' in pref_dict:
            for remark in pref_dict['remarks']:
                if not self.isSimilarToNIRsremarkDescr(remark):
                    pref_org_data['remarks/descr'].append(remark)
                    
                if 'whois.nic.ad.jp' in remark:
                    cmd =  'whois -h whois.nic.ad.jp "%s"/e' % prefix
                    out = subprocess.check_output(cmd, shell=True).split('\n')
                    for line in out:
                        if 'Network Name' in line:
                            net_name = line.split(']')[1].lstrip()
                            if not self.isInNIRsnames(net_name):
                                pref_org_data['netnames'].append(net_name)
                        elif 'Organization' in line:
                            org_name = line.split(']')[1].lstrip()
                            if not self.isSimilarToNIRsremarkDescr(org_name):
                                pref_org_data['remarks/descr'].append(org_name)
    
                elif 'whois.nic.or.kr' in remark or 'whois.krnic.net' in remark\
                    or 'whois.nida.or.kr' in remark:
                    # TODO Query KRNIC WHOIS
                    # TODO Verificar si whois.nic.or.kr responde por CLI o es solo web
                    # Para IPv4: Consultar por la IP inicial (no acepta consultar por bloque IPv4)
                    # Para IPv6 si consultar por bloque
                    '''
                    # ENGLISH

KRNIC is not an ISP but a National Internet Registry similar to APNIC.

[ Network Information ]
IPv4 Address       : 211.190.224.0 - 211.191.15.255 (/19+/20)
Organization Name  : Sejong Telecom
Service Name       : SHINBIRO
Address            : Seoul Gangdong-gu Sangil-ro 10-gil
Zip Code           : 05288
Registration Date  : 20000707

Name               : IP Manager
Phone              : +82-2-1666-0120
E-Mail             : ip@sejongtelecom.net

--------------------------------------------------------------------------------

More specific assignment information is as follows.

[ Network Information ]
IPv4 Address       : 211.190.230.0 - 211.190.231.255 (/23)
Organization Name  : KOREA HYDRO & NUCLEAR
Network Type       : CUSTOMER
Address            : Samseong-dong  Gangnam-gu  Seoul
Zip Code           : 06153
Registration Date  : 20000707

Name               : IP Manager
Phone              : +82-2-3456-2386
E-Mail             : xhdtls@khnp.co.kr

-------
# ENGLISH

KRNIC is not an ISP but a National Internet Registry similar to APNIC.

AS Number          : AS38660
AS Name            : KHNP-AS-KR

[ Organization Information ]
Name               : KHNP
Address            : KHNP, Gyeongsangbuk-do Gyeongju-si Bulguk-ro 1655
Zip Code           : 38120

Name               : AS Manager
Phone              : +82-54-704-2114
E-Mail             : khnpmaster@khnp.co.kr

'''
                    print 'TODO'
                    
                
      
        if 'descr' in pref_dict:      
            for descr in pref_dict['descr']:
                if not self.isSimilarToNIRremarkDescr(descr):
                    pref_org_data['remarks/descr'].append(descr)

        pref_org_data['admin/tech'] = []
        
        for contact_type in ['admin-c', 'tech-c']:
            if contact_type in pref_dict:
                for contact in pref_dict[contact_type]:
                    if not self.isInNIRsContacts(contact):
                        pref_org_data['admin/tech'].append(contact) 
                    
        pref_org_data['mntners'] = []
        
        for mntner_type in ['mnt-by', 'mnt-lower', 'mnt-routes']:
            if mntner_type in pref_dict:
                for mntner in pref_dict[mntner_type]:
                    if self.isMntnerOfInterest(mntner):
                        pref_org_data['mntners'].append(mntner)
                        
                        mntner_data = self.bulkWHOIS_data['mntner']['data'][mntner]
                        for key in mntner_data:
                            if key in ['admin-c', 'tech-c']:
                                if not self.isInNIRsContacts(mntner_data[key]):
                                    pref_org_data['admin/tech'].append(mntner_data[key])
                            
                            if key in ['descr', 'remarks']:
                                if not self.isSimilarToNIRsremarkDescr(mntner_data[key]):
                                    pref_org_data['remarks/descr'].append(mntner_data[key])
                            
        pref_org_data['address'] = []
        # TODO analizar y filtrar address
        '''
        Comparar contra address de cada NIR. Aplicar split y convertir en set
        y ver cuántas palabras se solapan. Calcular fracción respecto del total
        de palabras de la address (cuál? el máximo entre la address de la org y
        la address del NIR?). Considerar iguales si la fracción es mayor a
        cierto umbral. Usar también método similar.
        '''
    
        if 'mnt-irt' in pref_dict:
            pref_org_data['irts'] = []
            pref_org_data['phones'] = []
            
            for irt in pref_dict['mnt-irt']:
                if self.isIRTofInterest(irt):
                    pref_org_data['irts'].append(irt)
                    
                    irt_data = self.bulkWHOIS_data['irt']['data'][irt]
                    for key in irt_data:
                        if key in ['admin-c', 'tech-c']:
                            if not self.isInNIRsContacts(irt_data[key]):
                                pref_org_data['admin/tech'].append(irt_data[key])
                        
                        elif key in ['fax-no', 'phone']:
                            pref_org_data['phones'].append(irt_data[key])
                            
                        elif key == 'address':
                            # TODO completar cuando haya implementado el código
                            # para comparar con las direcciones de los NIRs
                            print 'TODO'

                        elif key == 'remarks':
                            if not self.isSimilarToNIRsremarkDescr(irt_data[key]):
                                pref_org_data['remarks/descr'].append(irt_data[key])
    
        return pref_org_data
        
    def getDataOfInterestForASN(self, asn_dict):
        print 'TODO'
        # TODO Implement
        # {'admin-c:', 'as-name:', 'descr:', 'mnt-by:',
#                                        'mnt-irt:', 'mnt-lower:', 'mnt-routes:',
#                                        'remarks:', 'tech-c:'}
        
        # TODO Ver si voy a consultar info de los as-blocks asociados al ASN