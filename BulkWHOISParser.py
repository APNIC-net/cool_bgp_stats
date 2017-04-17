# -*- coding: utf-8 -*-
"""
Created on Mon Apr 10 10:57:36 2017

@author: sofiasilva
"""
import os, gzip, getopt, sys
os.chdir(os.path.dirname(os.path.realpath(__file__)))
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
import radix
import ipaddress
import pickle

'''
https://ftp.apnic.net/apnic/whois/apnic.db.inetnum.gz
inetnum:      202.6.91.0 - 202.6.91.255
netname:      NLA
country:      AU
descr:        National Library of Australia
descr:        Parkes Place
descr:        Canberra  ACT 2600
admin-c:      NC68-AP
tech-c:       NC68-AP
status:       ASSIGNED PORTABLE
mnt-by:       APNIC-HM
mnt-routes:   MAINT-AU-NLA
source:       APNIC

https://ftp.apnic.net/apnic/whois/apnic.db.inet6num.gz
inet6num:     2001:200:600::/40
netname:      KDDLABS
descr:        KDD Labs IPv6 Network
country:      JP
admin-c:      TA19-AP
tech-c:       MF29-AP
tech-c:       AK44-AP
tech-c:       YH48-AP
status:       ASSIGNED NON-PORTABLE
remarks:      tunnel to otemachi-pop
mnt-by:       MAINT-JP-WIDE
source:       APNIC

https://ftp.apnic.net/apnic/whois/apnic.db.aut-num.gz
aut-num:        AS23553
as-name:        IITA-AS-KR
descr:          National IT Industry Promotion Agency
country:        KR
admin-c:        AM1923-AP
tech-c:         AM1923-AP
import:         from AS4766
                action pref=10;
                accept ANY
import:         from AS9318
                action pref=100;
                accept ANY
export:         to AS4766
                announce AS23553
export:         to AS9318
                announce AS23553
default:        to AS4766
                action pref=10;
                networks ANY
mnt-by:         MNT-KRNIC-AP
mnt-irt:        IRT-KRNIC-KR
source:         APNIC


https://ftp.apnic.net/apnic/whois/apnic.db.as-block.gz
as-block:     AS23612 - AS23643
descr:        JPNIC-2Byte-ASBLOCK-AP
descr:        for assignment to JPNIC members
country:      JP
admin-c:      JNIC1-AP
tech-c:       JNIC1-AP
remarks:      Authoritative information regarding AS Number
remarks:      assignments made from within this block can be
remarks:      queried at whois.nic.ad.jp.  To only display English output,
remarks:      add '/e' at the end of command,
remarks:      e.g. 'whois -h whois.nic.ad.jp xxx/e'.
mnt-by:       MAINT-JPNIC
source:       APNIC


https://ftp.apnic.net/apnic/whois/apnic.db.mntner.gz
mntner:       APNIC-HM
descr:        Maintainer for allocations/assignments made by APNIC
descr:        Objects maintained by this maintainer may
descr:        only be changed by APNIC Hostmasters.
admin-c:      HM20-AP
tech-c:       NO4-AP
upd-to:       ********
mnt-by:       APNIC-HM
referral-by:  APNIC-HM
source:       APNIC


https://ftp.apnic.net/apnic/whois/apnic.db.irt.gz
irt:            irt-CERT-NURUL
address:        6 Cordelia Street
address:        South Brisbane
address:        QLD 4101
phone:          +61738583100
fax-no:         +61738583199
abuse-mailbox:  ********
e-mail:         ********
signature:      PGPKEY-D7F5AA55
encryption:     PGPKEY-D7F5AA55
admin-c:        NR97-AP
tech-c:         NR97-AP
remarks:        CERT-NURUL is the CRT Team of Nurul
irt-nfy:        ********
mnt-by:         MAINT-NEW
source:         APNIC
'''

class BulkWHOISParser:
    whois_data = dict()

    whois_data['inetnum'] = dict()
    whois_data['inetnum']['url'] = 'https://ftp.apnic.net/apnic/whois/apnic.db.inetnum.gz'
    whois_data['inetnum']['filename'] = 'apnic.db.inetnum'
    whois_data['inetnum']['fields'] = {'admin-c:', 'descr:', 'mnt-by:', 'mnt-irt:',
                                         'mnt-lower:', 'mnt-routes:', 'netname:',
                                         'remarks:', 'tech-c:'}
    
    whois_data['inet6num'] = dict()
    whois_data['inet6num']['url'] = 'https://ftp.apnic.net/apnic/whois/apnic.db.inet6num.gz'
    whois_data['inet6num']['filename'] = 'apnic.db.inet6num'
    whois_data['inet6num']['fields'] = {'admin-c:', 'descr:', 'mnt-by:', 'mnt-irt:',
                                         'mnt-lower:', 'mnt-routes:', 'netname:',
                                         'remarks:', 'tech-c:'}
    
    whois_data['aut-num'] = dict()
    whois_data['aut-num']['url'] = 'https://ftp.apnic.net/apnic/whois/apnic.db.aut-num.gz'
    whois_data['aut-num']['filename'] = 'apnic.db.aut-num'
    whois_data['aut-num']['fields'] = {'admin-c:', 'as-name:', 'descr:', 'mnt-by:',
                                        'mnt-irt:', 'mnt-lower:', 'mnt-routes:',
                                        'remarks:', 'tech-c:'}

    whois_data['as-block'] = dict()
    whois_data['as-block']['url'] = 'https://ftp.apnic.net/apnic/whois/apnic.db.as-block.gz'
    whois_data['as-block']['filename'] = 'apnic.db.as-block'
    whois_data['as-block']['fields'] = {'admin-c:', 'descr:', 'mnt-by:', 'mnt-lower:',
                                        'remarks:', 'tech-c:'}

    whois_data['mntner'] = dict()
    whois_data['mntner']['url'] = 'https://ftp.apnic.net/apnic/whois/apnic.db.mntner.gz'
    whois_data['mntner']['filename'] = 'apnic.db.mntner'
    whois_data['mntner']['fields'] = {'admin-c:', 'mnt-by:', 'mnt-nfy:', 'referral-by:',
                                        'descr:', 'remarks:', 'tech-c:'}

    whois_data['irt'] = dict()
    whois_data['irt']['url'] = 'https://ftp.apnic.net/apnic/whois/apnic.db.irt.gz'
    whois_data['irt']['filename'] = 'apnic.db.irt'
    whois_data['irt']['fields'] = {'address:', 'admin-c:', 'fax-no:', 'mnt-by:',
                                    'phone:', 'remarks:', 'tech-c:'}

    
    def __init__(self, files_path, DEBUG):

        if not DEBUG:
            for item in self.whois_data:
                output_file = '{}/{}'.format(files_path, self.whois_data[item]['filename'])
                dest_file = '{}.gz'.format(output_file)
                
                get_file(self.whois_data[item]['url'], dest_file)
                                            
                with gzip.open(dest_file, 'rb') as gzip_file,\
                    open(output_file, 'wb') as output:
                    try:
                        output.write(gzip_file.read())
                    except IOError:
                        return ''
                
                gzip_file.close()
                output.close()

        for item in self.whois_data:
            if item == 'inetnum' or item == 'inet6num':
                current_structure = radix.Radix()
            elif item == 'as-block':
                current_structure = RangeDict()
            else:
                current_structure = dict()
                
            bulk_filename = '{}/{}'.format(files_path, self.whois_data[item]['filename'])
            
            with open(bulk_filename, 'rb') as bulk_file:
                current_dict = dict()
                ip_blocks = []
                asn = -1
                as_block = None
                contact_id = ''
                last_line = ''
                
                for line in bulk_file.readlines():
                    if not line.startswith('#') and not line.startswith('*') and\
                        not line.startswith('+'):
                        if line.startswith(' ') or line.startswith('\t'):
                            last_line = '{}{}'.format(last_line.strip(), line.strip())
                            continue
                        elif last_line == '':
                            last_line = line
                            continue
                        else:
                            ip_blocks, asn, as_block, contact_id, current_dict =\
                                self.processLine(last_line, item, current_structure,
                                                 current_dict, ip_blocks, asn,
                                                 as_block, contact_id)
                            
                            last_line = line
                
                # We process the last line of the file
                self.processLine(last_line, item, current_structure,
                                 current_dict, ip_blocks, asn,
                                 as_block, contact_id)
                                 
            with open('{}.pkl'.format(bulk_filename), 'wb') as f:
                pickle.dump(current_structure, f, pickle.HIGHEST_PROTOCOL)


    def insertElement(self, elementType, current_structure, current_dict,
                      ip_blocks, asn, as_block, contact_id):
        if elementType == 'inetnum' or elementType == 'inet6num':
            for ip_block in ip_blocks:
                new_node = current_structure.add(str(ip_block))
                for field in current_dict:
                    new_node.data[field] = current_dict[field]
            ip_blocks = []
            
        elif elementType == 'aut-num':
            current_structure[asn] = current_dict
            asn = -1
            
        elif elementType == 'as-block':
            current_structure[as_block] = current_dict
            as_block = None
            
        else:
            current_structure[contact_id] = current_dict
            contact_id = ''
            
        return ip_blocks, asn, as_block, contact_id            
                        
    def processLine(self, line, item, current_structure, current_dict, ip_blocks,
                    asn, as_block, contact_id):
        if ':' in line:
            first_colon = line.find(':')
            line_tag = line[0:first_colon]
            line_data = line[first_colon+1:].strip()
            
            if line_tag == 'remarks' or line_tag == 'descr':
                line_data_word_set = set(line_data.split())            
                additional_tags = line_data_word_set.intersection(self.whois_data[item]['fields'])

                if len(additional_tags) == 1:
                    line_tag = list(additional_tags)[0].replace(':', '')
                    line_data = line_data.replace(list(additional_tags)[0], '')

            if line_tag == item:
                if item == 'inetnum' or item == 'inet6num':
                    if len(current_dict) > 0 and len(ip_blocks) > 0:
                        ip_blocks, asn, as_block, contact_id =\
                            self.insertElement(item, current_structure, current_dict,
                                               ip_blocks, asn, as_block, contact_id)
                        current_dict = dict()
                        
                    if item == 'inetnum':
                        item_data_parts = line_data.split('-')
                        first_ip = unicode(item_data_parts[0].strip(), 'utf-8')
                        last_ip = unicode(item_data_parts[1].strip(), 'utf-8')
                        ip_blocks = [ipaddr for ipaddr in ipaddress.summarize_address_range(\
                                                            ipaddress.IPv4Address(first_ip),\
                                                            ipaddress.IPv4Address(last_ip))]
                    else:
                        ip_blocks = [line_data]
                
                elif item == 'aut-num':
                    if asn != -1 and len(current_dict) > 0:
                        ip_blocks, asn, as_block, contact_id =\
                            self.insertElement(item, current_structure, current_dict,
                                               ip_blocks, asn, as_block, contact_id)
                        current_dict = dict()
                            
                    asn = int(line_data[2:])
    
                elif item == 'as-block':
                    if as_block is not None and len(current_dict) > 0:
                        ip_blocks, asn, as_block, contact_id =\
                            self.insertElement(item, current_structure, current_dict,
                                               ip_blocks, asn, as_block, contact_id)
                        current_dict = dict()
                            
                    as_block_parts = line_data.split('-')
                    as_block = xrange(int(as_block_parts[0].strip()[2:]), int(as_block_parts[1].strip()[2:])+1)
                    
                else: # item = 'mntner' or item = 'irt'
                    if contact_id != '' and len(current_dict) > 0:
                        ip_blocks, asn, as_block, contact_id =\
                            self.insertElement(item, current_structure, current_dict,
                                               ip_blocks, asn, as_block, contact_id)
                        current_dict = dict()
                            
                    contact_id = line_data
                    
            elif '{}:'.format(line_tag) in self.whois_data[item]['fields']:
                if line_tag in current_dict:
                    current_dict[line_tag].append(line_data)
                else:
                    current_dict[line_tag] = [line_data]
            
        elif len(current_dict) > 0:
            ip_blocks, asn, as_block, contact_id =\
                self.insertElement(item, current_structure, current_dict, ip_blocks,
                                   asn, as_block, contact_id)
            current_dict = dict()

        return ip_blocks, asn, as_block, contact_id, current_dict

class RangeDict(dict):
    def __getitem__(self, item):
        if type(item) != xrange:
            dict_list = []
            for key in self:
                if item in key:
                    dict_list.append(self[key])
            return dict_list
        else:
            return super(RangeDict, self).__getitem__(item)

def main(argv):
    files_path = ''
    DEBUG = False
    
    try:
        opts, args = getopt.getopt(argv, 'hf:D', ['files_path='])
    except getopt.GetoptError:
        print 'Usage: BulkWHOISParser.py -h | -f <files path> -D'
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print "This script processes bulk WHOIS files in https://ftp.apnic.net/apnic/whois inserting the information of interest into data structures that are then serialized and saved to pickle files in the provided files_path folder."
            print 'Usage: BulkWHOISParser.py -h | -f <files path> -D'
            print 'h = Help'
            print "f = Path to folder in which files will be saved. (MANDATORY)"
            print "D = DEBUG. If this option is used bulk WHOIS files will not be downloaded."
            print "In DEBUG mode the bulk WHOIS files: apnic.db.inetnum, apnic.db.inet6num, apnic.db.aut-num, apnic.db.as-block, apnic.db.mntner and apnic.db.irt MUST be already present in the files_path folder."
        elif opt == '-f':
            files_path = os.path.abspath(arg)
        elif opt == '-D':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
                        
    BulkWHOISParser(files_path, DEBUG)

if __name__ == "__main__":
    main(sys.argv[1:])