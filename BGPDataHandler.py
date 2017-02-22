#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys
import os, subprocess, shlex
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
import bgp_rib
import pickle
import pytricia
import datetime, time
import pandas as pd
import hashlib
import ipaddress

# For some reason in my computer os.getenv('PATH') differs from echo $PATH
# /usr/local/bin is not in os.getenv('PATH')
bgpdump = '/usr/local/bin/bgpdump'

class BGPDataHandler:
    urls_file = './BGPoutputs.txt'
    # Data Frame containing routing info from RIB file or file with 'show ip bgp' output
    bgp_data = pd.DataFrame()
    # PyTricia indexed by routed IPv4 prefix containing the indexes of the rows in
    # the bgp_data Data Frame that contain info of BGP announcements of the IPv4 prefix
    ipv4_prefixes_indexes_pyt = pytricia.PyTricia()
    # PyTricia indexed by routed IPv6 prefix containing the indexes of the rows in
    # the bgp_data Data Frame that contain info of BGP announcements of the IPv6 prefix
    ipv6_prefixes_indexes_pyt = pytricia.PyTricia()
    # Dictionary indexed by AS containing all the prefixes originated by each AS
    ASes_originated_prefixes_dic = dict()
    # Dictionary indexed by AS containing all the prefixes propagated by each AS
    ASes_propagated_prefixes_dic = dict()
    # Numeric variable with the longest IPv4 prefix length
    ipv4_longest_pref = -1
    # Numeric variable with the longest IPv6 prefix length
    ipv6_longest_pref = -1   
            
    def __init__(self, urls, files_path, routing_file, KEEP, RIBfile, bgp_data_file,\
                    ipv4_prefixes_indexes_file, ipv6_prefixes_indexes_file,\
                    ASes_originated_prefixes_file, ASes_propagated_prefixes_file):
        if bgp_data_file != '' and ipv4_prefixes_indexes_file != '' and\
            ipv6_prefixes_indexes_file != '' and ASes_originated_prefixes_file != ''\
            and ASes_propagated_prefixes_file != '':
            self.bgp_data = pickle.load(open(bgp_data_file, "rb"))
            self.ipv4_prefixes_indexes_pyt = pickle.load(open(ipv4_prefixes_indexes_file, "rb"))
            self.ipv6_prefixes_indexes_pyt = pickle.load(open(ipv6_prefixes_indexes_file, "rb"))
            self.ASes_originated_prefixes_dic = pickle.load(open(ASes_originated_prefixes_file, "rb"))
            self.ASes_propagated_prefixes_dic = pickle.load(open(ASes_propagated_prefixes_file, "rb"))
            self.setLongestPrefixLengths()
        else:
            if urls == '':
                urls = self.urls_file
            
            if routing_file == '':
                urls_file_obj = open(urls, 'r')
                
                bgp_data = pd.DataFrame()
                ipv4_prefixes_indexes_pyt = pytricia.PyTricia()
                ipv6_prefixes_indexes_pyt = pytricia.PyTricia()
                ASes_originated_prefixes_dic = dict()
                ASes_propagated_prefixes_dic = dict()
                
                for line in urls_file_obj:
                    if line.strip() != '':
                        # If we work with several routing files
                        sys.stderr.write("Starting to work with %s" % line)
                        # We obtain partial data structures
                        bgp_data_partial, ipv4_prefixes_indexes_pyt_partial,\
                            ipv6_prefixes_indexes_pyt_partial,\
                            ASes_originated_prefixes_dic_partial,\
                            ASes_propagated_prefixes_dic_partial,\
                            ipv4_longest_pref, ipv6_longest_pref =\
                            self.processRoutingData(line.strip(), files_path,\
                            routing_file, KEEP, RIBfile)
                        
                        # that we then merge to the general data structures
                        bgp_data = pd.concat([bgp_data, bgp_data_partial])
            
                        for prefix in ipv4_prefixes_indexes_pyt_partial:
                            if ipv4_prefixes_indexes_pyt.has_key(prefix):
                                ipv4_prefixes_indexes_pyt[prefix].update(list(ipv4_prefixes_indexes_pyt_partial[prefix]))
                            else:
                                ipv4_prefixes_indexes_pyt[prefix] = ipv4_prefixes_indexes_pyt_partial[prefix]

                        for prefix in ipv6_prefixes_indexes_pyt_partial:
                            if ipv6_prefixes_indexes_pyt.has_key(prefix):
                                ipv6_prefixes_indexes_pyt[prefix].update(list(ipv6_prefixes_indexes_pyt_partial[prefix]))
                            else:
                                ipv6_prefixes_indexes_pyt[prefix] = ipv6_prefixes_indexes_pyt_partial[prefix]
        
                        for aut_sys, prefixes in ASes_originated_prefixes_dic_partial.iteritems():
                            if aut_sys in ASes_originated_prefixes_dic.keys():
                                ASes_originated_prefixes_dic[aut_sys].update(list(prefixes))
                            else:
                                ASes_originated_prefixes_dic[aut_sys] = prefixes

                        for aut_sys, prefixes in ASes_propagated_prefixes_dic_partial.iteritems():
                            if aut_sys in ASes_propagated_prefixes_dic.keys():
                                ASes_propagated_prefixes_dic[aut_sys].update(list(prefixes))
                            else:
                                ASes_propagated_prefixes_dic[aut_sys] = prefixes
                                
                        if ipv4_longest_pref > self.ipv4_longest_pref:
                            self.ipv4_longest_pref = ipv4_longest_pref
                            
                        if ipv6_longest_pref > self.ipv6_longest_pref:
                            self.ipv6_longest_pref = ipv6_longest_pref
                                
                urls_file_obj.close()
                
            else:
                bgp_data, ipv4_prefixes_indexes_pyt, ipv6_prefixes_indexes_pyt,\
                    ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
                    ipv4_longest_pref, ipv6_longest_pref  =\
                    self.processRoutingData('', files_path, routing_file,\
                    KEEP, RIBfile)
    
            self.bgp_data = bgp_data
            self.ipv4_prefixes_indexes_pyt = ipv4_prefixes_indexes_pyt
            self.ipv6_prefixes_indexes_pyt = ipv6_prefixes_indexes_pyt
            self.ASes_originated_prefixes_dic = ASes_originated_prefixes_dic
            self.ASes_propagated_prefixes_dic = ASes_propagated_prefixes_dic
            if ipv4_longest_pref != -1:
                self.ipv4_longest_pref = ipv4_longest_pref
            else:
                self.ipv4_longest_pref = 32
            if ipv6_longest_pref != -1:
                self.ipv6_longest_pref = ipv6_longest_pref
            else:
                self.ipv6_longest_pref = 64
                
            sys.stderr.write("BGPDataHandler instantiated successfully!\n")
        
        
    # This method converts a file containing the output of the 'show ip bgp' command
    # to a file in the same format used for BGPDump outputs
    def convertBGPoutput(self, routing_file, files_path, KEEP):
        today = datetime.date.today().strftime('%Y%m%d')        
        output_file_name = '%s/%s_%s.readable' % (files_path, '.'.join(routing_file.split('/')[-1].split('.')[:-1]), today)
        output_file = open(output_file_name, 'w')
        
        with open(routing_file, 'r') as file_h:
        	# load routing table info  (the next loop does it automatically)
#        	bgp_entries = []
        	for entry_n, bgp_entry in enumerate(bgp_rib.BGPRIB.parse_cisco_show_ip_bgp_generator(file_h)):
			date = bgp_entry[8]
#			date_part = str(date)[0:8]
#			time_part = str(date)[8:12]
			timestamp = time.mktime(datetime.datetime.strptime(date, "%Y%m%d%H%M").timetuple())
           		next_hop = bgp_entry[2]
           		prefix = bgp_entry[0]
           		as_path = bgp_entry[6]
           		if as_path:
                		nextas = as_path[0]
           		else:
                		nextas = ''

           		if bgp_entry[7] == 'i':
                		origin = "IGP"
           		elif bgp_entry[7] == 'e':
                		origin = "EGP"
           		elif bgp_entry[7] == "?":
                		origin = "INCOMPLETE"
           		else:
                		sys.stderr.write("Found invalid prefix at bgp entry %s, with content %s, on file %s" %(entry_n, bgp_entry, routing_file))
                		# ignore this line and continue
                		continue

            		# save information

            		#the order for each line is
            		#TABLE_DUMP2|date|A|nexthop|NextAS|prefix|AS_PATH|Origin
			output_file.write('TABLE_DUMP|'+str(timestamp)[:-2]+'|B|'+next_hop+'|'+nextas+'|'+prefix+'|'+" ".join(as_path)+'|'+origin+'\n')

#           		bgp_entries.append(['', '', '', next_hop, nextas, prefix, " ".join(as_path), origin])
        file_h.close()
        output_file.close()
        
        return output_file_name
 
    # This function processes a readable file with routing info
    # putting all the info in a Data Frame  
    def processReadableDF(self, readable_file_name):
        bgp_df = pd.read_table(readable_file_name, header=None, sep='|',\
                                index_col=False, usecols=[3,5,6,7],\
                                names=['peer',\
                                        'prefix',\
                                        'ASpath',\
                                        'origin'])
        # We create an index that is unique even amongst different routing files
        # so that we can merge partial data structures into a single structure
        file_id = hashlib.md5(readable_file_name).hexdigest()
        bgp_df['source_file'] = '%s_' % file_id
        bgp_df['index'] = bgp_df.index.astype(str)
        bgp_df['index'] = bgp_df['source_file'] + bgp_df['index']
        bgp_df.index = bgp_df['index']
        
        ipv4_prefixes_indexes_pyt = pytricia.PyTricia()
        ipv6_prefixes_indexes_pyt = pytricia.PyTricia()
        ASes_originated_prefixes_dic = dict()
        ASes_propagated_prefixes_dic = dict()
        
        ipv4_longest_prefix = -1
        ipv6_longest_prefix = -1
        
        for index, value in bgp_df.iterrows():
            prefix = value['prefix']
            ASpath = str(value['ASpath']).split(' ')
            originAS = ASpath[-1]
            middleASes = ASpath[:-1]
          
            network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
            if network.version == 4:
                if network.prefixlen > ipv4_longest_prefix:
                    ipv4_longest_prefix = network.prefixlen
                
                if ipv4_prefixes_indexes_pyt.has_key(prefix):
                    ipv4_prefixes_indexes_pyt[prefix].add(index)
                else:
                    ipv4_prefixes_indexes_pyt[prefix] = set([index])
                    
            elif network.version == 6:
                if network.prefixlen > ipv6_longest_prefix:
                    ipv6_longest_prefix = network.prefixlen  
                
                if ipv6_prefixes_indexes_pyt.has_key(prefix):
                    ipv6_prefixes_indexes_pyt[prefix].add(index)
                else:
                    ipv6_prefixes_indexes_pyt[prefix] = set([index])
                   
            if originAS in ASes_originated_prefixes_dic.keys():
                if prefix not in ASes_originated_prefixes_dic[originAS]:
                    ASes_originated_prefixes_dic[originAS].add(prefix)
            else:
                ASes_originated_prefixes_dic[originAS] = set([prefix])
                
            for asn in middleASes:
                if asn in ASes_propagated_prefixes_dic.keys():
                    if prefix not in ASes_propagated_prefixes_dic[asn]:
                        ASes_propagated_prefixes_dic[asn].add(prefix)
                else:
                    ASes_propagated_prefixes_dic[asn] = set([prefix])
              
        return bgp_df, ipv4_prefixes_indexes_pyt, ipv6_prefixes_indexes_pyt,\
                ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
                ipv4_longest_prefix, ipv6_longest_prefix
              
    def processRoutingData(self, url, files_path, routing_file, KEEP, RIBfile):
        # If a routing file is not provided, download it from the provided URL        
        if routing_file == '':
            routing_file = '%s/%s' % (files_path, url.split('/')[-1])
            get_file(url, routing_file)
        
        # If the routing file is a compressed RIB file, we unzip it
        # and process it using BGPdump
        if RIBfile:
            if KEEP:
                cmd = 'gunzip -k %s' % routing_file
                #  GUNZIP
                #  -k --keep            don't delete input files during operation
            else:
                cmd = 'gunzip %s' % routing_file
            
            subprocess.call(shlex.split(cmd))    
        
            decomp_file_name = '.'.join(routing_file.split('.')[:-1]) # Path to decompressed file
            today = datetime.date.today().strftime('%Y%m%d')
            readable_file_name = '%s_%s.readable' % (decomp_file_name, today)

            cmd = shlex.split('%s -m -O %s %s' % (bgpdump, readable_file_name, decomp_file_name))
            #        cmd = shlex.split('bgpdump -m -O %s %s' % (readable_file_name, decomp_file_name))   
    
            #  BGPDUMP
            #  -m         one-line per entry with unix timestamps
            #  -O <file>  output to <file> instead of STDOUT
    
            subprocess.call(cmd)
        
        # If the file contains the output of the 'show ip bgp' command,
        # we convert it to the same format used by BGPdump for its outputs
        else:
            readable_file_name = self.convertBGPoutput(routing_file, files_path, KEEP)

        # Finally, we process the readable file
        bgp_data, ipv4_prefixes_indexes_pyt, ipv6_prefixes_indexes_pyt,\
            ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
            ipv4_longest_pref, ipv6_longest_pref =\
            self.processReadableDF(readable_file_name)
        
        if not KEEP:
            try:
                os.remove(routing_file)
                os.remove(decomp_file_name)
                os.remove(readable_file_name)
            except OSError:
                pass
        
        return bgp_data, ipv4_prefixes_indexes_pyt, ipv6_prefixes_indexes_pyt,\
                ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
                ipv4_longest_pref, ipv6_longest_pref


    def saveDataToFiles(self, files_path):
        today = datetime.date.today().strftime('%Y%m%d')
        
        bgp_file_name = '%s/bgp_data_%s.pkl' % (files_path, today)
        with open(bgp_file_name, 'wb') as f:
            pickle.dump(self.bgp_data, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing DataFrame with BGP data." % bgp_file_name)

        ipv4_pyt_file_name = '%s/ipv4_prefixes_indexes_%s.pkl' % (files_path, today)
        with open(ipv4_pyt_file_name, 'wb') as f:
            pickle.dump(self.ipv4_prefixes_indexes_pyt, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing PyTricia with indexes in the BGP data DataFrame for each IPv4 prefix." % ipv4_pyt_file_name)

        ipv6_pyt_file_name = '%s/ipv6_prefixes_indexes_%s.pkl' % (files_path, today)
        with open(ipv6_pyt_file_name, 'wb') as f:
            pickle.dump(self.ipv6_prefixes_indexes_pyt, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing PyTricia with indexes in the BGP data DataFrame for each IPv6 prefix." % ipv6_pyt_file_name)

        o_ases_dic_file_name = '%s/ASes_originated_prefixes_%s.pkl' % (files_path, today)
        with open(o_ases_dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_originated_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes originated by each AS." % o_ases_dic_file_name)
        
        p_ases_dic_file_name = '%s/ASes_propagated_prefixes_%s.pkl' % (files_path, today)
        with open(p_ases_dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_propagated_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes propagated by each AS." % p_ases_dic_file_name)
            
        return bgp_file_name, ipv4_pyt_file_name, ipv6_pyt_file_name,\
                o_ases_dic_file_name, p_ases_dic_file_name

    def setLongestPrefixLengths(self):
        for prefix in self.ipv4_prefixes_indexes_pyt:
            network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
            
            if network.prefixlen > self.ipv4_longest_pref:
                self.ipv4_longest_pref = network.prefixlen
                
        for prefix in self.ipv6_prefixes_indexes_pyt:
            network = ipaddress.ip_network(unicode(prefix, 'utf-8'))

            if network.prefixlen > self.ipv6_longest_pref:
                self.ipv6_longest_pref = network.prefixlen