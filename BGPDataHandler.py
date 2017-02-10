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

# For some reason in my computer os.getenv('PATH') differs from echo $PATH
# /usr/local/bin is not in os.getenv('PATH')
bgpdump = '/usr/local/bin/bgpdump'

class BGPDataHandler:
    urls_file = './BGPoutputs.txt'
    bgp_data = pd.DataFrame()
    prefixes_indexes_pyt = pytricia.PyTricia()
    ASes_prefixes_dic = dict()
            
    def __init__(self, urls, files_path, routing_file, KEEP, RIBfile):
        if urls == '':
            urls = self.urls_file
        
        if routing_file == '':
            urls_file_obj = open(urls, 'r')
            
            bgp_data = pd.DataFrame()
            prefixes_indexes_pyt = pytricia.PyTricia()
            ASes_prefixes_dic = dict()
            
            for line in urls_file_obj:
                sys.stderr.write("Starting to work with %s" % line)
                bgp_data_partial, prefixes_indexes_pyt_partial, ASes_prefixes_dic_partial = self.processRoutingData(line, files_path, routing_file, KEEP, RIBfile)
                
                bgp_data = pd.concat([bgp_data, bgp_data_partial])
    
                for prefix in prefixes_indexes_pyt_partial:
                    if prefixes_indexes_pyt.has_key(prefix):
                        prefixes_indexes_pyt[prefix].update(list(prefixes_indexes_pyt_partial[prefix]))
                    else:
                        prefixes_indexes_pyt[prefix] = prefixes_indexes_pyt_partial[prefix]

                for aut_sys, prefixes in ASes_prefixes_dic_partial.iteritems():
                    if aut_sys in ASes_prefixes_dic.keys():
                        ASes_prefixes_dic[aut_sys].update(list(prefixes))
                    else:
                        ASes_prefixes_dic[aut_sys] = prefixes
                        
            urls_file_obj.close()
            
        else:
            bgp_data, prefixes_indexes_pyt, ASes_prefixes_dic = self.processRoutingData('', files_path, routing_file, KEEP, RIBfile)

        self.bgp_data = bgp_data
        self.prefixes_indexes_pyt = prefixes_indexes_pyt
        self.ASes_prefixes_dic = ASes_prefixes_dic
        
        
    # This method converts a file containing the output of the 'show ip bgp' command
    # to a file in the same format used for BGPDump outputs
    def convertBGPoutput(routing_file, files_path, KEEP):
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
 
        
    def processReadableDF(readable_file_name):
        bgp_df = pd.read_table(readable_file_name, header=None, sep='|',\
                                index_col=False, usecols=[3,5,6,7],\
                                names=['peer',\
                                        'prefix',\
                                        'ASpath',\
                                        'origin'])
                                        
        file_id = hashlib.md5(readable_file_name).hexdigest()
        bgp_df['source_file'] = '%s_' % file_id
        bgp_df['index'] = bgp_df.index.astype(str)
        bgp_df['index'] = bgp_df['source_file'] + bgp_df['index']
        bgp_df.index = bgp_df['index']
        
        prefixes_indexes_pyt = pytricia.PyTricia()
        ASes_prefixes_dic = dict()
        
        for index, value in bgp_df.iterrows():
            prefix = value['prefix']
            ASpath = str(value['ASpath']).split(' ')
            originAS = ASpath[-1]
          
            if prefixes_indexes_pyt.has_key(prefix):
                prefixes_indexes_pyt[prefix].add(index)
            else:
                prefixes_indexes_pyt[prefix] = set([index])
                   
            if originAS in ASes_prefixes_dic.keys():
                if prefix not in ASes_prefixes_dic[originAS]:
                    ASes_prefixes_dic[originAS].add(prefix)
            else:
                ASes_prefixes_dic[originAS] = set([prefix])
              
        return bgp_df, prefixes_indexes_pyt, ASes_prefixes_dic
              
    def processRoutingData(self, url, files_path, routing_file, KEEP, RIBfile):
        if routing_file == '':
            routing_file = '%s/%s' % (files_path, url.split('/')[-1])
            get_file(url, routing_file)
        
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
    
        else:
            readable_file_name = self.convertBGPoutput(routing_file, files_path, KEEP)

        bgp_data, prefixes_indexes_pyt, ASes_prefixes_dic = self.processReadableDF(readable_file_name)
        
        if not KEEP:
            try:
                os.remove(routing_file)
                os.remove(decomp_file_name)
                os.remove(readable_file_name)
            except OSError:
                pass
        
        return bgp_data, prefixes_indexes_pyt, ASes_prefixes_dic


    def saveDataToFiles(self, files_path):
        today = datetime.date.today().strftime('%Y%m%d')
        
        bgp_file_name = '%s/bgp_data_%s.pkl' % (files_path, today)
        with open(bgp_file_name, 'wb') as f:
            pickle.dump(self.bgp_data, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing DataFrame with BGP data." % bgp_file_name)

        pyt_file_name = '%s/prefixes_indexes_%s.pkl' % (files_path, today)
        with open(pyt_file_name, 'wb') as f:
            pickle.dump(self.prefixes_indexes_pyt, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing PyTricia with indexes in the BGP data DataFrame for each prefix." % pyt_file_name)

        dic_file_name = '%s/ASes_prefixes_%s.pkl' % (files_path, today)
        with open(dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes announced by each AS." % dic_file_name)
        
        return bgp_file_name, pyt_file_name, dic_file_name
