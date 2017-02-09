#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys
import os, subprocess, shlex
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
import pickle
import pytricia
import datetime
import pandas as pd

# For some reason in my computer os.getenv('PATH') differs from echo $PATH
# /usr/local/bin is not in os.getenv('PATH')
bgpdump = '/usr/local/bin/bgpdump'

class BGPDataHanler:
    urls_file = './Collectors.txt'     # TODO modificar URL para usar colector de APNIC
    prefixes_data_pyt = pytricia.PyTricia()
    ASes_prefixes_dic = dict()
            
    def __init__(self, urls, files_path, routing_file, KEEP):
        if urls == '':
            urls = self.urls_file
        
        if routing_file == '':
            urls_file_obj = open(urls, 'r')
             # TODO Merge the pyt and the dic generated during each loop
                # into general pyt and dic
            
            for line in urls_file_obj:
                sys.stderr.write("Starting to work with %s" % line)
                decomp_file_name = self.downloadAndUnzip(line.strip(), files_path, routing_file, KEEP)
                prefixes_data_pyt, ASes_prefixes_dic = self.decodeAndParse(decomp_file_name, KEEP)
               
            urls_file_obj.close()
            
        else:
            decomp_file_name = self.downloadAndUnzip('', files_path, routing_file, KEEP)
            self.prefixes_data_pyt, self.ASes_prefixes_dic = self.decodeAndParse(decomp_file_name, KEEP)
        
    def downloadAndUnzip(url, files_path, routing_file, KEEP):
        if routing_file == '':
            routing_file = '%s/%s' % (files_path, url.split('/')[-1])
            get_file(url, routing_file)
        
        if KEEP:
            cmd = 'gunzip -k %s' % routing_file
            #  GUNZIP
            #  -k --keep            don't delete input files during operation
        else:
            cmd = 'gunzip %s' % routing_file
            
        subprocess.call(shlex.split(cmd))    
        
        return '.'.join(routing_file.split('.')[:-1]) # Path to decompressed file
       
        
    def processReadableDF(readable_file_name):
        bgp_df = pd.read_table(readable_file_name, header=None, sep='|',\
                                index_col='prefix', usecols=[3,5,6,7],\
                                names=['peer',\
                                        'prefix',\
                                        'ASpath',\
                                        'origin']).sort_index()
        
        prefixes_data_pyt = pytricia.PyTricia()
        ASes_prefixes_dic = dict()
        
        for index, value in bgp_df.iterrows():
              prefix = index
              peer = value['peer']
              ASpath = value['ASpath'].split(' ')
              originAS = ASpath[-1]
              origin = value['origin']
              
              if prefixes_data_pyt.has_key(prefix): 
                    if originAS not in prefixes_data_pyt[prefix]['originAS']:
                        prefixes_data_pyt[prefix]['originAS'].append(originAS)
                    if peer not in prefixes_data_pyt[prefix]['peer']:
                        prefixes_data_pyt[prefix]['peer'].append(peer)
                    if ASpath not in prefixes_data_pyt[prefix]['ASpath']:
                        prefixes_data_pyt[prefix]['ASpath'].append(ASpath)
                    if origin not in prefixes_data_pyt[prefix]['origin']:
                        prefixes_data_pyt[prefix]['origin'].append(origin)
              else:
                    prefixes_data_pyt[prefix] = dict()
                    prefixes_data_pyt[prefix]['originAS'] = [originAS]
                    prefixes_data_pyt[prefix]['peer'] = [peer]
                    prefixes_data_pyt[prefix]['ASpath'] = [ASpath]
                    prefixes_data_pyt[prefix]['origin'] = [origin]
                   
              if originAS in ASes_prefixes_dic.keys():
                    if prefix not in ASes_prefixes_dic[originAS]:
                        ASes_prefixes_dic[originAS].append(prefix)
              else:
                    ASes_prefixes_dic[originAS] = [prefix]
              
        return prefixes_data_pyt, ASes_prefixes_dic
              
        
    def decodeAndParse(self, decomp_file_name, KEEP):
        readable_file_name = '%s.readable' % decomp_file_name    
        cmd = shlex.split('%s -m -O %s %s' % (bgpdump, readable_file_name, decomp_file_name))
#        cmd = shlex.split('bgpdump -m -O %s %s' % (readable_file_name, decomp_file_name))   
        
        #  BGPDUMP
        #  -m         one-line per entry with unix timestamps
        #  -O <file>  output to <file> instead of STDOUT
        
        subprocess.call(cmd)

        prefixes_data_pyt, ASes_prefixes_dic = self.processReadableDF(readable_file_name)
        
        if not KEEP:
            try:
                os.remove(decomp_file_name)
                os.remove(readable_file_name)
            except OSError:
                pass
        
        return prefixes_data_pyt, ASes_prefixes_dic

    def saveDataToFiles(self, files_path):
        today = datetime.date.today().strftime('%Y%m%d')
        
        pyt_file_name = '%s/prefixes_data_%s.pkl' % (files_path, today)
        with open(pyt_file_name, 'wb') as f:
            pickle.dump(self.prefixes_data_pyt, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing PyTricia with origin ASes for each prefix." % pyt_file_name)

        dic_file_name = '%s/ASes_prefixes_%s.pkl' % (files_path, today)
        with open(dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes announced by each AS." % dic_file_name)
        
        return pyt_file_name, dic_file_name
        
    # TODO create another method to download a process file containing 'show ip bgp' output
    #     http://www.potaroo.net/bgp/as2.0/bgptable.txt