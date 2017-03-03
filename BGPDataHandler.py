#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys
import os, subprocess, shlex, re, gzip
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
import bgp_rib
import pickle
import radix
import datetime, time
import pandas as pd
import hashlib
import ipaddress

# For some reason in my computer os.getenv('PATH') differs from echo $PATH
# /usr/local/bin is not in os.getenv('PATH')
# it also works in matong
bgpdump = '/usr/local/bin/bgpdump'

class BGPDataHandler:
    DEBUG = False
    files_path = ''
    KEEP = False
    RIBfiles = False
    COMPRESSED = False
    
    # Data Frame containing routing info from RIB file or file with 'show ip bgp' output
    bgp_data = pd.DataFrame()
    # Radix indexed by routed IPv4 prefix containing the indexes of the rows in
    # the bgp_data Data Frame that contain info of BGP announcements of the IPv4 prefix
    ipv4_prefixes_indexes_radix = radix.Radix()
    # Radix indexed by routed IPv6 prefix containing the indexes of the rows in
    # the bgp_data Data Frame that contain info of BGP announcements of the IPv6 prefix
    ipv6_prefixes_indexes_radix = radix.Radix()
    # Radix indexed by routed IPv4 prefix containing as values dictionaries
    # with two keys:
    # * periodsSeen - the value for this key is a list of tuples representing
    # periods of time during which the corresponding IPv4 prefix was seen
    # Each tuple has the format (startDate, endDate)
    # * firstSeen - the value for this key is the first date in which the
    # IPv4 prefix was seen
    ipv4_prefixesDates_radix = radix.Radix()
    # Radix indexed by routed IPv6 prefix containing as values dictionaries
    # with two keys:
    # * periodsSeen - the value for this key is a list of tuples representing
    # periods of time during which the corresponding IPv6 prefix was seen
    # Each tuple has the format (startDate, endDate)
    # * firstSeen - the value for this key is the first date in which the
    # IPv6 prefix was seen
    ipv6_prefixesDates_radix = radix.Radix()
    # Dictionary indexed by AS containing all the prefixes originated by each AS
    ASes_originated_prefixes_dic = dict()
    # Dictionary indexed by AS containing all the prefixes propagated by each AS
    ASes_propagated_prefixes_dic = dict()
    # Numeric variable with the longest IPv4 prefix length
    ipv4_longest_pref = -1
    # Numeric variable with the longest IPv6 prefix length
    ipv6_longest_pref = -1   
         
    # When we instantiate this class we set the variable with the path to the
    # folder we will use to store files (files_path), we set a boolean variable
    # specifying whether we want to KEEP the intermediate files generated by 
    # different functions, we set another boolean variable specifying whether
    # the routing files we will be working with are RIB files (if False we assume
    # they are outputs of the 'show ip bgp' command) and we set another boolean
    # variable specifying whether the routing files we will be working with are
    # COMPRESSED
    def __init__(self, DEBUG, files_path, KEEP, RIBfiles, COMPRESSED):
        self.DEBUG = DEBUG
        self.files_path = files_path
        self.KEEP = KEEP
        self.RIBfiles = RIBfiles
        self.COMPRESSED = COMPRESSED
        
        sys.stderr.write("BGPDataHandler instantiated successfully! Remember to load the data structures.\n")
    
    # This function loads the class variables ipv4_prefixesDates and ipv6_prefixesDates
    # from a previously generated pickle file containing Radixes indexed by
    # routed prefix containing as values dictionaries with two keys:
    # * periodsSeen - the value for this key is a list of tuples representing
    # periods of time during which the corresponding prefix was seen
    # Each tuple has the format (startDate, endDate)
    # * firstSeen - the value for this key is the first date in which the
    # prefix was seen
    def loadPrefixDatesFromFiles(self, ipv4_prefixesDates_file, ipv6_prefixesDates_file):
        if ipv4_prefixesDates_file != '':        
            self.ipv4_prefixesDates_radix = pickle.load(open(ipv4_prefixesDates_file, 'rb'))
            sys.stderr.write("Radix with dates in which IPv4 prefixes were seen was loaded successfully!\n")
    
        if ipv6_prefixesDates_file != '':        
            self.ipv6_prefixesDates_radix = pickle.load(open(ipv6_prefixesDates_file, 'rb'))
            sys.stderr.write("Radix with dates in which IPv6 prefixes were seen was loaded successfully!\n")

    
    # This function loads the data structures of the class from previously
    # generated pickle files containing the result of already processed routing data
    def loadStructuresFromFiles(self, bgp_data_file, ipv4_prefixes_indexes_file,\
                                ipv6_prefixes_indexes_file, ASes_originated_prefixes_file,\
                                ASes_propagated_prefixes_file):
     
        self.bgp_data = pickle.load(open(bgp_data_file, "rb"))
        self.ipv4_prefixes_indexes_radix = pickle.load(open(ipv4_prefixes_indexes_file, "rb"))
        self.ipv6_prefixes_indexes_radix = pickle.load(open(ipv6_prefixes_indexes_file, "rb"))
        self.ASes_originated_prefixes_dic = pickle.load(open(ASes_originated_prefixes_file, "rb"))
        self.ASes_propagated_prefixes_dic = pickle.load(open(ASes_propagated_prefixes_file, "rb"))
        self.setLongestPrefixLengths()
        sys.stderr.write("Class data structures were loaded successfully!\n")

        
    # This function processes the routing data contained in the files to which
    # the URLs in the urls_file point, and loads the data structures of the class
    # with the results from this processing
    def loadStructuresFromURLSfile(self, urls_file):
        bgp_data, ipv4_prefixes_indexes_radix, ipv6_prefixes_indexes_radix,\
            ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
            ipv4_longest_pref, ipv6_longest_pref  =\
                        self.processMultipleFiles(urls_file)
                        
        self.bgp_data = bgp_data
        self.ipv4_prefixes_indexes_radix = ipv4_prefixes_indexes_radix
        self.ipv6_prefixes_indexes_radix = ipv6_prefixes_indexes_radix
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

        sys.stderr.write("Class data structures were loaded successfully!\n")

                                                
    # This function processes the routing data contained in the routing_file
    # and loads the data structures of the class with the results from this processing                                           
    def loadStructuresFromRoutingFile(self, routing_file):
        readable_file_name =  self.getReadableFile(routing_file, False)
        bgp_data, ipv4_prefixes_indexes_radix, ipv6_prefixes_indexes_radix,\
            ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
            ipv4_longest_pref, ipv6_longest_pref =\
                                self.processReadableDF(readable_file_name)
                            
        self.bgp_data = bgp_data
        self.ipv4_prefixes_indexes_radix = ipv4_prefixes_indexes_radix
        self.ipv6_prefixes_indexes_radix = ipv6_prefixes_indexes_radix
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

        sys.stderr.write("Class data structures were loaded successfully!\n")

    
    # This function processes the routing data contained in the archive folder
    # provided, and loads the data structures of the class with the results
    # from this processing       
    def loadStructuresFromArchive(self, archive_folder, startDate):
        historical_files = self.getPathsToHistoricalData(archive_folder)
        mostRecent_routing_file = self.getMostRecentFromHistoricalList(historical_files)
       
        readable_file_name =  self.getReadableFile(mostRecent_routing_file, False)
        
        bgp_data, ipv4_prefixes_indexes_radix, ipv6_prefixes_indexes_radix,\
            ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
            ipv4_longest_pref, ipv6_longest_pref =\
                                self.processReadableDF(readable_file_name)
        
        self.bgp_data = bgp_data
        self.ipv4_prefixes_indexes_radix = ipv4_prefixes_indexes_radix
        self.ipv6_prefixes_indexes_radix = ipv6_prefixes_indexes_radix
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

        sys.stderr.write("Class data structures were loaded successfully!\n")

        self.loadPrefixesDates(historical_files, startDate)
        sys.stderr.write("Radix with dates in which prefixes were seen was loaded successfully!\n")
        

    # This function returns a path to the most recent file in the provided list 
    # of historical files
    def getMostRecentFromHistoricalList(self, historical_files):
        files_list_obj = open(historical_files, 'r')

        mostRecentDate = 0
        mostRecentFile = ''
        
        for line in files_list_obj:
            if not line.startswith('#') and line.strip() != '':
                date = self.getDateFromFileName(line.strip())
       
                if date > mostRecentDate:
                    mostRecentDate = date
                    mostRecentFile = line.strip()
        
        return mostRecentFile
        
    def getDateFromFileName(self, filename):
        date = ''
        
        dates = re.findall('[1-2][9,0][0,1,8,9][0-9]-[0-1][0-9]-[0-3][0-9]',\
                    filename)
                    
        if len(dates) > 0:
            date = int(dates[0][0:4]+dates[0][5:7]+dates[0][8:10])
        else:
            dates = re.findall('[1-2][9,0][0,1,8,9][0-9][0-1][0-9][0-3][0-9]',\
                        filename)
            if len(dates) > 0:
                date = int(dates[0])
        return date
    
    # This function loads the prefixesDates Radixes (class variables) which
    # are indexed by prefix and contain as values dictionaries with two keys:
    # * periodsSeen - the value for this key is a list of tuples representing
    # periods of time during which the corresponding prefix was seen
    # Each tuple has the format (startDate, endDate)
    # * firstSeen - the value for this key is the first date in which the
    # prefix was seen
    def loadPrefixesDates(self, historical_files, startDate):

        files_list_obj = open(historical_files, 'r')
        
        i = 0
        for line in files_list_obj:
            if startDate != '':
                file_date = self.getDateFromFileName(line)
                if file_date == '' or int(file_date) < int(startDate):
                    continue
            if not line.startswith('#') and line.strip() != '':
                 # If we work with several routing files
                sys.stderr.write("Starting to work with %s" % line)
                
                if line.strip().endswith('v6.dmp.gz'):
                    prefixesDates = self.ipv6_prefixesDates_radix
                else:
                    prefixesDates = self.ipv4_prefixesDates_radix

                prefixes_list, date = self.getPrefixesAndDate(line.strip())

                for pref in prefixes_list:
                    dateInserted = False
                    pref_node = prefixesDates.search_exact(pref)
                    if pref_node is not None:
                        if date < pref_node.data['firstSeen']:
                            pref_node.data['firstSeen'] = date
                            
                        for period in pref_node.data['periodsSeen']:
                            if date > period[0]:
                                if date < period[1]:
                                    dateInserted = True
                                    continue
                                elif date == period[1]+1:
                                    pref_node.data['periodsSeen'].remove(period)
                                    pref_node.data['periodsSeen'].append((period[0], date))
                                    dateInserted = True
                        if not dateInserted:
                            pref_node.data['periodsSeen'].append((date, date))
                    else:
                        pref_node = prefixesDates.add(pref)
                        pref_node.data['periodsSeen'] = [(date, date)]
                        pref_node.data['firstSeen'] = date
                        
            i += 1
            if self.DEBUG and i > 1:
                break

    # This function returns a list of prefixes for which the routing_file has
    # announcements and the date of the routing file
    # The function searches the routing file name looking for a date in the
    # formats YYYY-MM-DD or YYYYMMDD. If a date is not found in the file name,
    # the date is assumed to be the date of today.
    def getPrefixesAndDate(self, routing_file):
        readable_file_name = self.getReadableFile(routing_file, False)
        dates = re.findall('[1-2][9,0][0,1,8,9][0-9]-[0-1][0-9]-[0-3][0-9]',\
                            readable_file_name)
                            
        if len(dates) > 0:
            date = int(dates[0][0:4]+dates[0][5:7]+dates[0][8:10])
        # If there is no date in the file name, we use the date of today
        else:
            dates = re.findall('[1-2][9,0][0,1,8,9][0-9][0-1][0-9][0-3][0-9]',\
                            readable_file_name)
            if len(dates) > 0:
                date = int(dates[0])
            else:
                date =  datetime.date.today().strftime('%Y%m%d')

        bgp_df = pd.read_table(readable_file_name, header=None, sep='|',\
                                index_col=False, usecols=[1,3,5,6,7],\
                                names=['timestamp',\
                                        'peer',\
                                        'prefix',\
                                        'ASpath',\
                                        'origin'])

        if self.DEBUG:
            bgp_df = bgp_df[0:10]
        
        return set(bgp_df['prefix'].tolist()), date
                                        
        
    # This function downloads and processes all the files in the provided list.
    # The boolean variable containsURLs must be True if the files_list is a list
    # of URLs or False if it is a list of paths
    def processMultipleFiles(self, files_list, containsURLs):
        files_list_obj = open(files_list, 'r')
                    
        bgp_data = pd.DataFrame()
        ipv4_prefixes_indexes_radix = radix.Radix()
        ipv6_prefixes_indexes_radix = radix.Radix()
        ASes_originated_prefixes_dic = dict()
        ASes_propagated_prefixes_dic = dict()
        ipv4_longest_pref = -1
        ipv6_longest_pref = -1
        
        i = 0
        for line in files_list_obj:
            if not line.startswith('#') and line.strip() != '':
                # If we work with several routing files
                sys.stderr.write("Starting to work with %s" % line)

                # We obtain partial data structures
                if containsURLs:
                    readable_file_name =  self.getReadableFile(line.strip(), True)          
                    
                    bgp_data_partial, ipv4_prefixes_indexes_radix_partial,\
                        ipv6_prefixes_indexes_radix_partial,\
                        ASes_originated_prefixes_dic_partial,\
                        ASes_propagated_prefixes_dic_partial,\
                        ipv4_longest_pref_partial, ipv6_longest_pref_partial =\
                                self.processReadableDF(readable_file_name)
                else:
                    readable_file_name =  self.getReadableFile(line.strip(), False)
                    
                    bgp_data_partial, ipv4_prefixes_indexes_radix_partial,\
                        ipv6_prefixes_indexes_radix_partial,\
                        ASes_originated_prefixes_dic_partial,\
                        ASes_propagated_prefixes_dic_partial,\
                        ipv4_longest_pref_partial, ipv6_longest_pref_partial =\
                                self.processReadableDF(readable_file_name)
                
                # and then we merge them into the general data structures
                bgp_data = pd.concat([bgp_data, bgp_data_partial])
    
                for prefix in ipv4_prefixes_indexes_radix_partial.prefixes():
                    node_partial = ipv4_prefixes_indexes_radix_partial.search_exact(prefix)
                    node_gral= ipv4_prefixes_indexes_radix.search_exact(prefix)
                    if node_gral is not None:
                        node_gral.data['indexes'].update(list(node_partial.data['indexes']))
                    else:
                        node_gral.data['indexes'] = node_partial.data['indexes']

                for prefix in ipv6_prefixes_indexes_radix_partial.prefixes():
                    node_partial = ipv6_prefixes_indexes_radix_partial.search_exact(prefix)
                    node_gral= ipv6_prefixes_indexes_radix.search_exact(prefix)
                    if node_gral is not None:
                        node_gral.data['indexes'].update(list(node_partial.data['indexes']))
                    else:
                        node_gral.data['indexes'] = node_partial.data['indexes']
                        
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
                        
                if ipv4_longest_pref_partial > ipv4_longest_pref:
                    ipv4_longest_pref = ipv4_longest_pref_partial
                    
                if ipv6_longest_pref_partial > ipv6_longest_pref:
                    ipv6_longest_pref = ipv6_longest_pref_partial
            
            i += 1
            if self.DEBUG and i > 1:
                break
                        
        files_list_obj.close()
        
        return bgp_data, ipv4_prefixes_indexes_radix, ipv6_prefixes_indexes_radix,\
            ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
            ipv4_longest_pref, ipv6_longest_pref
        
    # This function converts a file containing the output of the 'show ip bgp' command
    # to a file in the same format used for BGPDump outputs
    def convertBGPoutput(self, routing_file):
        output_file_name = '%s/%s.readable' % (self.files_path, '.'.join(routing_file.split('/')[-1].split('.')[:-1]))
        output_file = open(output_file_name, 'w')
        
        i = 0
        # load routing table info  (the next loop does it automatically)
        for entry_n, bgp_entry in enumerate(bgp_rib.BGPRIB.parse_cisco_show_ip_bgp_generator(routing_file)):
            date = bgp_entry[8]
#           date_part = str(date)[0:8]
#           time_part = str(date)[8:12]
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
                sys.stderr.write("Found invalid prefix at bgp entry %s, with content %s, on file %s\n" %(entry_n, bgp_entry, routing_file))
            	# ignore this line and continue
                continue

            # save information

            #the order for each line is
            #TABLE_DUMP2|date|B|nexthop|NextAS|prefix|AS_PATH|Origin
            output_file.write('TABLE_DUMP|'+str(timestamp)[:-2]+'|B|'+next_hop+'|'+nextas+'|'+prefix+'|'+" ".join(as_path)+'|'+origin+'\n')
    
            i += 1
            if self.DEBUG and i > 1:
                break
            
        output_file.close()
        
        return output_file_name
 
    # This function processes a readable file with routing info
    # putting all the info into a Data Frame  
    def processReadableDF(self, readable_file_name):
        
        ipv4_prefixes_indexes_radix = radix.Radix()
        ipv6_prefixes_indexes_radix = radix.Radix()
        ASes_originated_prefixes_dic = dict()
        ASes_propagated_prefixes_dic = dict()
        
        ipv4_longest_prefix = -1
        ipv6_longest_prefix = -1
        
        bgp_df = pd.read_table(readable_file_name, header=None, sep='|',\
                                index_col=False, usecols=[1, 3,5,6,7],\
                                names=['timestamp',\
                                        'peer',\
                                        'prefix',\
                                        'ASpath',\
                                        'origin'])

        if bgp_df.shape[0] > 0:

            if self.DEBUG:
                bgp_df = bgp_df[0:10]
                
            # We create an index that is unique even amongst different routing files
            # so that we can merge partial data structures into a single structure
            file_id = hashlib.md5(readable_file_name).hexdigest()
            bgp_df['source_file'] = '%s_' % file_id
            bgp_df['index'] = bgp_df.index.astype(str)
            bgp_df['index'] = bgp_df['source_file'] + bgp_df['index']
            bgp_df.index = bgp_df['index']
            
             # We add a column to the Data Frame with the corresponding date
            bgp_df['date'] = bgp_df.apply(lambda row: datetime.datetime.fromtimestamp(row['timestamp']).strftime('%Y%m%d'), axis=1)
            
            for index, value in bgp_df.iterrows():
                prefix = value['prefix']
                ASpath = str(value['ASpath']).split(' ')
                originAS = ASpath[-1]
                middleASes = ASpath[:-1]
              
                network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
                if network.version == 4:
                    if network.prefixlen > ipv4_longest_prefix:
                        ipv4_longest_prefix = network.prefixlen
                    
                    ipv4_node = ipv4_prefixes_indexes_radix.search_exact(prefix)
                    if ipv4_node is not None:
                        ipv4_node.data['indexes'].add(index)
                    else:
                        ipv4_node = ipv4_prefixes_indexes_radix.add(prefix)
                        ipv4_node.data['indexes'] = set([index])
                        
                elif network.version == 6:
                    if network.prefixlen > ipv6_longest_prefix:
                        ipv6_longest_prefix = network.prefixlen  
                    
                    ipv6_node = ipv6_prefixes_indexes_radix.search_exact(prefix)
                    if ipv6_node is not None:
                        ipv6_node.data['indexes'].add(index)
                    else:
                        ipv6_node = ipv6_prefixes_indexes_radix.add(prefix)
                        ipv6_node.data['indexes'] = set([index])
                       
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
            
            if not self.KEEP:
                try:
                    os.remove(readable_file_name)
                except OSError:
                    pass
            
        return bgp_df, ipv4_prefixes_indexes_radix, ipv6_prefixes_indexes_radix,\
                ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
                ipv4_longest_prefix, ipv6_longest_prefix

    # This function downloads a routing file if the source provided is a URL
    # If the file is COMPRESSED, it is unzipped
    # and finally it is processed using BGPdump if the file is a RIBfile
    # or using the functions provided by get_rib.py is the file contains the
    # output of the 'show ip bgp' command
    # The path to the resulting readable file is returned
    def getReadableFile(self, source, isURL):
    
        # If a routing file is not provided, download it from the provided URL        
        if isURL:
            routing_file = '%s/%s' % (self.files_path, source.split('/')[-1])
            get_file(source, routing_file)
            source = routing_file
        
        # If the routing file is compressed we unzip it
        if self.COMPRESSED:
            output_file = '%s/%s' % (self.files_path,\
                                os.path.splitext(source)[0].split('/')[-1])
            
            with gzip.open(source, 'rb') as gzip_file,\
                open(output_file, 'wb') as output:
                output.write(gzip_file.read())
            gzip_file.close()
            output.close()
            
            source = output_file 
            
        # If the routing file is a RIB file, we process it using BGPdump
        if self.RIBfiles:            
            today = datetime.date.today().strftime('%Y%m%d')
            readable_file_name = '%s_%s.readable' % (source, today)

            cmd = shlex.split('%s -m -O %s %s' % (bgpdump, readable_file_name, source))
            #        cmd = shlex.split('bgpdump -m -O %s %s' % (readable_file_name, routing_file))   
    
            #  BGPDUMP
            #  -m         one-line per entry with unix timestamps
            #  -O <file>  output to <file> instead of STDOUT
    
            subprocess.call(cmd)
        
        # If the file contains the output of the 'show ip bgp' command,
        # we convert it to the same format used by BGPdump for its outputs
        else:
            readable_file_name = self.convertBGPoutput(source)

        return readable_file_name
           
    # This function walks a folder with historical routing info and creates a
    # file with a list of paths to the .dmp.gz files in the archive folder
    # It returns the path to the created file
    def getPathsToHistoricalData(self, archive_folder):
        files_list_file = '%s/RoutingFiles.txt' % self.files_path
        with open(files_list_file, 'wb') as list_file:
            for root, subdirs, files in os.walk(archive_folder):
                for filename in files:
                    if filename.endswith('dmp.gz'):
                        list_file.write('%s\n' % os.path.join(root, filename))
        list_file.close()
        return files_list_file

    # This function saves the data structures of the class to pickle files
    def saveDataToFiles(self):
        today = datetime.date.today().strftime('%Y%m%d')
        
        bgp_file_name = '%s/bgp_data_%s.pkl' % (self.files_path, today)
        with open(bgp_file_name, 'wb') as f:
            pickle.dump(self.bgp_data, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing DataFrame with BGP data." % bgp_file_name)

        ipv4_radix_file_name = '%s/ipv4_prefixes_indexes_%s.pkl' % (self.files_path, today)
        with open(ipv4_radix_file_name, 'wb') as f:
            pickle.dump(self.ipv4_prefixes_indexes_radix, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing Radix with indexes in the BGP data DataFrame for each IPv4 prefix." % ipv4_radix_file_name)

        ipv6_radix_file_name = '%s/ipv6_prefixes_indexes_%s.pkl' % (self.files_path, today)
        with open(ipv6_radix_file_name, 'wb') as f:
            pickle.dump(self.ipv6_prefixes_indexes_radix, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing Radix with indexes in the BGP data DataFrame for each IPv6 prefix." % ipv6_radix_file_name)

        o_ases_dic_file_name = '%s/ASes_originated_prefixes_%s.pkl' % (self.files_path, today)
        with open(o_ases_dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_originated_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes originated by each AS." % o_ases_dic_file_name)
        
        p_ases_dic_file_name = '%s/ASes_propagated_prefixes_%s.pkl' % (self.files_path, today)
        with open(p_ases_dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_propagated_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes propagated by each AS." % p_ases_dic_file_name)
        
        ipv4_prefDates_file_name = '%s/ipv4_prefixesDates_%s.pkl' % (self.files_path, today)
        with open(ipv4_prefDates_file_name, 'wb') as f:
            pickle.dump(self.ipv4_prefixesDates_radix, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing Radix with the dates in which each IPv4 prefix was seen." % ipv4_prefDates_file_name)
        
        ipv6_prefDates_file_name = '%s/ipv6_prefixesDates_%s.pkl' % (self.files_path, today)
        with open(ipv6_prefDates_file_name, 'wb') as f:
            pickle.dump(self.ipv6_prefixesDates_radix, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing Radix with the dates in which each IPv6 prefix was seen." % ipv6_prefDates_file_name)
            
        return bgp_file_name, ipv4_radix_file_name, ipv6_radix_file_name,\
                o_ases_dic_file_name, p_ases_dic_file_name,\
                ipv4_prefDates_file_name, ipv6_prefDates_file_name

    # This function sets the ipv4_longest_pref and ipv6_longest_pref class variables
    # with the corresponding maximum prefix lengths in the ipv4_prefixes_indexes
    # and ipv6_prefixes_indexes Radixes
    def setLongestPrefixLengths(self):
        for prefix in self.ipv4_prefixes_indexes_radix.prefixes():
            network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
            
            if network.prefixlen > self.ipv4_longest_pref:
                self.ipv4_longest_pref = network.prefixlen
                
        for prefix in self.ipv6_prefixes_indexes_radix.prefixes():
            network = ipaddress.ip_network(unicode(prefix, 'utf-8'))

            if network.prefixlen > self.ipv6_longest_pref:
                self.ipv6_longest_pref = network.prefixlen
                
    # This function returns a list of prefixes less specific than the one provided
    # that are included in the keys of the corresponding Radix
    def getRoutedParentAndGrandparents(self, prefix):
        network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
        
        if network.version == 4:
            indexes_radix = self.ipv4_prefixes_indexes_radix
        else:
            indexes_radix = self.ipv6_prefixes_indexes_radix
            
        less_specifics = []
       
        for less_spec_node in indexes_radix.search_covering(prefix):
            less_spec_pref = less_spec_node.prefix
        
            if less_spec_pref != prefix:
                less_specifics.append()
            
        return less_specifics
    
    # This function returns a list of prefixes more specific than the one provided
    # that are included in the keys of the corresponding Radix
    def getRoutedChildren(self, prefix):
        network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
        
        if network.version == 4:
            indexes_radix = self.ipv4_prefixes_indexes_radix
        else:
            indexes_radix = self.ipv6_prefixes_indexes_radix
            
        more_specifics = []
       
        for more_spec_node in indexes_radix.search_covered(prefix):
            more_specifics.append(more_spec_node.prefix)
                        
        return more_specifics
        
    # This function returns the origin AS for a specific prefix
    # according to the routing data included in the BGP_data class variable
    def getOriginASForBlock(self, prefix):
        network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
        
        if network.version == 4:
            indexes_radix = self.ipv4_prefixes_indexes_radix
        else:
            indexes_radix = self.ipv6_prefixes_indexes_radix
            
        originASes = set()
        for index in indexes_radix.search_exact(prefix).data['indexes']:
            originAS = self.bgp_data.ix[index, 'ASpath'].split(' ')[-1]
            originASes.add(originAS)
        
        if len(originASes) > 1:
            print "Found prefix originated by more than one AS (%s)" % prefix
            # TODO Analyze these special cases
            # Geoff says there are a lot of these
            # I have already asked him what to do in these cases
        if len(originASes) > 0:
            return list(originASes)[0]
        else:
            return None
    
    # This function returns a set with all the AS paths for a specific prefix
    # according to the routing data included in the BGP_data class variable
    def getASpathsForBlock(self, prefix):
        network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
        
        if network.version == 4:
            indexes_radix = self.ipv4_prefixes_indexes_radix
        else:
            indexes_radix = self.ipv6_prefixes_indexes_radix
            
        ASpaths = set()
        for index in indexes_radix.search_exact(prefix).data['indexes']:
            ASpaths.add(self.bgp_data.ix[index, 'ASpath'])
        
        return ASpaths