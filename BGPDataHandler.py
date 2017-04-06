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
from calendar import timegm
from datetime import datetime, date, timedelta
import pandas as pd
import ipaddress
from VisibilityDBHandler import VisibilityDBHandler

# For some reason in my computer os.getenv('PATH') differs from echo $PATH
# /usr/local/bin is not in os.getenv('PATH')
# it also works in matong
bgpdump = '/usr/local/bin/bgpdump'

class BGPDataHandler:
    DEBUG = False
    files_path = ''
    KEEP = False
  
    # STRUCTURES WITH CURRENT ROUTING DATA
    # Radix indexed by routed IPv4 prefix containing the routing data from the
    # routing file being considered
    ipv4Prefixes_radix = radix.Radix()

    # Radix indexed by routed IPv6 prefix containing the routing data from the
    # routing file being considered
    ipv6Prefixes_radix = radix.Radix()
    
    # Dictionary indexed by AS containing all the prefixes originated by each AS
    ASes_originated_prefixes_dic = dict()

    # Dictionary indexed by AS containing all the prefixes propagated by each AS
    ASes_propagated_prefixes_dic = dict()

    # Numeric variable with the longest IPv4 prefix length
    ipv4_longest_pref = -1

    # Numeric variable with the longest IPv6 prefix length
    ipv6_longest_pref = -1  
    
    routingDate = ''
         
    # When we instantiate this class we set a boolean variable specifying
    # whether we will be working on DEBUG mode, we also set the variable with
    # the path to the folder we will use to store files (files_path) and
    # we set a boolean variable specifying whether we want to KEEP the
    # intermediate files generated by different functions
    def __init__(self, DEBUG, files_path, KEEP):
        self.DEBUG = DEBUG
        self.files_path = files_path
        self.KEEP = KEEP        

        sys.stderr.write("BGPDataHandler instantiated successfully! Remember to load the data structures.\n")
    
   
    # This function loads the data structures of the class from previously
    # generated pickle files containing the result of already processed routing data
    def loadStructuresFromFiles(self, date, ipv4_prefixes_file, ipv6_prefixes_file,\
                                ASes_originated_prefixes_file,\
                                ASes_propagated_prefixes_file):
     
        self.routingDate = date
        self.ipv4Prefixes_radix = pickle.load(open(ipv4_prefixes_file, "rb"))
        self.ipv6Prefixes_radix = pickle.load(open(ipv6_prefixes_file, "rb"))
        self.ASes_originated_prefixes_dic = pickle.load(open(ASes_originated_prefixes_file, "rb"))
        self.ASes_propagated_prefixes_dic = pickle.load(open(ASes_propagated_prefixes_file, "rb"))
        self.setLongestPrefixLengths()
        sys.stderr.write("Class data structures were loaded successfully!\n")
        return True
    
    # This function processes the routing data contained in the files to which
    # the URLs in the urls_file point, and loads the data structures of the class
    # with the results from this processing
    def loadStructuresFromURLSfile(self, urls_file, READABLE, RIBfiles):
        date, ipv4Prefixes_radix, ipv6Prefixes_radix,\
            ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
            ipv4_longest_pref, ipv6_longest_pref  =\
                        self.processMultipleFiles(files_list=urls_file,\
                                                isList=False, containsURLs=True,\
                                                RIBfiles=RIBfiles, areReadable=READABLE)
                    
        self.routingDate = date
        self.ipv4Prefixes_radix = ipv4Prefixes_radix
        self.ipv6Prefixes_radix = ipv6Prefixes_radix
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
        return True

    # This function processes the routing data contained in the routing_file
    # and loads the data structures of the class with the results from this processing                                           
    def loadStructuresFromRoutingFile(self, routing_file, READABLE, RIBfile, COMPRESSED):
        if not READABLE:
            readable_file_name =  self.getReadableFile(routing_file, False, RIBfile, COMPRESSED)
        else:
            readable_file_name = routing_file
        
        if readable_file_name != '':
            date, ipv4Prefixes_radix, ipv6Prefixes_radix,\
                ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
                ipv4_longest_pref, ipv6_longest_pref =\
                                    self.processReadableDF(readable_file_name)
        
            self.routingDate = date              
            self.ipv4Prefixes_radix = ipv4Prefixes_radix
            self.ipv6Prefixes_radix = ipv6Prefixes_radix
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
            return True
        else:
            sys.stderr.write("Could not process routing file.\n")
            return False

    # This function loads the data structures of the class with the routing
    # data contained in the archive folder corresponding to the routing
    # date provided or to the most recent date present in the archive
    def loadStructuresFromArchive(self, archive_folder, extension, routing_date,
                                  READABLE, RIBfiles, COMPRESSED):
     
        historical_files = ''
        
        if routing_date == '':
            historical_files = self.getPathsToHistoricalData(archive_folder,
                                                         extension, '', '')
        
            if historical_files == '':
                sys.stderr.write("Archive is empty!\n")
                return False

            routing_files  =\
                        self.getMostRecentsFromHistoricalList(historical_files)
        else:
            routing_files = self.getSpecificFilesFromArchive(archive_folder,
                                                             extension,
                                                             routing_date)
            if len(routing_files) == 0:
                historical_files = self.getPathsToHistoricalData(archive_folder,
                                                         extension, '', '')
        
                if historical_files == '':
                    sys.stderr.write("Archive is empty!\n")
                    return False

                routing_files = self.getSpecificFilesFromHistoricalList(historical_files,
                                                                        routing_date)
                if len(routing_files) == 0:
                    sys.stderr.write("There is no routing file in the archive for the date provided.\n")
                    return False
                    
        date, ipv4Prefixes_radix, ipv6Prefixes_radix,\
            ASes_originated_prefixes_dic, ASes_propagated_prefixes_dic,\
            ipv4_longest_pref, ipv6_longest_pref  =\
                        self.processMultipleFiles(files_list=routing_files,
                                                isList=True, containsURLs=False,
                                                RIBfiles=RIBfiles, areReadable=READABLE,
                                                COMPRESSED=COMPRESSED)
                    
        self.routingDate = date
        self.ipv4Prefixes_radix = ipv4Prefixes_radix
        self.ipv6Prefixes_radix = ipv6Prefixes_radix
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

        if not self.KEEP:
            try:
                os.remove(historical_files)
            except OSError:
                pass
            
        return True
        
        
    def storeHistoricalDataFromArchive(self, archive_folder, extension,\
                                        READABLE, RIBfiles, COMPRESSED,\
                                        startDate, endDate):
        historical_files = self.getPathsToHistoricalData(archive_folder,
                                                         extension, startDate,
                                                         endDate)
        if historical_files == '':
            sys.stderr.write("There are no routing files in the archive that meet the criteria (extension and period of time).")
        else:
            self.storeHistoricalData(historical_files, False, READABLE, RIBfiles,
                                     COMPRESSED)
            sys.stderr.write("Historical data inserted into visibility database successfully!\n")
   
        if not self.KEEP:
            try:
                os.remove(historical_files)
            except OSError:
                pass
    
    # This function returns a list of paths to the routing files from the files
    # in the archive corresponding to the provided date
    def getSpecificFilesFromArchive(self, archive_folder, extension, routing_date):
        month = str(routing_date.month)
        if len(month) == 1:
            month = '0{}'.format(month)
        
        # We have to add 1 to the day of the provided date as the files in the
        # archive contain routing data corresponding to the day before to the
        # date specified in the file name.
        day = str(routing_date.day + 1)
        if len(day) == 1:
            day = '0{}'.format(day)
        
        routing_folder = '{}/{}/{}/{}'.format(archive_folder, routing_date.year,
                                                month, day)
        routing_files = []
        try:
            for item in os.listdir(routing_folder):
                if item.endswith(extension):
                    routing_files.append(os.path.join(routing_folder, item))
        except OSError:
            return []
        
        return routing_files
        
        
    # This function returns a path to the routing file from the provided list 
    # of historical files corresponding to the provided date
    def getSpecificFilesFromHistoricalList(self, historical_files, routing_date):
        files_list_obj = open(historical_files, 'r')
        
        routing_files = []
        for line in files_list_obj:
            if not line.startswith('#') and line.strip() != '':
                date = self.getDateFromFileName(line.strip())
                
                # We have to add 1 daye to the provided date as the files in the
                # archive contain routing data corresponding to the day before to the
                # date specified in the file name.
                if date == routing_date + timedelta(1):
                   routing_files.append(line.strip())
        
        return routing_files
        
    # This function returns a path to the most recent file in the provided list 
    # of historical files
    def getMostRecentFromHistoricalList(self, historical_files):
        files_list_obj = open(historical_files, 'r')

        mostRecentDate = datetime.strptime('1970', '%Y').date()
        mostRecentFiles = []
        
        for line in files_list_obj:
            if not line.startswith('#') and line.strip() != '':
                date = self.getDateFromFileName(line.strip())
                
                if date > mostRecentDate:
                    # We add 1 to the endDate because the files in the archive
                    # have routing data for the day before of the date in the
                    # name of the file
                    mostRecentDate = date
                    mostRecentFiles = [line.strip()]
                elif date == mostRecentDate:
                    mostRecentFiles.append(line.strip())
        
        return mostRecentFiles
        
    def getDateFromFileName(self, filename):
        date = ''
        
        dates = re.findall('[1-2][9,0][0,1,8,9][0-9]-[0-1][0-9]-[0-3][0-9]',\
                    filename)
                    
        if len(dates) > 0:
            date = dates[0][0:4]+dates[0][5:7]+dates[0][8:10]
        else:
            dates = re.findall('[1-2][9,0][0,1,8,9][0-9][0-1][0-9][0-3][0-9]',\
                        filename)
            if len(dates) > 0:
                date = dates[0]
        return datetime.strptime(date, '%Y%m%d').date()
    
    # This function stores the routing data from the files listed in the
    # historical_files file skipping the mostRecent routing file provided,
    # as the data contained in this file has already been stored.
    def storeHistoricalData(self, historical_files, isList, areReadable,
                            RIBfiles, COMPRESSED):

        if not isList:
            files_list_obj = open(historical_files, 'r')
        else:
            files_list_obj = historical_files
        
        i = 0
        for line in files_list_obj:
            line = line.strip()
                    
            if not line.startswith('#') and line != '':
                 # If we work with several routing files
                sys.stderr.write("Starting to work with %s\n" % line)

                self.storeHistoricalDataFromFile(line, areReadable, RIBfiles, COMPRESSED)
                        
            i += 1
            if self.DEBUG and i > 1:
                break
                    
    # This function stores the routing data from the routing_file provided
    # into the visibility database
    def storeHistoricalDataFromFile(self, routing_file, isReadable, RIBfile, COMPRESSED):
        prefixes, originASes, middleASes, date =\
                        self.getPrefixesASesAndDate(routing_file, isReadable,\
                                                    RIBfile, COMPRESSED)
        visibilityDB = VisibilityDBHandler(date)

        visibilityDB.storeListOfPrefixesSeen(prefixes, date)
        
        cleanOriginASes = []
        for originAS in originASes:
            if originAS is None or originAS == 'nan':
                continue
            elif '{' in str(originAS):
                # If the asn field contains a bracket ({}), there is an as-set
                # in first place in the AS path, therefore, we split it
                # (leaving the brackets out) and consider each AS separately.
                cleanOriginASes.extend(originAS.replace('{', '').replace('}', '').split(','))

            else:
                cleanOriginASes.append(originAS)
                
        visibilityDB.storeListOfASesSeen(list(set(cleanOriginASes)), True, date)

        cleanMiddleASes = []                
        for middleAS in middleASes:
            if middleAS is None or middleAS == 'nan':
                continue
            elif '{' in str(middleAS):
                cleanMiddleASes.extend(middleAS.replace('{', '').replace('}', '').split(','))

            else:
                cleanMiddleASes.append(middleAS)
        
        visibilityDB.storeListOfASesSeen(list(set(cleanMiddleASes)), False, date)
        
        visibilityDB.close()

    
    # This function returns a list of prefixes for which the routing_file has
    # announcements, a list of the origin ASes included in the routing_file,
    # a list of the middle ASes included in the routing file
    # and the date of the routing file.
    # The routing file is assumed to include routing data for a single day,
    # therefore the date is taken from the timestamp of the first row in the
    # bgp_df DataFrame.
    def getPrefixesASesAndDate(self, routing_file, isReadable, RIBfile, COMPRESSED):
        if not isReadable:
            readable_file_name = self.getReadableFile(routing_file, False,\
                                                        RIBfile, COMPRESSED)
        else:
            readable_file_name = routing_file
        
        if readable_file_name == '':
            return [], [], ''

        bgp_df = pd.read_table(readable_file_name, header=None, sep='|',\
                                index_col=False, usecols=[1,3,5,6,7],\
                                names=['timestamp',\
                                        'peer',\
                                        'prefix',\
                                        'ASpath',\
                                        'origin'])

        if self.DEBUG:
            bgp_df = bgp_df[0:10]
            
        date = datetime.utcfromtimestamp(bgp_df['timestamp'].tolist()[0]).strftime('%Y%m%d')
        
        # To get the origin ASes and middle ASes we split the ASpath column
        paths_parts = bgp_df.ASpath.str.rsplit(' ', n=1, expand=True)

        return set(bgp_df['prefix'].tolist()),\
                set(paths_parts[1].tolist()),\
                set([item for sublist in paths_parts[0].tolist() for item in\
                        str(sublist).split()]), date
                                        
        
    # This function downloads and processes all the files in the provided list.
    # The boolean variable containsURLs must be True if the files_list is a list
    # of URLs or False if it is a list of paths
    def processMultipleFiles(self, files_list, isList, containsURLs, RIBfiles,\
                                areReadable, COMPRESSED):
        if not isList:
            files_list = open(files_list, 'r')
                    
        ipv4Prefixes_radix = radix.Radix()
        ipv6Prefixes_radix = radix.Radix()
        ASes_originated_prefixes_dic = dict()
        ASes_propagated_prefixes_dic = dict()
        ipv4_longest_pref = -1
        ipv6_longest_pref = -1
        routingDate = datetime.strptime('1970', '%Y').date()
        
        i = 0
        for line in files_list:
            if not line.startswith('#') and line.strip() != '':
                # If we work with several routing files
                sys.stderr.write("Starting to work with %s\n" % line)

                # We obtain partial data structures
                if containsURLs:
                    if not areReadable:
                        readable_file_name =  self.getReadableFile(line.strip(), True, RIBfiles, COMPRESSED)          
                    
                        if readable_file_name == '':
                            continue
                    else:
                        readable_file_name = line.strip()
                    
                    date, ipv4Prefixes_radix_partial,\
                        ipv6Prefixes_radix_partial,\
                        ASes_originated_prefixes_dic_partial,\
                        ASes_propagated_prefixes_dic_partial,\
                        ipv4_longest_pref_partial, ipv6_longest_pref_partial =\
                                self.processReadableDF(readable_file_name)
                else:
                    if not areReadable:
                        readable_file_name =  self.getReadableFile(line.strip(), False, RIBfiles, COMPRESSED)
                    
                        if readable_file_name == '':
                            continue
                    else:
                        readable_file_name = line.strip()
                    
                    date, ipv4Prefixes_radix_partial,\
                        ipv6Prefixes_radix_partial,\
                        ASes_originated_prefixes_dic_partial,\
                        ASes_propagated_prefixes_dic_partial,\
                        ipv4_longest_pref_partial, ipv6_longest_pref_partial =\
                                self.processReadableDF(readable_file_name)
                
                if date > routingDate:
                    routingDate = date
                    
                for prefix in ipv4Prefixes_radix_partial.prefixes():
                    node_partial = ipv4Prefixes_radix_partial.search_exact(prefix)
                    node_gral= ipv4Prefixes_radix.search_exact(prefix)
                    if node_gral is not None:
                        node_gral.data['OriginASes'].update(node_partial.data['OriginASes'])
                        node_gral.data['ASpaths'].update(node_partial.data['ASpaths'])
                    else:
                        node_gral = ipv4Prefixes_radix.add(prefix)
                        node_gral.data['OriginASes'] = node_partial.data['OriginASes']
                        node_gral.data['ASpaths'] = node_partial.data['ASpaths']

                for prefix in ipv6Prefixes_radix_partial.prefixes():
                    node_partial = ipv6Prefixes_radix_partial.search_exact(prefix)
                    node_gral= ipv6Prefixes_radix.search_exact(prefix)
                    if node_gral is not None:
                        node_gral.data['OriginASes'].update(node_partial.data['OriginASes'])
                        node_gral.data['ASpaths'].update(node_partial.data['ASpaths'])
                    else:
                        node_gral = ipv6Prefixes_radix.add(prefix)
                        node_gral.data['OriginASes'] = node_partial.data['OriginASes']
                        node_gral.data['ASpaths'] = node_partial.data['ASpaths']
                        
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

        if not isList:        
            files_list.close()
        
        return routingDate, ipv4Prefixes_radix, ipv6Prefixes_radix,\
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
            timestamp = timegm(datetime.strptime(date, "%Y%m%d%H%M").timetuple())
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
            if self.DEBUG and i > 10:
                break
            
        output_file.close()
        
        return output_file_name
 
    # This function processes a readable file with routing info
    # putting all the info into a Data Frame  
    def processReadableDF(self, readable_file_name):
        
        date = ''
        ipv4Prefixes_radix = radix.Radix()
        ipv6Prefixes_radix = radix.Radix()
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

             # We add a column to the Data Frame with the corresponding date
            bgp_df['date'] = bgp_df.apply(lambda row: datetime.utcfromtimestamp(row['timestamp']).date(), axis=1)
            date = max(bgp_df['date'])
            
            ASpath_parts = bgp_df.ASpath.str.rsplit(' ', n=1, expand=True)
            bgp_df['middleASes'] = ASpath_parts[0]
            bgp_df['originAS'] = ASpath_parts[1]
            
            for prefix, prefix_subset in bgp_df.groupby('prefix'):
                network = ipaddress.ip_network(unicode(prefix, 'utf-8'))
                if network.version == 4:
                    if network.prefixlen > ipv4_longest_prefix:
                        ipv4_longest_prefix = network.prefixlen
                    prefixes_radix = ipv4Prefixes_radix
                    
                else:
                    if network.prefixlen > ipv6_longest_prefix:
                        ipv6_longest_prefix = network.prefixlen 
                    prefixes_radix = ipv6Prefixes_radix
                    
                node = prefixes_radix.add(prefix)
                node.data['ASpaths'] = set(prefix_subset['ASpath'])
                node.data['OriginASes'] = set(prefix_subset['originAS'])
                            
            for middleASes, middleASes_subset in  bgp_df.groupby('middleASes'):
                for asn in middleASes.split():
                    if asn is None or asn == 'nan':
                        continue
                    elif '{' in str(asn):
                        # If the asn field contains a bracket ({}), there is an as-set
                        # in first place in the AS path, therefore, we split it
                        # (leaving the brackets out) and consider each AS separately.
                        asnList = asn.replace('{', '').replace('}', '').split(',')
                        for asn in asnList:
                            asn = int(asn)
                            prefixes = set(middleASes_subset['prefix'].tolist())
                            if asn in ASes_propagated_prefixes_dic.keys():
                                ASes_propagated_prefixes_dic[asn] =\
                                    ASes_propagated_prefixes_dic[asn].union(prefixes)
                            else:
                                ASes_propagated_prefixes_dic[asn] = prefixes
                    else:
                        asn = int(asn)
                        prefixes = set(middleASes_subset['prefix'].tolist())
                        if asn in ASes_propagated_prefixes_dic.keys():
                            ASes_propagated_prefixes_dic[asn] =\
                                ASes_propagated_prefixes_dic[asn].union(prefixes)
                        else:
                            ASes_propagated_prefixes_dic[asn] = prefixes
                
            for originAS, originAS_subset in bgp_df.groupby('originAS'):
                if originAS is None or originAS == 'nan':
                    continue
                elif '{' in str(originAS):
                    # If the asn field contains a bracket ({}), there is an as-set
                    # in first place in the AS path, therefore, we split it
                    # (leaving the brackets out) and consider each AS separately.
                    asnList = originAS.replace('{', '').replace('}', '').split(',')
                    for originAS in asnList:
                        originAS = int(originAS)
                        ASes_originated_prefixes_dic[originAS] = set(originAS_subset['prefix'])
                else:
                    originAS = int(originAS)
                    ASes_originated_prefixes_dic[originAS] = set(originAS_subset['prefix'])
        
            if not self.KEEP:
                try:
                    os.remove(readable_file_name)
                except OSError:
                    pass
            
        return date, ipv4Prefixes_radix, ipv6Prefixes_radix, ASes_originated_prefixes_dic,\
                ASes_propagated_prefixes_dic, ipv4_longest_prefix, ipv6_longest_prefix

    # This function downloads a routing file if the source provided is a URL
    # If the file is COMPRESSED, it is unzipped
    # and finally it is processed using BGPdump if the file is a RIBfile
    # or using the functions provided by get_rib.py is the file contains the
    # output of the 'show ip bgp' command
    # The path to the resulting readable file is returned
    def getReadableFile(self, source, isURL, RIBfile, COMPRESSED):
    
        source_filename = source.split('/')[-1]
        
        # If a routing file is not provided, download it from the provided URL        
        if isURL:
            routing_file = '%s/%s' % (self.files_path, source_filename)
            get_file(source, routing_file)
            source = routing_file
        
        # If the routing file is compressed we unzip it
        if COMPRESSED:
            output_file = '%s/%s' % (self.files_path,\
                                os.path.splitext(source)[0].split('/')[-1])
            
            with gzip.open(source, 'rb') as gzip_file,\
                open(output_file, 'wb') as output:
                try:
                    output.write(gzip_file.read())
                except IOError:
                    return ''
            gzip_file.close()
            output.close()
            
            source = output_file 
            
        # If the routing file is a RIB file, we process it using BGPdump
        if RIBfile:            
            readable_file_name = '%s/%s.readable' % (self.files_path, os.path.splitext(source_filename)[0])

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
    # file with a list of paths to the files with the provided extension
    # in the archive folder
    # It returns the path to the created file
    def getPathsToHistoricalData(self, archive_folder, extension, startDate, endDate):
        dateStr = 'UNTIL{}'.format(endDate)
        
        if startDate != '':
            dateStr = 'SINCE{}{}'.format(startDate, dateStr)
            
        files_list_filename = '{}/RoutingFiles_{}.txt'.format(self.files_path, dateStr)
        
        files_list_list = []
        
        for root, subdirs, files in os.walk(archive_folder):
            for filename in files:
                if filename.endswith(extension):
                    file_date = self.getDateFromFileName(filename)
                    if (endDate != '' and file_date <= endDate + timedelta(1) or endDate == '') and\
                        (startDate != '' and file_date > startDate or startDate == ''):
                        files_list_list.append(os.path.join(root, filename))
        
        if len(files_list_list) == 0:
            return ''
        else:
            files_list_list_sorted = sorted(files_list_list)
            
            with open(files_list_filename, 'wb') as files_list:
                for filename in files_list_list_sorted:
                    files_list.write("%s\n" % filename)

            return files_list_filename

    # This function saves the data structures of the class to pickle files
    def saveDataToFiles(self):
        today = date.today().strftime('%Y%m%d')
        
        ipv4_radix_file_name = '%s/ipv4Prefixes_%s.pkl' % (self.files_path, today)
        with open(ipv4_radix_file_name, 'wb') as f:
            pickle.dump(self.ipv4Prefixes_radix, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing Radix with routing data for each IPv4 prefix.\n" % ipv4_radix_file_name)

        ipv6_radix_file_name = '%s/ipv6Prefixes_%s.pkl' % (self.files_path, today)
        with open(ipv6_radix_file_name, 'wb') as f:
            pickle.dump(self.ipv6Prefixes_radix, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing Radix with routing data for each IPv6 prefix.\n" % ipv6_radix_file_name)

        o_ases_dic_file_name = '%s/ASes_originated_prefixes_%s.pkl' % (self.files_path, today)
        with open(o_ases_dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_originated_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes originated by each AS.\n" % o_ases_dic_file_name)
        
        p_ases_dic_file_name = '%s/ASes_propagated_prefixes_%s.pkl' % (self.files_path, today)
        with open(p_ases_dic_file_name, 'wb') as f:
            pickle.dump(self.ASes_propagated_prefixes_dic, f, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary with prefixes propagated by each AS.\n" % p_ases_dic_file_name)
        
        return ipv4_radix_file_name, ipv6_radix_file_name,\
                o_ases_dic_file_name, p_ases_dic_file_name

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
    def getRoutedParentAndGrandparents(self, network):        
        if network.version == 4:
            prefixes_radix = self.ipv4Prefixes_radix
        else:
            prefixes_radix = self.ipv6Prefixes_radix
            
        less_specifics = []
       
        for less_spec_node in prefixes_radix.search_covering(str(network)):
            less_spec_pref = less_spec_node.prefix
        
            if less_spec_pref != str(network):
                less_specifics.append(less_spec_pref)
            
        return less_specifics
    
    # This function returns a list of prefixes more specific than the one provided
    # that are included in the keys of the corresponding Radix
    def getRoutedChildren(self, network):
        if network.version == 4:
            prefixes_radix = self.ipv4Prefixes_radix
        else:
            prefixes_radix = self.ipv6Prefixes_radix
            
        more_specifics = []
       
        for more_spec_node in prefixes_radix.search_covered(str(network)):
            more_specifics.append(more_spec_node.prefix)
                        
        return more_specifics
        
    # This function returns the origin AS for a specific prefix
    # according to the routing data included in the BGP_data class variable
    def getOriginASesForBlock(self, network):        
        if network.version == 4:
            prefixes_radix = self.ipv4Prefixes_radix
        else:
            prefixes_radix = self.ipv6Prefixes_radix

        pref_node = prefixes_radix.search_exact(str(network))
        if pref_node is not None:
            return set(pref_node.data['OriginASes'])
        else:
            return set()
    
    # This function returns a set with all the AS paths for a specific prefix
    # according to the routing data included in the BGP_data class variable
    def getASpathsForBlock(self, network):
        if network.version == 4:
            prefixes_radix = self.ipv4Prefixes_radix
        else:
            prefixes_radix = self.ipv6Prefixes_radix
            
        pref_node = prefixes_radix.search_exact(str(network))
        if pref_node is not None:
            return pref_node.data['ASpaths']
        else:
            return set()