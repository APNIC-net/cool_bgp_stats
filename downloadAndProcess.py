#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import os, subprocess, shlex
import re
# Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
from DelegatedHandler import DelegatedHandler
import ipaddress
import math

# For some reason in my computer os.getenv('PATH') differs from echo $PATH
# /usr/local/bin is not in os.getenv('PATH')
bgpdump = '/usr/local/bin/bgpdump'

   
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
    
def decodeAndParse(decomp_file_name, KEEP):
    readable_file_name = '%s.readable' % decomp_file_name    
    cmd = shlex.split('%s -m -O %s %s' % (bgpdump, readable_file_name, decomp_file_name))
#    cmd = shlex.split('bgpdump -m -O %s %s' % (readable_file_name, decomp_file_name))   
    
    #  BGPDUMP
    #  -m         one-line per entry with unix timestamps
    #  -O <file>  output to <file> instead of STDOUT
    subprocess.call(cmd)
    
    # For DEBUG
#    readable_file_name = '%s.test' % readable_file_name
    
    readable_file_obj = open(readable_file_name, 'r')

    prefixes_dic = dict()
    
    for line in readable_file_obj.readlines():
        pattern = re.compile("^TABLE_DUMP.?\|\d+\|B\|(.+?)\|.+?\|(.+?)\|(.+?)\|(.+?)\|.+")
        s = pattern.search(line)
    	
        if s:
#            peer = s.group(1)
            prefix = s.group(2)
            path = s.group(3)
            originAS = path.split(' ')[-1]
#            origin = s.group(4)
            if prefix in prefixes_dic.keys():
                if originAS not in prefixes_dic[prefix]:
                    prefixes_dic[prefix].extend([originAS])
            else:
                prefixes_dic[prefix] = [originAS]

    readable_file_obj.close()
    
    if not KEEP:
        try:
            os.remove(decomp_file_name)
            os.remove(readable_file_name)
        except OSError:
            pass
    
    return prefixes_dic
    
def computeRoutingStats(url, files_path, routing_file, KEEP):
    decomp_file_name = downloadAndUnzip(url, files_path, routing_file, KEEP)
    prefixes_ASes_dic = decodeAndParse(decomp_file_name)
    
    # For DEBUG
    DEBUG = True
    EXTENDED = True
    del_file = '/Users/sofiasilva/BGP_files/extended_apnic_20170201.txt'
    INCREMENTAL = False
    final_existing_date = ''
    year = 2016
    
    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, INCREMENTAL, final_existing_date, year)

    ipv4_subset = del_handler.delegated_df[del_handler.delegated_df['resource_type'] == 'ipv4']
    ipv6_subset = del_handler.delegated_df[del_handler.delegated_df['resource_type'] == 'ipv6']
    
    for index, row in ipv4_subset.iterrows():
        initial_ip = ipaddress.ip_address(unicode(row['initial_resource'], "utf-8"))
        count = int(row['count'])
        prefix_len = int(32 - math.log(count, 2))
        ip_network = ipaddress.ip_network('%s/%s' % (initial_ip, prefix_len))
        final_ip = initial_ip + count - 1
        
        if ip_network.broadcast_address != final_ip:
            print "Not a CIDR block"
            print initial_ip
            print count
            # TODO Convertir a bloque CIDR (Ver cómo!)
            # crear líneas nuevas en ipv4_subset copiando el resto de la info
        
        

def main(argv):
    
    urls_file = ''
    files_path = ''
    routing_file = ''
    KEEP = False
    
    #For DEBUG
    files_path = '/Users/sofiasilva/BGP_files'
    urls_file = './Collectors.txt'     # TODO modificar URL para usar colector de APNIC
    routing_file = '/Users/sofiasilva/BGP_files/bview.20170112.0800.gz'
    KEEP = True

    
    try:
        opts, args = getopt.getopt(argv, "hu:r:kp:", ["urls_file=", "routing_file=", "files_path="])
    except getopt.GetoptError:
        print 'Usage: downloadAndProcess.py -h | -u <urls file> [-r <routing file>] [-k] -p <files path>'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script downloads a file with Internet routing data, uncompresses it and decodes it using BGPDump"
            print 'Usage: downloadAndProcess.py -h | -u <urls file> [-k] -p <files path>'
            print 'h = Help'
            print 'u = URLs file. File which contains a list of URLs of the files to be downloaded.'
            print 'r = Use already downloaded Internet Routing data file.'
            print 'k = Keep downloaded Internet routing data file.'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            sys.exit()
        elif opt == '-u':
            urls_file = arg
        elif opt == '-r':
            routing_file = arg
        elif opt == '-k':
            KEEP = True
        elif opt == '-p':
            files_path = arg
        else:
            assert False, 'Unhandled option'
            
    if urls_file == '':
        print "You must provide the path to a file with the URLs of the files to be downloaded."
        sys.exit()
        
    if files_path == '':
        print "You must provide the path to a folder to save files."
        sys.exit()
    
    if routing_file == '':
        urls_file_obj = open(urls_file, 'r')
    
        for line in urls_file_obj:
            sys.stderr.write("Starting to work with %s" % line)
            computeRoutingStats(line.strip(), files_path, routing_file, KEEP)           
        
        urls_file_obj.close()
        
    else:
        computeRoutingStats('', files_path, routing_file, KEEP)
    
        
if __name__ == "__main__":
    main(sys.argv[1:])
