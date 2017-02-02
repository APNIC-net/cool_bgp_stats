#! /usr/bin/python2.7 
# -*- coding: utf8 -*-

import sys, getopt
import os, subprocess, shlex
import re
from get_file import get_file

# Just for DEBUG
#project_path = '/Users/sofiasilva/GitHub/cool_bgp_stats'
#sys.path.append(project_path)
#from get_file import get_file

# TODO Check if bgpdump is in a folder included in the path
# If it is not, include it or provide path to the binary
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
    
def decodeAndProcess(decomp_file_name, KEEP):
    readable_file_name = '%s.readable' % decomp_file_name    
    cmd = shlex.split('%s -m -O %s %s' % (bgpdump, readable_file_name, decomp_file_name))

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

def main(argv):
    
    url = ''
    files_path = ''
    routing_file = ''
    KEEP = False
    
    files_path = '/Users/sofiasilva/BGP_files'
    
    # TODO By now we work with a specific file.
    # In the future I have to list the contents of the folder and look for the most recent file?
    file_name = 'bview.20170112.0800'
    url = 'http://data.ris.ripe.net/rrc00/2017.01/%s.gz' % file_name
    
    try:
        opts, args = getopt.getopt(argv, "hu:r:kp:", ["url=", "routing_file=", "files_path="])
    except getopt.GetoptError:
        print 'Usage: downloadAndProcess.py -h | [-u <url>] [-k] -p <files path>'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script downloads a file with Internet routing data, uncompresses it and decodes it using BGPDump"
            print 'Usage: downloadAndProcess.py -h | [-u <year>] -p <files path>'
            print 'h = Help'
            print 'u = URL of the file to be downloaded.'
            print 'r = Use specific Internet Routing data file.'
            print 'k = Keep downloaded Internet routing data file.'
            print "p = Path to folder in which files will be saved. (MANDATORY)"
            sys.exit()
        elif opt == '-u':
            url = arg
        elif opt == '-r':
            routing_file = arg
        elif opt == '-k':
            KEEP = True
        elif opt == '-p':
            files_path = arg
        else:
            assert False, 'Unhandled option'
            
    if url == '':
        print "You must provide the URL from which you want a file to be downloaded."
        sys.exit()
        
    if files_path == '':
        print "You must provide a folder to save files."
        sys.exit()
        
    decomp_file_name = downloadAndUnzip(url, files_path, routing_file, KEEP)
    decodeAndProcess(decomp_file_name)
    
        
if __name__ == "__main__":
    main(sys.argv[1:])
