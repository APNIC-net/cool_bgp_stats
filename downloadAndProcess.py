#! /usr/bin/python

import os, subprocess, shlex, signal

dest_dir = '/Users/sofiasilva/Documents/APNIC/BGP_Project/BGP_files/'
file_name = 'bview.20170112.0800'

url = 'http://data.ris.ripe.net/rrc00/2017.01/%s.gz' % file_name

cmd = shlex.split('wget -q -4 -nv -nc -P %s %s' % (dest_dir, url))

#  -q,  --quiet                     quiet (no output)
#  -4,  --inet4-only                connect only to IPv4 addresses
#  -nv, --no-verbose                turn off verboseness, without being quiet
#  -nc, --no-clobber                skip downloads that would download to
#                                      existing files (overwriting them)
#  -P,  --directory-prefix=PREFIX   save files to PREFIX/..

process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
output, error = process.communicate()

cmd = shlex.split('gunzip -k %s%s.gz' % (dest_dir, file_name))

#  GUNZIP
#  -k --keep            don't delete input files during operation

process = subprocess.Popen(cmd, stdout=subprocess.PIPE)
output, error = process.communicate()

cmd2 = shlex.split('bgpdump -m -')

#  BGPDUMP
#  -m         one-line per entry with unix timestamps

decomp_file_obj = open('%s%s' % (dest_dir, file_name), 'r')
 
process2 = subprocess.Popen(cmd2, stdin=decomp_file_obj, stdout=subprocess.PIPE)

while True:
  line = process2.stdout.readline()
  if line != '':
    print "DEBUG:", line.rstrip()
  else:
    break


decomp_file_obj.close()

try:
    os.remove('%s%s' % (dest_dir, file_name))
except OSError:
    pass

