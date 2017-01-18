#! /usr/bin/python

import re


dump_file = '/Users/sofiasilva/Documents/APNIC/BGP_Project/BGP_files/dump_test.txt'

dump_file_obj = open(dump_file, 'r')

for line in dump_file_obj.readlines():
	pattern = re.compile("^TABLE_DUMP.?\|\d+\|B\|(.+?)\|.+?\|(.+?)\|(.+?)\|(.+?)\|.+")

	s = pattern.search(line)
	if s:
		peer = s.group(1)
		prefix = s.group(2)
		path = s.group(3)
		originAS = path.split(' ')[-1]
		origin = s.group(4)

		print "Prefix %s is originated by %s" % (prefix, originAS)		
