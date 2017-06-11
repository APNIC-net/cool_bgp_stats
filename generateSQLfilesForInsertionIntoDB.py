# -*- coding: utf-8 -*-
"""
Created on Tue May 30 09:00:15 2017

@author: sofiasilva
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
import sys, getopt
import datetime
import re
from VisibilityDBHandler import VisibilityDBHandler

def createSQLFile(item, existing_dates, files_path):
    sql_file = '{}/{}.sql'.format(files_path, item)
    
    generated_dates = set()
    
    if item == 'prefixes':
        table_name = item
    else:
        table_name = 'asns'
        
    with open(sql_file, 'wb') as f:
        f.write('\connect sofia\n')

        for existing_file in os.listdir(files_path):
            file_date = getDateFromFileName(existing_file)
            if existing_file.endswith('.csv') and\
                existing_file.startswith(item) and\
                file_date not in existing_dates:
                f.write("copy {} from '{}/{}' WITH (FORMAT csv);\n".format(table_name, files_path, existing_file))
                generated_dates.add(file_date)

    return sql_file, generated_dates

def getDateFromFileName(filename):        
    dates = re.findall('(?P<year>[1-2][9,0][0,1,8,9][0-9])[-_]*(?P<month>[0-1][0-9])[-_]*(?P<day>[0-3][0-9])',\
                filename)
                
    if len(dates) > 0:
        file_date = '{}{}{}'.format(dates[0][0], dates[0][1], dates[0][2])
        return datetime.datetime.strptime(file_date, '%Y%m%d').date()
    else:
        return None
    
def main(argv):
    files_path = '/home/sofia/BGP_stats_files/CSVsAndCTLs'

    try:
        opts, args = getopt.getopt(argv,"hp:", ['files_path=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path>'.format(sys.argv[0])
        print "p: Provide the path to the folder where the CSV files are."
        print "By default /home/sofia/BGP_stats_files/CSVsAndCTLs is used."
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files path>'.format(sys.argv[0])
            print "p: Provide the path to the folder where the CSV files are."
            print "By default /home/sofia/BGP_stats_files/CSVsAndCTLs is used."
            sys.exit()
        elif opt == '-p':
            files_path = os.path.abspath(arg)
        else:
            assert False, 'Unhandled option'

    db_handler = VisibilityDBHandler()

    existing_dates_prefixes = set(db_handler.getListOfDatesForPrefixes())
    prefixesSQLfile, generated_dates_prefixes = createSQLFile('prefixes',
                                                     existing_dates_prefixes,
                                                     files_path)
    
    sys.stdout.write('{} was generated for the insertion of prefixes.\n'.format(\
                    prefixesSQLfile))
    
    existing_dates_originASes = set(db_handler.getListOfDatesForOriginASes())
    originASesSQLfile, generated_dates_originASes =  createSQLFile('originASes',
                                                                   existing_dates_originASes,
                                                                   files_path)
    
    sys.stdout.write('{} was generated for the insertion of Origin ASes.\n'.format(\
                    originASesSQLfile))
    
    existing_dates_middleASes = set(db_handler.getListOfDatesForMiddleASes())
    middleASesSQLfile, generated_dates_middleASes = createSQLFile('middleASes',
                                                                  existing_dates_middleASes,
                                                                  files_path)
                                                                  
    sys.stdout.write('{} was generated for the insertion of Middle ASes.\n'.format(\
                    middleASesSQLfile))
    
if __name__ == "__main__":
    main(sys.argv[1:])