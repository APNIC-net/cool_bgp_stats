# -*- coding: utf-8 -*-
"""
Created on Tue May 30 09:00:15 2017

@author: sofiasilva
"""
import os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
import sys, getopt
from datetime import datetime, date, timedelta
import re
from collections import defaultdict
from VisibilityDBHandler import VisibilityDBHandler
from glob import glob

today = datetime.today().date()

def createSQLFile(item, suffix, existing_dates, files_path, generated_dates):
    sql_file = '{}/{}_{}.sql'.format(files_path, item, today)
        
    suffixes = ['v4andv6', 'v4', 'v6']
    
    if item == 'prefixes':
        table_name = item
        columns = '(prefix, dateseen)'
    elif item == 'originASes' or item == 'middleASes':
        table_name = 'asns'
        columns = '(asn, isorigin, dateseen)'
    elif item == 'routing_data':
        table_name = 'routing_data'
        columns = '(routing_date, extension, file_path)'
        suffixes = [suffix]
        
    with open(sql_file, 'wb') as f:
        f.write('\connect sofia\n')
        
        for suffix in suffixes:            
            for existing_file in glob('{}/{}_{}_*.csv'.format(files_path, item, suffix)):
                file_date = getDateFromFileName(existing_file)
                
                if file_date not in existing_dates and\
                    (file_date not in generated_dates or\
                    not generated_dates[file_date][suffix]):
                    f.write("copy {} {} from '{}' WITH (FORMAT csv);\n".format(table_name, columns, existing_file))
                    
                    if file_date not in generated_dates:
                        generated_dates[file_date] = defaultdict(bool)
                    
                    generated_dates[file_date][suffix] = True
                    
                    if item != 'routing_data' and suffix == 'v4andv6':
                        generated_dates[file_date]['v4'] = True
                        generated_dates[file_date]['v6'] = True

    return sql_file, generated_dates

def createSQLFileForUpdates(files_path):
    sql_file = '{}/updates_{}.sql'.format(files_path, today)
        
    with open(sql_file, 'wb') as f:
        f.write('\connect sofia\n')

        for extension in ['bgpupd.mrt', 'log.gz']:
            for existing_file in glob('{}/*{}.csv'.format(files_path, extension)):
                f.write('''copy updates (update_date, update_time, upd_type,
                            bgp_neighbor, peeras, prefix, source_file) from
                            '{}' WITH (FORMAT csv);\n'''.format(existing_file))

    return sql_file
    
def getDateFromFileName(filename):        
    dates = re.findall('(?P<year>[1-2][9,0][0,1,8,9][0-9])[-_]*(?P<month>[0-1][0-9])[-_]*(?P<day>[0-3][0-9])',\
                filename)
                
    if len(dates) > 0:
        file_date = '{}{}{}'.format(dates[0][0], dates[0][1], dates[0][2])
        return datetime.strptime(file_date, '%Y%m%d').date()
    else:
        return None
        
def checkMissing(item, output_file, completeDatesSet, existingInDB,
                 generated_dates):
                     
    with open(output_file, 'a') as output:
        for day in completeDatesSet:
            inDB = (day in existingInDB)
            
            if day in generated_dates:
                generated_v4 = generated_dates[day]['v4']
                generated_v6 = generated_dates[day]['v6']
                generated_v4andv6 = generated_dates[day]['v4andv6']

            else:
                generated_v4 = False
                generated_v6 = False
                generated_v4andv6  = False

            output.write('{}|{}|{}|{}|{}|{}\n'.format(day, item, inDB,
                                                 generated_v4, generated_v6,
                                                 generated_v4andv6))
            
            if not inDB:
                if not generated_v4:
                    sys.stdout.write('v4 {} not ready for date {}'.format(item, day))
                if not generated_v6:
                    sys.stdout.write('v6 {} not ready for date {}'.format(item, day))
                if not generated_v4andv6:
                    sys.stdout.write('v4andv6 {} not ready for date {}'.format(item, day))

def checkMissingRoutingData(suffix, output_file, completeDatesSet, existingInDB,
                            generated_dates):
                     
    with open(output_file, 'a') as output:
        for day in completeDatesSet:
            inDB = (day in existingInDB)
            
            if day in generated_dates:
                generated = generated_dates[day][suffix]
            else:
                generated = False

            output.write('{}|{}|{}|{}\n'.format(day, suffix, inDB,
                                                         generated))
            
            if not inDB and not generated:
                sys.stdout.write('{} routing data not ready for date {}\n'.format(suffix, day))
    
def main(argv):
    files_path = '/home/sofia/BGP_stats_files/CSVsAndCTLs'
    data_type = 'visibility'
    
    try:
        opts, args = getopt.getopt(argv,"hp:t:", ['files_path=', 'data_type=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> -t <visibility/routing/updates>'.format(sys.argv[0])
        print "p: Provide the path to the folder where the CSV files are."
        print "By default /home/sofia/BGP_stats_files/CSVsAndCTLs is used."
        print "t: Data type. Choose between 'visibility', 'routing' and 'updates'."
        print "By default 'visibility' is used."
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files path> -t <visibility/routing/updates>'.format(sys.argv[0])
            print "p: Provide the path to the folder where the CSV files are."
            print "By default /home/sofia/BGP_stats_files/CSVsAndCTLs is used."
            print "t: Data type. Choose between 'visibility', 'routing' and 'updates'."
            print "By default 'visibility' is used."
            sys.exit()
        elif opt == '-p':
            files_path = os.path.abspath(arg)
        elif opt == '-t':
            data_type = arg
        else:
            assert False, 'Unhandled option'
            
    if data_type != 'visibility' and data_type != 'routing' and data_type != 'updates':
        print "Wrong data type. You MUST choose between 'visibility', 'routing' and 'updates'."
        sys.exit(-1)
        
    output_file = '{}/generateSQLfiles_{}_{}.output'.format(files_path,
                                                            data_type,
                                                            date.today())

    db_handler = VisibilityDBHandler()

    initial_date = date(2007, 6, 11)
    final_date = date.today()
    numOfDays = (final_date - initial_date).days
    completeDatesSet = set([final_date - timedelta(days=x) for x in range(0, numOfDays)])
        
    if data_type == 'visibility':
        
        with open(output_file, 'a') as output:
            output.write('Date|Resource_type|inDB|v4_generated|v6_generated\n')
        
        existing_dates_prefixes = set(db_handler.getListOfDatesForPrefixes())
        prefixesSQLfile, generated_dates_prefixes = createSQLFile('prefixes', '',
                                                         existing_dates_prefixes,
                                                         files_path, dict())

        checkMissing('prefixes', output_file, completeDatesSet,
                     existing_dates_prefixes, generated_dates_prefixes)
        
        sys.stdout.write('{} was generated for the insertion of prefixes.\n'.format(\
                        prefixesSQLfile))
        
        existing_dates_originASes = set(db_handler.getListOfDatesForOriginASes())
        originASesSQLfile, generated_dates_originASes =  createSQLFile('originASes', '',
                                                                       existing_dates_originASes,
                                                                       files_path,
                                                                       dict())
        
        checkMissing('originASes', output_file, completeDatesSet,
                     existing_dates_originASes, generated_dates_originASes)
                     
        sys.stdout.write('{} was generated for the insertion of Origin ASes.\n'.format(\
                        originASesSQLfile))
        
        existing_dates_middleASes = set(db_handler.getListOfDatesForMiddleASes())
        middleASesSQLfile, generated_dates_middleASes = createSQLFile('middleASes', '',
                                                                      existing_dates_middleASes,
                                                                      files_path,
                                                                      dict())
        checkMissing('middleASes', output_file, completeDatesSet,
                     existing_dates_middleASes, generated_dates_middleASes)
                     
        sys.stdout.write('{} was generated for the insertion of Middle ASes.\n'.format(\
                        middleASesSQLfile))
                            
    elif data_type == 'routing':
        with open(output_file, 'a') as output:
            output.write('Date|Type|inDB|Generated\n')
            
        existing_dates_bgprib = set(db_handler.getListOfDatesForRoutingData_v4andv6())
        bgpribSQLfile, generated_dates_bgprib = createSQLFile('routing_data', 'v4andv6',
                                                          existing_dates_bgprib,
                                                          files_path,
                                                          dict())
                                                          
        checkMissingRoutingData('v4andv6', output_file, completeDatesSet,
                                existing_dates_bgprib, generated_dates_bgprib)
                     
        sys.stdout.write('{} was generated for the insertion of routing data from bgprib files.\n'.format(\
                        bgpribSQLfile))

        existing_dates_dmp = set(db_handler.getListOfDatesForRoutingData_v4Only())
        dmpSQLfile, generated_dates_dmp = createSQLFile('routing_data', 'v4',
                                                          existing_dates_dmp,
                                                          files_path,
                                                          dict())
                                                          
        checkMissingRoutingData('v4', output_file, completeDatesSet,
                                existing_dates_dmp, generated_dates_dmp)
                     
        sys.stdout.write('{} was generated for the insertion of routing data from dmp (v4 only) files.\n'.format(\
                        dmpSQLfile))

        existing_dates_v6dmp = set(db_handler.getListOfDatesForRoutingData_v6Only())
        v6dmpSQLfile, generated_dates_v6dmp = createSQLFile('routing_data', 'v6',
                                                          existing_dates_v6dmp,
                                                          files_path,
                                                          dict())
                                                          
        checkMissingRoutingData('v6', output_file, completeDatesSet,
                                existing_dates_v6dmp, generated_dates_v6dmp)
                     
        sys.stdout.write('{} was generated for the insertion of routing data from v6.dmp (v6 only) files.\n'.format(\
                        v6dmpSQLfile))
                        
    elif data_type == 'updates':
        updatesSQLfile = createSQLFileForUpdates(files_path)
        
        sys.stdout.write('{} was generated for the insertion of BGP updates.\n'.format(\
                        updatesSQLfile))
                        
    sys.stdout.write('Report about execution in {}\n'.format(output_file))
    
if __name__ == "__main__":
    main(sys.argv[1:])