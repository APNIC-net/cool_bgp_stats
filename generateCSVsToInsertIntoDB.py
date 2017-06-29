# -*- coding: utf-8 -*-
"""
Created on Mon May 22 16:18:28 2017

@author: sofiasilva
"""
import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler
from DBHandler import DBHandler
from time import time
from datetime import datetime, date, timedelta
import csv, re
from collections import defaultdict
from glob import glob
                       
def getDatesOfExistingCSVs(files_path, data_type, dates_ready):
    if data_type == 'visibility':
        for item in ['prefixes', 'originASes', 'middleASes']:
            for existing_file in glob('{}/{}*.csv'.format(files_path, item)):
                routing_date = getDateFromFileName(existing_file)

                for v in ['v4andv6', '_v4_', '_v6_']:    
                    if v in existing_file:
                        if routing_date not in dates_ready:
                            dates_ready[routing_date] = dict()
                        
                        if item not in dates_ready[routing_date]:
                            dates_ready[routing_date][item] = defaultdict(bool)
                            
                        dates_ready[routing_date][item][v.replace('_', '')] = True

    elif data_type == 'routing':
        for existing_file in glob('{}/routing_data*.csv'.format(files_path)):
            routing_date = getDateFromFileName(existing_file)

            for v in ['_v4andv6_', '_v4_', '_v6_']:
                if v in existing_file:
                    if routing_date not in dates_ready:
                        dates_ready[routing_date] = defaultdict(bool)
                    dates_ready[routing_date]['routing_{}'\
                                            .format(v.replace('_', ''))] = True
                
    return dates_ready
            
def generateFilesFromReadables(readables_path, data_type, dates_ready,\
                                files_path, bgp_handler, output_file,
                                archive_folder):

    # We look for readable files coming from bgprib.mrt files
    for readable_file in glob('{}/*.bgprib.readable'.format(readables_path)):
        routing_date = getDateFromFile(readable_file, output_file, bgp_handler)
        
        if routing_date is not None and (routing_date not in dates_ready or\
        ((data_type == 'visibility' and (routing_date in dates_ready and\
        ('prefixes' not in dates_ready[routing_date] or\
        not dates_ready[routing_date]['prefixes']['v4andv6'] or\
        'originASes' not in dates_ready[routing_date] or\
        not dates_ready[routing_date]['originASes']['v4andv6'] or\
        'middleASes' not in dates_ready[routing_date] or\
        not dates_ready[routing_date]['middleASes']['v4andv6']))) or\
        (data_type == 'routing' and routing_date in dates_ready and\
        not dates_ready[routing_date]['routing_v4andv6']))):
            dates_ready, csv_files = generateFilesFromRoutingFile(files_path,
                                                                   readable_file,
                                                                   bgp_handler,
                                                                   data_type,
                                                                   dates_ready,
                                                                   output_file,
                                                                   archive_folder)

    # We look for readable files coming from v6.dmp.gz files
    for readable_file in glob('{}/*.v6.dmp.readable'.format(readables_path)):
        routing_date = getDateFromFile(readable_file, output_file, bgp_handler)
        
        if routing_date is not None and (routing_date not in dates_ready or\
        (data_type == 'visibility' and (routing_date in dates_ready and\
        ('prefixes' not in dates_ready[routing_date] or\
        not dates_ready[routing_date]['prefixes']['v6'] or\
        'originASes' not in dates_ready[routing_date] or\
        not dates_ready[routing_date]['originASes']['v6'] or\
        'middleASes' not in dates_ready[routing_date] or\
        not dates_ready[routing_date]['middleASes']['v6']))) or\
        (data_type == 'routing' and routing_date in dates_ready and\
        not dates_ready[routing_date]['routing_v6'])):
            dates_ready, csv_files = generateFilesFromRoutingFile(files_path,
                                                                   readable_file,
                                                                   bgp_handler,
                                                                   data_type,
                                                                   dates_ready,
                                                                   output_file,
                                                                   archive_folder)

    # We look for the format used for readable files that come from dmp.gz files
    pattern = re.compile('.*20[0,1][0-9]-[0,1][0-9]-[0-3][0-9].dmp.readable$')
    for readable_file in glob('{}/*.dmp.readable'.format(readables_path)):
        if pattern.match(readable_file) is not None:
            routing_date = getDateFromFile(readable_file, output_file, bgp_handler)

            if routing_date is not None and (routing_date not in dates_ready or\
            (data_type == 'visibility' and (routing_date in dates_ready and\
            ('prefixes' not in dates_ready[routing_date] or\
            not dates_ready[routing_date]['prefixes']['v4'] or\
            'originASes' not in dates_ready[routing_date] or\
            not dates_ready[routing_date]['originASes']['v4'] or\
            'middleASes' not in dates_ready[routing_date] or\
            not dates_ready[routing_date]['middleASes']['v4']))) or\
            (data_type == 'routing' and routing_date in dates_ready and\
            not dates_ready[routing_date]['routing_v4'])):
                dates_ready, csv_files = generateFilesFromRoutingFile(files_path,
                                                                       readable_file,
                                                                       bgp_handler,
                                                                       data_type,
                                                                       dates_ready,
                                                                       output_file,
                                                                       archive_folder)

    return dates_ready
   
def generateFilesFromOtherRoutingFiles(archive_folder, data_type, dates_ready,
                                       files_path, bgp_handler, proc_num,
                                       extension, output_file):
    
    yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                        4:[2014, 2015], 5:[2016, 2017]}
                        
    if extension == 'bgprib.mrt':
        suffix = 'v4andv6'

    # Routing files in the archive folder for dates that haven't been
    # inserted into the DB yet
    for root, subdirs, files in os.walk(archive_folder):
        for filename in files:
            if filename.endswith(extension):
                file_date = getDateFromFileName(filename)
                
                if extension == 'dmp.gz':
                    if 'v6' in filename:
                        suffix = 'v6'
                    else:
                        suffix = 'v4'
    
                # For paralelization we check for the year of the file, so that
                # different files are processed by different scripts
                if file_date.year in yearsForProcNums[proc_num]:
    
                    full_filename = os.path.join(root, filename)
    
                    routing_date = getDateFromFile(full_filename, output_file,
                                                   bgp_handler)
                    
                    if routing_date is not None and\
                        (routing_date not in dates_ready or\
                        (data_type == 'visibility' and\
                        ('prefixes' not in dates_ready[routing_date] or\
                        not dates_ready[routing_date]['prefixes'][suffix] or\
                        'originASes' not in dates_ready[routing_date] or\
                        not dates_ready[routing_date]['originASes'][suffix] or\
                        'middleASes' not in dates_ready[routing_date] or\
                        not dates_ready[routing_date]['middleASes'][suffix])) or\
                        (data_type == 'routing' and\
                        (not dates_ready[routing_date]['routing_{}'.format(suffix)]))):
                            
                        dates_ready, csv_files = generateFilesFromRoutingFile(\
                                                                     files_path,
                                                                     full_filename,
                                                                     bgp_handler,
                                                                     data_type,
                                                                     dates_ready,
                                                                     output_file,
                                                                     archive_folder)

    return dates_ready


def writeCSVfiles(file_path, tuples):
    with open(file_path, 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(tuples)

def generateFilesForItem(item_name, suffix, item_list, files_path,\
                            routing_date, dates_ready):
    file_path = ''
    
    if len(item_list) > 0 and (routing_date not in dates_ready or\
        item_name not in dates_ready[routing_date] or\
        not dates_ready[routing_date][item_name][suffix]):

        start = time()
        
        if item_name == 'prefixes':
            tuples = zip(item_list, [routing_date]*len(item_list))
        elif item_name == 'originASes':
            tuples = zip(item_list, [True]*len(item_list), [routing_date]*len(item_list))
        else: # item_name == 'middleASes'
            tuples = zip(item_list, [False]*len(item_list), [routing_date]*len(item_list))
            
        file_path = '{}/{}_{}_{}.csv'.format(files_path, item_name, suffix, routing_date)
        
        if not os.path.exists(file_path):
            writeCSVfiles(file_path, tuples)
        
            end = time()
            sys.stdout.write('It took {} seconds to generate the CSV files for the insertion of {} ({}) for {}.\n'.format(end-start, item_name, suffix, routing_date))
        
            if routing_date not in dates_ready:
                dates_ready[routing_date] = dict()
            
            if item_name not in dates_ready[routing_date]:
                dates_ready[routing_date][item_name] = defaultdict(bool)
                
            dates_ready[routing_date][item_name][suffix] = True
            
            if suffix == 'v4andv6':
                dates_ready[routing_date][item_name]['v4'] = True
                dates_ready[routing_date][item_name]['v6'] = True
            
    return dates_ready, file_path
    
def generateFilesFromRoutingFile(files_path, routing_file, bgp_handler,\
                                    data_type, dates_ready, output_file,
                                    archive_folder):
                                        
    csvs_list = []
    
    sys.stdout.write('Starting to work with {}\n'.format(routing_file))
    
    if 'bgprib' in routing_file:
        suffix = 'v4andv6'
        extension = 'bgprib.mrt'
    elif 'v6' in routing_file:
        suffix = 'v6'
        extension = 'v6.dmp.gz'
    else:
        suffix = 'v4'
        extension = 'dmp.gz'
            
    if data_type == 'visibility':
        routing_date = getDateFromFile(routing_file, output_file, bgp_handler)

        prefixes_csv = ''
        originASes_csv = ''
        middleASes_csv = ''
        
        if routing_date is not None:
            if routing_date.year == 1970:
                os.remove(routing_file)
                # If the year is 1970, the timestamp was wrongly converted when
                # creating the readable file, that's why we remove the file.
                
                file_date = getDateFromFileName(routing_file)
                
                new_routing_file = '{}/{}/{}/{}/{}-{}-{}.{}'\
                                    .format(archive_folder, file_date.year,
                                    file_date.strftime('%m'),
                                    file_date.strftime('%d'), file_date.year,
                                    file_date.strftime('%m'),
                                    file_date.strftime('%d'), extension)
                dates_ready, csv_files = generateFilesFromRoutingFile(files_path,
                                                                     new_routing_file,
                                                                     bgp_handler,
                                                                     data_type,
                                                                     dates_ready,
                                                                     output_file,
                                                                     archive_folder)
            
            if (routing_date not in dates_ready or\
                'prefixes' not in dates_ready[routing_date] or\
                'originASes' not in dates_ready[routing_date] or\
                'middleASes' not in dates_ready[routing_date] or\
                not dates_ready[routing_date]['prefixes'][suffix] or\
                not dates_ready[routing_date]['originASes'][suffix] or\
                not dates_ready[routing_date]['middleASes'][suffix]):
                    
                start = time()
                prefixes, originASes, middleASes, routing_date =\
                                    bgp_handler.getPrefixesASesAndDate(routing_file)
                end = time()
                sys.stdout.write('It took {} seconds to get the lists of prefixes, origin ASes and middle ASes for {}.\n'.format(end-start, routing_date))
                
                try:
                    dates_ready, prefixes_csv = generateFilesForItem('prefixes',
                                                                     suffix,
                                                                     prefixes,
                                                                     files_path,
                                                                     routing_date,
                                                                     dates_ready)
                    
                except KeyboardInterrupt:
                    sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
                    dates_ready, prefixes_csv = generateFilesForItem('prefixes',
                                                                     suffix,
                                                                     prefixes,
                                                                     files_path,
                                                                     routing_date,
                                                                     dates_ready)
                                                                
                    dates_ready, originASes_csv = generateFilesForItem('originASes',
                                                                       suffix,
                                                                       originASes,
                                                                       files_path,
                                                                       routing_date,
                                                                       dates_ready)
    
                    dates_ready, middleASes_csv = generateFilesForItem('middleASes',
                                                                       suffix,
                                                                       middleASes,
                                                                       files_path,
                                                                       routing_date,
                                                                       dates_ready)
                            
                    sys.exit(0)
            
    
                try:
                    dates_ready, originASes_csv = generateFilesForItem('originASes',
                                                                       suffix,
                                                                       originASes,
                                                                       files_path,
                                                                       routing_date,
                                                                       dates_ready)
            
                except KeyboardInterrupt:
                    sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
                    dates_ready, originASes_csv = generateFilesForItem('originASes',
                                                                       suffix,
                                                                       originASes,
                                                                       files_path,
                                                                       routing_date,
                                                                       dates_ready)
                    
                    dates_ready, middleASes_csv = generateFilesForItem('middleASes',
                                                                       suffix,
                                                                       middleASes,
                                                                       files_path,
                                                                       routing_date,
                                                                       dates_ready)
            
                    sys.exit(0)
            
                try:
                    dates_ready, middleASes_csv = generateFilesForItem('middleASes',
                                                                       suffix,
                                                                       middleASes,
                                                                       files_path,
                                                                       routing_date,
                                                                       dates_ready)
            
                except KeyboardInterrupt:
                    sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
                    dates_ready, middleASes_csv = generateFilesForItem('middleASes',
                                                                       suffix,
                                                                       middleASes,
                                                                       files_path,
                                                                       routing_date,
                                                                       dates_ready)
    
                    sys.exit(0)

            csvs_list = [prefixes_csv, originASes_csv, middleASes_csv]
                        
    else: # data_type == 'routing'        
        routing_date = getDateFromFile(routing_file, output_file,
                                       bgp_handler)
        
        if routing_date is not None and (routing_date not in dates_ready or\
            not dates_ready[routing_date]['routing_{}'.format(suffix)]):
            
            # If the routing file does not come from the archive
            if not routing_file.startswith(archive_folder):
                file_date = getDateFromFileName(routing_file)
                file_date_str = str(file_date)
                year_str = file_date_str[0:4]
                month_str = file_date_str[5:7]
                day_str = file_date_str[8:10]
    
                original_file_woExt = '{}/{}/{}/{}/{}-{}-{}'.format(archive_folder,
                                                                    year_str,
                                                                    month_str,
                                                                    day_str,
                                                                    year_str,
                                                                    month_str,
                                                                    day_str)
                                                                    
                original_file = '{}.{}'.format(original_file_woExt, extension)
    
            else:
                original_file = routing_file
            
            csv_file = '{}/routing_data_{}_{}.csv'.format(files_path, suffix,
                                                            routing_date)
            
            with open(csv_file, 'a') as csv_f:
                csv_f.write('"{}","{}","{}"\n'.format(routing_date, extension,
                                                    original_file))
            
            if routing_date not in dates_ready:
                dates_ready[routing_date] = defaultdict(bool)
                
            dates_ready[routing_date]['routing_{}'.format(suffix)] = True
            
            csvs_list = [csv_file]
              
    return dates_ready, csvs_list

def getDateFromReadableFile(file_path, output_file):
    with open(file_path, 'rb') as readable_file:
        first_line = readable_file.readline()
        
        try:
            timestamp = float(first_line.split('|')[1])
            return datetime.utcfromtimestamp(timestamp).date()
        except IndexError:
            timestamp = ''
            for i in range(5):
                line = readable_file.readline()
                try:
                    timestamp = float(line.split('|')[1])
                    break
                except IndexError:
                    continue
            
            if timestamp == '':
                with open(output_file, 'a') as output:
                    output.write('Cannot get date from content of file {}\n'.format(file_path))
                    
                    file_size = os.path.getsize(file_path)
                    if file_size == 0:
                        output.write('File {} is empty. Deleting it.\n'.format(file_path))
                        os.remove(file_path)
                return None
            else:
                return datetime.utcfromtimestamp(timestamp).date()

                
# We assume the routing files have routing info for a single date,
# therefore we get the routing date from the first line of the file.
def getDateFromFile(file_path, output_file, bgp_handler):
    if 'readable' in file_path:       
        return getDateFromReadableFile(file_path, output_file)

    else:
        first_line = bgp_handler.getReadableFirstLine(file_path, False)
        
        try:
            timestamp = float(first_line.split('|')[1])
            return datetime.utcfromtimestamp(timestamp).date()
            
        except IndexError:
            readable_file = bgp_handler.getReadableFile(file_path, False)
            return getDateFromReadableFile(readable_file, output_file)
        
def getDateFromFileName(filename):        
    dates = re.findall('(?P<year>[1-2][9,0][0,1,8,9][0-9])[-_]*(?P<month>[0-1][0-9])[-_]*(?P<day>[0-3][0-9])',\
                filename)
                
    if len(dates) > 0:
        file_date = '{}{}{}'.format(dates[0][0], dates[0][1], dates[0][2])
        return datetime.strptime(file_date, '%Y%m%d').date()
    else:
        return None
        
def getCompleteDatesSet(proc_num):
    yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                        4:[2014, 2015], 5:[2016, 2017]}
    initial_date = date(yearsForProcNums[proc_num][0], 1, 1)
    final_date = date(yearsForProcNums[proc_num][-1], 12, 31)
    
    if final_date > date.today():
        final_date = date.today()
        
    numOfDays = (final_date - initial_date).days
    return set([final_date - timedelta(days=x) for x in range(0, numOfDays)])
    
def main(argv):
    routing_file = ''
    readables_path = ''
    archive_folder = '/data/wattle/bgplog'
    proc_num = -1
    data_type = 'visibility'
    DEBUG = False

    try:
        opts, args = getopt.getopt(argv,"ht:A:f:n:D", ['data_type=', 'archive_folder=', 'procNumber=', 'routingFile=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -t <visibility/routing> (-A <archive folder> -n <process number> | -f <readable routing file>) [-D]'.format(sys.argv[0])
        print "t: Data type. Type of data to be inserted into the DB."
        print "Visibility -> To insert the dates during which prefixes, origin ASes and middle ASes were seen in the routing table."
        print "Routing -> To insert into the routing_data table the list of rows in the BGP routing table for the available dates."
        print "Visibility will be used by default."
        print "A: Provide the path to the folder containing hitorical routing data."
        print "AND"
        print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
        print "OR"
        print "f: Provide the path to a routing file."
        print "D: DEBUG mode"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -t <visibility/routing> (-A <archive folder> -n <process number> | -f <readable routing file>) [-D]'.format(sys.argv[0])
            print "t: Data type. Type of data to be inserted into the DB."
            print "Visibility -> To insert the dates during which prefixes, origin ASes and middle ASes were seen in the routing table."
            print "Routing -> To insert into the routing_data table the list of rows in the BGP routing table for the available dates."
            print "Visibility will be used by default."
            print "A: Provide the path to the folder containing hitorical routing data."
            print "AND"
            print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
            print "OR"
            print "f: Provide the path to a routing file."
            print "D: DEBUG mode"
            sys.exit()
        elif opt == '-t':
            data_type = arg
            if data_type != 'visibility' and data_type != 'routing':
                print "Wrong data type! You MUST choose between 'visibility' and 'routing'."
                sys.exit(-1)
        elif opt == '-A':
            archive_folder = os.path.abspath(arg)
        elif opt == '-n':
            try:
                proc_num = int(arg)
            except ValueError:
                print "The process number MUST be a number."
                sys.exit(-1)
        elif opt == '-f':
            routing_file = os.path.abspath(arg)
        elif opt == '-D':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
    
    if proc_num == -1:
        if routing_file == '':
            print "If you don't provide the path to a routing file you MUST provide a process number."
            sys.exit(-1)
        else:
            file_date = getDateFromFileName(routing_file)
            
            if file_date.year in [2007, 2008, 2009]:
                proc_num = 1
            elif file_date.year in [2010, 2011]:
                proc_num = 2
            elif file_date.year in [2012, 2013]:
                proc_num = 3
            elif file_date.year in [2014, 2015]:
                proc_num = 4
            elif file_date.year in[2016, 2017]:
                proc_num = 5
            else:
                print "Routing file corresponds to date out of the considered range."
                sys.exit(-1)
            
    readables_path = '/home/sofia/BGP_stats_files/hist_part{}'.format(proc_num)
    
    files_path = '/home/sofia/BGP_stats_files/Visibility_Routing_CSVs/CSVs{}'.format(proc_num)
    
    output_file = '{}/CSVgeneration_{}_{}_{}.output'.format(files_path, data_type, proc_num, datetime.today().date())

    bgp_handler = BGPDataHandler(DEBUG, readables_path)
        
    if routing_file != '':
        generateFilesFromRoutingFile(files_path, routing_file,
                                             bgp_handler, data_type, dict(),
                                             output_file, archive_folder)
    else:
        db_handler = DBHandler()

        dates_ready = dict()

        if data_type == 'visibility':
            
            sys.stdout.write('Checking for dates already in the DB\n')
            
            existing_dates_pref = set(db_handler.getListOfDatesForPrefixes())
    
            for ex_date in existing_dates_pref:            
                # We don't want to insert duplicated data,
                # therefore, we assume that if the date is present
                # in the prefixes table, all the prefixes for that date,
                # v4 and v6, have already been inserted.
                # After finishing with the bulk insertion, all the dates need
                # to be checked to determine if there is any missing data.
                if ex_date not in dates_ready:
                    dates_ready[ex_date] = dict()
                if 'prefixes' not in dates_ready[ex_date]:
                    dates_ready[ex_date]['prefixes'] = defaultdict(bool)
                    
                dates_ready[ex_date]['prefixes']['v4'] = True
                dates_ready[ex_date]['prefixes']['v6'] = True
    
            existing_dates_orASes = set(db_handler.getListOfDatesForOriginASes())
            
            for ex_date in existing_dates_orASes:
                if ex_date not in dates_ready:
                    dates_ready[ex_date] = dict()
                if 'originASes' not in dates_ready[ex_date]:
                    dates_ready[ex_date]['originASes'] = defaultdict(bool)
                    
                dates_ready[ex_date]['originASes']['v4'] = True
                dates_ready[ex_date]['originASes']['v6'] = True
                
            existing_dates_midASes = set(db_handler.getListOfDatesForMiddleASes())

            for ex_date in existing_dates_midASes:
                if ex_date not in dates_ready:
                    dates_ready[ex_date] = dict()
                if 'middleASes' not in dates_ready[ex_date]:
                    dates_ready[ex_date]['middleASes'] = defaultdict(bool)
                    
                dates_ready[ex_date]['middleASes']['v4'] = True
                dates_ready[ex_date]['middleASes']['v6'] = True

        elif data_type == 'routing':
            existing_dates_v4 = set(db_handler.getListOfDatesForRoutingData_v4Only())
                        
            for ex_date in existing_dates_v4:
                if ex_date not in dates_ready:
                    dates_ready[ex_date] = defaultdict(bool)
                dates_ready[ex_date]['routing_v4'] = True

            existing_dates_v6 = set(db_handler.getListOfDatesForRoutingData_v6Only())
            
            for ex_date in existing_dates_v6:
                if ex_date not in dates_ready:
                    dates_ready[ex_date] = defaultdict(bool)
                dates_ready[ex_date]['routing_v6'] = True
                
            existing_dates_v4andv6 = set(db_handler.getListOfDatesForRoutingData_v4andv6())
            
            for ex_date in existing_dates_v4andv6:
                if ex_date not in dates_ready:
                    dates_ready[ex_date] = defaultdict(bool)
                dates_ready[ex_date]['routing_v4andv6'] = True
                
        db_handler.close()
                         
        sys.stdout.write('Checking for existing CSV files\n')
                                       
        dates_ready = getDatesOfExistingCSVs(files_path, data_type, dates_ready)

        sys.stdout.write('Starting to generate CSV files from readable files\n')
        dates_ready = generateFilesFromReadables(readables_path, data_type,
                                                 dates_ready, files_path,
                                                 bgp_handler, output_file,
                                                 archive_folder)

        sys.stdout.write('Starting to generate CSV files from bgprib.mrt files\n')
        dates_ready = generateFilesFromOtherRoutingFiles(\
                                        archive_folder, data_type, dates_ready,
                                        files_path, bgp_handler, proc_num,
                                        'bgprib.mrt', output_file)
        
        sys.stdout.write('Starting to generate CSV files from dmp.gz files\n')
        dates_ready = generateFilesFromOtherRoutingFiles(\
                                        archive_folder, data_type, dates_ready,
                                        files_path, bgp_handler, proc_num,
                                        'dmp.gz', output_file)
                                                                    
        completeDatesSet = getCompleteDatesSet(proc_num)

        with open(output_file, 'a') as output:
            if data_type == 'visibility':
                output.write('Dates that are not in the prefixes or in the asns tables in the DB and for which some of the CSV files were not created.\n')
    
                for ex_date in completeDatesSet:
                    if ex_date not in dates_ready:
                        output.write('Visibility data not ready for date {}\n'.format(ex_date))
                    else:
                        for item in ['prefixes', 'originASes', 'middleASes']:
                            if item not in dates_ready[ex_date]:
                                output.write('Visibility data for {} not ready for date {}\n'.format(item, ex_date))
                            else:
                                for v in ['v4', 'v6']:
                                    if not dates_ready[ex_date][item][v]:
                                        output.write('Visibility data for {} coming from {} file not ready for date {}.\n'.format(item, v, ex_date))

            elif data_type == 'routing':
                output.write('Dates that are not in the routing_data table in the DB and for which some of the CSV files were not created.\n')

                for ex_date in completeDatesSet:
                    if ex_date not in dates_ready:
                        output.write('Routing data about v4 prefixes not ready for date {}\n'.format(ex_date))
                        output.write('Routing data about v6 prefixes not ready for date {}\n'.format(ex_date))
                    else:
                        if not dates_ready[ex_date]['routing_v4']:
                            output.write('Routing data about v4 prefixes not ready for date {}\n'.format(ex_date))
                        if not dates_ready[ex_date]['routing_v6']:
                            output.write('Routing data about v6 prefixes not ready for date {}\n'.format(ex_date))
                            
            sys.stdout.write('Finished generating CSV files. Output file {} created.\n'.format(output_file))
            
            
if __name__ == "__main__":
    main(sys.argv[1:])