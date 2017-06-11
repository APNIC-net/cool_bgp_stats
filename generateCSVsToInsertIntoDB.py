# -*- coding: utf-8 -*-
"""
Created on Mon May 22 16:18:28 2017

@author: sofiasilva
"""
import os, sys, getopt
os.chdir(os.path.dirname(os.path.realpath(__file__)))
from BGPDataHandler import BGPDataHandler
from VisibilityDBHandler import VisibilityDBHandler
from time import time
from datetime import datetime
import csv, re
                       
def getDatesOfExistingCSVs(files_path, prefixes_dates_ready, existing_dates_orASes,\
                            existing_dates_midASes):
    for existing_file in os.listdir(files_path):
        if existing_file.endswith('.csv'):
            if 'prefixes' in existing_file:
                # No matter if the date was already present in the dict or not,
                # if there is a CSV for this date, both v4 and v6 are ready for
                # it, because when a CSV is generated, it is generated for the
                # total set of prefixes for the date (v4 and v6)
                prefixes_dates_ready[getDateFromFileName(existing_file)] =\
                                            {'v4_ready':True, 'v6_ready':True}
            elif 'originASes' in existing_file:
                existing_dates_orASes.add(getDateFromFileName(existing_file))
            elif 'middleASes' in existing_file:
                existing_dates_midASes.add(getDateFromFileName(existing_file))

    return prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes
            
def generateFilesFromReadables(readables_path, prefixes_dates_ready,\
                                existing_dates_orASes, existing_dates_midASes,\
                                files_path, bgp_handler, output_file):    
    for readable_file in os.listdir(readables_path):
        file_path = '{}/{}'.format(readables_path, readable_file)
        routing_date = getDateFromFile(file_path, output_file)
        if readable_file.endswith('readable') and\
            (routing_date not in prefixes_dates_ready or\
            (routing_date in prefixes_dates_ready and\
            not prefixes_dates_ready[routing_date]['v4_ready']) or\
            (routing_date in prefixes_dates_ready and\
            not prefixes_dates_ready[routing_date]['v6_ready']) or\
            routing_date not in existing_dates_orASes or\
            routing_date not in existing_dates_midASes):
            prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes =\
                generateFilesFromReadableRoutingFile(files_path, file_path,
                                                     bgp_handler,
                                                     prefixes_dates_ready,
                                                     existing_dates_orASes,
                                                     existing_dates_midASes,
                                                     output_file)
    
    return prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes


def getDatesForExistingReadables(files_path):
    readable_dates = set()    
    for root, subdirs, files in os.walk(files_path):
        for filename in files:
            if filename.endswith('readable'):
                readable_dates.add(getDateFromFileName(filename))

    return readable_dates                
   
def generateFilesFromOtherRoutingFiles(archive_folder, readable_dates,
                                       prefixes_dates_ready, existing_dates_orASes,
                                       existing_dates_midASes, files_path,
                                       bgp_handler, proc_num, extension, output_file):
    
    yearsForProcNums = {1:[2007, 2008, 2009], 2:[2010, 2011], 3:[2012, 2013],
                        4:[2014, 2015], 5:[2016, 2017]}

    # Routing files in the archive folder for dates that haven't been
    # inserted into the DB yet
    for root, subdirs, files in os.walk(archive_folder):
        for filename in files:
            file_date = getDateFromFileName(filename)
        
            # For paralelization we check for the year of the file, so that
            # different files are processed by different scripts
            if filename.endswith(extension) and file_date not in readable_dates and\
                file_date.year in yearsForProcNums[proc_num]:
                full_filename = os.path.join(root, filename)
                readable_file = bgp_handler.getReadableFile(full_filename, False)
                if readable_file == '':
                    with open(output_file, 'a') as output:
                        output.write('Got an empty readable file name for file {}\n'.format(full_filename))
                    continue
                
                readable_dates.add(file_date)
                
                routing_date = getDateFromFile(readable_file)
                
                if (routing_date not in prefixes_dates_ready or\
                    (routing_date in prefixes_dates_ready and\
                    not prefixes_dates_ready[routing_date]['v4_ready']) or\
                    (routing_date in prefixes_dates_ready and\
                    not prefixes_dates_ready[routing_date]['v6_ready']) or\
                    routing_date not in existing_dates_orASes or\
                    routing_date not in existing_dates_midASes):
                    prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes =\
                        generateFilesFromReadableRoutingFile(files_path,
                                                             readable_file,
                                                             bgp_handler,
                                                             prefixes_dates_ready,
                                                             existing_dates_orASes,
                                                             existing_dates_midASes,
                                                             output_file)

    return prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes,\
            readable_dates


def writeCSVfiles(file_path, tuples):
    with open(file_path, 'wb') as csv_file:
        wr = csv.writer(csv_file,
                        delimiter=',',
                        lineterminator='\n',
                        quoting=csv.QUOTE_ALL)
                        
        wr.writerows(tuples)

def generateFilesForItem(name, item_list, files_path, routing_date):
    start = time()
    
    if name == 'prefixes':
        tuples = zip(item_list, [routing_date]*len(item_list))
    elif name == 'originASes':
        tuples = zip(item_list, [True]*len(item_list), [routing_date]*len(item_list))
    else: # name == 'middleASes'
        tuples = zip(item_list, [False]*len(item_list), [routing_date]*len(item_list))
        
    file_path = '{}/{}_{}.csv'.format(files_path, name, routing_date)
    
    if not os.path.exists(file_path):
        writeCSVfiles(file_path, tuples)
    
        end = time()
        sys.stdout.write('It took {} seconds to generate the CSV and CTL files for the insertion of {} for {}.\n'.format(end-start, name, routing_date))
    
def generateFilesFromReadableRoutingFile(files_path, routing_file, bgp_handler,\
                                         prefixes_dates_ready, existing_dates_orASes,\
                                         existing_dates_midASes, output_file):
    start = time()
    prefixes, originASes, middleASes, routing_date =\
                        bgp_handler.getPrefixesASesAndDate(routing_file)
    end = time()
    sys.stdout.write('It took {} seconds to get the lists of prefixes, origin ASes and middle ASes for {}.\n'.format(end-start, routing_date))

    if routing_date is not None:
        if routing_date.year == 1970:
            os.remove(routing_file)
            file_date = getDateFromFileName(routing_file)
            routing_files = bgp_handler.getSpecificFilesFromArchive(file_date,
                                                                   extension='bgprib.mrt')
            
            if len(routing_files) > 0:
                start = time()
                prefixes, originASes, middleASes, routing_date =\
                                    bgp_handler.getPrefixesASesAndDate(routing_files[0])
                end = time()
                sys.stdout.write('It took {} seconds to get the lists of prefixes, origin ASes and middle ASes for {}.\n'.format(end-start, routing_date))
    
            else:
                return prefixes_dates_ready, existing_dates_orASes,\
                    existing_dates_midASes

        contains_v4 = False
        contains_v6 = False
        if 'bgprib' in routing_file:
            contains_v4 = True
            contains_v6 = True
        elif 'v6' in routing_file:
            contains_v6 = True
        else:
            contains_v4 = True
            
        if len(prefixes) > 0 and (routing_date not in prefixes_dates_ready or\
            (routing_date in prefixes_dates_ready and contains_v4 and\
            not prefixes_dates_ready[routing_date]['v4_ready']) or\
            (routing_date in prefixes_dates_ready and contains_v6 and\
            not prefixes_dates_ready[routing_date]['v6_ready'])):
            try:
                generateFilesForItem('prefixes', prefixes, files_path, routing_date)
                prefixes_dates_ready[routing_date] = {'v4_ready':contains_v4,
                                                        'v6_ready':contains_v6}
                
            except KeyboardInterrupt:
                sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
                generateFilesForItem('prefixes', prefixes, files_path, routing_date)
                prefixes_dates_ready[routing_date] = {'v4_ready':contains_v4,
                                                        'v6_ready':contains_v6}
                                                        
                if len(originASes) > 0 and routing_date not in existing_dates_orASes:
                    generateFilesForItem('originASes', originASes, files_path, routing_date)
                    existing_dates_orASes.add(routing_date)
                    
                if len(middleASes) > 0 and routing_date not in existing_dates_midASes:
                    generateFilesForItem('middleASes', middleASes, files_path, routing_date)
                    existing_dates_midASes.add(routing_date)
                    
                sys.exit(0)
    
        if len(originASes) > 0 and routing_date not in existing_dates_orASes:
            try:
                generateFilesForItem('originASes', originASes, files_path, routing_date)
                existing_dates_orASes.add(routing_date)
    
            except KeyboardInterrupt:
                sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
                generateFilesForItem('originASes', originASes, files_path, routing_date)
                existing_dates_orASes.add(routing_date)
    
                if len(middleASes) > 0 and routing_date not in existing_dates_midASes:
                    generateFilesForItem('middleASes', middleASes, files_path, routing_date)
                    existing_dates_midASes.add(routing_date)
    
                sys.exit(0)
    
        if len(middleASes) > 0 and routing_date not in existing_dates_midASes:        
            try:
                generateFilesForItem('middleASes', middleASes, files_path, routing_date)
                existing_dates_midASes.add(routing_date)
    
            except KeyboardInterrupt:
                sys.stdout.write('Keyboard Interrupt received. Files for current routing file will be generated before aborting.')
                generateFilesForItem('middleASes', middleASes, files_path, routing_date)
                existing_dates_midASes.add(routing_date)
                sys.exit(0)
    
    return prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes

# We assume the routing files have routing info for a single date,
# therefore we get the routing date from the first line of the file.
def getDateFromFile(file_path, output_file):        
    with open(file_path, 'rb') as readable_file:
        first_line = readable_file.readline()
        try:
            timestamp = float(first_line.split('|')[1])
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
                return None
        return datetime.utcfromtimestamp(timestamp).date()
        
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
    initial_date = datetime.date(yearsForProcNums[proc_num][0], 1, 1)
    final_date = datetime.date(yearsForProcNums[proc_num][-1], 12, 31)
    numOfDays = (final_date - initial_date).days
    return set([final_date - datetime.timedelta(days=x) for x in range(0, numOfDays)])
    
def main(argv):
    routing_file = ''
    files_path = '/home/sofia/BGP_stats_files'
    readables_path = ''
    archive_folder = '/data/wattle/bgplog'
    proc_num = -1
    DEBUG = False

    try:
        opts, args = getopt.getopt(argv,"hp:A:f:n:D", ['files_path=', 'archive_folder=', 'procNumber=', 'routingFile=',])
    except getopt.GetoptError:
        print 'Usage: {} -h | -p <files path> (-A <archive folder> -n <process number> | -f <readable routing file>) [-D]'.format(sys.argv[0])
        print "p: Provide the path to a folder to use to save files."
        print "A: Provide the path to the folder containing hitorical routing data."
        print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
        print "OR"
        print "f: Provide the name of a routing file in readable format."
        print "D: DEBUG mode"
        sys.exit()

    for opt, arg in opts:
        if opt == '-h':
            print 'Usage: {} -h | -p <files path> (-A <archive folder> -n <process number> | -f <readable routing file>) [-D]'.format(sys.argv[0])
            print "p: Provide the path to a folder to use to save files."
            print "A: Provide the path to the folder containing hitorical routing data."
            print "AND"
            print "n: Provide a process number from 1 to 5, which allows the script to process a specific subset of the available files so that different scripts can process different files."
            print "OR"
            print "f: Provide the name of a routing file in readable format."
            print "D: DEBUG mode"
            sys.exit()
        elif opt == '-p':
            files_path = os.path.abspath(arg)
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
        readables_path = '/home/sofia/BGP_stats_files/hist_part{}'.format(proc_num)
    
    output_file = '{}/{}_{}_{}.output'.format(files_path, sys.argv[0], proc_num, datetime.today().date())

    bgp_handler = BGPDataHandler(DEBUG, readables_path)
    
    if routing_file != '':
        generateFilesFromReadableRoutingFile(files_path, routing_file,
                                             bgp_handler, dict(), {}, {},
                                             output_file)
    else:
        db_handler = VisibilityDBHandler()

        prefixes_dates_ready = dict()
        
        existing_dates_pref = set(db_handler.getListOfDatesForPrefixes())

        for date in existing_dates_pref:
            # The insertions into the DB are bulk insertions, therefore, we
            # assume that if the date is present in the prefixes table,
            # all the prefixes for that date, v4 and v6, have already been inserted
            prefixes_dates_ready[date] = {'v4_ready':True, 'v6_ready':True}

        existing_dates_orASes = set(db_handler.getListOfDatesForOriginASes())
        existing_dates_midASes = set(db_handler.getListOfDatesForMiddleASes())
        
        prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes =\
            getDatesOfExistingCSVs(files_path, prefixes_dates_ready,
                                   existing_dates_orASes, existing_dates_midASes)
        
        prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes =\
            generateFilesFromReadables(readables_path, prefixes_dates_ready,
                                       existing_dates_orASes, existing_dates_midASes,
                                       files_path, bgp_handler, output_file)
        
        readable_dates = getDatesForExistingReadables(readables_path)
        
        prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes,\
            readable_dates = generateFilesFromOtherRoutingFiles(archive_folder,
                                                                readable_dates,
                                                                prefixes_dates_ready,
                                                                existing_dates_orASes,
                                                                existing_dates_midASes,
                                                                files_path,
                                                                bgp_handler,
                                                                proc_num,
                                                                'bgprib.mrt',
                                                                output_file)
        
        prefixes_dates_ready, existing_dates_orASes, existing_dates_midASes,\
            readable_dates = generateFilesFromOtherRoutingFiles(archive_folder,
                                                                readable_dates,
                                                                prefixes_dates_ready,
                                                                existing_dates_orASes,
                                                                existing_dates_midASes,
                                                                files_path,
                                                                bgp_handler,
                                                                proc_num,
                                                                'dmp.gz',
                                                                output_file)
                                                                
        completeDatesSet = getCompleteDatesSet(proc_num)
        
        with open(output_file, 'a') as output:
            output.write('Dates that are not in the prefixes or in the asns tables in the DB and for which some of the CSV files were not created.\n')

            for date in completeDatesSet:
                if date not in prefixes_dates_ready:
                    output.write('v4 not ready for date {}\n'.format(date))
                    output.write('v6 not ready for date {}\n'.format(date))
                else:
                    if not prefixes_dates_ready[date]['v4_ready']:
                        output.write('v4 not ready for date {}\n'.format(date))
                    if not prefixes_dates_ready[date]['v6_ready']:
                        output.write('v6 not ready for date {}\n'.format(date))

                if date not in existing_dates_orASes:
                    output.write('Origin ASes not ready for date {}\n'.format(date))

                if date not in existing_dates_midASes:
                    output.write('Middle ASes not ready for date {}\n'.format(date))                        
    
        sys.stdout.write('Finished generating CSV files. Output file {} created.\n'.format(output_file))
        
if __name__ == "__main__":
    main(sys.argv[1:])