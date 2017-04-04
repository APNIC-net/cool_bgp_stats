# -*- coding: utf-8 -*-
"""
Created on Fri Mar 17 08:58:14 2017

@author: sofiasilva
"""
import getopt, sys, datetime
import pandas as pd

# This function computes all the statistics for all the dates included in a given
# subset from a DataFrame, which contains info about delegations (delegated_subset),
# for a given combination of Geographic Area (a), Resource Type (r), Status(s)
# and Organization (o)
# The computed statistics are written to the provided file (stats_filename)
def computation_loop(delegated_subset, a, r, s, o, stats_filename):
                   
    # If we are working with a specific resource type, we group the info
    # about delegations just by date
    date_groups =\
            delegated_subset.groupby(delegated_subset['date']\
                                    .map(lambda x: x.strftime('%Y%m%d')))
        
    res_counts = date_groups['ResourceCount'].agg(np.sum)
    space_counts = date_groups['SpaceCount'].agg(np.sum)

    for date, delsInDate in date_groups:
        numOfDelegations = len(delsInDate['OriginalIndex'].unique())
        if r == 'ipv4' or r == 'ipv6':
            numOfResources = len(delsInDate)
            IPCount = res_counts[date]
            IPSpace = space_counts[date]
        else: # r == 'asn'
            numOfResources = res_counts[date]
            # IPCount and IPSpace do not make sense for r = 'asn'
            IPCount = ''
            IPSpace = ''
        
        with open(stats_filename, 'a') as stats_file:
            #Geographic Area,ResourceType,Status,Organization,Date,NumOfDelegations,NumOfResources,IPCount,IPSpace
            stats_file.write('{},{},{},{},{},{},{},{},{}\n'.format(a, r, s, o, date, numOfDelegations, numOfResources, IPCount, IPSpace))

    
    
# This function computes statistis for all the different combinations of
# Organization, Geographic Area, Resource Type and Status
def computeStatistics(del_handler, stats_filename):
    for org, org_df in del_handler.delegated_df.groupby(del_handler.delegated_df['opaque_id']):
            
        for country, area_df in org_df.groupby(org_df['cc']):
            for r, res_df in area_df.groupby(area_df['resource_type']):            
                for s, status_res_df in res_df.groupby(res_df['status']): 
                    computation_loop(status_res_df, country, r, s, org, stats_filename)
        
        for region, area_df in org_df.groupby(org_df['region']):
            for r, res_df in area_df.groupby(area_df['resource_type']):            
                for s, status_res_df in res_df.groupby(res_df['status']): 
                    computation_loop(status_res_df, region, r, s, org, stats_filename)
    


def main(argv):    
    files_path = ''
    stats_path = ''
    startDate = ''
    endDate = ''
    INCREMENTAL = False
    stats_file = ''
    DEBUG = False
    
    try:
        opts, args = getopt.getopt(argv, "hf:s:S:E:i:d", ["files_path=", "stats_path=", "StartDate=", "EndDate=", "stats_file="])
    except getopt.GetoptError:
        print 'Usage: summarizeRoutingStats.py -h | -f <files path> -s <stats path> [-S <Start Date>] [-E <End Date>] [-i <stats file>] [-d]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes daily statistics from one of the delegated files provided by the RIRs"
            print 'Usage: summarizeRoutingStats.py -h | -f <files path> -s <stats path> [-S <Start Date>] [-E <End Date>] [-i <stats file>] [-d]'
            print 'h = Help'
            print "f = Path to folder in which files will be saved. (MANDATORY)"
            print "s = Path to folder with routing statistics files (for prefixes and for ASes)."
            print 'S = Start date in format YYYY or YYYYmm or YYYYmmdd. Start date for the period of time for which summarized stats will be computed.'
            print 'E = End date in format YYYY or YYYYmm or YYYYmmdd. End date for the period of time for which summarized stats will be computed.'
            print "i = Incremental. Compute incremental statistics from existing stats file (CSV)."
            print "If option -i is used, a statistics file MUST be provided."
            print 'd = DEBUG mode.'
            sys.exit()
        elif opt == '-f':
            if arg != '':
                files_path = arg
            else:
                print "You must provide the path to a folder in which to save files."
                sys.exit()
        elif opt == '-s':
            if arg != '':
                stats_path = arg
            else:
                print "You must provide the path a folder that contains routing stats files."
                sys.exit()
        elif opt == '-S':
            if arg != '':
                startDate = arg
            else:
                print 'If you use option -S you MUST provide a start date.'
                sys.exit()
        elif opt == '-E':
            if arg != '':
                endDate = arg
            else:
                print 'If you use option -E you MUST provide an end date.'
                sys.exit()
        elif opt == '-i':
            if arg != '':
                INCREMENTAL = True
                stats_file = arg
            else:
                print 'If you use option -i you MUST provide the path to a file with already computed summarized stats.'
                sys.exit()
        elif opt == '-d':
            DEBUG = True
        else:
            assert False, 'Unhandled option'
            
    if startDate != '' and not (len(startDate) == 4 or len(startDate) == 6 or len(startDate) == 8):
        print 'Start date must be in the format YYYY or YYYYmm or YYYYmmdd.'
        sys.exit()

    if endDate != '' and not (len(endDate) == 4 or len(endDate) == 6 or len(endDate) == 8):
        print 'End date must be in the format YYYY or YYYYmm or YYYYmmdd.'
        sys.exit()
            
    today = datetime.date.today().strftime('%Y%m%d')
    
    if endDate == '':
        endDate = today
        
    if startDate == '':
        dateStr = 'UNTIL{}'.format(endDate)
    else:
        dateStr = 'SINCE{}UNTIL{}'.format(startDate, endDate)

    if DEBUG:
        file_name = '{}/routing_stats_{}_test'.format(files_path, dateStr)
    else:
        file_name = '{}/routing_stats_{}'.format(files_path, dateStr)
        
        
    if INCREMENTAL:
        try:
            existing_stats_df = pd.read_csv(stats_file, sep = ',')
            final_existing_date = str(max(existing_stats_df['mostRecentRoutingData_date']))
            del existing_stats_df
        except (ValueError, KeyError):
            final_existing_date = ''
            INCREMENTAL = False
    else:
        stats_file = '{}.csv'.format(file_name)
        
        # TODO Terminar si decido sumarizar en Python
        
#        with open(stats_file, 'w') as csv_file:
#            csv_file.write('Geographic Area,ResourceType,Status,Organization,Date,NumOfDelegations,NumOfResources,IPCount,IPSpace\n')
#        
#    del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, date, UNTIL,\
#                                    INCREMENTAL, final_existing_date )
#        
#    if not del_handler.delegated_df.empty:
#        start_time = time.time()
#        computeStatistics(del_handler, stats_file)       
#
#        end_time = time.time()
#        sys.stderr.write("Stats computed successfully!\n")
#        sys.stderr.write("Statistics computation took {} seconds\n".format(end_time-start_time))   
#
#        stats_df = pd.read_csv(stats_file, sep = ',')
#        json_filename = '{}.json'.format(file_name)
#        stats_df.to_json(json_filename, orient='index')
#        sys.stderr.write("Stats saved to JSON file successfully!\n")
#        sys.stderr.write("Files generated:\n{}\nand\n{})\n".format(stats_file, json_filename))
#        
#        if user != '' and password != '':
#            r = saveDFToElasticSearch(stats_df, user, password)
#            status_code = r.status_code
#            if status_code == 200:
#                sys.stderr.write("Stats saved to ElasticSearch successfully!\n")
#            else:
#                print "Something went wrong when trying to save stats to ElasticSearch.\n"

        
if __name__ == "__main__":
    main(sys.argv[1:])