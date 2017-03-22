#! /usr/bin/python2.7 
# -*- coding: utf8 -*-


import sys, getopt, os, bz2
os.chdir(os.path.dirname(os.path.realpath(__file__)))
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from get_file import get_file
from DelegatedHandler import DelegatedHandler
from BGPDataHandler import BGPDataHandler
import ipaddress
import pandas as pd
import numpy as np
import datetime, time
import math
import pickle, copy


# This function downloads information about relationships between ASes inferred
# by CAIDA and stores it in a dictionary in which all the active ASes appear as keys
# and the value is another dictionary that also has an AS as key and a string
# as value specifying whether it is a P2P, a P2C or a C2P relationship
# The serial variable must be 1 or 2 depending on CAIDAS's data to be used
def getASrelInfo(serial, files_path, KEEP):
    
    folder_url = 'http://data.caida.org/datasets/as-relationships/serial-{}/'.format(serial)
    index_file = '{}/CAIDA_index.txt'.format(files_path)
    get_file(folder_url, index_file)
    
    with open(index_file, 'r') as index:
        maxDate = 0
        for line in index.readlines():
            href_pos = line.find('href')
            if href_pos != -1 and line.find('as-rel') != -1:
                try:
                    date_pos = href_pos+6
                    date = int(line[date_pos:date_pos+8])
                    if date > maxDate:
                        maxDate = date
                except ValueError:
                    continue
            
    index.close()
    if not KEEP:
        os.remove(index_file)
    
    if serial == 1:
        as_rel_url = '{}{}.as-rel.txt.bz2'.format(folder_url, maxDate)
        as_rel_file = '{}/CAIDA_ASrel_{}.txt.bz2'.format(files_path, maxDate)
    elif serial == 2:
        as_rel_url = '{}{}.as-rel2.txt.bz2'.format(folder_url, maxDate)
        as_rel_file = '{}/CAIDA_ASrel2_{}.txt.bz2'.format(files_path, maxDate)

    get_file(as_rel_url, as_rel_file)
    
    print 'Working with file {}\n'.format(as_rel_file)
    
    as_rel_file_decomp = '.'.join(as_rel_file.split('.')[:-1])
    
    with bz2.BZ2File(as_rel_file, 'rb') as as_rel,\
            open(as_rel_file_decomp, 'wb') as as_rel_decomp:
        as_rel_decomp.write(as_rel.read())
        
    ASrels = pd.read_table(as_rel_file_decomp, header=None, sep='|',
                                index_col=False, comment='#', names=['AS1',
                                                                     'AS2',
                                                                     'rel_type_aux',
                                                                     'source'])
    ASrels['rel_type'] = np.where(ASrels.rel_type_aux == 0, 'P2P', 'P2C')
    del ASrels['rel_type_aux']
            
    if not KEEP:
        os.remove(as_rel_file)
        os.remove(as_rel_file_decomp)
        
    return ASrels
    
# Function that computes the Levenshtein distance between two AS paths
# From https://en.wikibooks.org/wiki/Algorithm_Implementation/Strings/Levenshtein_distance#Python
def levenshteinDist(ASpath1, ASpath2):
    if len(ASpath1) < len(ASpath2):
        return levenshteinDist(ASpath2, ASpath1)

    # So now we have len(ASpath1) >= len(ASpath2).
    if len(ASpath2) == 0:
        return len(ASpath1)

    # We call tuple() to force strings to be used as sequences
    # ('c', 'a', 't', 's') - numpy uses them as values by default.
    ASpath1 = np.array(tuple(ASpath1))
    ASpath2 = np.array(tuple(ASpath2))

    # We use a dynamic programming algorithm, but with the
    # added optimization that we only need the last two rows
    # of the matrix.
    previous_row = np.arange(ASpath2.size + 1)
    for s in ASpath1:
        # Insertion (ASpath2 grows longer than ASpath1):
        current_row = previous_row + 1

        # Substitution or matching:
        # ASpath2 and ASpath1 items are aligned, and either
        # are different (cost of 1), or are the same (cost of 0).
        current_row[1:] = np.minimum(
                current_row[1:],
                np.add(previous_row[:-1], ASpath2 != s))

        # Deletion (target grows shorter than source):
        current_row[1:] = np.minimum(
                current_row[1:],
                current_row[0:-1] + 1)

        previous_row = current_row

    return previous_row[-1]
        
def getLevenshteinDistances(blockASpaths, coveringPrefASpaths):
    levenshteinDistances = []
    for blockASpath in blockASpaths:
        for covPrefASpath in coveringPrefASpaths:
            levenshteinDistances.append(levenshteinDist(blockASpath, covPrefASpath))
    
    return levenshteinDistances

# Function that returns a dictionary with default values for all the keys that
# will be used to store the computed variables
def getDictionaryWithDefaults(booleanKeys, valueKeys, counterKeys):

    def_dict = dict()
    
    for booleanKey in booleanKeys:
        def_dict.setdefault(booleanKey, False)
    
    for valueKey in valueKeys:
        def_dict.setdefault(valueKey, float(np.nan))
    
    for counterKey in counterKeys:
        def_dict.setdefault(counterKey, 0)
    
    return def_dict
    
def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    variance = np.average((values-average)**2, weights=weights)  # Fast and numerically precise
    return (average, math.sqrt(variance))
    
def computeNetworkHistoryOfVisibility(network, statsForPrefix, bgp_handler,\
                                    first_seen_intact, visibilityPerPeriods):
    # History of Visibility (Visibility Evoluntion in Time) computation
  
    today = datetime.date.today()
    daysUsable = (today-statsForPrefix['del_date']).days + 1
    
    # Intact Allocation History of Visibility
    # The Intact Allocation History of Visibility only takes into
    # account the exact block that was delegated
    periodsIntact = bgp_handler.getPeriodsSeenExact(network)
    numOfPeriods = len(periodsIntact)
    
    if numOfPeriods > 0:
        last_seen_intact = bgp_handler.getDateLastSeenExact(network)
        statsForPrefix['isDeadIntact'] = ((today-last_seen_intact).days > 365)
        daysUsed = (last_seen_intact-first_seen_intact).days + 1
        daysSeen = bgp_handler.getTotalDaysSeenExact(network)
        
        statsForPrefix['relUsedTimeIntact'] = 100*float(daysUsed)/daysUsable
        statsForPrefix['effectiveUsageIntact'] = 100*float(daysSeen)/daysUsed
        statsForPrefix['timeFragmentationIntact'] = numOfPeriods/(float(daysUsed)/60)
                        
        periodsLengths = []
        for period in periodsIntact:
            periodsLengths.append(\
                (datetime.datetime.strptime(str(period[1]), '%Y%m%d').date() -\
                datetime.datetime.strptime(str(period[0]), '%Y%m%d').date()).days + 1)
        
        periodsLengths = np.array(periodsLengths)
        statsForPrefix['avgPeriodLengthIntact'] = periodsLengths.mean()
        statsForPrefix['stdPeriodLengthIntact'] = periodsLengths.std()
        statsForPrefix['minPeriodLengthIntact'] = periodsLengths.min()
        statsForPrefix['maxPeriodLengthIntact'] = periodsLengths.max()
        
    # General History of Visibility of Prefix
    # The General History of Visibility takes into account not only
    # the block itself being routed but also its fragments
    periodsGral = bgp_handler.getPeriodsSeenGral(network)
    
    if len(periodsGral) > 0:
        
        numsOfPeriodsGral = []
        daysUsedGral = []
        daysSeenGral = []
        periodsLengthsGral = []
        prefixesPerPeriod = dict()
        timeBreaks = []
        
        for fragment in periodsGral:
            frag_network = ipaddress.ip_network(unicode(fragment, "utf-8"))
            numsOfPeriodsGral.append(len(periodsGral[fragment]))
            daysUsedGral.append(\
                (bgp_handler.getDateLastSeenExact(frag_network) -\
                bgp_handler.getDateFirstSeenExact(frag_network)).days+1)
            daysSeenGral.append(bgp_handler.getTotalDaysSeenExact(frag_network))
            
            for period in periodsGral[fragment]:
                timeBreaks.append(period[0])
                timeBreaks.append(period[1])
                
                if period not in prefixesPerPeriod:
                    prefixesPerPeriod[period] = [fragment]
                else:
                    prefixesPerPeriod[period].append(fragment)
                    
                periodsLengthsGral.append(\
                    (datetime.datetime.strptime(str(period[1]), '%Y%m%d').date() -\
                    datetime.datetime.strptime(str(period[0]), '%Y%m%d').date()).days+1)
             
        timeBreaks = np.unique(timeBreaks)
             
        numsOfPeriodsGral = np.array(numsOfPeriodsGral)
        avgNumOfPeriodsGral = numsOfPeriodsGral.mean()
#       stdNumOfPeriodsGral = numsOfPeriodsGral.std()
#       minNumOfPeriodsGral = numsOfPeriodsGral.min()
#       maxNumOfPeriodsGral = numsOfPeriodsGral.max()
#                    
        daysUsedGral = np.array(daysUsedGral)
        avgDaysUsedGral = daysUsedGral.mean()
#       stdDaysUsedGral = daysUsedGral.std()
#       minDaysUsedGral = daysUsedGral.min()
#       maxDaysUsedGral = daysUsedGral.max()

        statsForPrefix['avgRelUsedTimeGral'] =\
                                    100*float(avgDaysUsedGral)/daysUsable

        statsForPrefix['avgTimeFragmentationGral'] =\
                            avgNumOfPeriodsGral/(float(avgDaysUsedGral)/60)
        
        daysSeenGral = np.array(daysSeenGral)
        avgDaysSeenGral = daysSeenGral.mean()
#       stdDaysSeenGral = daysSeenGral.std()
#       minDaysSeenGral = daysSeenGral.min()
#       maxDaysSeenGral = daysSeenGral.max()
        
        statsForPrefix['avgEffectiveUsageGral'] =\
                                    100*float(avgDaysSeenGral)/avgDaysUsedGral
        
        periodsLengthsGral = np.array(periodsLengthsGral)
        statsForPrefix['avgPeriodLengthGral'] = periodsLengthsGral.mean()
        statsForPrefix['stdPeriodLengthGral'] = periodsLengthsGral.std()
        statsForPrefix['minPeriodLengthGral'] = periodsLengthsGral.min()
        statsForPrefix['maxPeriodLengthGral'] = periodsLengthsGral.max()
    
        # We summarize all the prefixes seen during each period
        for period in prefixesPerPeriod:
            aggregated_fragments = [ipaddr for ipaddr in
                        ipaddress.collapse_addresses(
                        [ipaddress.ip_network(unicode(ip_net, 'utf-8'))
                        for ip_net in prefixesPerPeriod[period]])]
            prefixesPerPeriod[period] = aggregated_fragments
           
        if len(timeBreaks) > 0:
            # Evolution of Visibility (Visibility per period)
            # Taking into account the block itself being routed and
            # and its fragments being routed
            visibilityPerPeriods[str(network)] = dict()
            visibilities = []
            periodLengths = []
            
            visibilityPerPeriods[str(network)][\
                (int(statsForPrefix['del_date'].strftime('%Y%m%d')),\
                                                        int(timeBreaks[0]))] = 0
            
            last_seen = bgp_handler.getDateLastSeen(network)
            if last_seen < today:
                visibilityPerPeriods[str(network)][(int(last_seen.strftime('%Y%m%d')),
                                    int(today.strftime('%Y%m%d')))] = 0
            
            if len(timeBreaks) == 1:
                numOfIPsVisible = 0
                
                for period in prefixesPerPeriod:
                    if timeBreaks[0] >= period[0] and timeBreaks[0]\
                                                        <= period[1]:
                        for prefix in prefixesPerPeriod[period]:
                            numOfIPsVisible += prefix.num_addresses
                
                visibility = float(numOfIPsVisible)*100/network.num_addresses
                visibilityPerPeriods[str(network)][(int(timeBreaks[0]),\
                                    int(timeBreaks[0]))] = visibility
                visibilities.append(visibility)
                periodLengths.append(1)
            
            for i in range(len(timeBreaks)-1):
                numOfIPsVisible = 0
                
                for period in prefixesPerPeriod:
                    if timeBreaks[i] >= period[0] and timeBreaks[i+1]\
                                                        <= period[1]:
                        for prefix in prefixesPerPeriod[period]:
                            numOfIPsVisible += prefix.num_addresses
                
                visibility = float(numOfIPsVisible)*100/network.num_addresses
                visibilityPerPeriods[str(network)][(int(timeBreaks[i]),\
                                int(timeBreaks[i+1]))] = visibility
                visibilities.append(visibility)
                periodLengths.append(\
                    (datetime.datetime.strptime(timeBreaks[i+1], '%Y%m%d').date()\
                    - datetime.datetime.strptime(timeBreaks[i], '%Y%m%d').date()).days+1)
            
            # Computation of the weighted average and weighted
            # standard deviation of visibilities only considering
            # the period of time during which the prefix was used
            # (between the date it was first seen and the date it
            # was last seen)
            weightedAvgVisibility, weightedStdVisibility =\
                    weighted_avg_and_std(visibilities, periodLengths)
            statsForPrefix['avgVisibility'] = weightedAvgVisibility
            statsForPrefix['stdVisibility'] = weightedStdVisibility
            statsForPrefix['minVisibility'] = np.min(visibilities)
            statsForPrefix['maxVisibility'] = np.max(visibilities)
                                    
            statsForPrefix['isDead'] = ((today-last_seen).days > 365)

def computeNetworkUsageLatency(network, statsForPrefix, bgp_handler): 
    # Usage Latency computation
    first_seen = bgp_handler.getDateFirstSeen(network)
                
    if first_seen is not None and statsForPrefix['del_date'] is not None:
        statsForPrefix['UsageLatencyGral'] =\
                                (first_seen-statsForPrefix['del_date']).days + 1
    else:
        statsForPrefix['UsageLatencyGral'] = float('inf')
      
    # Intact Allocation Usage Latency computation
    # We compute the number of days between the date the block
    # was delegated and the date the block as-is was first seen
    if first_seen is not None:
        first_seen_intact = bgp_handler.getDateFirstSeenExact(network)
    else:
        first_seen_intact = None
                
    if first_seen_intact is not None:
        statsForPrefix['UsageLatencyIntact'] =\
                        (first_seen_intact-statsForPrefix['del_date']).days + 1
    else:
        statsForPrefix['UsageLatencyIntact'] = float('inf')
    
    return first_seen_intact

# This function classifies the provided prefix into Covering or Covered
# (Level 1 or Level 2+). If the prefix is a covered prefix, it is further
# classified into the following classes depending on the relation between its
# origin ASes and its covering prefix's origin ASes and the relation between its
# AS paths and its covering prefix's AS paths: 'SOSP', 'SODP1', 'SODP2', 'DOSP',
# 'DODP1', 'DODP2', 'DODP3'.
# These classes are taken from those defined in [1]
# The corresponding variables in the statsForPrefix dictionary are incremented
# to keep track of the number of prefixes in each class.
def classifyPrefixAndUpdateCounters(routedPrefix, statsForPrefix, levenshteinDists,\
                                    bgp_handler, ASrels):
    # TODO Debug
    routedNetwork = ipaddress.ip_network(unicode(routedPrefix, "utf-8"))
        
    # Find routed blocks related to the prefix of interest 
    net_less_specifics = bgp_handler.getRoutedParentAndGrandparents(routedNetwork)
    net_more_specifics = bgp_handler.getRoutedChildren(routedNetwork)
   
    # If there is at least one less specific block being routed,
    # the block is a covered prefix
    if len(net_less_specifics) > 0:        
        if len(net_less_specifics) == 1:
            statsForPrefix['numOfCoveredLevel1MoreSpec'] += 1
        else:
            statsForPrefix['numOfCoveredLevel2plusMoreSpec'] += 1

        # We classify the covered prefix based on its AS paths
        # relative to those of its corresponding covering prefix

        blockOriginASes = bgp_handler.getOriginASesForBlock(routedNetwork)
        blockASpaths = bgp_handler.getASpathsForBlock(routedNetwork)
        
        # The corresponding covering prefix is the last prefix in the
        # list of less specifics
        coveringPref = net_less_specifics[-1]
        coveringNet = ipaddress.ip_network(unicode(coveringPref, 'utf-8'))
        coveringPrefOriginASes = bgp_handler.getOriginASesForBlock(coveringNet)
        coveringPrefASpaths = bgp_handler.getASpathsForBlock(coveringNet)       
  
        if len(blockOriginASes.intersection(coveringPrefOriginASes)) > 0:
            if len(blockASpaths) == 1:
                if blockASpaths.issubset(coveringPrefASpaths):
                    statsForPrefix['numOfSOSPMoreSpec'] += 1
                else:
                    statsForPrefix['numOfSODP2MoreSpec'] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
                                                
            else: # len(blockASpaths) >= 2
                if len(coveringPrefASpaths.intersection(blockASpaths)) > 0 and\
                    len(blockASpaths.difference(coveringPrefASpaths)) > 0:
                    statsForPrefix['numOfSODP1MoreSpec'] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
                        
        else:
            if len(blockASpaths) == 1:
                if len(blockOriginASes) == 1:
                    blockOriginAS = int(blockOriginASes.pop())

                    # We check whether the origin AS of the
                    # block is a customer of any of the origin ASes
                    # of the covering prefix                       
                    for coveringPrefOriginAS in coveringPrefOriginASes:
                        if len(ASrels[(ASrels['AS1'] == coveringPrefOriginAS) &\
                                (ASrels['AS2'] == blockOriginAS) &\
                                (ASrels['rel_type'] == 'P2C')]):

                                for coveringPrefASpath in coveringPrefASpaths:
                                    if coveringPrefASpath.split(' ')[-1] == coveringPrefOriginAS:
                                        blockASpath_woOrigin = ' '.join(list(blockASpaths)[0].split(' ')[0:-1])
                                        
                                        if blockASpath_woOrigin == coveringPrefASpath:
                                            statsForPrefix['numOfDOSPMoreSpec'] += 1
                                            break
                                else:
                                    continue
                                break
                                
                if not blockASpaths.issubset(coveringPrefASpaths):
                    statsForPrefix['numOfDODP1MoreSpec'] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
                                                
            else: # len(blockASpaths) >= 2
                blockASpaths_woOrigin = set()
                for ASpath in blockASpaths:
                    blockASpaths_woOrigin.add(' '.join(ASpath.split(' ')[0:-1]))
                
                if len(coveringPrefASpaths.intersection(blockASpaths_woOrigin)) > 0 and\
                    len(blockASpaths_woOrigin.difference(coveringPrefASpaths)) > 0:
                    statsForPrefix['numOfDODP2MoreSpec'] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
              
            if len(coveringPrefASpaths.intersection(blockASpaths)) == 0:
                blockOriginASesCustomers = set()
                for blockOriginAS in blockOriginASes:
                    blockOriginASesCustomers.\
                        union(ASrels[(ASrels['AS1'] == blockOriginAS) &\
                                (ASrels['rel_type'] == 'P2C')]['AS2'].tolist())
                
                coveringPrefOriginASesCustomers = set()
                for coveringPrefOriginAS in coveringPrefOriginASes:
                    coveringPrefOriginASesCustomers.\
                        union(ASrels[(ASrels['AS1'] == coveringPrefOriginAS) &\
                                (ASrels['rel_type'] == 'P2C')]['AS2'].tolist())
                
                commonCustomers = blockOriginASesCustomers.\
                                    intersection(coveringPrefOriginASesCustomers)
                if len(commonCustomers) > 0:
                    for commonCustomer in commonCustomers:
                        correspondingOriginASes =\
                            set(ASrels[(ASrels['AS2'] == commonCustomer) &\
                                (ASrels['rel_type'] == 'P2C')]['AS1'].tolist()).\
                                intersection(blockOriginASes)
                        for correspondingOriginAS in correspondingOriginASes:
                            if len(bgp_handler.ASes_originated_prefixes_dic[correspondingOriginAS]) >= 2:
                                statsForPrefix['numOfDODP3MoreSpec'] += 1
                                levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                coveringPrefASpaths))
                                break
                        else:
                            continue
                        break
                    
    # If there are no less specific blocks being routed
    else:      
        # If the list of more specific blocks being routed only
        # includes the block itself, taking into account we are 
        # under the case of the block not having less specific
        # blocks being routed,
        if routedPrefix in net_more_specifics and len(net_more_specifics) == 1:
            # the block is a Lonely prefix
            # • Lonely: a prefix that does not overlap
            # with any other prefix.
            statsForPrefix['numOfLonelyMoreSpec'] += 1
        elif (routedPrefix not in net_more_specifics and len(net_more_specifics) > 0)\
            or (routedPrefix in net_more_specifics and len(net_more_specifics) > 1):
        # If there are more specific blocks being routed apart from
        # the block itself, taking into account we are under the case
        # of the block not having less specific blocks being routed,
            # The block is a Covering prefix  
            statsForPrefix['numOfCoveringMoreSpec'] += 1

    
        

def writeStatsLineToFile(statsForPrefix, allAttr, stats_filename):
    line = statsForPrefix[allAttr[0]]
        
    for i in range(len(allAttr)-1):
        line = '{},{}'.format(line, statsForPrefix[allAttr[i+1]])
    
    line = line + '\n'

    with open(stats_filename, 'a') as stats_file:
        stats_file.write(line)


def computePerPrefixStats(bgp_handler, del_handler, ASrels, stats_filename,
                            def_dict, allAttr):
    # Obtain a subset of all the delegated prefixes
    delegatedNetworks = del_handler.delegated_df[\
                            (del_handler.delegated_df['resource_type'] == 'ipv4') |\
                            (del_handler.delegated_df['resource_type'] == 'ipv6')]
    
     # Obtain ASN delegation data
    asn_del = del_handler.fullASN_df
                
    visibilityPerPeriods = dict()

    for i in delegatedNetworks.index:
        prefix_row = delegatedNetworks.ix[i]
        prefix = '{}/{}'.format(prefix_row['initial_resource'],\
                                int(prefix_row['count/prefLen']))
        statsForPrefix = def_dict.copy()

        statsForPrefix['delegated_prefix'] = prefix
        statsForPrefix['del_date'] = prefix_row['date'].date()
        statsForPrefix['opaque_id'] = prefix_row['opaque_id']
        statsForPrefix['cc'] = prefix_row['cc']
        statsForPrefix['region'] = prefix_row['region']
        statsForPrefix['mostRecentRoutingData_date'] =\
                        str(max(pd.to_numeric(bgp_handler.bgp_data['date'])))

        delNetwork = ipaddress.ip_network(unicode(prefix, "utf-8"))
        
        first_seen_intact = computeNetworkUsageLatency(delNetwork, statsForPrefix,\
                                                                bgp_handler)
        computeNetworkHistoryOfVisibility(delNetwork, statsForPrefix, bgp_handler,\
                                    first_seen_intact, visibilityPerPeriods)

        ips_delegated = delNetwork.num_addresses 

        # Find routed blocks related to the prefix of interest 
        less_specifics = bgp_handler.getRoutedParentAndGrandparents(delNetwork)
        more_specifics = bgp_handler.getRoutedChildren(delNetwork)
        
        if len(less_specifics) > 0:
            statsForPrefix['currentVisibility'] = 100
            statsForPrefix['numOfLessSpecificsRouted'] = len(less_specifics)
            # What should we do with the routed less specific prefixes?
            # They could be related to other delegations
            
        if len(more_specifics) > 0:
            statsForPrefix['numOfMoreSpecificsRouted'] = len(more_specifics)

            more_specifics_wo_prefix = copy.copy(more_specifics)

            if prefix in more_specifics:
                more_specifics_wo_prefix.remove(prefix)

                statsForPrefix['isRoutedIntact'] = True
                statsForPrefix['currentVisibility'] = 100
                
                if len(more_specifics) == 1 and len(less_specifics) == 0:
                    statsForPrefix['onlyRoot'] = True
                
                blockOriginASes = bgp_handler.getOriginASesForBlock(delNetwork)
                statsForPrefix['numOfOriginASes'] = len(blockOriginASes)
            
                # We check if the prefix and all its origin ASes were delegated
                # to the same organization
            
                originASesOrgs = set()
                
                for blockOriginAS in blockOriginASes:
                    originASorg = asn_del[(pd.to_numeric(asn_del['initial_resource'])\
                                            <= int(blockOriginAS)) &\
                                            (pd.to_numeric(asn_del['initial_resource'])+\
                                            pd.to_numeric(asn_del['count/prefLen'])>\
                                            int(blockOriginAS))]['opaque_id'].get_values()
                    if len(originASorg) == 1:
                        originASesOrgs.add(originASorg[0])
                    # There can not be more than one result in the ASN DataFrame,
                    # therefore, if the len is not 1, it's 0 and it means the origin AS was
                    # not delegated by APNIC
                    else:
                        originASesOrgs.add('UNKNOWN Org')
                        
                sameOrgs = True
                    
                for org in originASesOrgs:
                    if org != statsForPrefix['opaque_id']:
                        sameOrgs = False
                        break
                        
                if not sameOrgs:
                    with open('/Users/sofiasilva/BGP_files/DiffOrgs.txt', 'w') as orgs_file:
                        orgs_file.write('Prefix: {}, Org for Prefix: {}, Origin ASes: {}, Orgs for Origin ASes: {}\n'.format(prefix, statsForPrefix['opaque_id'], blockOriginASes, originASesOrgs))
                        
                    # TODO check with WHOIS/RDAP
                    # Get Org ID for prefix and for origin ASes and compare
                    # https://www.apnic.net/about-apnic/whois_search/about/rdap/
                    # Check with bootstrap right registry to query
                    
                    # If even after checking with WHOIS/RDAP we have different orgs:
                    # If the prefix is being originated by an AS delegated to
                    # a different organization from the organization that
                    # received the delegation of the block                 
            #            statsForPrefix['originatedByDiffOrg'] = True
                
                # We get the set of AS paths for the block
                blockASpaths = bgp_handler.getASpathsForBlock(delNetwork)
                statsForPrefix['numOfASPaths'] = len(blockASpaths)
                
                ASpathsLengths = []
                for path in blockASpaths:
                    ASpathsLengths.append(len(path.split()))
                    
                ASpathsLengths = np.array(ASpathsLengths)
                statsForPrefix['avgASPathLength'] = ASpathsLengths.mean()
                statsForPrefix['stdASPathLength'] = ASpathsLengths.std()
                statsForPrefix['minASPathLength'] = ASpathsLengths.min()
                statsForPrefix['maxASPathLength'] = ASpathsLengths.max()
                
                if len(more_specifics) > 1:    
                    aggr_more_spec = [ipaddr for ipaddr in
                                        ipaddress.collapse_addresses(
                                        [ipaddress.ip_network(unicode(ip_net, 'utf-8'))
                                        for ip_net in more_specifics_wo_prefix])]
                                        
                    if len(more_specifics) >= 3 and prefix in\
                                    [str(ip_net) for ip_net in aggr_more_spec]:
                        # • root/MS-complete: The root prefix and at least two subprefixes
                        # are announced. The set of all sub-prefixes spans the whole root prefix.
                        statsForPrefix['rootMScompl'] = True
                    elif len(more_specifics) >= 2 and prefix not in\
                                    [str(ip_net) for ip_net in aggr_more_spec]:
                        # • root/MS-incomplete: The root prefix and at least one subprefix
                        # is announced. Together, the set of announced subprefixes
                        # does not cover the root prefix.
                        statsForPrefix['rootMSincompl'] = True

            else:
                # We summarize the more specific routed blocks without the block itself
                # to get the maximum aggregation possible of the more specifics
                aggr_more_spec = [ipaddr for ipaddr in
                                    ipaddress.collapse_addresses(
                                    [ipaddress.ip_network(unicode(ip_net, 'utf-8'))
                                    for ip_net in more_specifics_wo_prefix])]
    
                # ips_routed is obtained from the summarized routed blocks
                # so that IPs contained in overlapping announcements are not
                # counted more than once
                ips_routed = 0            
                for aggr_r in aggr_more_spec:
                    ips_routed += aggr_r.num_addresses
                                    
                # The visibility of the block is the percentaje of IPs
                # that are visible
                statsForPrefix['currentVisibility'] = float(ips_routed*100)/ips_delegated
        
                if len(more_specifics) >= 2 and prefix in\
                                    [str(ip_net) for ip_net in aggr_more_spec]:
                    # • no root/MS-complete: The root prefix is not announced.
                    # However, there are at least two sub-prefixes which together
                    # cover the complete root prefix.
                    statsForPrefix['noRootMScompl'] = True
                elif len(more_specifics) >= 1 and prefix not in\
                                    [str(ip_net) for ip_net in aggr_more_spec]:
                    # • no root/MS-incomplete: The root prefix is not announced.
                    # There is at least one sub-prefix. Taking all sub-prefixes
                    # together, they do not cover the complete root prefix.
                    statsForPrefix['noRootMSincompl'] = True
        
            levenshteinDists = []
            for moreSpec in more_specifics:
                classifyPrefixAndUpdateCounters(moreSpec, statsForPrefix,
                                           levenshteinDists, bgp_handler, ASrels)

            if len(levenshteinDists) > 0:
                levenshteinDists = np.array(levenshteinDists)
                statsForPrefix['avgLevenshteinDist'] = levenshteinDists.mean()
                statsForPrefix['stdLevenshteinDist'] = levenshteinDists.std()
                statsForPrefix['minLevenshteinDist'] = levenshteinDists.min()
                statsForPrefix['maxLevenshteinDist'] = levenshteinDists.max()
            
            writeStatsLineToFile(statsForPrefix, allAttr, stats_filename)
            
    return visibilityPerPeriods

# This function determines whether the allocated ASNs are active
# either as middle AS, origin AS or both
# Returns dictionary with an ASN as key and a dictionary containing:
# * a numeric variable (numOfPrefixesPropagated) specifying the number of prefixes propagated by the AS
# (BGP announcements for which the AS appears in the middle of the AS path)
# * a numeric variable (numOfPrefixesOriginated) specifying the number of prefixes originated by the AS
def computeASesStats(bgp_handler, del_handler, ASrels, stats_filename, def_dict, allAttr):
    today = datetime.date.today()
    expanded_del_asns_df = del_handler.getExpandedASNsDF() 
        
    for i in expanded_del_asns_df.index:
        statsForAS = def_dict.copy()
        
        asn = int(expanded_del_asns_df.ix[i]['initial_resource'])
        statsForAS['asn'] = asn
        statsForAS['asn_type'] = expanded_del_asns_df.ix[i]['status']
        statsForAS['opaque_id'] = expanded_del_asns_df.ix[i]['opaque_id']
        statsForAS['cc'] = expanded_del_asns_df.ix[i]['cc']
        statsForAS['region'] = expanded_del_asns_df.ix[i]['region']
        del_date = expanded_del_asns_df.ix[i]['date'].to_pydatetime().date()
        statsForAS['del_date'] = del_date
        statsForAS['numOfUpstreams'] = len(ASrels[(ASrels['rel_type'] == 'P2C')\
                                            & (ASrels['AS2'] == asn)])

        try:
            statsForAS['numOfPrefixesOriginated_curr'] =\
                                len(bgp_handler.ASes_originated_prefixes_dic[asn])
        except KeyError:
            statsForAS['numOfPrefixesOriginated_curr'] = 0
       
        try:
            statsForAS['numOfPrefixesPropagated_curr'] =\
                                len(bgp_handler.ASes_propagated_prefixes_dic[asn])
        except KeyError:
            statsForAS['numOfPrefixesPropagated_curr'] = 0
                
        asn_dates_dict = None
        
        if asn in bgp_handler.originASesDates_dict:
            asn_dates_dict = bgp_handler.originASesDates_dict[asn]
            
        if asn in bgp_handler.middleASesDates_dict:
            asn_dates_dict = bgp_handler.middleASesDates_dict[asn]

        if asn_dates_dict is not None:
            daysUsable = (today-del_date).days + 1

            # Usage Latency
            # We define usage latency of an ASN as the time interval between
            # the delegation date and the first the ASN is seen as an origin AS
            # or as a middle AS in the BGP routing table being analyzed
            first_seen = datetime.datetime.strptime(asn_dates_dict['firstSeen'],\
                                                    '%Y%m%d').date()
                        
            if first_seen is not None:
                statsForAS['UsageLatency'] = (first_seen-del_date).days + 1
            else:
                statsForAS['UsageLatency'] = float('inf')               
                
            # History of Activity
            periodsActive = asn_dates_dict['periodsSeen']
            numOfPeriods = len(periodsActive)
                
            if numOfPeriods > 0:
                last_seen = datetime.datetime.strptime(asn_dates_dict['lastSeen'],\
                                                        '%Y%m%d').date()
                statsForAS['isDead'] = ((today-last_seen).days > 365)
                
                daysUsed = (last_seen-first_seen).days + 1
                daysSeen = asn_dates_dict['totalDays']
                    
                # We define the "relative used time" as the percentage of
                # days the ASN was used from the total number of days
                # the ASN could have been used (Usable time)
                statsForAS['relUsedTime'] = 100*float(daysUsed)/daysUsable
                
                # We define the "effective usage" as the percentage of days
                # the ASN was seen from the number of days the ASN was used
                statsForAS['effectiveUsage'] = 100*float(daysSeen)/daysUsed
                
                # We define the "time fragmentation" as the average number
                # of periods in a 60 days (aprox 2 months) time lapse.
                # We chose to use 60 days to be coherent with the
                # considered interval of time used to analyze visibility
                # stability in [1]
                statsForAS['timeFragmentation'] = numOfPeriods/(float(daysUsed)/60)
                                                
                periodsLengths = []
                for period in periodsActive:
                    periodsLengths.append(\
                        (datetime.datetime.strptime(str(period[1]), '%Y%m%d').date() -\
                        datetime.datetime.strptime(str(period[0]), '%Y%m%d').date()).days + 1)
                
                periodsLengths = np.array(periodsLengths)
                statsForAS['avgPeriodLength'] = periodsLengths.mean()
                statsForAS['stdPeriodLength'] = periodsLengths.std()
                statsForAS['minPeriodLength'] = periodsLengths.min()
                statsForAS['maxPeriodLength'] = periodsLengths.max()

        line = statsForAS[allAttr[0]]
        
        for i in range(len(allAttr)-1):
            line = '{},{}'.format(line, statsForAS[allAttr[i+1]])
        
        line = line + '\n'

        with open(stats_filename, 'a') as stats_file:
            stats_file.write(line)
            
    
def main(argv):
    
    urls_file = './BGPoutputs.txt'
    urls_provided = False
    RIBfiles = True
    files_path = ''
    routing_file = ''
    KEEP = False
    COMPUTE = True 
    DEBUG = False
    EXTENDED = False
    date = ''
    UNTIL = False
    del_file = ''
    prefixes_stats_file = ''
    ases_stats_file = ''
    fromFiles = False
    bgp_data_file = ''
    ipv4_prefixes_indexes_file = ''
    ipv6_prefixes_indexes_file = ''
    ASes_originated_prefixes_file = ''
    ASes_propagated_prefixes_file = ''
    ipv4_prefixesDates_file = ''
    ipv6_prefixesDates_file = ''
    originASesDates_file = ''
    middleASesDates_file = ''
    archive_folder = '' 
    ext = ''
    COMPRESSED = False
    startDate = ''
    INCREMENTAL = False
    final_existing_date = ''

#For DEBUG
    files_path = '/Users/sofiasilva/BGP_files'
#    routing_file = '/Users/sofiasilva/BGP_files/bgptable.txt'
    KEEP = True
#    RIBfiles = False
    DEBUG = True
    EXTENDED = True
    del_file = '/Users/sofiasilva/BGP_files/extended_apnic_20170315.txt'
    archive_folder = '/Users/sofiasilva/BGP_files'
    ext = 'bgprib.mrt'
    UNTIL = True
    date = '20170115'
#    COMPRESSED = True
#    COMPUTE = False    
    
    try:
        opts, args = getopt.getopt(argv, "hf:u:r:H:E:I:ocknD:Ud:ep:a:b:4:6:O:P:F:S:A:M:", ["files_path=", "urls_file=", "routing_file=", "Historcial_data_folder=", "Extension=", "InitialDate=", "Date=", "delegated_file=", "prefixes_stats_file=", "ases_stats_file=", "bgp_data_file=", "IPv4_prefixes_ASes_file=", "IPv6_prefixes_ASes_file=", "ASes_Originated_prefixes_file=", "ASes_Propagated_prefixes_file=", "ipv4_prefixesDates_file=", "ipv6_prefixesDates_file=", "originASesDates_file=", "middleASesDates_file="])
    except getopt.GetoptError:
        print 'Usage: routing_stats_prefixesAndASes.py -h | -f <files path> [-u <urls file> | -r <routing file> | -H <Historical data folder> -E <extension> [-I <Initial date>]] [-o] [-c] [-k] [-n] [-D Date [-U]] [-d <delegated file>] [-e] [-p <prefixes stats file> -a <ases stats file>] [-b <bgp_data file> -4 <IPv4 prefixes_indexes file> -6 <IPv6 prefixes_indexes file> -O <ASes_Originated_prefixes file> -P <ASes_Propagated_prefixes file>] [-F <ipv4_prefixesDates file>] [-S <ipv6_prefixesDates file>] [-A <originASesDates_file>] [-M <middleASesDates_file>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes routing statistics from files containing Internet routing data and a delegated file."
            print 'Usage: routing_stats_prefixesAndASes.py -h | -f <files path> [-u <urls file> | -r <routing file> | -H <Historical data folder> -E <extension> [-I <Initial date>]] [-o] [-c] [-k] [-n] [-D Date [-U]] [-d <delegated file>] [-e] [-p <prefixes stats file> -a <ases stats file>] [-b <bgp_data file> -4 <IPv4 prefixes_indexes file> -6 <IPv6 prefixes_indexes file> -O <ASes_Originated_prefixes file> -P <ASes_Propagated_prefixes file>] [-F <ipv4_prefixesDates file>] [-S <ipv6_prefixesDates file>] [-A <originASesDates_file>] [-M <middleASesDates_file>]'
            print 'h = Help'
            print "f = Path to folder in which Files will be saved. (MANDATORY)"
            print 'u = URLs file. File which contains a list of URLs of the files to be downloaded.'
            print 'All the URLs must point either to RIB files or to files containing "show ip bgp" outputs.'
            print 'If the URLs point to files containing "show ip bgp" outputs, the "-o" option must be used to specify this.'
            print 'r = Use already downloaded Internet Routing data file.'
            print 'If the routing file contains a "show ip bgp" output, the "-o" option must be used to specify this.'
            print "H = Historical data. Instead of processing a single file, process the routing data contained in the archive folder provided."
            print "E = Extension. If you use the -H option you MUST also use the -E option to provide the extension of the files in the archive you want to work with."
            print "I = Incremental dates. If you use this option you must provide a start date for the period of time for which you want to get the dates in which the prefixes were seen."
            print "If you also use the -F or the -S option to provide an existing prefixesDates file, the new dates will be added to the existing dates in the Radix."
            print "If none of the three options -u, -r or -H are provided, the script will try to work with routing data from URLs included ./BGPoutputs.txt"
            print 'o = The routing data to be processed is in the format of "show ip bgp" outputs.'
            print 'c = Compressed. The files containing routing data are compressed.'
            print 'k = Keep downloaded Internet routing data file.'
            print 'n = No computation. If this option is used, statistics will not be computed, just the dictionaries with prefixes/origin ASes will be created and saved to disk.'
            print 'D = Date in format YYYY or YYYYmm or YYYYmmdd. Delegation date of the resources for which you want the stats to be computed or or until which (if you use the -U option) you want to consider delegations.'
            print 'U = Until. If you use the -U option the resources for which you want the statistics to be computed will be filtered so that they have a delegation date before the provided date and the routing data considered corresponds to dates before the provided date.'
            print 'If you use the -U option, you MUST also use the -H option and provide the path to the archive folder.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'e = Use Extended file'
            print "If option -e is used in DEBUG mode, delegated file must be a extended file."
            print "If option -e is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "p = Compute incremental statistics from existing prefixes stats file (CSV)."
            print "a = Compute incremental statistics from existing ASes stats file (CSV)."
            print "Both the -p and the -a options should be used or none of them should be used."
            print "If options -p and -a are used, the corresponding paths to files with existing statistics for prefixes and ASes respectively MUST be provided."
            print "b = BGP_data file. Path to pickle file containing bgp_data DataFrame."
            print "4 = IPv4 prefixes_indexes file. Path to pickle file containing IPv4 prefixes_indexes Radix."
            print "6 = IPv6 prefixes_indexes file. Path to pickle file containing IPv6 prefixes_indexes Radix."
            print "O = ASes_Originated_prefixes file. Path to pickle file containing ASes_Originated_prefixes dictionary."
            print "P = ASes_Propagated_prefixes file. Path to pickle file containing ASes_Propagated_prefixes dictionary."
            print "If you want to work with BGP data from files, the five options -b, -4, -6, -O and -P must be used."
            print "If not, none of these five options should be used."
            print "F = IPv4 (Four) prefixesDates file. Path to pickle file containing prefixesDates Radix with the dates in which each IPv4 prefix was seen."
            print "S = IPv6 (Six) prefixesDates file. Path to pickle file containing prefixesDates Radix with the dates in which each IPv6 prefix was seen."
            print "A = Origin ASes Dates file. Path to pickle file containing originASesDates dictionary with the date in which each ASN originated prefixes."
            print "M = Middle ASes Dates file. Path to pickle file containing middleASesDates dictionary with the date in which each ASN propagated prefixes."
            sys.exit()
        elif opt == '-u':
            if arg != '':
                urls_file = os.path.abspath(arg)
            else:
                print "If option -u is used, the path to a file which contains a list of URLs of the files to be downloaded MUST be provided."
                sys.exit()
            urls_provided = True
        elif opt == '-o':
            RIBfiles = False
        elif opt == '-c':
            COMPRESSED = True
        elif opt == '-r':
            if arg != '':
                routing_file = os.path.abspath(arg)
            else:
                print "If option -r is used, the path to a file with Internet routing data MUST be provided."
                sys.exit()
        elif opt == '-k':
            KEEP = True
        elif opt == '-n':
            COMPUTE = False
        elif opt == '-D':
            date = arg
            if date == '':
                print "If option -D is used, a date MUST be provided."
                sys.exit()
        elif opt == '-U':
            UNTIL = True
        elif opt == '-d':
            DEBUG = True
            if arg != '':
                del_file = os.path.abspath(arg)
            else:
                print "If you choose to run in DEBUG mode you must provide the path to\
                    a delegated file that has already been downloaded."
                sys.exit()
        elif opt == '-e':
            EXTENDED = True
        elif opt == '-f':
            if arg != '':
                files_path = os.path.abspath(arg.rstrip('/'))
            else:
                print "You must provide the path to a folder to save files."
                sys.exit()
        elif opt == '-p':
            if arg != '':
                prefixes_stats_file = os.path.abspath(arg)
                INCREMENTAL = True
            else:
                print "If option -p is used, the path to a file with statistics for prefixes MUST be provided."
                sys.exit()
        elif opt == '-a':
            if arg != '':
                ases_stats_file = os.path.abspath(arg)
                INCREMENTAL = True
            else:
                print "If option -p is used, the path to a file with statistics for prefixes MUST be provided."
                sys.exit()
        elif opt == '-b':
            if arg != '':
                bgp_data_file = os.path.abspath(arg)
                fromFiles = True
            else:
                print "If option -b is used, the path to a file with BGP data MUST be provided."
                sys.exit()
        elif opt == '-4':
            if arg != '':
                ipv4_prefixes_indexes_file = os.path.abspath(arg)
                fromFiles = True
            else:
                print "If option -4 is used, the path to a pickle file containing IPv4 prefixes_indexes Radix MUST be provided."
                sys.exit()
        elif opt == '-6':
            if arg != '':
                ipv6_prefixes_indexes_file = os.path.abspath(arg)
                fromFiles = True
            else:
                print "If option -6 is used, the path to a pickle file containing IPv6 prefixes_indexes Radix MUST be provided."
                sys.exit()
        elif opt == '-O':
            if arg != '':
                ASes_originated_prefixes_file = os.path.abspath(arg)
                fromFiles = True
            else:
                print "If option -O is used, the path to a pickle file containing ASes_Originated_prefixes dictionary MUST be provided."
                sys.exit()
        elif opt == '-P':
            if arg != '':
                ASes_propagated_prefixes_file = os.path.abspath(arg)
                fromFiles = True
            else:
                print "If option -O is used, the path to a pickle file containing ASes_Propagated_prefixes dictionary MUST be provided."
                sys.exit()
        elif opt == '-H':
            if arg != '':
                archive_folder = os.path.abspath(arg.rstrip('/'))
            else:
                print "If option -H is used, the path to a folder containing historical BGP data MUST be provided."
                sys.exit()
        elif opt == '-E':
            ext = arg
        elif opt == '-I':
            startDate = int(arg)
        elif opt == '-F':
            if arg != '':
                ipv4_prefixesDates_file = os.path.abspath(arg)
            else:
                print "If option -F is used, the path to a pickle file containing prefixesDates Radix with the dates in which each IPv4 prefix was seen MUST be provided."
                sys.exit()
        elif opt == '-S':
            if arg != '':
                ipv6_prefixesDates_file = os.path.abspath(arg)
            else:
                print "If option -S is used, the path to a pickle file containing prefixesDates Radix with the dates in which each IPv6 prefix was seen MUST be provided."
                sys.exit()
        elif opt == '-A':
            if arg != '':
                originASesDates_file = os.path.abspath(arg)
            else:
                print "If option -A is used, the path to a pickle file containing originASesDates dictionary with the date in which each ASN originated prefixes MUST be provided."
                sys.exit()
        elif opt == '-M':
            if arg != '':
                middleASesDates_file = os.path.abspath(arg)
            else:
                print "If option -M is used, the path to a pickle file containing middleASesDates dictionary with the date in which each ASN propagated prefixes MUST be provided."
                sys.exit()
        else:
            assert False, 'Unhandled option'
            
    if urls_provided and (routing_file != '' or archive_folder != '') or\
        routing_file != '' and (urls_provided or archive_folder != '') or\
        archive_folder != '' and (urls_provided or routing_file != ''):

        print "You MUST NOT use more than one of the -u, -r and -H options."
        sys.exit()
        
    if date != '' and not (len(date) == 4 or len(date) == 6 or len(date) == 8):
        print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
        sys.exit()
        
    if UNTIL and len(date) != 8:
        print 'If you use the -U option, you MUST provide a full date in format YYYYmmdd'
        sys.exit()
        
    if UNTIL and archive_folder == '':
        print 'If you use the -U option, you MUST also use the -H option and provide the path to the archive folder.'
        sys.exit()
        
    # If files_path does not exist, we create it
    if not os.path.exists(files_path):
        os.makedirs(files_path)
        
    if archive_folder != '' and ext == '':
        print "If you use the -H option you MUST also use the -E option to provide the extension of the files in the archive you want to work with."
        sys.exit()

        
    if INCREMENTAL:
        if prefixes_stats_file == '' or ases_stats_file == '':
            print "You CANNOT use only one of the option -p and -a."            
            print "Both the -p and the -a options should be used or none of them should be used."
            print "If options -p and -a are used, the corresponding paths to files with existing statistics for prefixes and ASes respectively MUST be provided."
        try:
            maxDate_prefixes = max(pd.read_csv(prefixes_stats_file, sep = ',')['Date'])
            maxDate_ases = max(pd.read_csv(ases_stats_file, sep = ',')['Date'])
            final_existing_date = str(max(maxDate_prefixes, maxDate_ases))
        except (ValueError, pd.EmptyDataError, KeyError):
            final_existing_date = ''
            INCREMENTAL = False
    
    if fromFiles and (bgp_data_file == '' or ipv4_prefixes_indexes_file == '' or\
        ipv6_prefixes_indexes_file == '' or ASes_originated_prefixes_file == '' or\
        ASes_propagated_prefixes_file == ''):
        print "If you want to work with BGP data from files, the three options -b, -x, -a and -s must be used."
        print "If not, none of these three options should be used."
        sys.exit()
        
    today = datetime.date.today().strftime('%Y%m%d')
    
    if date == '':
        dateStr = 'AllDates'
    elif UNTIL:
        dateStr = 'UNTIL{}'.format(date)
    else:
        dateStr = date
        
    if not DEBUG:
        file_name = '%s/routing_stats_%s' % (files_path, dateStr)
        
        if EXTENDED:
            del_file = '{}/extended_apnic_{}.txt'.format(files_path, today)
        else:
            del_file = '{}/delegated_apnic_{}.txt'.format(files_path, today)

    else:
        file_name = '%s/routing_stats_test_%s' % (files_path, dateStr)
        
    bgp_handler = BGPDataHandler(DEBUG, files_path, KEEP, RIBfiles, COMPRESSED)

    loaded = False 
    
    if ipv4_prefixesDates_file != '' or ipv6_prefixesDates_file != '':
        bgp_handler.loadPrefixDatesFromFiles(ipv4_prefixesDates_file, ipv6_prefixesDates_file)    
    
    if originASesDates_file != '':
        bgp_handler.loadOriginASesDatesFromFile(originASesDates_file)

    if middleASesDates_file != '':
        bgp_handler.loadMiddleASesDatesFromFile(middleASesDates_file)
        
    if fromFiles:
        loaded = bgp_handler.loadStructuresFromFiles(bgp_data_file, ipv4_prefixes_indexes_file,
                                ipv6_prefixes_indexes_file, ASes_originated_prefixes_file,
                                ASes_propagated_prefixes_file)
    else:
        if routing_file == '' and archive_folder == '':
            loaded = bgp_handler.loadStructuresFromURLSfile(urls_file)
        elif routing_file != '':
            loaded = bgp_handler.loadStructuresFromRoutingFile(routing_file)
        else: # archive_folder not null
            loaded = bgp_handler.loadStructuresFromArchive(archive_folder, ext,
                                                           startDate, date)
    
    if not loaded:
        print "Data structures not loaded!\n"
        sys.exit()
        
    if COMPUTE: 
        del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file, date, UNTIL,
                                        INCREMENTAL, final_existing_date)

        ASrels = getASrelInfo(serial=2, files_path=files_path, KEEP=KEEP)
                        
        # isRoutedIntact: Boolean variable that will be True is the prefix that
        # was delegated appears as-is in the routing table
        # isDead: Boolean variable that will be True is the prefix or any part
        # of it haven't been visible in the routing table for more than a year
        # isDeadIntact: Boolean variable that will be True if the prefix as-is
        # hasn't been visible in the routing table for more than a year
        # OriginatedByDiffOrg: Boolean key to store info about the prefix being
        # originated by an AS that was delegated to an organization that is not
        # the same that received the delegation of the block

        # Usage Latency :  Based on [1]
        # We define usage latency of a delegated address block as
        # the time interval between the delegation date and the first
        # time a part of, or the entire, delegated block shows up in
        # the BGP routing table baing analyzed.
        # We compute UsageLatencyGral considering the block itself and its
        # fragments and UsageLatencyIntact only considering the block itself.

        # We define the "relative used time" as the percentage of days the
        # prefix was used from the total number of days the prefix could have
        # been used (Usable time). We compute relUsedTimeIntact only considering
        # the prefix itself and avgRelUsedTimeGral using average values for the
        # values corresponding to the prefix and its fragments.

        # We define the "effective usage" as the percentage of days the prefix
        # was seen from the number of days the prefix was used. We compute
        # effectiveUsageIntact only considering the prefix itself and
        # avgEffectiveUsageGral using average values for the values
        # corresponding to the prefix and its fragments

        # We define the "time fragmentation" as the average number of periods
        # in a 60 days (aprox 2 months) time lapse. We chose to use 60 days
        # to be coherent with the considered interval of time used to analyze
        # visibility stability in [1]. We compute timeFragmentationIntact only
        # considering the prefix itself and avgTimeFragmentationGral using
        # average values for the values corresponding to the prefix and its fragments

        # The period length is the number of days corresponding to a period of
        # time during which a prefix or its fragments were seen. We compute the
        # average, the standard deviation, the maximum and the minimum amongst
        # all the periods of time of the prefix and its fragments (Gral) and
        # only taking into account the prefix (Intact).

        # The visibility is the percentaje of IP addresses, from the total number
        # of IP addresses included in the delegated prefix, that are visible in
        # the routing table at a specific time. We compute the average, the
        # standard deviation, the maximum and the minimum amongst the values of
        # visibility for the prefix over time.

        # If the delegated prefix is being routed, we get a set of the ASes that
        # originate the prefix and save the length of this set (numOfOriginASes).
        # We also get a set of all the AS paths in the BGP announcements of the
        # prefix and save the number of AS paths (numOfASpaths) and compute and
        # save the average, standard distribution, maximum and minimum amongst
        # the lengths of these AS paths.

        # For the routed fragments of the delegated prefix that are classified 
        # as SODP or DODP we compute the Levenshtein Distance between the AS path
        # of the fragment and the AS path of its covering prefix. After computing
        # this distance for all the corresponding fragments, we compute the
        # average, standard deviation, maximum and minimum amongst all the distances.

        # Without taking into consideration the historical routing data, but
        # just from the most recent routing file being considered, we compute
        # the current visibility of the delegated prefix.

        
        # The rest of the variables correspond to concepts defined in [1] and [2].
        # We just use the concepts defined in [2] that are not redundant with the
        # concepts defined in [1] 
        # Prefixes classified as Top in [2] are the prefixes classified as Covering
        # in [1]. Prefixes classified as Deaggregated in [2] are those classified as
        # SOSP or SODP in [1]. Prefixes classified as Delegated in [2] are those
        # classified as DOSP or DODP in [1].     
        
        booleanKeys_pref = ['isRoutedIntact', 'isDead', 'isDeadIntact',
                            'originatedByDiffOrg', 'onlyRoot', 'rootMSCompl',
                            'rootMSIncompl', 'noRootMSCompl', 'noRootMSIncompl']
                                       
        valueKeys_pref = ['UsageLatencyGral', 'UsageLatencyIntact',
                          'relUsedTimeIntact', 'avgRelUsedTimeGral',
                          'effectiveUsageIntact', 'avgEffectiveUsageGral',
                          'timeFragmentationIntact', 'avgTimeFragmentationGral',
                          'avgPeriodLengthIntact', 'stdPeriodLengthIntact',
                          'maxPeriodLengthIntact', 'minPeriodLengthIntact',
                          'avgPeriodLengthGral', 'stdPeriodLengthGral',
                          'maxPeriodLengthGral', 'minPeriodLengthGral',
                          'avgVisibility', 'stdVisibility', 'maxVisibility',
                          'minVisibility', 'avgASPathLength', 'stdASPathLength',
                          'maxASPathLength', 'minASPathLength',
                          'avgLevenshteinDist', 'stdLevenshteinDist',
                          'minLevenshteinDist', 'maxLevenshteinDist',
                          'currentVisibility']
                          
        counterKeys_pref = ['numOfOriginASes', 'numOfASPaths',
                            'numOfLessSpecificsRouted', 'numOfMoreSpecificsRouted',
                            'numOfCoveringMoreSpec', 'numOfCoveredLevel1MoreSpec',
                            'numOfCoveredLevel2MoreSpec', 'numOfSOSPMoreSpec',
                            'numOfSODP1MoreSpec', 'numOfSODP2MoreSpec',
                            'numOfDOSPMoreSpec', 'numOfDODP1MoreSpec',
                            'numOfDODP2MoreSpec', 'numOfDODP3MoreSpec',
                            'numOfLonelyMoreSpec']

        other_data_columns = ['delegated_prefix', 'del_date', 'opaque_id', 'cc',
                              'region', 'mostRecentRoutingData_date']
        
        allAttr_pref = other_data_columns + booleanKeys_pref +\
                        valueKeys_pref + counterKeys_pref

        if not INCREMENTAL:
            line = allAttr_pref[0]
        
            for i in range(len(allAttr_pref)-1):
                line = '{},{}'.format(line, allAttr_pref[i+1])
        
            line = line + '\n'
        
            prefixes_stats_file = '{}_prefixes.csv'.format(file_name)
        
            with open(prefixes_stats_file, 'w') as csv_file:
                csv_file.write(line)
        
        def_dict_pref = getDictionaryWithDefaults(booleanKeys_pref, valueKeys_pref, counterKeys_pref)

        start_time = time.time()
        visibilityPerPeriods = computePerPrefixStats(bgp_handler, del_handler,\
                                                    ASrels, prefixes_stats_file,\
                                                    def_dict_pref, allAttr_pref)
        end_time = time.time()
        sys.stderr.write("Stats for prefixes computed successfully!\n")
        sys.stderr.write("Statistics computation took {} seconds\n".format(end_time-start_time))   

        prefixes_stats_df = pd.read_csv(prefixes_stats_file, sep = ',')
        prefixes_json_filename = '{}_prefixes.json'.format(file_name)
        prefixes_stats_df.to_json(prefixes_json_filename, orient='index')
        sys.stderr.write("Prefixes stats saved to JSON file successfully!\n")
        sys.stderr.write("Files generated:\n{}\nand\n{})\n".format(prefixes_stats_file,
                                                        prefixes_json_filename))

        visibility_file_name = '{}/visibilityPerPeriods_{}.pkl'.format(files_path, today)
        with open(visibility_file_name, 'wb') as visibility_file:
            pickle.dump(visibilityPerPeriods, visibility_file, pickle.HIGHEST_PROTOCOL)
            sys.stderr.write("Saved to disk %s pickle file containing dictionary of the visibiity of each prefix during each period of time.\n" % visibility_file_name)

        valueKeys_ases = ['UsageLatency', 'relUsedTime', 'effectiveUsage',
                            'timeFragmentation', 'avgPeriodLength', 'stdPeriodLength',
                            'minPeriodLength', 'maxPeriodLength']

        counterKeys_ases = ['numOfPrefixesOriginated_curr',
                            'numOfPrefixesPropagated_curr']
        booleanKeys_ases = ['isDead']
        
        expanded_del_asn_df_columns = ['asn', 'del_date', 'asn_type',
                                        'opaque_id', 'cc', 'region']        

        allAttr_ases = expanded_del_asn_df_columns + booleanKeys_ases +\
                        valueKeys_ases + counterKeys_ases

        line = allAttr_ases[0]
        
        for i in range(len(allAttr_ases)-1):
            line = '{},{}'.format(line, allAttr_ases[i+1])
        
        line = line + '\n'

        if not INCREMENTAL:
            ases_stats_file = '{}_ASes.csv'.format(file_name)
        
            with open(ases_stats_file, 'w') as csv_file:
                csv_file.write(line)
                
        def_dict_ases = getDictionaryWithDefaults(booleanKeys_ases,
                                                  valueKeys_ases, counterKeys_ases)
        
        start_time = time.time()
        computeASesStats(bgp_handler, del_handler, ASrels, ases_stats_file,\
                            def_dict_ases, allAttr_ases)
        end_time = time.time()
        sys.stderr.write("Stats for ASes computed successfully!\n")
        sys.stderr.write("Statistics computation took {} seconds\n".format(end_time-start_time))   

        
    else:
       bgp_handler.saveDataToFiles()
        
        
if __name__ == "__main__":
    main(sys.argv[1:])

# [1] http://irl.cs.ucla.edu/papers/05-ccr-address.pdf
# [2] http://www.eecs.qmul.ac.uk/~steve/papers/JSAC-deaggregation.pdf