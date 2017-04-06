#! /usr/bin/python2.7 
# -*- coding: utf8 -*-


import sys, getopt, os
os.chdir(os.path.dirname(os.path.realpath(__file__)))
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from RoutingStats import RoutingStats
from OrgHeuristics import OrgHeuristics
import ipaddress
import pandas as pd
import numpy as np
import math
import copy
from datetime import date, datetime, timedelta
from calendar import monthrange
from time import time
from ElasticSearchImporter import ElasticSearchImporter
import prefStats_ES_properties
import ASesStats_ES_properties
    
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
    
def weighted_avg_and_std(values, weights):
    """
    Return the weighted average and standard deviation.

    values, weights -- Numpy ndarrays with the same shape.
    """
    average = np.average(values, weights=weights)
    variance = np.average((values-average)**2, weights=weights)  # Fast and numerically precise
    return (average, math.sqrt(variance))
    
def computeNetworkHistoryOfVisibility(network, statsForPrefix, db_handler, first_seen_intact):
    prefix = str(network)
    # History of Visibility (Visibility Evoluntion in Time) computation
    today = date.today()
    daysUsable = (today-statsForPrefix['del_date']).days + 1
    
    # Intact Allocation History of Visibility
    # The Intact Allocation History of Visibility only takes into
    # account the exact block that was delegated
    periodsIntact = db_handler.getPeriodsSeenExact(prefix)
    numOfPeriods = len(periodsIntact)
    
    if numOfPeriods > 0:
        last_seen_intact = db_handler.getDateLastSeenExact(prefix)
        statsForPrefix['isDeadIntact'] = ((today-last_seen_intact).days > 365)
        daysUsed = (last_seen_intact-first_seen_intact).days + 1
        daysSeen = db_handler.getTotalDaysSeenExact(prefix)
        
        statsForPrefix['relUsedTimeIntact'] = 100*float(daysUsed)/daysUsable
        statsForPrefix['effectiveUsageIntact'] = 100*float(daysSeen)/daysUsed
        statsForPrefix['timeFragmentationIntact'] = numOfPeriods/(float(daysUsed)/60)
                        
        periodsLengths = []
        for period in periodsIntact:
            periodsLengths.append((period[1] - period[0]).days + 1)
        
        periodsLengths = np.array(periodsLengths)
        statsForPrefix['avgPeriodLengthIntact'] = periodsLengths.mean()
        statsForPrefix['stdPeriodLengthIntact'] = periodsLengths.std()
        statsForPrefix['minPeriodLengthIntact'] = periodsLengths.min()
        statsForPrefix['maxPeriodLengthIntact'] = periodsLengths.max()
        
    # General History of Visibility of Prefix
    # The General History of Visibility takes into account not only
    # the block itself being routed but also its fragments
    periodsGral = db_handler.getPeriodsSeenGral(prefix)
    
    if len(periodsGral) > 0:
        
        numsOfPeriodsGral = []
        daysUsedGral = []
        daysSeenGral = []
        periodsLengthsGral = []
        prefixesPerPeriod = dict()
        timeBreaks = []
        
        for fragment in periodsGral:
            numsOfPeriodsGral.append(len(periodsGral[fragment]))
            daysUsedGral.append(\
                (db_handler.getDateLastSeenExact(fragment) -\
                db_handler.getDateFirstSeenExact(fragment)).days+1)
            daysSeenGral.append(db_handler.getTotalDaysSeenExact(fragment))
            
            for period in periodsGral[fragment]:
                timeBreaks.append(period[0])
                timeBreaks.append(period[1])
                
                if period not in prefixesPerPeriod:
                    prefixesPerPeriod[period] = [fragment]
                else:
                    prefixesPerPeriod[period].append(fragment)
                    
                periodsLengthsGral.append((period[1] - period[0]).days+1)
             
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
            visibilities = []
            periodLengths = []
                        
            last_seen = db_handler.getDateLastSeen(prefix)
            
            if len(timeBreaks) == 1:
                numOfIPsVisible = 0
                
                for period in prefixesPerPeriod:
                    if timeBreaks[0] >= period[0] and timeBreaks[0]\
                                                        <= period[1]:
                        for prefix in prefixesPerPeriod[period]:
                            numOfIPsVisible += prefix.num_addresses
                
                visibility = float(numOfIPsVisible)*100/network.num_addresses
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
                visibilities.append(visibility)
                periodLengths.append((timeBreaks[i+1] - timeBreaks[i]).days+1)
            
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

def computeNetworkUsageLatency(prefix, statsForPrefix, db_handler): 
    # Usage Latency computation
    first_seen = db_handler.getDateFirstSeen(prefix)
                
    if first_seen is not None and statsForPrefix['del_date'] is not None:
        statsForPrefix['UsageLatencyGral'] =\
                                (first_seen-statsForPrefix['del_date']).days + 1
    else:
        statsForPrefix['UsageLatencyGral'] = float('inf')
      
    # Intact Allocation Usage Latency computation
    # We compute the number of days between the date the block
    # was delegated and the date the block as-is was first seen
    if first_seen is not None:
        first_seen_intact = db_handler.getDateFirstSeenExact(prefix)
    else:
        first_seen_intact = None
                
    if first_seen_intact is not None:
        statsForPrefix['UsageLatencyIntact'] =\
                        (first_seen_intact-statsForPrefix['del_date']).days + 1
    else:
        statsForPrefix['UsageLatencyIntact'] = float('inf')
    
    return first_seen_intact

def getASNOpaqueID(asn, del_handler):
    asn_del = del_handler.fullASN_df
    
    opaque_id = asn_del[(pd.to_numeric(asn_del['initial_resource']) <= long(asn)) &\
                            (pd.to_numeric(asn_del['initial_resource'])+\
                            pd.to_numeric(asn_del['count/prefLen'])>\
                            long(asn))]['opaque_id'].get_values()
    if len(opaque_id) == 1:
        return opaque_id[0]
    # There can not be more than one result in the ASN DataFrame,
    # therefore, if the len is not 1, it's 0 and it means the origin AS was
    # not delegated by APNIC
    else:
        return 'UNKNOWN'
        
def checkIfSameOrg(routedPrefix, blockOriginASes, prefix_org, del_handler):
    nirs_opaque_ids = ['A91A7381', 'A9186214', 'A9162E3D', 'A918EDB2',
                       'A9149F3E', 'A91BDB29', 'A91A560A']
                       
    sameOrgs = True
              
    for blockOriginAS in blockOriginASes:
        originASorg = getASNOpaqueID(blockOriginAS, del_handler)
        if prefix_org in nirs_opaque_ids or\
            originASorg in nirs_opaque_ids:
                sameOrgs = OrgHeuristics.checkIfSameOrg(routedPrefix, blockOriginAS)
        elif originASorg == 'UNKNOWN':
            # If we get an UNKNOWN organization for the AS it means the AS
            # was not delegated by APNIC, therefore, it cannot be the same
            # organization than the one that received the delegation of
            # the prefix.
            sameOrgs = False
        else:
            # If we have both opaque ids (for the prefix and for the
            # origin AS), and neither of them are opaque ids of NIRs,
            # we just compare the opaque ids.
            sameOrgs = (prefix_org == originASorg)
            
        if not sameOrgs:
            break
    
    return sameOrgs
    

# This function classifies the provided prefix into Covering or Covered
# (Level 1 or Level 2+). If the prefix is a covered prefix, it is further
# classified into the following classes depending on the relation between its
# origin ASes and its covering prefix's origin ASes and the relation between its
# AS paths and its covering prefix's AS paths: 'SOSP', 'SODP1', 'SODP2', 'DOSP',
# 'DODP1', 'DODP2', 'DODP3'.
# These classes are taken from those defined in [1]
# The corresponding variables in the statsForPrefix dictionary are incremented
# to keep track of the number of prefixes in each class.
def classifyPrefixAndUpdateVariables(routedPrefix, statsForPrefix, variables,\
                                        diffOrg_var, levenshteinDists,\
                                        routingStatsObj, files_path):
    
    routedNetwork = ipaddress.ip_network(unicode(routedPrefix, "utf-8"))

    blockOriginASes = routingStatsObj.bgp_handler.getOriginASesForBlock(routedNetwork)
    blockASpaths = routingStatsObj.bgp_handler.getASpathsForBlock(routedNetwork)
        
    # If the delegated prefix is not already marked as having fragments being
    # announced by an AS delegated to an organization different from the
    # organization that received the delegation of the prefix
    if not statsForPrefix[diffOrg_var]:
        # We check if the routed prefix  (fragment of the delegated prefix)
        # and all its origin ASes were delegated to the same organization
        statsForPrefix[diffOrg_var] = not checkIfSameOrg(routedPrefix,\
                                                                    blockOriginASes,\
                                                                    statsForPrefix['opaque_id'],\
                                                                    routingStatsObj.del_handler)
                
                    
    # Find routed blocks related to the prefix of interest 
    net_less_specifics = routingStatsObj.bgp_handler.getRoutedParentAndGrandparents(routedNetwork)
    net_more_specifics = routingStatsObj.bgp_handler.getRoutedChildren(routedNetwork)
   
    # If there is at least one less specific block being routed,
    # the block is a covered prefix
    if len(net_less_specifics) > 0:        
        if len(net_less_specifics) == 1:
            statsForPrefix[variables['coveredLevel1']] += 1
        else:
            statsForPrefix[variables['coveredLevel2plus']] += 1

        # We classify the covered prefix based on its AS paths
        # relative to those of its corresponding covering prefix

        # The corresponding covering prefix is the last prefix in the
        # list of less specifics
        coveringPref = net_less_specifics[-1]
        coveringNet = ipaddress.ip_network(unicode(coveringPref, 'utf-8'))
        coveringPrefOriginASes = routingStatsObj.bgp_handler.getOriginASesForBlock(coveringNet)
        coveringPrefASpaths = routingStatsObj.bgp_handler.getASpathsForBlock(coveringNet)       
  
        if len(blockOriginASes.intersection(coveringPrefOriginASes)) > 0:
            if len(blockASpaths) == 1:
                if blockASpaths.issubset(coveringPrefASpaths):
                    statsForPrefix[variables['SOSP']] += 1
                else:
                    statsForPrefix[variables['SODP2']] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
                                                
            else: # len(blockASpaths) >= 2
                if len(coveringPrefASpaths.intersection(blockASpaths)) > 0 and\
                    len(blockASpaths.difference(coveringPrefASpaths)) > 0:
                    statsForPrefix[variables['SODP1']] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
                        
        else:
            if len(blockASpaths) == 1:
                if len(blockOriginASes) == 1:
                    blockOriginAS = long(blockOriginASes.pop())

                    # We check whether the origin AS of the
                    # block is a customer of any of the origin ASes
                    # of the covering prefix                       
                    for coveringPrefOriginAS in coveringPrefOriginASes:
                        if len(routingStatsObj.ASrels[(routingStatsObj.ASrels['AS1'] == long(coveringPrefOriginAS)) &\
                                (routingStatsObj.ASrels['AS2'] == blockOriginAS) &\
                                (routingStatsObj.ASrels['rel_type'] == 'P2C')]):

                                for coveringPrefASpath in coveringPrefASpaths:
                                    if coveringPrefASpath.split(' ')[-1] == coveringPrefOriginAS:
                                        blockASpath_woOrigin = ' '.join(list(blockASpaths)[0].split(' ')[0:-1])
                                        
                                        if blockASpath_woOrigin == coveringPrefASpath:
                                            statsForPrefix[variables['DOSP']] += 1
                                            break
                                else:
                                    continue
                                break
                                
                if not blockASpaths.issubset(coveringPrefASpaths):
                    statsForPrefix[variables['DODP1']] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
                                                
            else: # len(blockASpaths) >= 2
                blockASpaths_woOrigin = set()
                for ASpath in blockASpaths:
                    blockASpaths_woOrigin.add(' '.join(ASpath.split(' ')[0:-1]))
                
                if len(coveringPrefASpaths.intersection(blockASpaths_woOrigin)) > 0 and\
                    len(blockASpaths_woOrigin.difference(coveringPrefASpaths)) > 0:
                    statsForPrefix[variables['DODP2']] += 1
                    
                    levenshteinDists.extend(getLevenshteinDistances(blockASpaths,
                                                                    coveringPrefASpaths))
              
            if len(coveringPrefASpaths.intersection(blockASpaths)) == 0:
                blockOriginASesCustomers = set()
                for blockOriginAS in blockOriginASes:
                    blockOriginASesCustomers.\
                        union(routingStatsObj.ASrels[(routingStatsObj.ASrels['AS1'] == long(blockOriginAS)) &\
                                (routingStatsObj.ASrels['rel_type'] == 'P2C')]['AS2'].tolist())
                
                coveringPrefOriginASesCustomers = set()
                for coveringPrefOriginAS in coveringPrefOriginASes:
                    coveringPrefOriginASesCustomers.\
                        union(routingStatsObj.ASrels[(routingStatsObj.ASrels['AS1'] == long(coveringPrefOriginAS)) &\
                                (routingStatsObj.ASrels['rel_type'] == 'P2C')]['AS2'].tolist())
                
                commonCustomers = blockOriginASesCustomers.\
                                    intersection(coveringPrefOriginASesCustomers)
                if len(commonCustomers) > 0:
                    for commonCustomer in commonCustomers:
                        correspondingOriginASes =\
                            set(routingStatsObj.ASrels[(routingStatsObj.ASrels['AS2'] == long(commonCustomer)) &\
                                (routingStatsObj.ASrels['rel_type'] == 'P2C')]['AS1'].tolist()).\
                                intersection(blockOriginASes)
                        for correspondingOriginAS in correspondingOriginASes:
                            if len(routingStatsObj.bgp_handler.ASes_originated_prefixes_dic[correspondingOriginAS]) >= 2:
                                statsForPrefix[variables['DODP3']] += 1
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
            statsForPrefix[variables['lonely']] += 1
        elif (routedPrefix not in net_more_specifics and len(net_more_specifics) > 0)\
            or (routedPrefix in net_more_specifics and len(net_more_specifics) > 1):
        # If there are more specific blocks being routed apart from
        # the block itself, taking into account we are under the case
        # of the block not having less specific blocks being routed,
            # The block is a Covering prefix  
            statsForPrefix[variables['covering']] += 1

    
        

def writeStatsLineToFile(statsForPrefix, allAttr, stats_filename):
    line = statsForPrefix[allAttr[0]]
        
    for i in range(len(allAttr)-1):
        line = '{},{}'.format(line, statsForPrefix[allAttr[i+1]])
    
    line = line + '\n'

    with open(stats_filename, 'a') as stats_file:
        stats_file.write(line)

# Decide which historical data has to be taken into account in this case
def computePerPrefixStats(routingStatsObj, stats_filename, files_path, TEMPORAL_DATA):
    # Obtain a subset of all the delegated prefixes
    delegatedNetworks = routingStatsObj.del_handler.delegated_df[\
                            (routingStatsObj.del_handler.delegated_df['resource_type'] == 'ipv4') |\
                            (routingStatsObj.del_handler.delegated_df['resource_type'] == 'ipv6')]
                
    for i in delegatedNetworks.index:
        prefix_row = delegatedNetworks.ix[i]
        prefix = '{}/{}'.format(prefix_row['initial_resource'],\
                                int(prefix_row['count/prefLen']))
        statsForPrefix = routingStatsObj.def_dict_pref.copy()

        statsForPrefix['prefix'] = prefix
        statsForPrefix['del_date'] = prefix_row['date'].date()
        statsForPrefix['resource_type'] = prefix_row['resource_type']
        statsForPrefix['status'] = prefix_row['status']
        statsForPrefix['opaque_id'] = prefix_row['opaque_id']
        statsForPrefix['cc'] = prefix_row['cc']
        statsForPrefix['region'] = prefix_row['region']
        statsForPrefix['routing_date'] = str(routingStatsObj.bgp_handler.routingDate)

        delNetwork = ipaddress.ip_network(unicode(prefix, "utf-8"))
        
        if TEMPORAL_DATA:
            first_seen_intact = computeNetworkUsageLatency(prefix, statsForPrefix, routingStatsObj.db_handler)
            computeNetworkHistoryOfVisibility(delNetwork, statsForPrefix, routingStatsObj.db_handler, first_seen_intact)

        ips_delegated = delNetwork.num_addresses 

        # Find routed blocks related to the prefix of interest 
        less_specifics = routingStatsObj.bgp_handler.getRoutedParentAndGrandparents(delNetwork)
        more_specifics = routingStatsObj.bgp_handler.getRoutedChildren(delNetwork)
        
        if len(less_specifics) > 0:
            statsForPrefix['currentVisibility'] = 100
            statsForPrefix['numOfLessSpecificsRouted'] = len(less_specifics)
            
            levenshteinDists = []
            for lessSpec in less_specifics:
                classifyPrefixAndUpdateVariables(lessSpec, statsForPrefix,
                                                 routingStatsObj.lessSpec_variables,
                                                 'hasLessSpecificsOriginatedByDiffOrg',
                                                 levenshteinDists, routingStatsObj,
                                                 files_path)
            
            if len(levenshteinDists) > 0:
                levenshteinDists = np.array(levenshteinDists)
                statsForPrefix['avgLevenshteinDistLessSpec'] = levenshteinDists.mean()
                statsForPrefix['stdLevenshteinDistLessSpec'] = levenshteinDists.std()
                statsForPrefix['minLevenshteinDistLessSpec'] = levenshteinDists.min()
                statsForPrefix['maxLevenshteinDistLessSpec'] = levenshteinDists.max()
            
            
        if len(more_specifics) > 0:
            statsForPrefix['numOfMoreSpecificsRouted'] = len(more_specifics)

            more_specifics_wo_prefix = copy.copy(more_specifics)

            if prefix in more_specifics:
                
                blockOriginASes = routingStatsObj.bgp_handler.getOriginASesForBlock(delNetwork)
                statsForPrefix['numOfOriginASesIntact'] = len(blockOriginASes)
                
                statsForPrefix['originatedByDiffOrg'] = not checkIfSameOrg(\
                                                                prefix,\
                                                                blockOriginASes,\
                                                                statsForPrefix['opaque_id'],\
                                                                routingStatsObj.del_handler)
                more_specifics_wo_prefix.remove(prefix)

                statsForPrefix['isRoutedIntact'] = True
                statsForPrefix['currentVisibility'] = 100
                
                if len(more_specifics) == 1 and len(less_specifics) == 0:
                    statsForPrefix['onlyRoot'] = True
                
                # We get the set of AS paths for the block
                blockASpaths = routingStatsObj.bgp_handler.getASpathsForBlock(delNetwork)
                statsForPrefix['numOfASPathsIntact'] = len(blockASpaths)
                
                ASpathsLengths = []
                for path in blockASpaths:
                    ASpathsLengths.append(len(path.split()))
                    
                ASpathsLengths = np.array(ASpathsLengths)
                statsForPrefix['avgASPathLengthIntact'] = ASpathsLengths.mean()
                statsForPrefix['stdASPathLengthIntact'] = ASpathsLengths.std()
                statsForPrefix['minASPathLengthIntact'] = ASpathsLengths.min()
                statsForPrefix['maxASPathLengthIntact'] = ASpathsLengths.max()

                if len(more_specifics) > 1:    
                    aggr_more_spec = [ipaddr for ipaddr in
                                        ipaddress.collapse_addresses(
                                        [ipaddress.ip_network(unicode(ip_net, 'utf-8'))
                                        for ip_net in more_specifics_wo_prefix])]
                                        
                    if len(more_specifics) >= 3 and prefix in\
                                    [str(ip_net) for ip_net in aggr_more_spec]:
                        # • root/MS-complete: The root prefix and at least two subprefixes
                        # are announced. The set of all sub-prefixes spans the whole root prefix.
                        statsForPrefix['rootMSCompl'] = True
                    elif len(more_specifics) >= 2 and prefix not in\
                                    [str(ip_net) for ip_net in aggr_more_spec]:
                        # • root/MS-incomplete: The root prefix and at least one subprefix
                        # is announced. Together, the set of announced subprefixes
                        # does not cover the root prefix.
                        statsForPrefix['rootMSIncompl'] = True

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
                    statsForPrefix['noRootMSCompl'] = True
                elif len(more_specifics) >= 1 and prefix not in\
                                    [str(ip_net) for ip_net in aggr_more_spec]:
                    # • no root/MS-incomplete: The root prefix is not announced.
                    # There is at least one sub-prefix. Taking all sub-prefixes
                    # together, they do not cover the complete root prefix.
                    statsForPrefix['noRootMSIncompl'] = True
            
            numsOfOriginASesGral = []
            numsOfASPathsGral = []
            ASPathLengthsGral = []                
            for fragment in more_specifics:
                fragmentNetwork = ipaddress.ip_network(unicode(fragment, "utf-8"))
                numsOfOriginASesGral.append(len(routingStatsObj.bgp_handler.getOriginASesForBlock(fragmentNetwork)))
                fragmentASpaths = routingStatsObj.bgp_handler.getASpathsForBlock(fragmentNetwork)
                numsOfASPathsGral.append(len(fragmentASpaths))
                for asPath in fragmentASpaths:
                    ASPathLengthsGral.append(len(asPath.split()))
            
            numsOfOriginASesGral = np.array(numsOfOriginASesGral)
            statsForPrefix['avgNumOfOriginASesGral'] = numsOfOriginASesGral.mean()
            statsForPrefix['stdNumOfOriginASesGral'] = numsOfOriginASesGral.std()
            statsForPrefix['minNumOfOriginASesGral'] = numsOfOriginASesGral.min()
            statsForPrefix['maxNumOfOriginASesGral'] = numsOfOriginASesGral.max()

            numsOfASPathsGral = np.array(numsOfASPathsGral)
            statsForPrefix['avgNumOfASPathsGral'] = numsOfASPathsGral.mean()
            statsForPrefix['stdNumOfASPathsGral'] = numsOfASPathsGral.std()
            statsForPrefix['minNumOfASPathsGral'] = numsOfASPathsGral.min()
            statsForPrefix['maxNumOfASPathsGral'] = numsOfASPathsGral.max()

            ASPathLengthsGral = np.array(ASPathLengthsGral)
            statsForPrefix['avgASPathLengthGral'] = ASPathLengthsGral.mean()
            statsForPrefix['stdASPathLengthGral'] = ASPathLengthsGral.std()
            statsForPrefix['minASPathLengthGral'] = ASPathLengthsGral.min()
            statsForPrefix['maxASPathLengthGral'] = ASPathLengthsGral.max()
        
            levenshteinDists = []
            for moreSpec in more_specifics:
                classifyPrefixAndUpdateVariables(moreSpec, statsForPrefix,
                                                 routingStatsObj.moreSpec_variables,
                                                 'hasFragmentsOriginatedByDiffOrg',
                                                 levenshteinDists, routingStatsObj,
                                                 files_path)

            if len(levenshteinDists) > 0:
                levenshteinDists = np.array(levenshteinDists)
                statsForPrefix['avgLevenshteinDistMoreSpec'] = levenshteinDists.mean()
                statsForPrefix['stdLevenshteinDistMoreSpec'] = levenshteinDists.std()
                statsForPrefix['minLevenshteinDistMoreSpec'] = levenshteinDists.min()
                statsForPrefix['maxLevenshteinDistMoreSpec'] = levenshteinDists.max()
            
        writeStatsLineToFile(statsForPrefix, routingStatsObj.allAttr_pref, stats_filename)
        
# This function determines whether the allocated ASNs are active
# either as middle AS, origin AS or both
# Returns dictionary with an ASN as key and a dictionary containing:
# * a numeric variable (numOfPrefixesPropagated) specifying the number of prefixes propagated by the AS
# (BGP announcements for which the AS appears in the middle of the AS path)
# * a numeric variable (numOfPrefixesOriginated) specifying the number of prefixes originated by the AS
def computeASesStats(routingStatsObj, stats_filename, TEMPORAL_DATA):
    today = date.today()
    expanded_del_asns_df = routingStatsObj.del_handler.getExpandedASNsDF() 
        
    for i in expanded_del_asns_df.index:
        statsForAS = routingStatsObj.def_dict_ases.copy()
        
        asn = long(expanded_del_asns_df.ix[i]['initial_resource'])
        statsForAS['asn'] = asn
        statsForAS['asn_type'] = expanded_del_asns_df.ix[i]['status']
        statsForAS['opaque_id'] = expanded_del_asns_df.ix[i]['opaque_id']
        statsForAS['cc'] = expanded_del_asns_df.ix[i]['cc']
        statsForAS['region'] = expanded_del_asns_df.ix[i]['region']
        del_date = expanded_del_asns_df.ix[i]['date'].to_pydatetime().date()
        statsForAS['del_date'] = del_date
        statsForAS['routing_date'] = str(routingStatsObj.bgp_handler.routingDate)
        statsForAS['numOfUpstreams'] = len(routingStatsObj.ASrels[(routingStatsObj.ASrels['rel_type'] == 'P2C')\
                                            & (routingStatsObj.ASrels['AS2'] == long(asn))])

        try:
            statsForAS['numOfPrefixesOriginated_curr'] =\
                                len(routingStatsObj.bgp_handler.ASes_originated_prefixes_dic[asn])
        except KeyError:
            statsForAS['numOfPrefixesOriginated_curr'] = 0
       
        try:
            statsForAS['numOfPrefixesPropagated_curr'] =\
                                len(routingStatsObj.bgp_handler.ASes_propagated_prefixes_dic[asn])
        except KeyError:
            statsForAS['numOfPrefixesPropagated_curr'] = 0
                
        if TEMPORAL_DATA:
            daysUsable = (today-del_date).days + 1
    
            # Usage Latency
            # We define usage latency of an ASN as the time interval between
            # the delegation date and the first date the ASN is seen as an origin AS
            # or as a middle AS in the BGP routing table being analyzed
            first_seen = routingStatsObj.db_handler.getDateASNFirstSeen(asn)
                        
            if first_seen is not None:
                statsForAS['UsageLatency'] = (first_seen-del_date).days + 1
            else:
                statsForAS['UsageLatency'] = float('inf')               
                
            # History of Activity
            periodsActive = routingStatsObj.db_handler.getPeriodsASNSeen(asn)
            numOfPeriods = len(periodsActive)
                
            if numOfPeriods > 0:
                last_seen = routingStatsObj.db_handler.getDateASNLastSeen(asn)
                statsForAS['isDead'] = ((today-last_seen).days > 365)
                
                daysUsed = (last_seen-first_seen).days + 1
                daysSeen = routingStatsObj.db_handler.getTotalDaysASNSeen(asn)
                    
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
                    periodsLengths.append((period[1] - period[0]).days + 1)
                
                periodsLengths = np.array(periodsLengths)
                statsForAS['avgPeriodLength'] = periodsLengths.mean()
                statsForAS['stdPeriodLength'] = periodsLengths.std()
                statsForAS['minPeriodLength'] = periodsLengths.min()
                statsForAS['maxPeriodLength'] = periodsLengths.max()

        line = statsForAS[routingStatsObj.allAttr_ases[0]]
        
        for i in range(len(routingStatsObj.allAttr_ases)-1):
            line = '{},{}'.format(line, statsForAS[routingStatsObj.allAttr_ases[i+1]])
        
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
    startDate = ''
    endDate = ''
    del_file = ''
    prefixes_stats_file = ''
    ases_stats_file = ''
    fromFiles = False
    ipv4_prefixes_file = ''
    ipv6_prefixes_file = ''
    ASes_originated_prefixes_file = ''
    ASes_propagated_prefixes_file = ''
    routing_date = ''
    archive_folder = '' 
    ext = ''
    COMPRESSED = False
    INCREMENTAL = False
    final_existing_date = ''
    TEMPORAL_DATA = False
    READABLE = True
    es_host = ''
    
    try:
        opts, args = getopt.getopt(argv, "hf:u:r:H:e:S:E:TNocknd:xp:a:4:6:O:P:R:D:",
                                        ["files_path=", "urls_file=", "routing_file=",
                                        "Historcial_data_folder=", "extension=",
                                        "StartDate=", "EndDate=", "delegated_file=",
                                        "prefixes_stats_file=", "ases_stats_file=",
                                        "IPv4_prefixes_ASes_file=",
                                        "IPv6_prefixes_ASes_file=",
                                        "ASes_Originated_prefixes_file=",
                                        "ASes_Propagated_prefixes_file=",
                                        "Routing date=",
                                        "ElasticsearchDB_host="])
    except getopt.GetoptError:
        print 'Usage: computeRoutingStats.py -h | -f <files path> [-u <urls file> | -r <routing file> | -H <Historical data folder> -e <extension>] [-S <Start date>] [-E <End Date>] [-T] [-N] [-o] [-c] [-k] [-n] [-d <delegated file>] [-x] [-p <prefixes stats file> -a <ases stats file>] [-4 <IPv4 prefixes file> -6 <IPv6 prefixes file> -O <ASes_Originated_prefixes file> -P <ASes_Propagated_prefixes file> -R <Routing data date>] [-D <Elasticsearch Database host>]'
        sys.exit()
    for opt, arg in opts:
        if opt == '-h':
            print "This script computes routing statistics from files containing Internet routing data and a delegated file."
            print 'Usage: computeRoutingStats.py -h | -f <files path> [-u <urls file> | -r <routing file> | -H <Historical data folder> -e <extension>] [-S <Start date>] [-E <End Date>] [-T] [-N] [-o] [-c] [-k] [-n] [-d <delegated file>] [-x] [-p <prefixes stats file> -a <ases stats file>] [-4 <IPv4 prefixes file> -6 <IPv6 prefixes file> -O <ASes_Originated_prefixes file> -P <ASes_Propagated_prefixes file> -R <Routing data date>] [-D <Elasticsearch Database host>]'
            print 'h = Help'
            print "f = Path to folder in which Files will be saved. (MANDATORY)"
            print 'u = URLs file. File which contains a list of URLs of the files to be downloaded.'
            print 'All the URLs must point either to RIB files or to files containing "show ip bgp" outputs.'
            print 'If the URLs point to files containing "show ip bgp" outputs, the "-o" option must be used to specify this.'
            print 'r = Use already downloaded Internet Routing data file.'
            print 'If the routing file contains a "show ip bgp" output, the "-o" option must be used to specify this.'
            print "H = Historical data. Instead of processing a single file, process the routing data contained in the archive folder provided."
            print "e = Extension. If you use the -H option you MUST also use the -E option to provide the extension of the files in the archive you want to work with."
            print "If none of the three options -u, -r or -H are provided, the script will try to work with routing data from URLs included ./BGPoutputs.txt"
            print "S = Start date in format YYYY or YYYYmm or YYYYmmdd. The start date of the period of time during which the considered resources were delegated."
            print 'E = End date in format YYYY or YYYYmm or YYYYmmdd. The end date of the period of time during which the considered resources were delegated.'
            print "T = Temporal data available. Use this option if there is temporal data available in the visibility database and you want to use it, even if you don't provide the path to the archive."
            print "N = Not readable. The routing data provided (in the files to which the URLs file points, in the routing file or in the archive are not in the 'readable' format (BGPdump output))."
            print 'o = The routing data to be processed is in the format of "show ip bgp" outputs.'
            print 'c = Compressed. The files containing routing data are compressed.'
            print 'k = Keep downloaded Internet routing data file.'
            print 'n = No computation. This option MUST only be used in combination with the -H option. If this option is used, statistics will not be computed and the historical data from the archive will be stored into the database.'
            print 'd = DEBUG mode. Provide path to delegated file. If not in DEBUG mode the latest delegated file will be downloaded from ftp://ftp.apnic.net/pub/stats/apnic'
            print 'x = Use eXtended file'
            print "If option -x is used in DEBUG mode, delegated file must be a extended file."
            print "If option -x is not used in DEBUG mode, delegated file must be delegated file not extended."
            print "p = Compute incremental statistics from existing prefixes stats file (CSV)."
            print "a = Compute incremental statistics from existing ASes stats file (CSV)."
            print "Both the -p and the -a options should be used or none of them should be used."
            print "If options -p and -a are used, the corresponding paths to files with existing statistics for prefixes and ASes respectively MUST be provided."
            print "4 = IPv4 prefixes file. Path to pickle file containing IPv4 prefixes Radix."
            print "6 = IPv6 prefixes file. Path to pickle file containing IPv6 prefixes Radix."
            print "O = ASes_Originated_prefixes file. Path to pickle file containing ASes_Originated_prefixes dictionary."
            print "P = ASes_Propagated_prefixes file. Path to pickle file containing ASes_Propagated_prefixes dictionary."
            print "R = Routing data Date in format YYYYmmdd. Date for which routing stats will be computed."
            print "If a routing date is not provided, statistics for the day before today will be computed."
            print "If you use the options -4, -6, -O and -P you MUST provide the date for the routing data in these files."
            print "If you want to work with BGP data from files, the five options -4, -6, -O, -P and -R must be used."
            print "If not, none of these five options should be used."
            print "D = Elasticsearch Database host. Host running Elasticsearch into which computed stats will be stored."
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
        elif opt == '-S':
            startDate = arg
            if startDate == '':
                print "If option -S is used, a start date MUST be provided."
                sys.exit()
        elif opt == '-E':
            endDate = arg
            if endDate == '':
                print "If option -E is used, an end date MUST be provided."
                sys.exit()
        elif opt == '-d':
            DEBUG = True
            if arg != '':
                del_file = os.path.abspath(arg)
            else:
                print "If you choose to run in DEBUG mode you must provide the path to\
                    a delegated file that has already been downloaded."
                sys.exit()
        elif opt == '-x':
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
        elif opt == '-4':
            if arg != '':
                ipv4_prefixes_file = os.path.abspath(arg)
                fromFiles = True
            else:
                print "If option -4 is used, the path to a pickle file containing IPv4 prefixes Radix MUST be provided."
                sys.exit()
        elif opt == '-6':
            if arg != '':
                ipv6_prefixes_file = os.path.abspath(arg)
                fromFiles = True
            else:
                print "If option -6 is used, the path to a pickle file containing IPv6 prefixes Radix MUST be provided."
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
        elif opt == '-R':
            try:
                routing_date = datetime.strptime(arg, '%Y%m%d').date()
            except ValueError:
                print "If option -R is used, a date in format YYYYmmdd MUST be provided."
                sys.exit()
        elif opt == '-H':
            if arg != '':
                archive_folder = os.path.abspath(arg.rstrip('/'))
            else:
                print "If option -H is used, the path to a folder containing historical BGP data MUST be provided."
                sys.exit()
        elif opt == '-e':
            ext = arg
        elif opt == '-T':
            TEMPORAL_DATA = True
        elif opt == '-N':
            READABLE = False
        elif opt == '-D':
            es_host = arg
        else:
            assert False, 'Unhandled option'
    
    if not COMPUTE and archive_folder == '':
        print "If you just want historical data to be stored into the database, you MUST provide a path to the archive folder."
        sys.exit()
            
    if urls_provided and (routing_file != '' or archive_folder != '') or\
        routing_file != '' and (urls_provided or archive_folder != '') or\
        archive_folder != '' and (urls_provided or routing_file != ''):

        print "You MUST NOT use more than one of the -u, -r and -H options."
        sys.exit()
        
    if startDate != '':
        try:
            if len(startDate) == 4:
                startDate_date = datetime.strptime(startDate, '%Y').date()
            elif len(startDate) == 6:
                startDate_date = datetime.strptime(startDate, '%Y%m').date()
            elif len(startDate) == 8:
                startDate_date = datetime.strptime(startDate, '%Y%m%d').date()
            else:
                print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
                sys.exit()
        except ValueError:
            print "Error when parsing start date.\n"
            print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
            sys.exit()
    else:
        startDate_date = ''

    today = date.today()

    if endDate == '':
        endDate_date = today - timedelta(1)
    else:
        if len(endDate) == 4:
            endYear = endDate
            endMonth = '12'
            endDay = monthrange(int(endYear), int(endMonth))[1]
        elif len(endDate) == 6:
            endYear = endDate[0:4]
            endMonth = endDate[4:6]
            endDay = monthrange(int(endYear), int(endMonth))[1]
        elif len(endDate) == 8:
            endYear = endDate[0:4]
            endMonth = endDate[4:6]
            endDay = endDate[6:8]
        else:
            print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
            sys.exit()

        try:  
            endDate_date = datetime.strptime('{}{}{}'.format(endYear, endMonth, endDay), '%Y%m%d').date()   
        except ValueError:
            print "Error when parsing end date.\n"
            print 'You must provide a date in the format YYYY or YYYYmm or YYYYmmdd.'
            sys.exit()
    
    # If a routing date is not provided, stats are computed for the day before today
    if routing_date == '':
        routing_date = today - timedelta(1)
    
    if endDate_date > routing_date:
        print 'The routing date provided must be higher than the end of the period of time for the considered delegations.'
        sys.exit()
        
    # If files_path does not exist, we create it
    if not os.path.exists(files_path):
        os.makedirs(files_path)
        
    if archive_folder != '' and ext == '':
        print "If you use the -H option you MUST also use the -e option to provide the extension of the files in the archive you want to work with."
        sys.exit()

        
    dateStr = 'Delegated_BEFORE{}'.format(endDate)

    if startDate != '':
        dateStr = 'Delegated_AFTER{}{}'.format(startDate, dateStr)
    
    dateStr = '{}_AsOf{}'.format(dateStr, routing_date)
        
    if not DEBUG:
        file_name = '%s/routing_stats_%s' % (files_path, dateStr)
        
        if EXTENDED:
            del_file = '{}/extended_apnic_{}.txt'.format(files_path, today)
        else:
            del_file = '{}/delegated_apnic_{}.txt'.format(files_path, today)

    else:
        file_name = '%s/routing_stats_test_%s' % (files_path, dateStr)
        
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
    else:
        prefixes_stats_file = '{}_prefixes.csv'.format(file_name)
        ases_stats_file = '{}_ases.csv'.format(file_name)
    
    if fromFiles and (ipv4_prefixes_file == '' or\
        ipv6_prefixes_file == '' or ASes_originated_prefixes_file == '' or\
        ASes_propagated_prefixes_file == '' or routing_date == ''):
        print "If you want to work with BGP data from files, the five options -4, -6, -O, -P and -R must be used."
        print "If not, none of these five options should be used."
        sys.exit()
    
    routingStatsObj = RoutingStats(files_path, DEBUG, KEEP, COMPUTE, EXTENDED,
                                    del_file, startDate_date, endDate_date,
                                    routing_date, INCREMENTAL,
                                    final_existing_date, file_name)
    
    if COMPUTE:
        loaded = False 
            
        if fromFiles:
            loaded = routingStatsObj.bgp_handler.loadStructuresFromFiles(routing_date, ipv4_prefixes_file,
                                    ipv6_prefixes_file, ASes_originated_prefixes_file,
                                    ASes_propagated_prefixes_file)
                                    
        else:
            if routing_file == '' and archive_folder == '':
                loaded = routingStatsObj.bgp_handler.loadStructuresFromURLSfile(\
                            urls_file, READABLE, RIBfiles)
    
            elif routing_file != '':
                loaded = routingStatsObj.bgp_handler.loadStructuresFromRoutingFile(\
                            routing_file, READABLE, RIBfiles, COMPRESSED)
    
            else: # archive_folder not null
                loaded = routingStatsObj.bgp_handler.loadStructuresFromArchive(\
                            archive_folder, ext, routing_date, READABLE, RIBfiles,
                            COMPRESSED)
        
        if not loaded:
            print "Data structures not loaded!\n"
            sys.exit()
        
        start_time = time()
        computePerPrefixStats(routingStatsObj, prefixes_stats_file, files_path,
                              TEMPORAL_DATA)
        end_time = time()
        sys.stderr.write("Stats for prefixes computed successfully!\n")
        sys.stderr.write("Statistics computation took {} seconds\n".format(end_time-start_time))   

        prefixes_stats_df = pd.read_csv(prefixes_stats_file, sep = ',')
        prefixes_json_filename = '{}_prefixes.json'.format(file_name)
        prefixes_stats_df.to_json(prefixes_json_filename, orient='index')
        sys.stderr.write("Prefixes stats saved to JSON file successfully!\n")
        sys.stderr.write("Files generated:\n{}\nand\n{})\n".format(prefixes_stats_file,
                                                        prefixes_json_filename))
        
        if es_host != '':
            esImporter = ElasticSearchImporter(es_host)
            esImporter.createIndex(prefStats_ES_properties.mapping,
                                   prefStats_ES_properties.index_name)
            numOfDocs = esImporter.ES.count(prefStats_ES_properties.index_name)['count']

            prefixes_stats_df = prefixes_stats_df.fillna(-1)
            
            bulk_data, numOfDocs = esImporter.prepareData(prefixes_stats_df,
                                                          prefStats_ES_properties.index_name,
                                                          prefStats_ES_properties.doc_type,
                                                          numOfDocs,
                                                          prefStats_ES_properties.unique_index)
                                                
            dataImported = esImporter.inputData(prefStats_ES_properties.index_name,
                                                bulk_data, numOfDocs)
    
            if dataImported:
                sys.stderr.write("Stats about usage of prefixes delegated during the period {} were saved to ElasticSearch successfully!\n".format(dateStr))
            else:
                sys.stderr.write("Stats about usage of prefixes delegated during the period {} could not be saved to ElasticSearch.\n".format(dateStr))
            
        
        start_time = time()
        computeASesStats(routingStatsObj, ases_stats_file, TEMPORAL_DATA)
        end_time = time()
        sys.stderr.write("Stats for ASes computed successfully!\n")
        sys.stderr.write("Statistics computation took {} seconds\n".format(end_time-start_time))   

        routingStatsObj.db_handler.close()

        ases_stats_df = pd.read_csv(ases_stats_file, sep = ',')
        ases_json_filename = '{}_ases.json'.format(file_name)
        ases_stats_df.to_json(ases_json_filename, orient='index')
        sys.stderr.write("ASes stats saved to JSON file successfully!\n")
        sys.stderr.write("Files generated:\n{}\nand\n{})\n".format(ases_stats_file,
                                                                    ases_json_filename))
        
        if es_host != '':
            esImporter.createIndex(ASesStats_ES_properties.mapping,
                                   ASesStats_ES_properties.index_name)
            numOfDocs = esImporter.ES.count(ASesStats_ES_properties.index_name)['count']

            ases_stats_df = ases_stats_df.fillna(-1)
            
            bulk_data, numOfDocs = esImporter.prepareData(ases_stats_df,
                                                          ASesStats_ES_properties.index_name,
                                                          ASesStats_ES_properties.doc_type,
                                                          numOfDocs,
                                                          ASesStats_ES_properties.unique_index)
                                                
            dataImported = esImporter.inputData(ASesStats_ES_properties.index_name,
                                                bulk_data, numOfDocs)
    
            if dataImported:
                sys.stderr.write("Stats about usage of ASNs delegated during the period {} were saved to ElasticSearch successfully!\n".format(dateStr))
            else:
                sys.stderr.write("Stats about usage of ASNs delegated during the period {} could not be saved to ElasticSearch.\n".format(dateStr))
                    
    else:
        if startDate == endDate:
            routing_files = routingStatsObj.bgp_handler.getSpecificFilesFromArchive(\
                                                        archive_folder, 'bgprib.mrt',
                                                        startDate)
            if len(routing_files) == 0:
                routing_files = routingStatsObj.bgp_handler.getSpecificFilesFromArchive(\
                                                        archive_folder, 'dmp.gz',
                                                        startDate)
                if len(routing_files) == 0:
                    sys.stderr.write("No routing file for the date provided was found in the archive.\n")
                
                else:
                    for routing_file in routing_files:
                        routingStatsObj.bgp_handler.storeHistoricalDataFromFile(\
                                                    routing_file, False, False, True)
            else:
                for routing_file in routing_files:
                    routingStatsObj.bgp_handler.storeHistoricalDataFromFile(\
                                                    routing_file, False, True, False)
        else:
            routingStatsObj.bgp_handler.storeHistoricalDataFromArchive(archive_folder,
                                                                       ext, READABLE,
                                                                       RIBfiles,
                                                                       COMPRESSED,
                                                                       startDate_date,
                                                                       endDate_date)
        
if __name__ == "__main__":
    main(sys.argv[1:])

# [1] http://irl.cs.ucla.edu/papers/05-ccr-address.pdf
# [2] http://www.eecs.qmul.ac.uk/~steve/papers/JSAC-deaggregation.pdf