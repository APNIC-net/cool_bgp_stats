# -*- coding: utf-8 -*-
"""
Created on Sat Mar 25 09:39:25 2017

@author: sofiasilva
"""
import os, bz2
os.chdir(os.path.dirname(os.path.realpath(__file__)))
#Just for DEBUG
#os.chdir('/Users/sofiasilva/GitHub/cool_bgp_stats')
from DelegatedHandler import DelegatedHandler
from DBHandler import DBHandler
from OrgHeuristics import OrgHeuristics
from get_file import get_file
import pandas as pd
import numpy as np

class RoutingStats:
    
    def __init__(self, files_path, DEBUG, KEEP, EXTENDED, del_file,\
                startDate, endDate, routing_date, prefixes_stats_file,
                ases_stats_file, TEMPORAL_DATA):
        
        self.files_path = files_path
        
        self.del_handler = DelegatedHandler(DEBUG, EXTENDED, del_file,
                                            startDate, endDate, False,
                                            '', KEEP)

        if TEMPORAL_DATA:
            self.db_handler = DBHandler(routing_date)
        
        # We just declare the orgHeuristics variable
        # The class will be instantiated the first time it is needed
        self.orgHeuristics = None
        
        # We comment this line out as the URL to download CAIDA's AS relationships
        # dataset does not always work. It is not conceived to be automatically
        # downloaded but access to the data has to be requested.
#            self.ASrels = self.getASrelInfo(serial=2, files_path=files_path, KEEP=KEEP)
                        
        # isRoutedIntact: Boolean variable that will be True is the prefix that
        # was delegated appears as-is in the routing table
        # isDead: Boolean variable that will be True is the prefix or any part
        # of it haven't been visible in the routing table for more than a year
        # isDeadIntact: Boolean variable that will be True if the prefix as-is
        # hasn't been visible in the routing table for more than a year
        # originatedByDiffOrg: Boolean key to store info about the prefix being
        # originated by an AS that was delegated to an organization that is not
        # the same that received the delegation of the block
        # hasFragmentsOriginatedByDiffOrg and hasLessSpecificsOriginatedByDiffOrg
        # are analogous boolean variables storing info about the routed fragments
        # (more specifics) and routed less specifics of the delegated block
        # being originated by an AS that was delegated to an organization
        # that is not the same that received the delegation of the block.

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
        # originate the prefix and save the length of this set (numOfOriginASesIntact).
        # We also get a set of all the AS paths in the BGP announcements of the
        # prefix and save the number of AS paths (numOfASpathsIntact) and compute and
        # save the average, standard distribution, maximum and minimum amongst
        # the lengths of these AS paths (avg, std, min and max ASPathLengthIntact).

        # Taking into account the routed fragments we compute the avg, std,
        # min and max of the number of originASes (numOfOriginASesGral), of
        # the number of AS paths (numOfASPathsGral) and of the length of the
        # AS paths (ASPathLengthGral).

        # For the routed prefixes that have a different AS path from the
        # AS path of its covering prefix, we compute the Levenshtein Distance
        # between the AS path of the covered prefix and the AS path of its
        # covering prefix. After computing this distance for all the
        # corresponding fragments, we compute the average, standard
        # deviation, maximum and minimum amongst all the distances.

        # Without taking into consideration the historical routing data, but
        # just from the most recent routing file being considered, we compute
        # the current visibility of the delegated prefix.

        # numOfAnnouncements and numOfWithdraws count the number of BGP
        # announcements and BGP withdraws during the updatesDate for a routed
        # prefix or for an origin AS. We also compute the average, standard
        # deviation, maximum and minimum number of announcements and number of
        # withdraws amongst the values for the routed more specific prefixes and
        # for the routed less specific prefixes of a delegated prefix.

        
        # The rest of the variables correspond to concepts defined in [1] and [2].
        
        booleanGralKeys_pref = ['isRoutedIntact', 'originatedByDiffOrg',
                                'hasFragmentsOriginatedByDiffOrg',
                                'hasLessSpecificsOriginatedByDiffOrg',
                                'onlyRoot', 'rootMSCompl', 'rootMSIncompl',
                                'noRootMSCompl', 'noRootMSIncompl']
        
        booleanGralKeysTemp_pref = ['isDead', 'isDeadIntact']
        
        self.prefix_variables = {'lonely': 'isLonely',
                                 'holePunching': 'isHolePunching',
                                 'trafficEngineering': 'isTrafficEngineering',
                                 'overlay': 'isOverlay',
                                 'coveredLevel1': 'isCoveredLevel1',
                                 'coveredLevel2plus': 'isCoveredLevel2plus',
                                 'covering': 'isCovering'}
                                   
        booleanKeys_pref = booleanGralKeys_pref + self.prefix_variables.values()
        
        if TEMPORAL_DATA:
            booleanKeys_pref = booleanKeys_pref + booleanGralKeysTemp_pref
        
        valueKeys_pref = ['avgASPathLengthIntact', 'stdASPathLengthIntact',
                          'maxASPathLengthIntact', 'minASPathLengthIntact',
                          'avgNumOfOriginASesMoreSpec', 'stdNumOfOriginASesMoreSpec',
                          'minNumOfOriginASesMoreSpec', 'maxNumOfOriginASesMoreSpec',
                          'avgNumOfASPathsMoreSpec', 'stdNumOfASPathsMoreSpec',
                          'minNumOfASPathsMoreSpec', 'maxNumOfASPathsMoreSpec',
                          'avgASPathLengthMoreSpec', 'stdASPathLengthMoreSpec',
                          'minASPathLengthMoreSpec', 'maxASPathLengthMoreSpec',
                          'avgNumOfOriginASesLessSpec', 'stdNumOfOriginASesLessSpec',
                          'minNumOfOriginASesLessSpec', 'maxNumOfOriginASesLessSpec',
                          'avgNumOfASPathsLessSpec', 'stdNumOfASPathsLessSpec',
                          'minNumOfASPathsLessSpec', 'maxNumOfASPathsLessSpec',
                          'avgASPathLengthLessSpec', 'stdASPathLengthLessSpec',
                          'minASPathLengthLessSpec', 'maxASPathLengthLessSpec',
                          'avgLevenshteinDistPrefix', 'stdLevenshteinDistPrefix',
                          'minLevenshteinDistPrefix', 'maxLevenshteinDistPrefix',
                          'avgLevenshteinDistMoreSpec','stdLevenshteinDistMoreSpec',
                          'minLevenshteinDistMoreSpec', 'maxLevenshteinDistMoreSpec',
                          'avgLevenshteinDistLessSpec', 'stdLevenshteinDistLessSpec',
                          'minLevenshteinDistLessSpec', 'maxLevenshteinDistLessSpec',
                          'currentVisibility',
                          'avgNumOfAnnouncementsLessSpec', 'stdNumOfAnnouncementsLessSpec',
                          'minNumOfAnnouncementsLessSpec', 'maxNumOfAnnouncementsLessSpec',
                          'avgNumOfWithdrawsLessSpec', 'stdNumOfWithdrawsLessSpec',
                          'maxNumOfWithdrawsLessSpec', 'minNumOfWithdrawsLessSpec',
                          'avgNumOfAnnouncementsMoreSpec', 'stdNumOfAnnouncementsMoreSpec',
                          'minNumOfAnnouncementsMoreSpec', 'maxNumOfAnnouncementsMoreSpec',
                          'avgNumOfWithdrawsMoreSpec', 'stdNumOfWithdrawsMoreSpec',
                          'maxNumOfWithdrawsMoreSpec', 'minNumOfWithdrawsMoreSpec']
        
        valueKeysTemp_pref = ['UsageLatencyGral', 'UsageLatencyIntact',
                              'relUsedTimeIntact', 'avgRelUsedTimeGral',
                              'effectiveUsageIntact', 'avgEffectiveUsageGral',
                              'timeFragmentationIntact', 'avgTimeFragmentationGral',
                              'avgPeriodLengthIntact', 'stdPeriodLengthIntact',
                              'maxPeriodLengthIntact', 'minPeriodLengthIntact',
                              'avgPeriodLengthGral', 'stdPeriodLengthGral',
                              'maxPeriodLengthGral', 'minPeriodLengthGral',
                              'avgVisibility', 'stdVisibility', 'maxVisibility',
                              'minVisibility']
        
        if TEMPORAL_DATA:
            valueKeys_pref = valueKeys_pref + valueKeysTemp_pref
                          
        gralCounterKeys_pref = ['numOfOriginASesIntact', 'numOfASPathsIntact',
                            'numOfLessSpecificsRouted', 'numOfMoreSpecificsRouted',
                            'numOfAnnouncements', 'numOfWithdraws']
                                   
        self.moreSpec_variables = {'lonely': 'numOfLonelyMoreSpec',
                                   'holePunching': 'numOfHolePunchingMoreSpec',
                                   'trafficEngineering': 'numOfTrafficEngineeringMoreSpec',
                                   'overlay': 'numOfOverlayMoreSpec',
                                   'coveredLevel1': 'numOfCoveredLevel1MoreSpec',
                                   'coveredLevel2plus': 'numOfCoveredLevel2plusMoreSpec',
                                   'covering': 'numOfCoveringMoreSpec'}
        
        self.lessSpec_variables = {'lonely': 'numOfLonelyLessSpec',
                                   'holePunching': 'numOfHolePunchingLessSpec',
                                   'trafficEngineering': 'numOfTrafficEngineeringLessSpec',
                                   'overlay': 'numOfOverlayLessSpec',
                                   'coveredLevel1': 'numOfCoveredLevel1LessSpec',
                                   'coveredLevel2plus': 'numOfCoveredLevel2plusLessSpec',
                                   'covering': 'numOfCoveringLessSpec'}
                                   
        counterKeys_pref = gralCounterKeys_pref +\
                            self.moreSpec_variables.values() +\
                            self.lessSpec_variables.values()
        
            
        other_data_columns = ['prefix', 'del_date', 'resource_type', 'status',
                              'opaque_id', 'cc', 'region', 'routing_date',
                              'prefLength']
                              
        other_data_columnsTemp = ['lastSeen', 'lastSeenIntact']
        
        if TEMPORAL_DATA:
            other_data_columns = other_data_columns + other_data_columnsTemp
        
        self.allAttr_pref = other_data_columns + booleanKeys_pref +\
                        valueKeys_pref + counterKeys_pref

        line = self.allAttr_pref[0]
    
        for i in range(len(self.allAttr_pref)-1):
            line = '{},{}'.format(line, self.allAttr_pref[i+1])
    
        line = line + '\n'
                
        with open(prefixes_stats_file, 'w') as csv_file:
            csv_file.write(line)
        
        self.def_dict_pref = self.getDictionaryWithDefaults(booleanKeys_pref, valueKeys_pref, counterKeys_pref)
 
        if TEMPORAL_DATA:            
            valueKeys_ases = ['UsageLatency', 'relUsedTime', 'effectiveUsage',
                              'timeFragmentation', 'avgPeriodLength',
                              'stdPeriodLength', 'minPeriodLength',
                              'maxPeriodLength']
        else:
            valueKeys_ases = []

        counterKeys_ases = ['numOfPrefixesOriginated',
                            'numOfPrefixesPropagated',
                            'numOfAnnouncements',
                            'numOfWithdraws']
        
        if TEMPORAL_DATA:                   
            booleanKeys_ases = ['isDead']
        else:
            booleanKeys_ases = []
        
        expanded_del_asn_df_columns = ['asn', 'del_date', 'asn_type',
                                        'opaque_id', 'cc', 'region']
        
        if TEMPORAL_DATA:
            other_ases_data_columns = ['routing_date', 'lastSeen']
        else:
            other_ases_data_columns = ['routing_date']

        self.allAttr_ases = expanded_del_asn_df_columns + other_ases_data_columns +\
                            booleanKeys_ases + valueKeys_ases + counterKeys_ases

        line = self.allAttr_ases[0]
        
        for i in range(len(self.allAttr_ases)-1):
            line = '{},{}'.format(line, self.allAttr_ases[i+1])
        
        line = line + '\n'

        with open(ases_stats_file, 'w') as csv_file:
            csv_file.write(line)
                
        self.def_dict_ases = self.getDictionaryWithDefaults(booleanKeys_ases,
                                                  valueKeys_ases, counterKeys_ases)
    
    def instantiateOrgHeuristics(self):
        self.orgHeuristics = OrgHeuristics(self.files_path)
        
    # This function downloads information about relationships between ASes inferred
    # by CAIDA and stores it in a dictionary in which all the active ASes appear as keys
    # and the value is another dictionary that also has an AS as key and a string
    # as value specifying whether it is a P2P, a P2C or a C2P relationship
    # The serial variable must be 1 or 2 depending on CAIDAS's data to be used
    @staticmethod    
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
    
    @staticmethod
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